"""
Buddyliko Request Logger
────────────────────────
Middleware FastAPI che logga OGNI richiesta HTTP in PostgreSQL.
Traccia: chi, cosa, quando, quanto, esito.

Tabella separata da audit_logs (alta volumetria).
Retention configurabile (default 30 giorni).

Usage:
    from request_logger import RequestLoggerMiddleware, RequestLogManager

    log_manager = RequestLogManager(conn, RealDictCursor)
    app.add_middleware(RequestLoggerMiddleware, log_manager=log_manager)
"""
import time
import uuid
import json
import traceback
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response


class RequestLogManager:
    """Gestisce la tabella request_logs su PostgreSQL."""

    def __init__(self, conn, RealDictCursor, retention_days: int = 30):
        self.conn = conn
        self.RealDictCursor = RealDictCursor
        self.retention_days = retention_days
        self._init_table()

    def _init_table(self):
        cur = self.conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS request_logs (
                id VARCHAR(36) PRIMARY KEY,
                timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                method VARCHAR(10) NOT NULL,
                path VARCHAR(2048) NOT NULL,
                query_string VARCHAR(2048),
                status_code INTEGER,
                duration_ms INTEGER,
                ip_address VARCHAR(64),
                user_agent VARCHAR(512),
                content_type VARCHAR(255),
                request_size BIGINT,
                response_size BIGINT,
                response_content_type VARCHAR(255),
                user_id VARCHAR(255),
                user_email VARCHAR(255),
                error_message TEXT,
                error_traceback TEXT,
                metadata JSONB DEFAULT '{}'
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_reqlog_timestamp ON request_logs(timestamp DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_reqlog_path ON request_logs(path)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_reqlog_status ON request_logs(status_code)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_reqlog_method_path ON request_logs(method, path)")
        self.conn.commit()
        print("✅ Request logger table initialized")

    def log(self, **kwargs) -> str:
        """Scrive un record di request log."""
        log_id = str(uuid.uuid4())
        try:
            cur = self.conn.cursor()
            cur.execute("""
                INSERT INTO request_logs (
                    id, timestamp, method, path, query_string,
                    status_code, duration_ms, ip_address, user_agent,
                    content_type, request_size, response_size,
                    response_content_type, user_id, user_email,
                    error_message, error_traceback, metadata
                ) VALUES (
                    %s, NOW(), %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s
                )
            """, (
                log_id,
                kwargs.get('method'),
                kwargs.get('path'),
                kwargs.get('query_string'),
                kwargs.get('status_code'),
                kwargs.get('duration_ms'),
                kwargs.get('ip_address'),
                kwargs.get('user_agent'),
                kwargs.get('content_type'),
                kwargs.get('request_size'),
                kwargs.get('response_size'),
                kwargs.get('response_content_type'),
                kwargs.get('user_id'),
                kwargs.get('user_email'),
                kwargs.get('error_message'),
                kwargs.get('error_traceback'),
                json.dumps(kwargs.get('metadata') or {}),
            ))
            self.conn.commit()
        except Exception as e:
            print(f"⚠️ Request log write failed: {e}")
            try:
                self.conn.rollback()
            except Exception:
                pass
        return log_id

    def query(self,
              method: Optional[str] = None,
              path_contains: Optional[str] = None,
              status_code: Optional[int] = None,
              status_gte: Optional[int] = None,
              date_from: Optional[str] = None,
              date_to: Optional[str] = None,
              ip_address: Optional[str] = None,
              limit: int = 100,
              offset: int = 0) -> Dict:
        """Legge request logs con filtri."""
        cur = self.conn.cursor(cursor_factory=self.RealDictCursor)
        where, vals = [], []

        if method:
            where.append("method = %s")
            vals.append(method.upper())
        if path_contains:
            where.append("path LIKE %s")
            vals.append(f"%{path_contains}%")
        if status_code:
            where.append("status_code = %s")
            vals.append(status_code)
        if status_gte:
            where.append("status_code >= %s")
            vals.append(status_gte)
        if date_from:
            where.append("timestamp >= %s")
            vals.append(date_from)
        if date_to:
            where.append("timestamp <= %s")
            vals.append(date_to)
        if ip_address:
            where.append("ip_address = %s")
            vals.append(ip_address)

        where_sql = " AND ".join(where) if where else "1=1"

        # Count
        cur.execute(f"SELECT COUNT(*) as total FROM request_logs WHERE {where_sql}", vals)
        total = cur.fetchone()['total']

        # Fetch
        cur.execute(f"""
            SELECT * FROM request_logs
            WHERE {where_sql}
            ORDER BY timestamp DESC
            LIMIT %s OFFSET %s
        """, vals + [limit, offset])
        rows = cur.fetchall()

        return {"total": total, "logs": rows, "limit": limit, "offset": offset}

    def get_stats(self, hours: int = 24) -> Dict:
        """Statistiche richieste nelle ultime N ore."""
        cur = self.conn.cursor(cursor_factory=self.RealDictCursor)

        cur.execute("""
            SELECT
                COUNT(*) as total_requests,
                COUNT(*) FILTER (WHERE status_code >= 200 AND status_code < 300) as success_2xx,
                COUNT(*) FILTER (WHERE status_code >= 400 AND status_code < 500) as client_error_4xx,
                COUNT(*) FILTER (WHERE status_code >= 500) as server_error_5xx,
                ROUND(AVG(duration_ms)::numeric, 1) as avg_duration_ms,
                MAX(duration_ms) as max_duration_ms,
                ROUND(AVG(request_size)::numeric, 0) as avg_request_size,
                MAX(request_size) as max_request_size
            FROM request_logs
            WHERE timestamp > NOW() - INTERVAL '%s hours'
        """ % int(hours))
        summary = cur.fetchone()

        # Top paths by error count
        cur.execute("""
            SELECT path, method, COUNT(*) as error_count,
                   array_agg(DISTINCT status_code) as status_codes
            FROM request_logs
            WHERE status_code >= 400
              AND timestamp > NOW() - INTERVAL '%s hours'
            GROUP BY path, method
            ORDER BY error_count DESC
            LIMIT 10
        """ % int(hours))
        top_errors = cur.fetchall()

        # Slowest endpoints
        cur.execute("""
            SELECT path, method,
                   ROUND(AVG(duration_ms)::numeric, 1) as avg_ms,
                   MAX(duration_ms) as max_ms,
                   COUNT(*) as count
            FROM request_logs
            WHERE timestamp > NOW() - INTERVAL '%s hours'
            GROUP BY path, method
            HAVING COUNT(*) >= 3
            ORDER BY avg_ms DESC
            LIMIT 10
        """ % int(hours))
        slowest = cur.fetchall()

        return {"summary": summary, "top_errors": top_errors, "slowest_endpoints": slowest}

    def cleanup(self, older_than_days: Optional[int] = None) -> int:
        """Elimina log più vecchi di N giorni."""
        days = older_than_days or self.retention_days
        cur = self.conn.cursor()
        cur.execute(
            "DELETE FROM request_logs WHERE timestamp < NOW() - INTERVAL '%s days'" % int(days)
        )
        deleted = cur.rowcount
        self.conn.commit()
        return deleted


class RequestLoggerMiddleware(BaseHTTPMiddleware):
    """
    Middleware che logga ogni richiesta HTTP.
    Cattura: metodo, path, status, durata, IP, user-agent, dimensioni, errori.
    """

    # Paths da non loggare (health check, static, troppo rumorosi)
    SKIP_PATHS = {'/api/health', '/api/docs', '/api/redoc', '/api/openapi.json', '/favicon.ico'}
    SKIP_PREFIXES = ('/api/docs/', '/api/redoc/')

    def __init__(self, app, log_manager: RequestLogManager):
        super().__init__(app)
        self.log_manager = log_manager

    async def dispatch(self, request: Request, call_next):
        path = request.url.path

        # Skip noisy paths
        if path in self.SKIP_PATHS or path.startswith(self.SKIP_PREFIXES):
            return await call_next(request)

        t0 = time.time()
        error_msg = None
        error_tb = None
        status_code = 500
        response_size = 0
        response_ct = ''

        try:
            response = await call_next(request)
            status_code = response.status_code
            response_ct = response.headers.get('content-type', '')
            # Stima response size dall'header (non sempre presente)
            cl = response.headers.get('content-length')
            response_size = int(cl) if cl else 0
        except Exception as e:
            error_msg = str(e)
            error_tb = traceback.format_exc()
            raise
        finally:
            duration_ms = int((time.time() - t0) * 1000)

            # Estrai user info dal request state (se impostato da auth middleware)
            user_id = None
            user_email = None
            if hasattr(request.state, 'user') and request.state.user:
                user_id = str(request.state.user.get('id', ''))
                user_email = request.state.user.get('email', '')

            # Metadata extra per upload
            metadata = {}
            if 'multipart/form-data' in (request.headers.get('content-type') or ''):
                metadata['upload'] = True
            if request.headers.get('x-forwarded-for'):
                metadata['x_forwarded_for'] = request.headers.get('x-forwarded-for')

            self.log_manager.log(
                method=request.method,
                path=path,
                query_string=str(request.url.query) if request.url.query else None,
                status_code=status_code,
                duration_ms=duration_ms,
                ip_address=request.client.host if request.client else None,
                user_agent=(request.headers.get('user-agent') or '')[:512],
                content_type=(request.headers.get('content-type') or '')[:255],
                request_size=int(request.headers.get('content-length', 0) or 0),
                response_size=response_size,
                response_content_type=response_ct[:255] if response_ct else None,
                user_id=user_id,
                user_email=user_email,
                error_message=error_msg,
                error_traceback=error_tb,
                metadata=metadata if metadata else None,
            )

        return response
