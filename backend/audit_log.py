#!/usr/bin/env python3
"""
Buddyliko - Audit Log System
Traccia ogni operazione: chi, cosa, quando, con cosa, esito.
"""

from datetime import datetime, timezone
from typing import Optional, Dict, Any, List
from enum import Enum
import json
import uuid
import csv
import io


# ===========================================================================
# ENUMS
# ===========================================================================

class AuditLevel(str, Enum):
    MINIMAL  = "MINIMAL"   # solo chi/cosa/quando/esito
    STANDARD = "STANDARD"  # + metadati (file, dimensione, formato, errori)
    FULL     = "FULL"       # + preview dati (primi 500 char input/output)


class AuditAction(str, Enum):
    # Auth
    LOGIN           = "LOGIN"
    LOGOUT          = "LOGOUT"
    LOGIN_FAILED    = "LOGIN_FAILED"
    REGISTER        = "REGISTER"
    OAUTH_LOGIN     = "OAUTH_LOGIN"
    TOKEN_REFRESH   = "TOKEN_REFRESH"
    # Trasformazioni
    TRANSFORM       = "TRANSFORM"
    TRANSFORM_ASYNC = "TRANSFORM_ASYNC"
    # File
    FILE_UPLOAD     = "FILE_UPLOAD"
    FILE_DOWNLOAD   = "FILE_DOWNLOAD"
    FILE_DELETE     = "FILE_DELETE"
    FILE_COPY       = "FILE_COPY"
    FILE_VIEW       = "FILE_VIEW"
    FILE_SHARE      = "FILE_SHARE"
    # Schemi
    SCHEMA_CREATE   = "SCHEMA_CREATE"
    SCHEMA_UPDATE   = "SCHEMA_UPDATE"
    SCHEMA_DELETE   = "SCHEMA_DELETE"
    SCHEMA_IMPORT   = "SCHEMA_IMPORT"
    # Progetti
    PROJECT_SAVE    = "PROJECT_SAVE"
    PROJECT_LOAD    = "PROJECT_LOAD"
    PROJECT_DELETE  = "PROJECT_DELETE"
    # Code generation
    CODE_GENERATE   = "CODE_GENERATE"
    # Admin
    USER_STATUS_CHANGE = "USER_STATUS_CHANGE"
    USER_ROLE_CHANGE   = "USER_ROLE_CHANGE"
    GROUP_CREATE    = "GROUP_CREATE"
    GROUP_DELETE    = "GROUP_DELETE"
    MEMBER_ADD      = "MEMBER_ADD"
    MEMBER_REMOVE   = "MEMBER_REMOVE"
    PERMISSION_SET  = "PERMISSION_SET"
    PERMISSION_DEL  = "PERMISSION_DEL"
    SETTINGS_CHANGE = "SETTINGS_CHANGE"
    # Job
    JOB_CREATE      = "JOB_CREATE"
    JOB_COMPLETE    = "JOB_COMPLETE"
    JOB_FAIL        = "JOB_FAIL"
    JOB_CANCEL      = "JOB_CANCEL"


class AuditOutcome(str, Enum):
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    WARNING = "WARNING"
    PENDING = "PENDING"


# ===========================================================================
# AUDIT LOG MANAGER
# ===========================================================================

class AuditLogManager:
    """
    Gestisce la scrittura e lettura dei log di audit su PostgreSQL.
    Il livello di log è configurabile a runtime (recuperato dalla tabella settings).
    """

    DEFAULT_LEVEL = AuditLevel.STANDARD

    def __init__(self, conn, RealDictCursor):
        self.conn = conn
        self.RealDictCursor = RealDictCursor
        self._level_cache: Optional[AuditLevel] = None
        self._level_cache_ts: float = 0
        self._init_table()

    def _init_table(self):
        cur = self.conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS audit_logs (
                id VARCHAR(36) PRIMARY KEY,
                timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                user_id VARCHAR(255),
                user_email VARCHAR(255),
                user_role VARCHAR(50),
                action VARCHAR(50) NOT NULL,
                resource_type VARCHAR(100),
                resource_id VARCHAR(255),
                outcome VARCHAR(20) NOT NULL DEFAULT 'SUCCESS',
                ip_address VARCHAR(64),
                user_agent VARCHAR(512),
                duration_ms INTEGER,
                -- STANDARD fields
                file_name VARCHAR(500),
                file_size_bytes BIGINT,
                input_format VARCHAR(50),
                output_format VARCHAR(50),
                error_message TEXT,
                metadata JSONB DEFAULT '{}',
                -- FULL fields (nullable, abilitati solo con level=FULL)
                input_preview TEXT,
                output_preview TEXT
            )
        """)
        # Indici per query frequenti
        cur.execute("CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON audit_logs(timestamp DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_audit_user_id ON audit_logs(user_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_audit_action ON audit_logs(action)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_audit_outcome ON audit_logs(outcome)")

        # Tabella settings per configurazione audit
        cur.execute("""
            CREATE TABLE IF NOT EXISTS system_settings (
                key VARCHAR(100) PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TIMESTAMPTZ DEFAULT NOW(),
                updated_by VARCHAR(255)
            )
        """)
        # Inserisci default se non esiste
        cur.execute("""
            INSERT INTO system_settings (key, value)
            VALUES ('audit_level', 'STANDARD')
            ON CONFLICT (key) DO NOTHING
        """)
        print("✅ Audit log tables initialized")

    # ------------------------------------------------------------------
    # LIVELLO DI LOG
    # ------------------------------------------------------------------

    def get_level(self) -> AuditLevel:
        """Legge il livello dal DB (con cache 60s per non martellare il DB)."""
        import time
        now = time.time()
        if self._level_cache and (now - self._level_cache_ts) < 60:
            return self._level_cache
        try:
            cur = self.conn.cursor()
            cur.execute("SELECT value FROM system_settings WHERE key = 'audit_level'")
            row = cur.fetchone()
            val = row[0] if row else 'STANDARD'
            self._level_cache = AuditLevel(val)
            self._level_cache_ts = now
        except Exception:
            self._level_cache = AuditLevel.STANDARD
        return self._level_cache

    def set_level(self, level: AuditLevel, updated_by: str = None):
        """Cambia il livello di log (chiamato dall'admin)."""
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO system_settings (key, value, updated_at, updated_by)
            VALUES ('audit_level', %s, NOW(), %s)
            ON CONFLICT (key) DO UPDATE SET
                value = EXCLUDED.value,
                updated_at = EXCLUDED.updated_at,
                updated_by = EXCLUDED.updated_by
        """, (level.value, updated_by))
        # Invalida cache
        self._level_cache = level
        self._level_cache_ts = 0

    # ------------------------------------------------------------------
    # SCRITTURA LOG
    # ------------------------------------------------------------------

    def log(self,
            action: AuditAction,
            outcome: AuditOutcome = AuditOutcome.SUCCESS,
            user: Optional[Dict] = None,
            resource_type: Optional[str] = None,
            resource_id: Optional[str] = None,
            ip_address: Optional[str] = None,
            user_agent: Optional[str] = None,
            duration_ms: Optional[int] = None,
            # STANDARD
            file_name: Optional[str] = None,
            file_size_bytes: Optional[int] = None,
            input_format: Optional[str] = None,
            output_format: Optional[str] = None,
            error_message: Optional[str] = None,
            metadata: Optional[Dict] = None,
            # FULL
            input_preview: Optional[str] = None,
            output_preview: Optional[str] = None,
            ) -> str:
        """Scrive un record di audit. Filtra i campi in base al livello configurato."""

        level = self.get_level()
        log_id = str(uuid.uuid4())

        # Estrai dati utente
        uid = str(user.get('id', '')) if user else None
        email = user.get('email', '') if user else None
        role = user.get('role', '') if user else None

        # Tronca preview a 500 caratteri
        def _preview(text):
            if not text: return None
            return str(text)[:500]

        # In base al livello, azzera campi non necessari
        if level == AuditLevel.MINIMAL:
            file_name = None
            file_size_bytes = None
            input_format = None
            output_format = None
            error_message = error_message  # mantieni sempre gli errori
            metadata = None
            input_preview = None
            output_preview = None
        elif level == AuditLevel.STANDARD:
            input_preview = None
            output_preview = None
        else:  # FULL
            input_preview = _preview(input_preview)
            output_preview = _preview(output_preview)

        try:
            cur = self.conn.cursor()
            cur.execute("""
                INSERT INTO audit_logs (
                    id, timestamp, user_id, user_email, user_role,
                    action, resource_type, resource_id, outcome,
                    ip_address, user_agent, duration_ms,
                    file_name, file_size_bytes, input_format, output_format,
                    error_message, metadata, input_preview, output_preview
                ) VALUES (
                    %s, NOW(), %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s
                )
            """, (
                log_id, uid, email, role,
                action.value, resource_type, str(resource_id) if resource_id else None, outcome.value,
                ip_address, user_agent, duration_ms,
                file_name, file_size_bytes, input_format, output_format,
                error_message, json.dumps(metadata or {}),
                input_preview, output_preview
            ))
        except Exception as e:
            # L'audit log non deve mai far crashare l'applicazione
            print(f"⚠️ Audit log write failed: {e}")

        return log_id

    # ------------------------------------------------------------------
    # LETTURA LOG
    # ------------------------------------------------------------------

    def query(self,
              user_id: Optional[str] = None,
              action: Optional[str] = None,
              outcome: Optional[str] = None,
              date_from: Optional[str] = None,
              date_to: Optional[str] = None,
              resource_type: Optional[str] = None,
              limit: int = 100,
              offset: int = 0,
              requester_role: str = 'MASTER',
              requester_id: Optional[str] = None,
              ) -> Dict:
        """
        Legge i log con filtri.
        - MASTER/ADMIN: vede tutto
        - Utente normale: vede solo i propri log
        """
        cur = self.conn.cursor(cursor_factory=self.RealDictCursor)
        where, vals = [], []

        # Restrizione per utenti non admin
        if requester_role not in ('MASTER', 'ADMIN'):
            where.append("user_id = %s")
            vals.append(requester_id)
        elif user_id:
            where.append("user_id = %s")
            vals.append(user_id)

        if action:
            where.append("action = %s")
            vals.append(action)
        if outcome:
            where.append("outcome = %s")
            vals.append(outcome)
        if resource_type:
            where.append("resource_type = %s")
            vals.append(resource_type)
        if date_from:
            where.append("timestamp >= %s")
            vals.append(date_from)
        if date_to:
            where.append("timestamp <= %s")
            vals.append(date_to + 'T23:59:59')

        sql_where = ("WHERE " + " AND ".join(where)) if where else ""

        # Count
        cur.execute(f"SELECT COUNT(*) FROM audit_logs {sql_where}", vals)
        total = cur.fetchone()['count']

        # Data
        cur.execute(
            f"SELECT * FROM audit_logs {sql_where} ORDER BY timestamp DESC LIMIT %s OFFSET %s",
            vals + [limit, offset]
        )
        rows = [dict(r) for r in cur.fetchall()]
        # Serializza timestamp
        for r in rows:
            if r.get('timestamp'):
                r['timestamp'] = r['timestamp'].isoformat()

        return {"logs": rows, "total": total, "limit": limit, "offset": offset}

    # ------------------------------------------------------------------
    # EXPORT
    # ------------------------------------------------------------------

    def export_csv(self, **query_kwargs) -> str:
        """Esporta i log come CSV string."""
        query_kwargs['limit'] = 10000
        result = self.query(**query_kwargs)
        rows = result['logs']
        if not rows:
            return ""
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
        return output.getvalue()

    def get_stats(self) -> Dict:
        """Statistiche per la dashboard admin."""
        cur = self.conn.cursor(cursor_factory=self.RealDictCursor)
        cur.execute("""
            SELECT
                COUNT(*) as total_logs,
                COUNT(*) FILTER (WHERE outcome = 'SUCCESS') as successes,
                COUNT(*) FILTER (WHERE outcome = 'FAILURE') as failures,
                COUNT(*) FILTER (WHERE timestamp > NOW() - INTERVAL '24 hours') as last_24h,
                COUNT(*) FILTER (WHERE timestamp > NOW() - INTERVAL '7 days') as last_7d,
                COUNT(DISTINCT user_id) as unique_users,
                action,
                COUNT(*) as action_count
            FROM audit_logs
            GROUP BY action
            ORDER BY action_count DESC
        """)
        rows = [dict(r) for r in cur.fetchall()]

        cur.execute("""
            SELECT
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE outcome = 'SUCCESS') as successes,
                COUNT(*) FILTER (WHERE outcome = 'FAILURE') as failures,
                COUNT(*) FILTER (WHERE timestamp > NOW() - INTERVAL '24 hours') as last_24h,
                COUNT(*) FILTER (WHERE timestamp > NOW() - INTERVAL '7 days') as last_7d,
                COUNT(DISTINCT user_id) as unique_users
            FROM audit_logs
        """)
        totals = dict(cur.fetchone())

        return {"totals": totals, "by_action": rows}


# ===========================================================================
# DECORATOR / CONTEXT MANAGER PER AUTO-LOG
# ===========================================================================

import time
from functools import wraps


def audit_endpoint(action: AuditAction, resource_type: str = None):
    """
    Decorator FastAPI per loggare automaticamente un endpoint.
    Uso:
        @app.post("/api/transform/execute")
        @audit_endpoint(AuditAction.TRANSFORM, "transform")
        async def execute_transform(..., user=Depends(get_current_user), request: Request):
            ...
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            start = time.time()
            user = kwargs.get('user')
            request = kwargs.get('request')
            ip = None
            ua = None
            if request:
                ip = request.client.host if request.client else None
                ua = request.headers.get('user-agent', '')[:200]

            try:
                result = await func(*args, **kwargs)
                duration = int((time.time() - start) * 1000)
                # Log asincrono (non blocca la risposta)
                _try_log(kwargs, action, AuditOutcome.SUCCESS, user, resource_type, ip, ua, duration)
                return result
            except Exception as e:
                duration = int((time.time() - start) * 1000)
                _try_log(kwargs, action, AuditOutcome.FAILURE, user, resource_type, ip, ua, duration,
                         error_message=str(e)[:500])
                raise
        return wrapper
    return decorator


def _try_log(kwargs, action, outcome, user, resource_type, ip, ua, duration, **extra):
    """Tenta di loggare senza crashare."""
    try:
        audit: AuditLogManager = kwargs.get('_audit')
        if audit:
            audit.log(action, outcome, user=user, resource_type=resource_type,
                      ip_address=ip, user_agent=ua, duration_ms=duration, **extra)
    except Exception:
        pass
