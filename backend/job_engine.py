#!/usr/bin/env python3
"""
Buddyliko - Async Job Engine
Gestisce trasformazioni asincrone con polling.
Nessuna dipendenza da Redis/Celery: usa asyncio + PostgreSQL.
"""

import asyncio
import uuid
import json
import time
from datetime import datetime
from typing import Optional, Dict, Any, Callable, Awaitable
from enum import Enum


class JobStatus(str, Enum):
    PENDING    = "PENDING"
    RUNNING    = "RUNNING"
    COMPLETED  = "COMPLETED"
    FAILED     = "FAILED"
    CANCELLED  = "CANCELLED"


class JobType(str, Enum):
    TRANSFORM       = "TRANSFORM"
    CODE_GENERATE   = "CODE_GENERATE"
    DB_IMPORT       = "DB_IMPORT"
    DB_EXPORT       = "DB_EXPORT"
    BULK_TRANSFORM  = "BULK_TRANSFORM"


class JobEngine:
    """
    Motore di job asincroni basato su asyncio e PostgreSQL.
    
    Flusso:
        POST /api/jobs/submit  →  job_id
        GET  /api/jobs/{id}    →  { status, progress, result, error }
        DELETE /api/jobs/{id}  →  cancel
    
    I job completati vengono mantenuti in DB per 7 giorni (per audit).
    """

    MAX_CONCURRENT_JOBS = 5  # per VPS small, aumenta in base alla RAM

    def __init__(self, conn, RealDictCursor):
        self.conn = conn
        self.RealDictCursor = RealDictCursor
        self._running: Dict[str, asyncio.Task] = {}
        self._semaphore = asyncio.Semaphore(self.MAX_CONCURRENT_JOBS)
        self._init_table()

    def _init_table(self):
        cur = self.conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id VARCHAR(36) PRIMARY KEY,
                job_type VARCHAR(50) NOT NULL,
                status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
                progress INTEGER DEFAULT 0,
                user_id VARCHAR(255),
                user_email VARCHAR(255),
                created_at TIMESTAMPTZ DEFAULT NOW(),
                started_at TIMESTAMPTZ,
                completed_at TIMESTAMPTZ,
                -- Input
                input_params JSONB DEFAULT '{}',
                input_file_path TEXT,
                input_file_name TEXT,
                input_size_bytes BIGINT,
                -- Output
                result JSONB,
                output_file_path TEXT,
                output_file_name TEXT,
                output_size_bytes BIGINT,
                -- Error
                error_message TEXT,
                error_traceback TEXT,
                -- Metadata
                metadata JSONB DEFAULT '{}'
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_jobs_user ON jobs(user_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_jobs_created ON jobs(created_at DESC)")
        print("✅ Job engine tables initialized")

    # ------------------------------------------------------------------
    # CRUD JOBS
    # ------------------------------------------------------------------

    def create_job(self, job_type: JobType, user: Dict, input_params: Dict = None,
                   input_file_path: str = None, input_file_name: str = None,
                   input_size_bytes: int = None, metadata: Dict = None) -> str:
        job_id = str(uuid.uuid4())
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO jobs (
                id, job_type, status, user_id, user_email,
                input_params, input_file_path, input_file_name, input_size_bytes, metadata
            ) VALUES (%s, %s, 'PENDING', %s, %s, %s, %s, %s, %s, %s)
        """, (
            job_id, job_type.value,
            str(user.get('id', '')), user.get('email', ''),
            json.dumps(input_params or {}),
            input_file_path, input_file_name, input_size_bytes,
            json.dumps(metadata or {})
        ))
        return job_id

    def get_job(self, job_id: str, user_id: str = None, is_admin: bool = False) -> Optional[Dict]:
        cur = self.conn.cursor(cursor_factory=self.RealDictCursor)
        cur.execute("SELECT * FROM jobs WHERE id = %s", (job_id,))
        row = cur.fetchone()
        if not row:
            return None
        job = dict(row)
        # Controllo accesso: solo il proprietario o admin
        if not is_admin and str(job.get('user_id')) != str(user_id):
            return None
        # Serializza timestamps
        for field in ('created_at', 'started_at', 'completed_at'):
            if job.get(field):
                job[field] = job[field].isoformat()
        return job

    def list_jobs(self, user_id: str = None, is_admin: bool = False,
                  status: str = None, limit: int = 20, offset: int = 0) -> Dict:
        cur = self.conn.cursor(cursor_factory=self.RealDictCursor)
        where, vals = [], []
        if not is_admin and user_id:
            where.append("user_id = %s")
            vals.append(user_id)
        if status:
            where.append("status = %s")
            vals.append(status)
        sql_where = ("WHERE " + " AND ".join(where)) if where else ""
        cur.execute(f"SELECT COUNT(*) FROM jobs {sql_where}", vals)
        total = cur.fetchone()['count']
        cur.execute(
            f"SELECT id, job_type, status, progress, user_email, created_at, started_at, completed_at, "
            f"input_file_name, output_file_name, error_message FROM jobs {sql_where} "
            f"ORDER BY created_at DESC LIMIT %s OFFSET %s",
            vals + [limit, offset]
        )
        rows = []
        for r in cur.fetchall():
            d = dict(r)
            for field in ('created_at', 'started_at', 'completed_at'):
                if d.get(field):
                    d[field] = d[field].isoformat()
            rows.append(d)
        return {"jobs": rows, "total": total}

    def _update_job(self, job_id: str, **fields):
        """Aggiorna campi del job nel DB."""
        allowed = {
            'status', 'progress', 'started_at', 'completed_at',
            'result', 'output_file_path', 'output_file_name', 'output_size_bytes',
            'error_message', 'error_traceback'
        }
        updates, vals = [], []
        for k, v in fields.items():
            if k in allowed:
                updates.append(f"{k} = %s")
                if isinstance(v, (dict, list)):
                    vals.append(json.dumps(v))
                elif isinstance(v, datetime):
                    vals.append(v.isoformat())
                else:
                    vals.append(v)
        if not updates:
            return
        vals.append(job_id)
        cur = self.conn.cursor()
        cur.execute(f"UPDATE jobs SET {', '.join(updates)} WHERE id = %s", vals)

    def cancel_job(self, job_id: str, user_id: str = None, is_admin: bool = False) -> bool:
        job = self.get_job(job_id, user_id, is_admin)
        if not job:
            return False
        if job['status'] not in (JobStatus.PENDING, JobStatus.RUNNING):
            return False
        # Cancella il task asyncio se in esecuzione
        task = self._running.get(job_id)
        if task and not task.done():
            task.cancel()
        self._update_job(job_id, status=JobStatus.CANCELLED,
                         completed_at=datetime.utcnow().isoformat())
        return True

    # ------------------------------------------------------------------
    # ESECUZIONE
    # ------------------------------------------------------------------

    async def submit(self, job_id: str,
                     handler: Callable[[str, Dict, Callable], Awaitable[Dict]],
                     input_params: Dict) -> str:
        """
        Avvia il job in background.
        handler(job_id, params, progress_cb) -> result_dict
        progress_cb(pct: int) aggiorna il progresso 0-100.
        """
        task = asyncio.create_task(
            self._run(job_id, handler, input_params)
        )
        self._running[job_id] = task
        return job_id

    async def _run(self, job_id: str,
                   handler: Callable,
                   input_params: Dict):
        async with self._semaphore:
            self._update_job(job_id,
                             status=JobStatus.RUNNING,
                             started_at=datetime.utcnow().isoformat(),
                             progress=0)
            try:
                def progress_cb(pct: int):
                    self._update_job(job_id, progress=min(int(pct), 99))

                result = await handler(job_id, input_params, progress_cb)
                self._update_job(
                    job_id,
                    status=JobStatus.COMPLETED,
                    progress=100,
                    completed_at=datetime.utcnow().isoformat(),
                    result=result if isinstance(result, dict) else {'value': str(result)},
                    output_file_path=result.get('output_file_path') if isinstance(result, dict) else None,
                    output_file_name=result.get('output_file_name') if isinstance(result, dict) else None,
                    output_size_bytes=result.get('output_size_bytes') if isinstance(result, dict) else None,
                )
            except asyncio.CancelledError:
                self._update_job(job_id, status=JobStatus.CANCELLED,
                                 completed_at=datetime.utcnow().isoformat())
            except Exception as e:
                import traceback
                self._update_job(
                    job_id,
                    status=JobStatus.FAILED,
                    completed_at=datetime.utcnow().isoformat(),
                    error_message=str(e)[:500],
                    error_traceback=traceback.format_exc()[:2000]
                )
            finally:
                self._running.pop(job_id, None)

    # ------------------------------------------------------------------
    # CLEANUP
    # ------------------------------------------------------------------

    async def cleanup_old_jobs(self, days: int = 7):
        """Rimuove job completati/falliti più vecchi di N giorni."""
        cur = self.conn.cursor()
        cur.execute("""
            DELETE FROM jobs
            WHERE status IN ('COMPLETED', 'FAILED', 'CANCELLED')
            AND completed_at < NOW() - INTERVAL '%s days'
        """, (days,))
        deleted = cur.rowcount
        if deleted > 0:
            print(f"🧹 Cleaned up {deleted} old jobs")
        return deleted

    async def start_cleanup_loop(self, interval_hours: int = 24):
        """Loop di cleanup in background."""
        while True:
            await asyncio.sleep(interval_hours * 3600)
            try:
                await self.cleanup_old_jobs()
            except Exception as e:
                print(f"⚠️ Job cleanup error: {e}")
