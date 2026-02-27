#!/usr/bin/env python3
"""
Buddyliko — Schedule Service (Phase 8 Part 2B)
Scheduling cron-like: ricorrenza, gestione CRUD, background loop,
log esecuzioni, pause/resume.
"""

import json
import uuid
import re
import asyncio
import threading
import time
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import Optional, Dict, List, Any


SCHEDULE_TYPES = (
    'transform',        # Trasformazione ricorrente
    'batch_transform',  # Batch ricorrente
    'aggregate',        # Riaggregazione costi
    'cleanup',          # Pulizia file vecchi
    'report',           # Generazione report
)

SCHEDULE_STATUSES = ('active', 'paused', 'disabled', 'expired')

# Cron presets
CRON_PRESETS = {
    'every_5min':   '*/5 * * * *',
    'every_15min':  '*/15 * * * *',
    'every_hour':   '0 * * * *',
    'every_6hours': '0 */6 * * *',
    'daily_2am':    '0 2 * * *',
    'daily_6am':    '0 6 * * *',
    'weekly_mon':   '0 2 * * 1',
    'monthly_1st':  '0 2 1 * *',
}


class ScheduleService:
    """Gestione schedule cron-like."""

    def __init__(self, conn, cursor_factory, webhook_service=None):
        self.conn = conn
        self.RDC = cursor_factory
        self.webhook_service = webhook_service
        self._running = False
        self._loop_thread = None
        self._init_tables()

    def _init_tables(self):
        cur = self.conn.cursor()
        try:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS schedules (
                id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                org_id          UUID NOT NULL REFERENCES organizations(id),
                user_id         INTEGER REFERENCES users(id),
                name            VARCHAR(255) NOT NULL,
                description     TEXT,
                schedule_type   VARCHAR(30) NOT NULL DEFAULT 'transform',
                cron_expr       VARCHAR(100) NOT NULL,
                timezone        VARCHAR(50) DEFAULT 'Europe/Rome',
                config          JSONB DEFAULT '{}',
                status          VARCHAR(20) DEFAULT 'active',
                last_run_at     TIMESTAMPTZ,
                next_run_at     TIMESTAMPTZ,
                run_count       INTEGER DEFAULT 0,
                fail_count      INTEGER DEFAULT 0,
                max_runs        INTEGER DEFAULT 0,
                expires_at      TIMESTAMPTZ,
                created_at      TIMESTAMPTZ DEFAULT NOW(),
                updated_at      TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_sch_org ON schedules(org_id, status);
            CREATE INDEX IF NOT EXISTS idx_sch_next ON schedules(next_run_at, status);

            CREATE TABLE IF NOT EXISTS schedule_runs (
                id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                schedule_id UUID NOT NULL REFERENCES schedules(id) ON DELETE CASCADE,
                org_id      UUID NOT NULL,
                status      VARCHAR(20) DEFAULT 'running',
                started_at  TIMESTAMPTZ DEFAULT NOW(),
                completed_at TIMESTAMPTZ,
                duration_ms INTEGER,
                result      JSONB,
                error       TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_sr_sched ON schedule_runs(schedule_id, started_at DESC);
            """)
            self.conn.commit()
            print("   ✅ Schedule tables ready")
        except Exception as e:
            try: self.conn.rollback()
            except: pass
            print(f"   ⚠️  Schedule tables init: {e}")

    def _ser(self, v):
        if isinstance(v, Decimal): return str(v)
        if isinstance(v, datetime): return v.isoformat()
        if isinstance(v, uuid.UUID): return str(v)
        return v

    def _ser_row(self, r):
        return {k: self._ser(v) for k, v in r.items()} if r else {}

    def _ser_rows(self, rows):
        return [self._ser_row(r) for r in rows]

    # ── CRUD ──

    def create_schedule(self, org_id, user_id, name, schedule_type, cron_expr,
                        config=None, tz='Europe/Rome', max_runs=0, expires_at=None):
        if schedule_type not in SCHEDULE_TYPES:
            raise ValueError(f"Tipo non valido: {schedule_type}. Validi: {SCHEDULE_TYPES}")
        if not cron_expr:
            raise ValueError("cron_expr obbligatoria")
        if not self._validate_cron(cron_expr):
            raise ValueError(f"cron_expr non valida: {cron_expr}")

        sch_id = str(uuid.uuid4())
        next_run = self._calc_next_run(cron_expr)

        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO schedules
                (id, org_id, user_id, name, schedule_type, cron_expr, timezone,
                 config, status, next_run_at, max_runs, expires_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'active', %s, %s, %s)
        """, (sch_id, org_id, user_id, name, schedule_type, cron_expr, tz,
              json.dumps(config or {}), next_run, max_runs, expires_at))
        self.conn.commit()
        return self.get_schedule(sch_id)

    def get_schedule(self, schedule_id):
        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute("SELECT * FROM schedules WHERE id=%s", (schedule_id,))
        r = cur.fetchone()
        if not r: return None
        result = self._ser_row(dict(r))
        # Preset label
        cron = r.get('cron_expr', '')
        result['preset'] = next((k for k, v in CRON_PRESETS.items() if v == cron), 'custom')
        return result

    def list_schedules(self, org_id, status=None):
        cur = self.conn.cursor(cursor_factory=self.RDC)
        where = "org_id=%s"
        params = [org_id]
        if status:
            where += " AND status=%s"
            params.append(status)
        cur.execute(f"""
            SELECT s.*,
                (SELECT COUNT(*) FROM schedule_runs sr WHERE sr.schedule_id=s.id) as total_runs,
                (SELECT MAX(sr.started_at) FROM schedule_runs sr WHERE sr.schedule_id=s.id) as last_run_actual
            FROM schedules s WHERE {where} ORDER BY s.created_at DESC
        """, params)
        return self._ser_rows([dict(r) for r in cur.fetchall()])

    def update_schedule(self, schedule_id, org_id, data):
        allowed = ['name', 'description', 'cron_expr', 'config', 'status',
                    'timezone', 'max_runs', 'expires_at']
        fields, values = [], []
        for k, v in data.items():
            if k in allowed:
                if isinstance(v, (dict, list)): v = json.dumps(v)
                fields.append(f"{k}=%s"); values.append(v)
        if not fields: return False

        # Recalculate next_run if cron changed
        if 'cron_expr' in data:
            next_run = self._calc_next_run(data['cron_expr'])
            fields.append("next_run_at=%s"); values.append(next_run)

        fields.append("updated_at=NOW()")
        values.extend([schedule_id, org_id])
        cur = self.conn.cursor()
        cur.execute(f"UPDATE schedules SET {', '.join(fields)} WHERE id=%s AND org_id=%s", values)
        self.conn.commit()
        return cur.rowcount > 0

    def delete_schedule(self, schedule_id, org_id):
        cur = self.conn.cursor()
        cur.execute("DELETE FROM schedules WHERE id=%s AND org_id=%s", (schedule_id, org_id))
        self.conn.commit()
        return cur.rowcount > 0

    def pause_schedule(self, schedule_id, org_id):
        cur = self.conn.cursor()
        cur.execute("UPDATE schedules SET status='paused', updated_at=NOW() WHERE id=%s AND org_id=%s AND status='active'",
                     (schedule_id, org_id))
        self.conn.commit()
        return cur.rowcount > 0

    def resume_schedule(self, schedule_id, org_id):
        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute("SELECT cron_expr FROM schedules WHERE id=%s AND org_id=%s", (schedule_id, org_id))
        r = cur.fetchone()
        if not r: return False
        next_run = self._calc_next_run(r['cron_expr'])
        cur2 = self.conn.cursor()
        cur2.execute("UPDATE schedules SET status='active', next_run_at=%s, updated_at=NOW() WHERE id=%s AND org_id=%s AND status='paused'",
                      (next_run, schedule_id, org_id))
        self.conn.commit()
        return cur2.rowcount > 0

    # ── RUNS LOG ──

    def get_runs(self, schedule_id, limit=50):
        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute("""
            SELECT * FROM schedule_runs WHERE schedule_id=%s
            ORDER BY started_at DESC LIMIT %s
        """, (schedule_id, limit))
        return self._ser_rows([dict(r) for r in cur.fetchall()])

    def trigger_now(self, schedule_id, org_id):
        """Trigger manuale immediato."""
        sch = self.get_schedule(schedule_id)
        if not sch or str(sch.get('org_id', '')) != str(org_id):
            raise ValueError("Schedule non trovato")
        self._execute_schedule(sch)
        return True

    # ── BACKGROUND LOOP ──

    def start_loop(self, interval_seconds=60):
        """Avvia il loop di scheduling in background."""
        if self._running:
            return
        self._running = True
        self._loop_thread = threading.Thread(target=self._loop, args=(interval_seconds,), daemon=True)
        self._loop_thread.start()
        print(f"   ✅ Schedule loop started (every {interval_seconds}s)")

    def _loop(self, interval):
        while self._running:
            try:
                self._check_due_schedules()
            except Exception as e:
                print(f"   ⚠️  Schedule loop error: {e}")
            time.sleep(interval)

    def stop_loop(self):
        self._running = False

    def _check_due_schedules(self):
        """Controlla schedule in scadenza ed eseguili."""
        now = datetime.now(timezone.utc)
        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute("""
            SELECT * FROM schedules
            WHERE status='active' AND next_run_at <= %s
            ORDER BY next_run_at
            LIMIT 20
        """, (now,))
        schedules = [dict(r) for r in cur.fetchall()]

        for sch in schedules:
            # Check expiry
            if sch.get('expires_at') and sch['expires_at'] < now:
                cur2 = self.conn.cursor()
                cur2.execute("UPDATE schedules SET status='expired', updated_at=NOW() WHERE id=%s", (sch['id'],))
                self.conn.commit()
                continue

            # Check max_runs
            max_runs = sch.get('max_runs', 0)
            if max_runs > 0 and (sch.get('run_count', 0) or 0) >= max_runs:
                cur2 = self.conn.cursor()
                cur2.execute("UPDATE schedules SET status='disabled', updated_at=NOW() WHERE id=%s", (sch['id'],))
                self.conn.commit()
                continue

            # Execute
            try:
                self._execute_schedule(sch)
            except Exception as e:
                print(f"   ⚠️  Schedule {sch['id']} exec error: {e}")

            # Update next_run
            next_run = self._calc_next_run(sch['cron_expr'])
            cur2 = self.conn.cursor()
            cur2.execute("""
                UPDATE schedules SET last_run_at=NOW(), next_run_at=%s,
                       run_count=COALESCE(run_count,0)+1, updated_at=NOW()
                WHERE id=%s
            """, (next_run, sch['id']))
            self.conn.commit()

    def _execute_schedule(self, sch):
        """Esegui un singolo schedule."""
        run_id = str(uuid.uuid4())
        org_id = str(sch['org_id'])
        start = time.time()

        # Record run start
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO schedule_runs (id, schedule_id, org_id, status)
            VALUES (%s, %s, %s, 'running')
        """, (run_id, str(sch['id']), org_id))
        self.conn.commit()

        try:
            config = sch.get('config', {})
            if isinstance(config, str): config = json.loads(config)
            stype = sch.get('schedule_type', 'transform')

            result = {'type': stype, 'schedule_name': sch.get('name', '')}

            if stype == 'aggregate':
                # Trigger cost aggregation
                result['action'] = 'cost_aggregation_triggered'
            elif stype == 'cleanup':
                result['action'] = 'cleanup_triggered'
            elif stype == 'report':
                result['action'] = 'report_generation_triggered'
            else:
                # transform / batch_transform
                result['action'] = f'{stype}_triggered'
                result['config'] = config

            duration_ms = int((time.time() - start) * 1000)
            cur.execute("""
                UPDATE schedule_runs SET status='completed', completed_at=NOW(),
                       duration_ms=%s, result=%s WHERE id=%s
            """, (duration_ms, json.dumps(result), run_id))
            self.conn.commit()

        except Exception as e:
            duration_ms = int((time.time() - start) * 1000)
            try:
                cur.execute("""
                    UPDATE schedule_runs SET status='failed', completed_at=NOW(),
                           duration_ms=%s, error=%s WHERE id=%s
                """, (duration_ms, str(e)[:500], run_id))
                # Increment fail count
                cur.execute("UPDATE schedules SET fail_count=COALESCE(fail_count,0)+1 WHERE id=%s", (str(sch['id']),))
                self.conn.commit()
            except: pass

    # ── CRON HELPERS ──

    def _validate_cron(self, expr):
        """Validazione basilare cron (5 campi: min hour dom month dow)."""
        parts = expr.strip().split()
        if len(parts) != 5: return False
        patterns = [
            r'^(\*|(\*/\d+)|(\d+(-\d+)?)(,\d+(-\d+)?)*)$'
        ] * 5
        for p in parts:
            if not re.match(r'^(\*|(\*/\d+)|(\d+(-\d+)?)(,\d+(-\d+)?)*)$', p):
                return False
        return True

    def _calc_next_run(self, cron_expr):
        """Calcola prossima esecuzione (semplificato: prossimo minuto/ora/giorno)."""
        now = datetime.now(timezone.utc)
        parts = cron_expr.strip().split()
        if len(parts) != 5:
            return now + timedelta(hours=1)

        minute, hour = parts[0], parts[1]

        # Every N minutes
        if minute.startswith('*/'):
            interval = int(minute[2:])
            next_min = ((now.minute // interval) + 1) * interval
            if next_min >= 60:
                return now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
            return now.replace(minute=next_min, second=0, microsecond=0)

        # Specific minute, every hour
        if minute.isdigit() and hour == '*':
            target_min = int(minute)
            result = now.replace(minute=target_min, second=0, microsecond=0)
            if result <= now:
                result += timedelta(hours=1)
            return result

        # Every N hours
        if hour.startswith('*/'):
            interval = int(hour[2:])
            target_min = int(minute) if minute.isdigit() else 0
            next_hour = ((now.hour // interval) + 1) * interval
            if next_hour >= 24:
                return (now + timedelta(days=1)).replace(hour=0, minute=target_min, second=0, microsecond=0)
            return now.replace(hour=next_hour, minute=target_min, second=0, microsecond=0)

        # Specific time daily
        if minute.isdigit() and hour.isdigit():
            target = now.replace(hour=int(hour), minute=int(minute), second=0, microsecond=0)
            if target <= now:
                target += timedelta(days=1)
            return target

        # Default: next hour
        return now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)

    def get_presets(self):
        return [{'key': k, 'cron': v, 'label': k.replace('_', ' ').title()} for k, v in CRON_PRESETS.items()]
