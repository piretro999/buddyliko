#!/usr/bin/env python3
"""
Buddyliko — Batch Service (Phase 8 Part 2A)
Batch di N trasformazioni: crea batch, processa item, progress tracking,
result collection, webhook integration.
Lavora sopra job_engine (pre-esistente) e cost_service (Phase 2).
"""

import json
import uuid
import os
import io
import zipfile
import tempfile
import threading
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional, Dict, List, Any


BATCH_STATUSES = ('pending', 'processing', 'completed', 'failed', 'cancelled', 'partial')
ITEM_STATUSES = ('pending', 'processing', 'completed', 'failed', 'skipped')


class BatchService:
    """Gestione batch di trasformazioni multiple."""

    def __init__(self, conn, cursor_factory, cost_service=None,
                 webhook_service=None, budget_service=None):
        self.conn = conn
        self.RDC = cursor_factory
        self.cost_service = cost_service
        self.webhook_service = webhook_service
        self.budget_service = budget_service
        self._init_tables()

    def _init_tables(self):
        cur = self.conn.cursor()
        try:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS batch_jobs (
                id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                org_id          UUID NOT NULL REFERENCES organizations(id),
                user_id         INTEGER REFERENCES users(id),
                name            VARCHAR(255),
                description     TEXT,
                template_id     UUID,
                operation_type  VARCHAR(30) DEFAULT 'transform',
                config          JSONB DEFAULT '{}',
                status          VARCHAR(20) DEFAULT 'pending',
                total_items     INTEGER DEFAULT 0,
                completed_items INTEGER DEFAULT 0,
                failed_items    INTEGER DEFAULT 0,
                progress_pct    INTEGER DEFAULT 0,
                started_at      TIMESTAMPTZ,
                completed_at    TIMESTAMPTZ,
                error           TEXT,
                result_summary  JSONB DEFAULT '{}',
                output_zip_path TEXT,
                created_at      TIMESTAMPTZ DEFAULT NOW(),
                updated_at      TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_bj_org ON batch_jobs(org_id, status);
            CREATE INDEX IF NOT EXISTS idx_bj_user ON batch_jobs(user_id);

            CREATE TABLE IF NOT EXISTS batch_items (
                id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                batch_id    UUID NOT NULL REFERENCES batch_jobs(id) ON DELETE CASCADE,
                seq         INTEGER NOT NULL,
                input_name  VARCHAR(500),
                input_path  TEXT,
                input_size  BIGINT DEFAULT 0,
                status      VARCHAR(20) DEFAULT 'pending',
                output_path TEXT,
                output_size BIGINT DEFAULT 0,
                error       TEXT,
                duration_ms INTEGER,
                cost_eur    NUMERIC(10,4) DEFAULT 0,
                started_at  TIMESTAMPTZ,
                completed_at TIMESTAMPTZ
            );
            CREATE INDEX IF NOT EXISTS idx_bi_batch ON batch_items(batch_id, seq);
            """)
            self.conn.commit()
            print("   ✅ Batch tables ready")
        except Exception as e:
            try: self.conn.rollback()
            except: pass
            print(f"   ⚠️  Batch tables init: {e}")

    def _ser(self, v):
        if isinstance(v, Decimal): return str(v)
        if isinstance(v, datetime): return v.isoformat()
        if isinstance(v, uuid.UUID): return str(v)
        return v

    def _ser_row(self, r):
        return {k: self._ser(v) for k, v in r.items()} if r else {}

    def _ser_rows(self, rows):
        return [self._ser_row(r) for r in rows]

    # ── CREATE BATCH ──

    def create_batch(self, org_id, user_id, name, items_data, config=None,
                     template_id=None, operation_type='transform'):
        """Crea un batch job con N item.
        items_data = [{'name': 'file1.xml', 'path': '/tmp/file1.xml', 'size': 1234}, ...]
        config = {'output_format': 'json', 'mapping_rules': {...}, ...}
        """
        if not items_data:
            raise ValueError("Almeno un item richiesto")

        # Budget check
        if self.budget_service:
            allowed, msg = self.budget_service.check_budget(org_id)
            if not allowed:
                raise ValueError(f"Budget: {msg}")

        batch_id = str(uuid.uuid4())
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO batch_jobs
                (id, org_id, user_id, name, template_id, operation_type,
                 config, status, total_items)
            VALUES (%s, %s, %s, %s, %s, %s, %s, 'pending', %s)
        """, (batch_id, org_id, user_id, name or f"Batch {len(items_data)} items",
              template_id, operation_type, json.dumps(config or {}), len(items_data)))

        for i, item in enumerate(items_data):
            cur.execute("""
                INSERT INTO batch_items (id, batch_id, seq, input_name, input_path, input_size)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (str(uuid.uuid4()), batch_id, i + 1,
                  item.get('name', f'item_{i+1}'),
                  item.get('path', ''),
                  item.get('size', 0)))

        self.conn.commit()
        return self.get_batch(batch_id)

    # ── GET BATCH ──

    def get_batch(self, batch_id, include_items=True):
        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute("SELECT * FROM batch_jobs WHERE id=%s", (batch_id,))
        row = cur.fetchone()
        if not row:
            return None
        result = self._ser_row(dict(row))

        if include_items:
            cur.execute("SELECT * FROM batch_items WHERE batch_id=%s ORDER BY seq", (batch_id,))
            result['items'] = self._ser_rows([dict(r) for r in cur.fetchall()])

        return result

    def list_batches(self, org_id, status=None, limit=50):
        cur = self.conn.cursor(cursor_factory=self.RDC)
        where = "org_id=%s"
        params = [org_id]
        if status:
            where += " AND status=%s"
            params.append(status)
        params.append(limit)
        cur.execute(f"""
            SELECT * FROM batch_jobs WHERE {where}
            ORDER BY created_at DESC LIMIT %s
        """, params)
        return self._ser_rows([dict(r) for r in cur.fetchall()])

    # ── PROCESS BATCH ──

    def start_batch(self, batch_id, transform_fn=None):
        """Avvia il processamento del batch in background.
        transform_fn(input_path, config) -> (output_path, output_size) or raises
        """
        batch = self.get_batch(batch_id, include_items=True)
        if not batch:
            raise ValueError("Batch non trovato")
        if batch['status'] != 'pending':
            raise ValueError(f"Batch in stato {batch['status']}, non avviabile")

        # Update status
        cur = self.conn.cursor()
        cur.execute("""
            UPDATE batch_jobs SET status='processing', started_at=NOW(), updated_at=NOW()
            WHERE id=%s
        """, (batch_id,))
        self.conn.commit()

        # Process in background
        t = threading.Thread(
            target=self._process_batch,
            args=(batch_id, batch.get('items', []), batch.get('config', {}), transform_fn),
            daemon=True)
        t.start()

    def _process_batch(self, batch_id, items, config, transform_fn):
        """Worker thread: processa ogni item sequenzialmente."""
        completed = 0
        failed = 0
        total = len(items)
        total_cost = Decimal('0')
        output_paths = []

        for item in items:
            item_id = item['id']
            start = time.time()

            # Update item status
            try:
                cur = self.conn.cursor()
                cur.execute("UPDATE batch_items SET status='processing', started_at=NOW() WHERE id=%s", (item_id,))
                self.conn.commit()
            except: pass

            try:
                if transform_fn:
                    out_path, out_size = transform_fn(item.get('input_path', ''), config)
                else:
                    # Stub: just mark as completed
                    out_path = item.get('input_path', '')
                    out_size = item.get('input_size', 0)

                duration_ms = int((time.time() - start) * 1000)
                completed += 1
                output_paths.append(out_path)

                cur = self.conn.cursor()
                cur.execute("""
                    UPDATE batch_items SET status='completed', output_path=%s, output_size=%s,
                           duration_ms=%s, completed_at=NOW() WHERE id=%s
                """, (out_path, out_size, duration_ms, item_id))
                self.conn.commit()

            except Exception as e:
                duration_ms = int((time.time() - start) * 1000)
                failed += 1
                try:
                    cur = self.conn.cursor()
                    cur.execute("""
                        UPDATE batch_items SET status='failed', error=%s,
                               duration_ms=%s, completed_at=NOW() WHERE id=%s
                    """, (str(e)[:500], duration_ms, item_id))
                    self.conn.commit()
                except: pass

            # Update batch progress
            pct = int((completed + failed) / total * 100) if total > 0 else 0
            try:
                cur = self.conn.cursor()
                cur.execute("""
                    UPDATE batch_jobs SET completed_items=%s, failed_items=%s,
                           progress_pct=%s, updated_at=NOW() WHERE id=%s
                """, (completed, failed, pct, batch_id))
                self.conn.commit()
            except: pass

        # Finalize
        final_status = 'completed' if failed == 0 else ('partial' if completed > 0 else 'failed')
        summary = {
            'total': total, 'completed': completed, 'failed': failed,
            'total_cost_eur': str(total_cost),
        }

        # Create ZIP of outputs if there are results
        zip_path = None
        if output_paths and completed > 0:
            try:
                zip_path = self._create_output_zip(batch_id, output_paths)
            except: pass

        try:
            cur = self.conn.cursor()
            cur.execute("""
                UPDATE batch_jobs SET status=%s, completed_at=NOW(), progress_pct=100,
                       result_summary=%s, output_zip_path=%s, updated_at=NOW()
                WHERE id=%s
            """, (final_status, json.dumps(summary), zip_path, batch_id))
            self.conn.commit()
        except: pass

        # Fire webhook
        if self.webhook_service:
            try:
                cur2 = self.conn.cursor(cursor_factory=self.RDC)
                cur2.execute("SELECT org_id FROM batch_jobs WHERE id=%s", (batch_id,))
                row = cur2.fetchone()
                if row:
                    event = 'batch.completed' if final_status in ('completed', 'partial') else 'batch.failed'
                    self.webhook_service.fire_event(str(row['org_id']), event, {
                        'batch_id': batch_id, 'status': final_status, **summary
                    })
            except: pass

    def _create_output_zip(self, batch_id, paths):
        """Crea ZIP con tutti gli output."""
        zip_dir = os.path.join(tempfile.gettempdir(), 'buddyliko_batch')
        os.makedirs(zip_dir, exist_ok=True)
        zip_path = os.path.join(zip_dir, f"batch_{batch_id[:8]}.zip")

        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            for p in paths:
                if p and os.path.exists(p):
                    zf.write(p, os.path.basename(p))

        return zip_path

    # ── CANCEL ──

    def cancel_batch(self, batch_id, org_id):
        cur = self.conn.cursor()
        cur.execute("""
            UPDATE batch_jobs SET status='cancelled', completed_at=NOW(), updated_at=NOW()
            WHERE id=%s AND org_id=%s AND status IN ('pending','processing')
        """, (batch_id, org_id))
        # Cancel pending items
        cur.execute("""
            UPDATE batch_items SET status='skipped'
            WHERE batch_id=%s AND status='pending'
        """, (batch_id,))
        self.conn.commit()
        return cur.rowcount > 0

    # ── STATS ──

    def get_batch_stats(self, org_id):
        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute("""
            SELECT
                COUNT(*) as total_batches,
                COUNT(*) FILTER (WHERE status='completed') as completed,
                COUNT(*) FILTER (WHERE status='processing') as processing,
                COUNT(*) FILTER (WHERE status='failed') as failed,
                COUNT(*) FILTER (WHERE status='partial') as partial,
                SUM(total_items) as total_items,
                SUM(completed_items) as completed_items
            FROM batch_jobs WHERE org_id=%s
        """, (org_id,))
        return self._ser_row(dict(cur.fetchone()))
