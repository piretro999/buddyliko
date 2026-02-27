#!/usr/bin/env python3
"""
Buddyliko — Webhook Service (Phase 8A)
Gestione webhook: CRUD endpoint, fire events, retry, HMAC signing, delivery log.
"""

import json
import uuid
import hmac
import hashlib
import time
import threading
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import Optional, Dict, List, Any
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

# Events che possono triggerare webhook
WEBHOOK_EVENTS = (
    'transform.completed', 'transform.failed',
    'ai.completed', 'ai.failed',
    'budget.warning', 'budget.exceeded', 'budget.blocked',
    'org.member_added', 'org.member_removed',
    'org.suspended', 'org.reactivated',
    'token.created', 'token.revoked',
    'partner.sub_org_created', 'partner.sub_org_suspended',
    'template.purchased', 'template.reviewed',
    'batch.completed', 'batch.failed',
)

MAX_RETRIES = 3
RETRY_DELAYS = [60, 300, 900]  # 1min, 5min, 15min


class WebhookService:
    """Gestione webhook per notifiche eventi."""

    def __init__(self, conn, cursor_factory):
        self.conn = conn
        self.RDC = cursor_factory
        self._init_tables()

    def _init_tables(self):
        cur = self.conn.cursor()
        try:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS webhooks (
                id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                org_id      UUID NOT NULL REFERENCES organizations(id),
                name        VARCHAR(255) NOT NULL,
                url         TEXT NOT NULL,
                secret      VARCHAR(255),
                events      JSONB NOT NULL DEFAULT '[]',
                headers     JSONB DEFAULT '{}',
                is_active   BOOLEAN DEFAULT TRUE,
                created_by  INTEGER REFERENCES users(id),
                created_at  TIMESTAMPTZ DEFAULT NOW(),
                updated_at  TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_wh_org ON webhooks(org_id, is_active);

            CREATE TABLE IF NOT EXISTS webhook_deliveries (
                id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                webhook_id    UUID NOT NULL REFERENCES webhooks(id) ON DELETE CASCADE,
                org_id        UUID NOT NULL,
                event         VARCHAR(60) NOT NULL,
                payload       JSONB,
                status_code   INTEGER,
                response_body TEXT,
                error         TEXT,
                attempt       INTEGER DEFAULT 1,
                success       BOOLEAN DEFAULT FALSE,
                duration_ms   INTEGER,
                delivered_at  TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_wd_webhook ON webhook_deliveries(webhook_id);
            CREATE INDEX IF NOT EXISTS idx_wd_org ON webhook_deliveries(org_id, delivered_at DESC);
            """)
            self.conn.commit()
            print("   ✅ Webhook tables ready")
        except Exception as e:
            try: self.conn.rollback()
            except: pass
            print(f"   ⚠️  Webhook tables init: {e}")

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

    def create_webhook(self, org_id, user_id, name, url, events, secret=None, headers=None):
        """Crea un webhook endpoint."""
        if not name or not url:
            raise ValueError("Name e URL obbligatori")
        if not events:
            raise ValueError("Seleziona almeno un evento")

        # Validate events
        for ev in events:
            if ev != '*' and ev not in WEBHOOK_EVENTS:
                raise ValueError(f"Evento non valido: {ev}")

        # Generate secret if not provided
        if not secret:
            secret = 'whsec_' + uuid.uuid4().hex

        wh_id = str(uuid.uuid4())
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO webhooks (id, org_id, name, url, secret, events, headers, created_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (wh_id, org_id, name, url, secret,
              json.dumps(events), json.dumps(headers or {}), user_id))
        self.conn.commit()
        return self.get_webhook(wh_id)

    def get_webhook(self, webhook_id):
        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute("SELECT * FROM webhooks WHERE id = %s", (webhook_id,))
        r = cur.fetchone()
        return self._ser_row(dict(r)) if r else None

    def list_webhooks(self, org_id):
        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute("""
            SELECT w.*,
                (SELECT COUNT(*) FROM webhook_deliveries wd WHERE wd.webhook_id=w.id AND wd.success=TRUE) as success_count,
                (SELECT COUNT(*) FROM webhook_deliveries wd WHERE wd.webhook_id=w.id AND wd.success=FALSE) as fail_count,
                (SELECT MAX(wd.delivered_at) FROM webhook_deliveries wd WHERE wd.webhook_id=w.id) as last_delivery
            FROM webhooks w
            WHERE w.org_id = %s ORDER BY w.created_at DESC
        """, (org_id,))
        return self._ser_rows([dict(r) for r in cur.fetchall()])

    def update_webhook(self, webhook_id, org_id, data):
        allowed = ['name', 'url', 'events', 'headers', 'is_active', 'secret']
        fields, values = [], []
        for k, v in data.items():
            if k in allowed:
                if isinstance(v, (dict, list)): v = json.dumps(v)
                if isinstance(v, bool): pass  # psycopg handles bool
                fields.append(f"{k} = %s"); values.append(v)
        if not fields: return False
        fields.append("updated_at = NOW()")
        values.extend([webhook_id, org_id])
        cur = self.conn.cursor()
        cur.execute(f"UPDATE webhooks SET {', '.join(fields)} WHERE id=%s AND org_id=%s", values)
        self.conn.commit()
        return cur.rowcount > 0

    def delete_webhook(self, webhook_id, org_id):
        cur = self.conn.cursor()
        cur.execute("DELETE FROM webhooks WHERE id=%s AND org_id=%s", (webhook_id, org_id))
        self.conn.commit()
        return cur.rowcount > 0

    # ── FIRE EVENT ──

    def fire_event(self, org_id, event, payload=None):
        """Invia evento a tutti i webhook attivi dell'org che ascoltano quell'evento.
        Esegue in background thread per non bloccare la request."""
        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute("""
            SELECT * FROM webhooks WHERE org_id=%s AND is_active=TRUE
        """, (org_id,))
        webhooks = [dict(r) for r in cur.fetchall()]

        for wh in webhooks:
            events = wh.get('events', [])
            if isinstance(events, str): events = json.loads(events)
            if '*' in events or event in events:
                t = threading.Thread(
                    target=self._deliver, args=(wh, org_id, event, payload or {}),
                    daemon=True)
                t.start()

    def _deliver(self, webhook, org_id, event, payload, attempt=1):
        """Esegue la delivery HTTP con HMAC signing."""
        wh_id = str(webhook['id'])
        url = webhook['url']
        secret = webhook.get('secret', '')
        custom_headers = webhook.get('headers', {})
        if isinstance(custom_headers, str):
            custom_headers = json.loads(custom_headers)

        body_dict = {
            'event': event,
            'org_id': str(org_id),
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'data': payload,
            'webhook_id': wh_id,
            'attempt': attempt,
        }
        body_bytes = json.dumps(body_dict).encode('utf-8')

        # HMAC-SHA256 signing
        signature = ''
        if secret:
            sig = hmac.new(secret.encode('utf-8'), body_bytes, hashlib.sha256).hexdigest()
            signature = f"sha256={sig}"

        headers = {
            'Content-Type': 'application/json',
            'User-Agent': 'Buddyliko-Webhook/1.0',
            'X-Buddyliko-Event': event,
            'X-Buddyliko-Signature': signature,
            'X-Buddyliko-Delivery': str(uuid.uuid4()),
            **custom_headers,
        }

        status_code = None
        response_body = ''
        error = None
        success = False
        start = time.time()

        try:
            req = Request(url, data=body_bytes, headers=headers, method='POST')
            with urlopen(req, timeout=10) as resp:
                status_code = resp.status
                response_body = resp.read().decode('utf-8', errors='replace')[:2000]
                success = 200 <= status_code < 300
        except HTTPError as e:
            status_code = e.code
            try: response_body = e.read().decode('utf-8', errors='replace')[:2000]
            except: pass
            error = str(e)
        except Exception as e:
            error = str(e)[:500]

        duration_ms = int((time.time() - start) * 1000)

        # Log delivery
        try:
            cur = self.conn.cursor()
            cur.execute("""
                INSERT INTO webhook_deliveries
                    (webhook_id, org_id, event, payload, status_code, response_body, error, attempt, success, duration_ms)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (wh_id, str(org_id), event, json.dumps(body_dict),
                  status_code, response_body, error, attempt, success, duration_ms))
            self.conn.commit()
        except: pass

        # Retry on failure
        if not success and attempt < MAX_RETRIES:
            delay = RETRY_DELAYS[attempt - 1] if attempt - 1 < len(RETRY_DELAYS) else 900
            time.sleep(min(delay, 5))  # In-process: cap at 5s for demo. In prod use task queue.
            self._deliver(webhook, org_id, event, payload, attempt + 1)

    # ── DELIVERY LOG ──

    def get_deliveries(self, org_id, webhook_id=None, limit=50):
        cur = self.conn.cursor(cursor_factory=self.RDC)
        if webhook_id:
            cur.execute("""
                SELECT * FROM webhook_deliveries WHERE org_id=%s AND webhook_id=%s
                ORDER BY delivered_at DESC LIMIT %s
            """, (org_id, webhook_id, limit))
        else:
            cur.execute("""
                SELECT wd.*, w.name as webhook_name
                FROM webhook_deliveries wd
                JOIN webhooks w ON wd.webhook_id = w.id
                WHERE wd.org_id=%s ORDER BY wd.delivered_at DESC LIMIT %s
            """, (org_id, limit))
        return self._ser_rows([dict(r) for r in cur.fetchall()])

    def get_available_events(self):
        return list(WEBHOOK_EVENTS)

    def test_webhook(self, webhook_id, org_id):
        """Invia un evento di test."""
        wh = self.get_webhook(webhook_id)
        if not wh or str(wh.get('org_id', '')) != str(org_id):
            raise ValueError("Webhook non trovato")
        self.fire_event(org_id, 'test.ping', {
            'message': 'Test webhook delivery',
            'webhook_id': webhook_id,
            'timestamp': datetime.now(timezone.utc).isoformat()
        })
        return True
