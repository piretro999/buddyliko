#!/usr/bin/env python3
"""
Buddyliko — Budget Service (Phase 8B)
Budget cap mensile per org con alert e auto-block.
Tabella org_budgets, check pre-operation, notifiche soglie.
"""

import json
import uuid
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional, Dict, List, Any


ALERT_THRESHOLDS = [50, 75, 90, 100]  # %


class BudgetService:
    """Gestione budget mensili per organizzazioni."""

    def __init__(self, conn, cursor_factory, cost_service=None, webhook_service=None):
        self.conn = conn
        self.RDC = cursor_factory
        self.cost_service = cost_service
        self.webhook_service = webhook_service
        self._init_tables()

    def _init_tables(self):
        cur = self.conn.cursor()
        try:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS org_budgets (
                id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                org_id          UUID NOT NULL REFERENCES organizations(id),
                budget_eur      NUMERIC(10,2) NOT NULL DEFAULT 0,
                alert_pct       JSONB DEFAULT '[50,75,90,100]',
                auto_block      BOOLEAN DEFAULT FALSE,
                block_message   TEXT DEFAULT 'Budget mensile superato. Contatta l''amministratore.',
                notified_pcts   JSONB DEFAULT '[]',
                current_month   VARCHAR(7),
                is_blocked      BOOLEAN DEFAULT FALSE,
                blocked_at      TIMESTAMPTZ,
                created_at      TIMESTAMPTZ DEFAULT NOW(),
                updated_at      TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE(org_id)
            );

            CREATE TABLE IF NOT EXISTS budget_alerts (
                id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                org_id      UUID NOT NULL REFERENCES organizations(id),
                month       VARCHAR(7) NOT NULL,
                threshold   INTEGER NOT NULL,
                spent_eur   NUMERIC(10,2),
                budget_eur  NUMERIC(10,2),
                pct_used    NUMERIC(5,1),
                alert_type  VARCHAR(20) DEFAULT 'warning',
                created_at  TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_ba_org ON budget_alerts(org_id, month);
            """)
            self.conn.commit()
            print("   ✅ Budget tables ready")
        except Exception as e:
            try: self.conn.rollback()
            except: pass
            print(f"   ⚠️  Budget tables init: {e}")

    def _ser(self, v):
        if isinstance(v, Decimal): return str(v)
        if isinstance(v, datetime): return v.isoformat()
        if isinstance(v, uuid.UUID): return str(v)
        return v

    def _ser_row(self, r):
        return {k: self._ser(v) for k, v in r.items()} if r else {}

    def _cur_month(self):
        return datetime.now(timezone.utc).strftime('%Y-%m')

    # ── CRUD ──

    def set_budget(self, org_id, budget_eur, auto_block=False, alert_pct=None, block_message=None):
        """Imposta o aggiorna il budget mensile dell'org."""
        if budget_eur < 0:
            raise ValueError("Budget deve essere >= 0")

        cur = self.conn.cursor()
        alerts = json.dumps(alert_pct or ALERT_THRESHOLDS)
        msg = block_message or "Budget mensile superato. Contatta l'amministratore."

        cur.execute("""
            INSERT INTO org_budgets (id, org_id, budget_eur, auto_block, alert_pct, block_message, current_month)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (org_id) DO UPDATE SET
                budget_eur=%s, auto_block=%s, alert_pct=%s, block_message=%s,
                updated_at=NOW()
        """, (str(uuid.uuid4()), org_id, budget_eur, auto_block, alerts, msg, self._cur_month(),
              budget_eur, auto_block, alerts, msg))
        self.conn.commit()
        return self.get_budget(org_id)

    def get_budget(self, org_id):
        """Ritorna il budget dell'org con stato corrente."""
        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute("SELECT * FROM org_budgets WHERE org_id=%s", (org_id,))
        row = cur.fetchone()
        if not row:
            return {
                'org_id': str(org_id), 'budget_eur': '0', 'auto_block': False,
                'is_blocked': False, 'has_budget': False,
                'spent_eur': '0', 'pct_used': 0, 'remaining_eur': '0',
            }

        budget = self._ser_row(dict(row))
        budget['has_budget'] = True

        # Calcola spesa corrente
        month = self._cur_month()
        spent = self._get_month_spend(org_id, month)
        budget_val = Decimal(str(row['budget_eur']))
        pct = (spent / budget_val * 100).quantize(Decimal('0.1')) if budget_val > 0 else Decimal('0')

        budget['spent_eur'] = str(spent)
        budget['pct_used'] = float(pct)
        budget['remaining_eur'] = str(max(Decimal('0'), budget_val - spent))
        budget['month'] = month

        return budget

    def remove_budget(self, org_id):
        """Rimuove il budget (e sblocca l'org)."""
        cur = self.conn.cursor()
        cur.execute("DELETE FROM org_budgets WHERE org_id=%s", (org_id,))
        self.conn.commit()
        return cur.rowcount > 0

    # ── CHECK PRE-OPERATION ──

    def check_budget(self, org_id):
        """Chiamato prima di ogni operazione fatturabile.
        Ritorna (allowed, message).
        """
        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute("SELECT * FROM org_budgets WHERE org_id=%s", (org_id,))
        row = cur.fetchone()
        if not row:
            return True, ""  # Nessun budget = nessun limite

        budget = dict(row)
        budget_eur = Decimal(str(budget['budget_eur']))
        if budget_eur <= 0:
            return True, ""

        # Reset notifiche al cambio mese
        month = self._cur_month()
        if budget.get('current_month') != month:
            cur2 = self.conn.cursor()
            cur2.execute("""
                UPDATE org_budgets SET current_month=%s, notified_pcts='[]',
                       is_blocked=FALSE, blocked_at=NULL, updated_at=NOW()
                WHERE org_id=%s
            """, (month, org_id))
            self.conn.commit()
            budget['notified_pcts'] = []
            budget['is_blocked'] = False

        # Spesa corrente
        spent = self._get_month_spend(org_id, month)
        pct = float((spent / budget_eur * 100)) if budget_eur > 0 else 0

        # Check alert thresholds
        alert_pcts = budget.get('alert_pct', [])
        if isinstance(alert_pcts, str): alert_pcts = json.loads(alert_pcts)
        notified = budget.get('notified_pcts', [])
        if isinstance(notified, str): notified = json.loads(notified)

        for threshold in sorted(alert_pcts):
            if pct >= threshold and threshold not in notified:
                notified.append(threshold)
                self._record_alert(org_id, month, threshold, spent, budget_eur, pct)
                # Fire webhook
                if self.webhook_service:
                    evt = 'budget.exceeded' if threshold >= 100 else 'budget.warning'
                    self.webhook_service.fire_event(org_id, evt, {
                        'threshold': threshold, 'pct_used': round(pct, 1),
                        'spent_eur': str(spent), 'budget_eur': str(budget_eur)
                    })

        # Update notified list
        if notified != (budget.get('notified_pcts') or []):
            cur2 = self.conn.cursor()
            cur2.execute("UPDATE org_budgets SET notified_pcts=%s WHERE org_id=%s",
                         (json.dumps(notified), org_id))
            self.conn.commit()

        # Auto-block check
        if pct >= 100 and budget.get('auto_block'):
            if not budget.get('is_blocked'):
                cur2 = self.conn.cursor()
                cur2.execute("""
                    UPDATE org_budgets SET is_blocked=TRUE, blocked_at=NOW()
                    WHERE org_id=%s
                """, (org_id,))
                self.conn.commit()
                if self.webhook_service:
                    self.webhook_service.fire_event(org_id, 'budget.blocked', {
                        'spent_eur': str(spent), 'budget_eur': str(budget_eur)
                    })
            return False, budget.get('block_message', 'Budget superato')

        return True, ""

    def unblock(self, org_id):
        """Sblocca manualmente un'org bloccata per budget."""
        cur = self.conn.cursor()
        cur.execute("""
            UPDATE org_budgets SET is_blocked=FALSE, blocked_at=NULL, updated_at=NOW()
            WHERE org_id=%s AND is_blocked=TRUE
        """, (org_id,))
        self.conn.commit()
        return cur.rowcount > 0

    # ── ALERTS LOG ──

    def get_alerts(self, org_id, month=None, limit=50):
        cur = self.conn.cursor(cursor_factory=self.RDC)
        if month:
            cur.execute("""
                SELECT * FROM budget_alerts WHERE org_id=%s AND month=%s
                ORDER BY created_at DESC LIMIT %s
            """, (org_id, month, limit))
        else:
            cur.execute("""
                SELECT * FROM budget_alerts WHERE org_id=%s
                ORDER BY created_at DESC LIMIT %s
            """, (org_id, limit))
        return [self._ser_row(dict(r)) for r in cur.fetchall()]

    def _record_alert(self, org_id, month, threshold, spent, budget_eur, pct):
        try:
            cur = self.conn.cursor()
            alert_type = 'blocked' if threshold >= 100 else ('critical' if threshold >= 90 else 'warning')
            cur.execute("""
                INSERT INTO budget_alerts (org_id, month, threshold, spent_eur, budget_eur, pct_used, alert_type)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (org_id, month, threshold, spent, budget_eur, pct, alert_type))
            self.conn.commit()
        except: pass

    def _get_month_spend(self, org_id, month):
        """Ottieni la spesa del mese corrente da usage_aggregates."""
        try:
            cur = self.conn.cursor(cursor_factory=self.RDC)
            cur.execute("""
                SELECT COALESCE(billable_eur_total, 0) as spent
                FROM usage_aggregates
                WHERE org_id=%s AND period_type='monthly' AND period_key=%s
                  AND auth_type='all' AND environment='live'
            """, (org_id, month))
            r = cur.fetchone()
            return Decimal(str(r['spent'])) if r else Decimal('0')
        except:
            return Decimal('0')

    # ── PLATFORM OVERVIEW ──

    def get_platform_budgets(self):
        """Admin: panoramica budget di tutte le org."""
        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute("""
            SELECT b.*, o.name as org_name, o.plan, o.status as org_status
            FROM org_budgets b
            JOIN organizations o ON b.org_id = o.id
            ORDER BY b.budget_eur DESC
        """)
        rows = [self._ser_row(dict(r)) for r in cur.fetchall()]
        month = self._cur_month()
        for r in rows:
            spent = self._get_month_spend(r['org_id'], month)
            budget = Decimal(str(r.get('budget_eur', 0)))
            r['spent_eur'] = str(spent)
            r['pct_used'] = float((spent / budget * 100).quantize(Decimal('0.1'))) if budget > 0 else 0
        return rows
