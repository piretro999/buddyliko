#!/usr/bin/env python3
"""
Buddyliko - Stripe Billing System
Gestisce piani Free/Pro/Enterprise, checkout, webhook, usage limits.

Piani:
  FREE       €0      100 trasf/mese, 1MB/file, no codegen, no API
  PRO        €49/m   Illimitate, 50MB, codegen, API, 5 utenti
  ENTERPRISE €299/m  Tutto PRO + DB connector, SSO, audit full, utenti illimitati

Integrazione:
  - POST /api/billing/checkout       → crea Stripe Checkout Session
  - POST /api/billing/portal         → Stripe Customer Portal (gestisci abbonamento)
  - POST /api/billing/webhook        → riceve eventi Stripe
  - GET  /api/billing/status         → piano corrente + usage
  - POST /api/billing/admin/override → admin assegna piano gratuito permanente

Dipendenza: pip install stripe
"""

import json
import uuid
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, List, Any, Tuple
from enum import Enum


# ===========================================================================
# PIANO DEFINITIONS
# ===========================================================================

class Plan(str, Enum):
    FREE       = "FREE"
    PRO        = "PRO"
    ENTERPRISE = "ENTERPRISE"
    CUSTOM     = "CUSTOM"   # assegnato manualmente dall'admin (bypass Stripe)


PLAN_LIMITS = {
    Plan.FREE: {
        "transforms_per_month": 100,
        "max_file_size_mb": 1,
        "code_generation": False,
        "api_access": False,
        "max_users": 1,
        "db_connector": False,
        "audit_level": "MINIMAL",
        "async_transforms": False,
        "sla": None,
    },
    Plan.PRO: {
        "transforms_per_month": -1,      # illimitato
        "max_file_size_mb": 50,
        "code_generation": True,
        "api_access": True,
        "max_users": 5,
        "db_connector": False,
        "audit_level": "STANDARD",
        "async_transforms": True,
        "sla": None,
    },
    Plan.ENTERPRISE: {
        "transforms_per_month": -1,
        "max_file_size_mb": 500,
        "code_generation": True,
        "api_access": True,
        "max_users": -1,                 # illimitato
        "db_connector": True,
        "audit_level": "FULL",
        "async_transforms": True,
        "sla": "99.9%",
    },
    Plan.CUSTOM: {
        "transforms_per_month": -1,
        "max_file_size_mb": 500,
        "code_generation": True,
        "api_access": True,
        "max_users": -1,
        "db_connector": True,
        "audit_level": "FULL",
        "async_transforms": True,
        "sla": None,
    },
}

PLAN_PRICES_EUR = {
    Plan.FREE:       {"monthly": 0,   "yearly": 0},
    Plan.PRO:        {"monthly": 49,  "yearly": 490},
    Plan.ENTERPRISE: {"monthly": 299, "yearly": 2990},
}


# ===========================================================================
# BILLING MANAGER
# ===========================================================================

class BillingManager:
    """
    Gestisce tutta la logica Stripe.
    Richiede: pip install stripe
    Configurazione in config.yaml:
      billing:
        stripe_secret_key: sk_live_...
        stripe_webhook_secret: whsec_...
        stripe_publishable_key: pk_live_...
        success_url: https://app.buddyliko.com/billing/success
        cancel_url: https://app.buddyliko.com/billing/cancel
        price_ids:
          pro_monthly: price_xxx
          pro_yearly: price_xxx
          enterprise_monthly: price_xxx
          enterprise_yearly: price_xxx
    """

    def __init__(self, conn, RealDictCursor, config: Dict):
        self.conn = conn
        self.RealDictCursor = RealDictCursor
        self.config = config
        self._stripe = None
        self._init_tables()
        self._init_stripe()

    def _init_stripe(self):
        secret_key = self.config.get('stripe_secret_key', '')
        if not secret_key or secret_key.startswith('sk_test_PLACEHOLDER'):
            print("⚠️  Stripe: no secret key configured — billing disabled")
            return
        try:
            import stripe
            stripe.api_key = secret_key
            self._stripe = stripe
            print("✅ Stripe initialized")
        except ImportError:
            print("⚠️  Stripe: pip install stripe required")

    @property
    def enabled(self) -> bool:
        return self._stripe is not None

    def _init_tables(self):
        cur = self.conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS subscriptions (
                id VARCHAR(36) PRIMARY KEY,
                user_id VARCHAR(255) NOT NULL,
                stripe_customer_id VARCHAR(255),
                stripe_subscription_id VARCHAR(255),
                plan VARCHAR(50) NOT NULL DEFAULT 'FREE',
                status VARCHAR(50) NOT NULL DEFAULT 'active',
                -- 'active' | 'trialing' | 'past_due' | 'canceled' | 'custom'
                period_start TIMESTAMPTZ,
                period_end TIMESTAMPTZ,
                cancel_at_period_end BOOLEAN DEFAULT FALSE,
                override_by_admin VARCHAR(255),   -- email admin se piano assegnato manualmente
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW(),
                metadata JSONB DEFAULT '{}'
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sub_user ON subscriptions(user_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sub_stripe ON subscriptions(stripe_subscription_id)")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS usage_counters (
                user_id VARCHAR(255) NOT NULL,
                month VARCHAR(7) NOT NULL,   -- YYYY-MM
                transforms_count INTEGER DEFAULT 0,
                api_calls_count INTEGER DEFAULT 0,
                bytes_processed BIGINT DEFAULT 0,
                codegen_count INTEGER DEFAULT 0,
                PRIMARY KEY (user_id, month)
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS billing_events (
                id VARCHAR(36) PRIMARY KEY,
                stripe_event_id VARCHAR(255) UNIQUE,
                event_type VARCHAR(100),
                user_id VARCHAR(255),
                payload JSONB,
                processed_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        print("✅ Billing tables initialized")

    # ------------------------------------------------------------------
    # PLAN LOOKUP
    # ------------------------------------------------------------------

    def get_user_plan(self, user_id: str) -> Dict:
        """Ritorna piano corrente, limiti e usage del mese corrente."""
        cur = self.conn.cursor(cursor_factory=self.RealDictCursor)
        cur.execute("""
            SELECT plan, status, period_end, cancel_at_period_end, override_by_admin
            FROM subscriptions
            WHERE user_id = %s
            ORDER BY created_at DESC LIMIT 1
        """, (str(user_id),))
        row = cur.fetchone()

        plan_name = Plan.FREE
        sub_status = 'active'
        period_end = None
        cancel_at = False
        is_custom = False

        if row:
            try:
                plan_name = Plan(row['plan'])
            except ValueError:
                plan_name = Plan.FREE
            sub_status = row['status']
            period_end = row['period_end'].isoformat() if row['period_end'] else None
            cancel_at = row['cancel_at_period_end']
            is_custom = bool(row['override_by_admin'])

        # Se piano scaduto (period_end nel passato), degrada a FREE
        if period_end and not is_custom and sub_status == 'canceled':
            plan_name = Plan.FREE

        limits = PLAN_LIMITS[plan_name]
        usage = self._get_monthly_usage(user_id)

        return {
            "plan": plan_name.value,
            "status": sub_status,
            "period_end": period_end,
            "cancel_at_period_end": cancel_at,
            "is_custom": is_custom,
            "limits": limits,
            "usage": usage,
            "prices": PLAN_PRICES_EUR.get(plan_name, {}),
        }

    def check_limit(self, user_id: str, feature: str, file_size_bytes: int = 0) -> Tuple[bool, str]:
        """
        Controlla se l'utente può usare una feature.
        Ritorna (allowed: bool, reason: str)
        """
        plan_data = self.get_user_plan(user_id)
        limits = plan_data["limits"]
        usage = plan_data["usage"]
        plan = plan_data["plan"]

        if feature == "transform":
            max_t = limits["transforms_per_month"]
            if max_t != -1 and usage["transforms_count"] >= max_t:
                return False, f"Piano {plan}: limite {max_t} trasformazioni/mese raggiunto. Aggiorna a PRO."
            max_mb = limits["max_file_size_mb"]
            if file_size_bytes > max_mb * 1024 * 1024:
                return False, f"Piano {plan}: file max {max_mb}MB. Questo file è {file_size_bytes/1024/1024:.1f}MB."
            return True, ""

        if feature == "code_generation":
            if not limits["code_generation"]:
                return False, f"Piano {plan}: code generation non incluso. Aggiorna a PRO."
            return True, ""

        if feature == "api_access":
            if not limits["api_access"]:
                return False, f"Piano {plan}: API access non incluso. Aggiorna a PRO."
            return True, ""

        if feature == "db_connector":
            if not limits["db_connector"]:
                return False, f"Piano {plan}: DB connector non incluso. Aggiorna a ENTERPRISE."
            return True, ""

        if feature == "async_transform":
            if not limits["async_transforms"]:
                return False, f"Piano {plan}: trasformazioni asincrone non incluse. Aggiorna a PRO."
            return True, ""

        return True, ""

    def increment_usage(self, user_id: str, field: str = "transforms_count",
                        value: int = 1):
        """Incrementa il contatore usage del mese corrente."""
        month = datetime.now(timezone.utc).strftime('%Y-%m')
        allowed_fields = {"transforms_count", "api_calls_count", "bytes_processed", "codegen_count"}
        if field not in allowed_fields:
            return
        cur = self.conn.cursor()
        cur.execute(f"""
            INSERT INTO usage_counters (user_id, month, {field})
            VALUES (%s, %s, %s)
            ON CONFLICT (user_id, month)
            DO UPDATE SET {field} = usage_counters.{field} + %s
        """, (str(user_id), month, value, value))

    def _get_monthly_usage(self, user_id: str) -> Dict:
        month = datetime.now(timezone.utc).strftime('%Y-%m')
        cur = self.conn.cursor(cursor_factory=self.RealDictCursor)
        cur.execute("""
            SELECT transforms_count, api_calls_count, bytes_processed, codegen_count
            FROM usage_counters WHERE user_id = %s AND month = %s
        """, (str(user_id), month))
        row = cur.fetchone()
        if row:
            return dict(row)
        return {"transforms_count": 0, "api_calls_count": 0,
                "bytes_processed": 0, "codegen_count": 0}

    # ------------------------------------------------------------------
    # STRIPE CHECKOUT
    # ------------------------------------------------------------------

    def create_checkout_session(self, user_id: str, user_email: str,
                                 plan: str, billing_period: str = "monthly",
                                 coupon: str = None) -> Dict:
        """
        Crea una Stripe Checkout Session.
        Ritorna { checkout_url, session_id }
        """
        if not self.enabled:
            raise RuntimeError("Stripe not configured")

        price_ids = self.config.get('price_ids', {})
        price_key = f"{plan.lower()}_{billing_period}"
        price_id = price_ids.get(price_key)
        if not price_id:
            raise ValueError(f"No Stripe price ID for {price_key}. Configure billing.price_ids in config.yaml")

        # Trova o crea customer Stripe
        customer_id = self._get_or_create_customer(user_id, user_email)

        params = {
            "customer": customer_id,
            "mode": "subscription",
            "line_items": [{"price": price_id, "quantity": 1}],
            "success_url": self.config.get('success_url',
                'https://app.buddyliko.com/billing/success?session_id={CHECKOUT_SESSION_ID}'),
            "cancel_url": self.config.get('cancel_url',
                'https://app.buddyliko.com/billing/cancel'),
            "metadata": {"user_id": str(user_id), "plan": plan},
            "subscription_data": {
                "metadata": {"user_id": str(user_id), "plan": plan}
            },
            "allow_promotion_codes": True,  # permette coupon Stripe Dashboard
        }

        if coupon:
            params["discounts"] = [{"coupon": coupon}]

        session = self._stripe.checkout.Session.create(**params)
        return {"checkout_url": session.url, "session_id": session.id}

    def create_portal_session(self, user_id: str) -> str:
        """Customer Portal Stripe per gestire/cancellare abbonamento."""
        if not self.enabled:
            raise RuntimeError("Stripe not configured")
        customer_id = self._find_customer(user_id)
        if not customer_id:
            raise ValueError("No Stripe customer found for this user")
        session = self._stripe.billing_portal.Session.create(
            customer=customer_id,
            return_url=self.config.get('portal_return_url',
                'https://app.buddyliko.com/billing')
        )
        return session.url

    def _get_or_create_customer(self, user_id: str, email: str) -> str:
        existing = self._find_customer(user_id)
        if existing:
            return existing
        customer = self._stripe.Customer.create(
            email=email,
            metadata={"user_id": str(user_id)}
        )
        # Salva il customer_id nella subscription record
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO subscriptions (id, user_id, stripe_customer_id, plan, status)
            VALUES (%s, %s, %s, 'FREE', 'active')
            ON CONFLICT (id) DO NOTHING
        """, (str(uuid.uuid4()), str(user_id), customer.id))
        return customer.id

    def _find_customer(self, user_id: str) -> Optional[str]:
        cur = self.conn.cursor()
        cur.execute("""
            SELECT stripe_customer_id FROM subscriptions
            WHERE user_id = %s AND stripe_customer_id IS NOT NULL
            ORDER BY created_at DESC LIMIT 1
        """, (str(user_id),))
        row = cur.fetchone()
        return row[0] if row else None

    # ------------------------------------------------------------------
    # WEBHOOK HANDLER
    # ------------------------------------------------------------------

    def handle_webhook(self, payload: bytes, sig_header: str) -> Dict:
        """
        Riceve e processa eventi Stripe.
        Chiamato da POST /api/billing/webhook.
        """
        if not self.enabled:
            return {"received": True}

        webhook_secret = self.config.get('stripe_webhook_secret', '')
        try:
            event = self._stripe.Webhook.construct_event(
                payload, sig_header, webhook_secret
            )
        except self._stripe.error.SignatureVerificationError as e:
            raise ValueError(f"Invalid Stripe signature: {e}")

        event_type = event['type']
        event_id = event['id']

        # Idempotenza: non rielaborare eventi già processati
        cur = self.conn.cursor()
        cur.execute("SELECT id FROM billing_events WHERE stripe_event_id = %s", (event_id,))
        if cur.fetchone():
            return {"received": True, "status": "already_processed"}

        # Processa l'evento
        handler = {
            'checkout.session.completed':          self._on_checkout_completed,
            'customer.subscription.created':       self._on_subscription_created,
            'customer.subscription.updated':       self._on_subscription_updated,
            'customer.subscription.deleted':       self._on_subscription_deleted,
            'invoice.payment_succeeded':           self._on_payment_succeeded,
            'invoice.payment_failed':              self._on_payment_failed,
        }.get(event_type)

        result = {}
        if handler:
            result = handler(event['data']['object']) or {}

        # Salva evento processato
        user_id = result.get('user_id', '')
        cur.execute("""
            INSERT INTO billing_events (id, stripe_event_id, event_type, user_id, payload)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (stripe_event_id) DO NOTHING
        """, (
            str(uuid.uuid4()), event_id, event_type, str(user_id),
            json.dumps(dict(event['data']['object']))
        ))

        return {"received": True, "event_type": event_type, **result}

    def _on_checkout_completed(self, obj) -> Dict:
        """Checkout completato → aggiorna piano utente."""
        user_id = obj.get('metadata', {}).get('user_id', '')
        plan = obj.get('metadata', {}).get('plan', 'PRO').upper()
        subscription_id = obj.get('subscription', '')
        customer_id = obj.get('customer', '')
        if user_id:
            self._upsert_subscription(user_id, Plan(plan),
                                       subscription_id, customer_id,
                                       status='active')
        return {"user_id": user_id, "plan": plan}

    def _on_subscription_created(self, obj) -> Dict:
        return self._sync_subscription(obj)

    def _on_subscription_updated(self, obj) -> Dict:
        return self._sync_subscription(obj)

    def _on_subscription_deleted(self, obj) -> Dict:
        sub_id = obj.get('id', '')
        cur = self.conn.cursor()
        cur.execute("""
            UPDATE subscriptions SET status = 'canceled', updated_at = NOW()
            WHERE stripe_subscription_id = %s
        """, (sub_id,))
        # Cerca user_id per ritornarlo
        cur.execute("SELECT user_id FROM subscriptions WHERE stripe_subscription_id = %s",
                    (sub_id,))
        row = cur.fetchone()
        user_id = row[0] if row else ''
        # Aggiorna plan in users table
        if user_id:
            self._update_users_plan(user_id, 'FREE')
        return {"user_id": user_id, "plan": "FREE"}

    def _on_payment_succeeded(self, obj) -> Dict:
        sub_id = obj.get('subscription', '')
        if sub_id:
            cur = self.conn.cursor()
            cur.execute("""
                UPDATE subscriptions SET status = 'active', updated_at = NOW()
                WHERE stripe_subscription_id = %s
            """, (sub_id,))
        return {}

    def _on_payment_failed(self, obj) -> Dict:
        sub_id = obj.get('subscription', '')
        if sub_id:
            cur = self.conn.cursor()
            cur.execute("""
                UPDATE subscriptions SET status = 'past_due', updated_at = NOW()
                WHERE stripe_subscription_id = %s
            """, (sub_id,))
        return {}

    def _sync_subscription(self, obj) -> Dict:
        """Sincronizza subscription Stripe con il DB."""
        sub_id = obj.get('id', '')
        customer_id = obj.get('customer', '')
        status = obj.get('status', 'active')
        user_id = obj.get('metadata', {}).get('user_id', '')
        cancel_at_period_end = obj.get('cancel_at_period_end', False)

        # Ricava period_end
        period_end = None
        if obj.get('current_period_end'):
            period_end = datetime.fromtimestamp(obj['current_period_end'], tz=timezone.utc)

        # Ricava piano dal price item
        plan = Plan.PRO  # default
        items = obj.get('items', {}).get('data', [])
        if items:
            price_id = items[0].get('price', {}).get('id', '')
            price_ids = self.config.get('price_ids', {})
            for key, pid in price_ids.items():
                if pid == price_id:
                    if 'enterprise' in key:
                        plan = Plan.ENTERPRISE
                    elif 'pro' in key:
                        plan = Plan.PRO
                    break

        if not user_id:
            # Cerca user_id dal customer_id
            cur = self.conn.cursor()
            cur.execute("""
                SELECT user_id FROM subscriptions
                WHERE stripe_customer_id = %s LIMIT 1
            """, (customer_id,))
            row = cur.fetchone()
            user_id = row[0] if row else ''

        if user_id:
            self._upsert_subscription(user_id, plan, sub_id, customer_id,
                                       status=status, period_end=period_end,
                                       cancel_at_period_end=cancel_at_period_end)

        return {"user_id": user_id, "plan": plan.value, "status": status}

    def _upsert_subscription(self, user_id: str, plan: Plan,
                              stripe_sub_id: str = None, customer_id: str = None,
                              status: str = 'active', period_end=None,
                              cancel_at_period_end: bool = False):
        """Crea o aggiorna il record subscription e il piano nell'users table."""
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO subscriptions (
                id, user_id, stripe_customer_id, stripe_subscription_id,
                plan, status, period_end, cancel_at_period_end, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (id) DO UPDATE SET
                plan = EXCLUDED.plan,
                status = EXCLUDED.status,
                stripe_subscription_id = EXCLUDED.stripe_subscription_id,
                stripe_customer_id = EXCLUDED.stripe_customer_id,
                period_end = EXCLUDED.period_end,
                cancel_at_period_end = EXCLUDED.cancel_at_period_end,
                updated_at = NOW()
        """, (
            str(uuid.uuid4()), str(user_id), customer_id, stripe_sub_id,
            plan.value, status, period_end, cancel_at_period_end
        ))
        # Aggiorna anche la colonna plan nella tabella users
        self._update_users_plan(user_id, plan.value)

    def _update_users_plan(self, user_id: str, plan_value: str):
        cur = self.conn.cursor()
        cur.execute("UPDATE users SET plan = %s WHERE id = %s",
                    (plan_value, str(user_id)))

    # ------------------------------------------------------------------
    # ADMIN OVERRIDE
    # ------------------------------------------------------------------

    def admin_set_plan(self, user_id: str, plan: str,
                        admin_email: str, note: str = '') -> Dict:
        """
        Admin assegna piano gratuito permanente (bypass Stripe).
        Piano CUSTOM: nessuna scadenza, nessun addebito.
        """
        try:
            plan_enum = Plan(plan.upper())
        except ValueError:
            raise ValueError(f"Piano non valido: {plan}")

        cur = self.conn.cursor()
        # Prima cancella eventuali subscription precedenti
        cur.execute("""
            UPDATE subscriptions SET status = 'canceled', updated_at = NOW()
            WHERE user_id = %s AND status = 'active'
        """, (str(user_id),))

        # Inserisci override
        cur.execute("""
            INSERT INTO subscriptions (
                id, user_id, plan, status, override_by_admin, updated_at
            ) VALUES (%s, %s, %s, 'custom', %s, NOW())
        """, (str(uuid.uuid4()), str(user_id), plan_enum.value, admin_email))
        self._update_users_plan(user_id, plan_enum.value)

        return {"success": True, "user_id": user_id, "plan": plan_enum.value,
                "override_by": admin_email, "note": note}

    # ------------------------------------------------------------------
    # USAGE STATS (admin)
    # ------------------------------------------------------------------
    # USAGE HISTORY & ALERTS
    # ------------------------------------------------------------------

    def get_usage_history(self, user_id: str, months: int = 6) -> List[Dict]:
        """Storico usage degli ultimi N mesi per un utente."""
        cur = self.conn.cursor(cursor_factory=self.RealDictCursor)
        cur.execute("""
            SELECT month, transforms_count, api_calls_count,
                   bytes_processed, codegen_count
            FROM usage_counters
            WHERE user_id = %s
              AND month >= to_char(NOW() - INTERVAL '6 months', 'YYYY-MM')
            ORDER BY month DESC
        """, (str(user_id),))
        return [dict(r) for r in cur.fetchall()]

    def get_all_users_usage(self, month: str = None) -> List[Dict]:
        """Usage aggregato tutti gli utenti — admin dashboard."""
        target_month = month or datetime.now(timezone.utc).strftime('%Y-%m')
        cur = self.conn.cursor(cursor_factory=self.RealDictCursor)
        cur.execute("""
            SELECT
                uc.user_id,
                u.email,
                u.name,
                COALESCE(s.plan, 'FREE') as plan,
                uc.month,
                uc.transforms_count,
                uc.api_calls_count,
                uc.bytes_processed,
                uc.codegen_count
            FROM usage_counters uc
            LEFT JOIN users u ON u.id::text = uc.user_id
            LEFT JOIN LATERAL (
                SELECT plan FROM subscriptions
                WHERE user_id = uc.user_id
                ORDER BY created_at DESC LIMIT 1
            ) s ON true
            WHERE uc.month = %s
            ORDER BY uc.transforms_count DESC
        """, (target_month,))
        return [dict(r) for r in cur.fetchall()]

    def check_usage_alerts(self, user_id: str) -> List[Dict]:
        """
        Controlla se l'utente si avvicina ai limiti del piano.
        Ritorna lista alert: [{feature, used, limit, pct, level}]
        level: 'warning' (80%) | 'critical' (95%) | 'exceeded' (100%)
        """
        plan_data = self.get_user_plan(user_id)
        limits = plan_data["limits"]
        usage = plan_data["usage"]
        alerts = []
        max_t = limits.get("transforms_per_month", -1)
        if max_t > 0:
            used = usage.get("transforms_count", 0)
            pct = (used / max_t) * 100
            level = None
            if pct >= 100:   level = "exceeded"
            elif pct >= 95:  level = "critical"
            elif pct >= 80:  level = "warning"
            if level:
                alerts.append({"feature": "transforms", "used": used,
                                "limit": max_t, "pct": round(pct, 1), "level": level})
        return alerts

    def reset_monthly_usage_cleanup(self) -> int:
        """
        Cancella counters più vecchi di 13 mesi (pulizia periodica).
        Il reset implicito avviene automaticamente: ogni mese crea un nuovo
        record (user_id, YYYY-MM) — i vecchi rimangono per storico.
        """
        cur = self.conn.cursor()
        cur.execute("""
            DELETE FROM usage_counters
            WHERE month < to_char(NOW() - INTERVAL '13 months', 'YYYY-MM')
        """)
        deleted = cur.rowcount
        return deleted

    # ------------------------------------------------------------------

    def get_revenue_stats(self) -> Dict:
        """Statistiche subscription per dashboard admin."""
        cur = self.conn.cursor(cursor_factory=self.RealDictCursor)
        cur.execute("""
            SELECT
                plan,
                COUNT(*) as count,
                COUNT(*) FILTER (WHERE status = 'active') as active,
                COUNT(*) FILTER (WHERE status = 'past_due') as past_due,
                COUNT(*) FILTER (WHERE status = 'canceled') as canceled,
                COUNT(*) FILTER (WHERE override_by_admin IS NOT NULL) as custom_overrides
            FROM subscriptions
            GROUP BY plan
            ORDER BY plan
        """)
        by_plan = [dict(r) for r in cur.fetchall()]

        # MRR (Monthly Recurring Revenue) stimato
        mrr = 0
        for row in by_plan:
            plan = row['plan']
            active = row['active']
            price = PLAN_PRICES_EUR.get(Plan(plan) if plan in Plan._value2member_map_ else Plan.FREE, {}).get('monthly', 0)
            mrr += active * price

        cur.execute("""
            SELECT
                month,
                SUM(transforms_count) as total_transforms,
                SUM(bytes_processed) as total_bytes,
                COUNT(DISTINCT user_id) as active_users
            FROM usage_counters
            WHERE month >= to_char(NOW() - INTERVAL '6 months', 'YYYY-MM')
            GROUP BY month ORDER BY month
        """)
        monthly_usage = [dict(r) for r in cur.fetchall()]

        return {
            "mrr_eur": mrr,
            "by_plan": by_plan,
            "monthly_usage": monthly_usage,
        }
