#!/usr/bin/env python3
"""
Buddyliko — Cost Service (Phase 2: Cost Engine)
Gestione costi, ricavi, margini per ogni operazione.

Responsabilità:
  - Creazione tabelle: transformation_costs, plan_pricing, usage_aggregates
  - Calcolo costo/ricavo per singola operazione (5 livelli di pricing)
  - Registrazione costi nel DB
  - Aggregazione automatica (daily/monthly)
  - Query per report
  - Seed dati plan_pricing
"""

import json
import uuid
import time
import threading
from datetime import datetime, timezone, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional, Dict, List, Any, Tuple
from dataclasses import dataclass, asdict

# ===========================================================================
# CONSTANTS
# ===========================================================================

DEFAULT_USD_EUR_RATE = Decimal("0.92")
COMPUTE_COST_PER_MB = Decimal("0.0005")
COMPUTE_COST_PER_SEC = Decimal("0.0002")
SANDBOX_DAILY_LIMIT = 100

VALID_OPERATIONS = (
    'transform', 'validate', 'ai_mapping', 'ai_codegen',
    'batch_transform', 'ai_validate', 'ai_debug'
)

# ===========================================================================
# DATA CLASSES
# ===========================================================================

@dataclass
class CostBreakdown:
    ai_cost_usd: Decimal = Decimal("0")
    ai_cost_eur: Decimal = Decimal("0")
    compute_cost_eur: Decimal = Decimal("0")
    platform_cost_eur: Decimal = Decimal("0")
    billable_amount_eur: Decimal = Decimal("0")
    margin_eur: Decimal = Decimal("0")
    within_inclusion: bool = False
    is_sandbox: bool = False
    pricing_note: str = ""
    def to_dict(self):
        return {k: str(v) if isinstance(v, Decimal) else v for k, v in asdict(self).items()}


@dataclass
class OperationRecord:
    org_id: str
    auth_type: str              # 'user' | 'api_token'
    auth_id: str
    auth_name: str = ""
    environment: str = "live"
    partner_id: Optional[str] = None
    tags: dict = None
    job_id: Optional[str] = None
    operation: str = "transform"
    input_format: str = ""
    output_format: str = ""
    input_bytes: int = 0
    output_bytes: int = 0
    records_count: int = 0
    ai_provider: str = ""
    ai_model: str = ""
    ai_input_tokens: int = 0
    ai_output_tokens: int = 0
    ai_cost_usd: float = 0.0
    started_at: datetime = None
    completed_at: datetime = None
    duration_ms: int = 0
    status: str = "completed"
    error_message: str = ""


# ===========================================================================
# PLAN PRICING SEED DATA
# ===========================================================================

PLAN_PRICING_SEED = [
    {
        "plan": "FREE", "monthly_fee_eur": 0, "yearly_fee_eur": 0,
        "included_transforms": 50, "included_ai_calls": 10, "included_storage_mb": 100,
        "included_users": 1, "included_api_tokens": 0, "included_partners": 0, "included_groups": 1,
        "per_transform_eur": 0, "per_ai_call_eur": 0, "per_gb_storage_eur": 0, "per_extra_user_eur": 0,
        "ai_markup_pct": 30.00,
        "max_transforms_month": 50, "max_ai_calls_month": 10, "max_storage_mb": 100,
        "max_users": 1, "max_api_tokens": 0, "max_partners": 0, "max_groups": 1,
        "max_file_size_mb": 10, "max_sub_orgs": 0, "max_depth": 0,
        "features": json.dumps({"visual_mapper":True,"ai_assist":True,"batch_transform":False,"api_access":False,"sftp_monitor":False,"webhooks":False,"sandbox":False,"db_connector":False,"scheduling":False,"sso":False,"custom_branding":False,"mapping_marketplace":False,"mapping_templates_included":0,"priority_support":False,"admin_visibility":False}),
    },
    {
        "plan": "PRO", "monthly_fee_eur": 49, "yearly_fee_eur": 490,
        "included_transforms": 2000, "included_ai_calls": 200, "included_storage_mb": 5120,
        "included_users": 5, "included_api_tokens": 3, "included_partners": 10, "included_groups": 5,
        "per_transform_eur": 0.02, "per_ai_call_eur": 0.05, "per_gb_storage_eur": 2.00, "per_extra_user_eur": 8.00,
        "ai_markup_pct": 30.00,
        "max_transforms_month": 10000, "max_ai_calls_month": 1000, "max_storage_mb": 20480,
        "max_users": 20, "max_api_tokens": 10, "max_partners": 50, "max_groups": 20,
        "max_file_size_mb": 50, "max_sub_orgs": 0, "max_depth": 0,
        "features": json.dumps({"visual_mapper":True,"ai_assist":True,"batch_transform":True,"api_access":True,"sftp_monitor":False,"webhooks":False,"sandbox":False,"db_connector":True,"scheduling":False,"sso":False,"custom_branding":False,"mapping_marketplace":True,"mapping_templates_included":999,"priority_support":False,"admin_visibility":True}),
    },
    {
        "plan": "ENTERPRISE", "monthly_fee_eur": 299, "yearly_fee_eur": 2990,
        "included_transforms": 20000, "included_ai_calls": 2000, "included_storage_mb": 51200,
        "included_users": 999, "included_api_tokens": 50, "included_partners": 9999, "included_groups": 9999,
        "per_transform_eur": 0.015, "per_ai_call_eur": 0.03, "per_gb_storage_eur": 1.00, "per_extra_user_eur": 5.00,
        "ai_markup_pct": 20.00,
        "max_transforms_month": 0, "max_ai_calls_month": 0, "max_storage_mb": 0,
        "max_users": 0, "max_api_tokens": 200, "max_partners": 0, "max_groups": 0,
        "max_file_size_mb": 200, "max_sub_orgs": 0, "max_depth": 0,
        "features": json.dumps({"visual_mapper":True,"ai_assist":True,"batch_transform":True,"api_access":True,"sftp_monitor":True,"webhooks":True,"sandbox":True,"db_connector":True,"scheduling":True,"sso":True,"custom_branding":True,"mapping_marketplace":True,"mapping_templates_included":999,"priority_support":True,"admin_visibility":True}),
    },
    {
        "plan": "PARTNER", "monthly_fee_eur": 0, "yearly_fee_eur": 0,
        "included_transforms": 0, "included_ai_calls": 0, "included_storage_mb": 10240,
        "included_users": 999, "included_api_tokens": 100, "included_partners": 9999, "included_groups": 9999,
        "per_transform_eur": 0, "per_ai_call_eur": 0, "per_gb_storage_eur": 0, "per_extra_user_eur": 0,
        "ai_markup_pct": 0,
        "max_transforms_month": 0, "max_ai_calls_month": 0, "max_storage_mb": 0,
        "max_users": 0, "max_api_tokens": 500, "max_partners": 0, "max_groups": 0,
        "max_file_size_mb": 200, "max_sub_orgs": 0, "max_depth": 5,
        "features": json.dumps({"visual_mapper":True,"ai_assist":True,"batch_transform":True,"api_access":True,"sftp_monitor":True,"webhooks":True,"sandbox":True,"db_connector":True,"scheduling":True,"sso":True,"custom_branding":True,"mapping_marketplace":True,"mapping_templates_included":999,"priority_support":True,"admin_visibility":True}),
    },
]


# ===========================================================================
# COST SERVICE CLASS
# ===========================================================================

class CostService:

    def __init__(self, conn, cursor_factory):
        self.conn = conn
        self.RDC = cursor_factory
        self._usd_eur_rate = DEFAULT_USD_EUR_RATE
        self._plan_cache: Dict[str, dict] = {}
        self._cache_lock = threading.Lock()
        self._init_tables()
        self._seed_pricing()
        self._load_plan_cache()

    # ── TABLE INIT ──

    def _init_tables(self):
        cur = self.conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS transformation_costs (
                id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                org_id          UUID NOT NULL REFERENCES organizations(id),
                auth_type       VARCHAR(20) NOT NULL,
                auth_id         VARCHAR(255) NOT NULL,
                auth_name       VARCHAR(255),
                environment     VARCHAR(10) NOT NULL DEFAULT 'live',
                partner_id      UUID,
                tags            JSONB DEFAULT '{}',
                job_id          VARCHAR(36),
                operation       VARCHAR(50) NOT NULL,
                input_format    VARCHAR(30),
                output_format   VARCHAR(30),
                input_bytes     BIGINT DEFAULT 0,
                output_bytes    BIGINT DEFAULT 0,
                records_count   INTEGER DEFAULT 0,
                ai_provider     VARCHAR(30),
                ai_model        VARCHAR(80),
                ai_input_tokens  INTEGER DEFAULT 0,
                ai_output_tokens INTEGER DEFAULT 0,
                ai_cost_usd     NUMERIC(10,6) DEFAULT 0,
                platform_cost_eur NUMERIC(10,4) DEFAULT 0,
                billable_amount_eur NUMERIC(10,4) DEFAULT 0,
                margin_eur      NUMERIC(10,4) DEFAULT 0,
                started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                completed_at    TIMESTAMPTZ,
                duration_ms     INTEGER,
                status          VARCHAR(20) DEFAULT 'completed',
                error_message   TEXT,
                billing_month   VARCHAR(7) NOT NULL,
                invoiced        BOOLEAN DEFAULT FALSE,
                invoice_id      VARCHAR(36),
                created_at      TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        for idx in [
            "CREATE INDEX IF NOT EXISTS idx_tc_org_month ON transformation_costs(org_id, billing_month)",
            "CREATE INDEX IF NOT EXISTS idx_tc_auth ON transformation_costs(auth_type, auth_id)",
            "CREATE INDEX IF NOT EXISTS idx_tc_partner ON transformation_costs(partner_id) WHERE partner_id IS NOT NULL",
            "CREATE INDEX IF NOT EXISTS idx_tc_env ON transformation_costs(environment)",
            "CREATE INDEX IF NOT EXISTS idx_tc_tags ON transformation_costs USING gin(tags)",
            "CREATE INDEX IF NOT EXISTS idx_tc_operation ON transformation_costs(operation, billing_month)",
            "CREATE INDEX IF NOT EXISTS idx_tc_invoiced ON transformation_costs(invoiced, billing_month) WHERE NOT invoiced",
            "CREATE INDEX IF NOT EXISTS idx_tc_org_env_month ON transformation_costs(org_id, environment, billing_month)",
        ]:
            cur.execute(idx)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS plan_pricing (
                id              SERIAL PRIMARY KEY,
                plan            VARCHAR(50) NOT NULL,
                monthly_fee_eur      NUMERIC(10,2) NOT NULL DEFAULT 0,
                yearly_fee_eur       NUMERIC(10,2) NOT NULL DEFAULT 0,
                included_transforms  INTEGER NOT NULL DEFAULT 0,
                included_ai_calls    INTEGER NOT NULL DEFAULT 0,
                included_storage_mb  INTEGER NOT NULL DEFAULT 0,
                included_users       INTEGER NOT NULL DEFAULT 1,
                included_api_tokens  INTEGER NOT NULL DEFAULT 0,
                included_partners    INTEGER NOT NULL DEFAULT 0,
                included_groups      INTEGER NOT NULL DEFAULT 1,
                per_transform_eur    NUMERIC(8,4) NOT NULL DEFAULT 0,
                per_ai_call_eur      NUMERIC(8,4) NOT NULL DEFAULT 0,
                per_gb_storage_eur   NUMERIC(8,4) NOT NULL DEFAULT 0,
                per_extra_user_eur   NUMERIC(8,4) NOT NULL DEFAULT 0,
                ai_markup_pct        NUMERIC(5,2) NOT NULL DEFAULT 30.00,
                max_transforms_month INTEGER NOT NULL DEFAULT 0,
                max_ai_calls_month   INTEGER NOT NULL DEFAULT 0,
                max_storage_mb       INTEGER NOT NULL DEFAULT 0,
                max_users            INTEGER NOT NULL DEFAULT 0,
                max_api_tokens       INTEGER NOT NULL DEFAULT 0,
                max_partners         INTEGER NOT NULL DEFAULT 0,
                max_groups           INTEGER NOT NULL DEFAULT 0,
                max_file_size_mb     INTEGER NOT NULL DEFAULT 50,
                max_sub_orgs         INTEGER NOT NULL DEFAULT 0,
                max_depth            INTEGER NOT NULL DEFAULT 0,
                features             JSONB NOT NULL DEFAULT '{}',
                effective_from       DATE NOT NULL DEFAULT CURRENT_DATE,
                effective_to         DATE,
                active               BOOLEAN DEFAULT TRUE,
                created_at           TIMESTAMPTZ DEFAULT NOW()
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS usage_aggregates (
                id              SERIAL PRIMARY KEY,
                org_id          UUID NOT NULL REFERENCES organizations(id),
                period_type     VARCHAR(10) NOT NULL,
                period_key      VARCHAR(10) NOT NULL,
                auth_type       VARCHAR(20) NOT NULL DEFAULT 'all',
                environment     VARCHAR(10) NOT NULL DEFAULT 'live',
                transforms_count     INTEGER DEFAULT 0,
                validations_count    INTEGER DEFAULT 0,
                ai_calls_count       INTEGER DEFAULT 0,
                ai_codegen_count     INTEGER DEFAULT 0,
                input_bytes_total    BIGINT DEFAULT 0,
                output_bytes_total   BIGINT DEFAULT 0,
                records_total        BIGINT DEFAULT 0,
                ai_cost_usd_total    NUMERIC(12,4) DEFAULT 0,
                platform_cost_eur    NUMERIC(12,4) DEFAULT 0,
                billable_eur_total   NUMERIC(12,4) DEFAULT 0,
                margin_eur_total     NUMERIC(12,4) DEFAULT 0,
                avg_duration_ms      INTEGER DEFAULT 0,
                error_count          INTEGER DEFAULT 0,
                unique_users         INTEGER DEFAULT 0,
                unique_tokens        INTEGER DEFAULT 0,
                unique_partners      INTEGER DEFAULT 0,
                updated_at           TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE(org_id, period_type, period_key, auth_type, environment)
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_ua_lookup ON usage_aggregates(org_id, period_type, period_key)")
        self.conn.commit()
        print("   ✅ Cost Engine tables OK (transformation_costs, plan_pricing, usage_aggregates)")

    # ── SEED ──

    def _seed_pricing(self):
        cur = self.conn.cursor()
        cur.execute("SELECT COUNT(*) FROM plan_pricing WHERE active = TRUE")
        if cur.fetchone()[0] > 0:
            return
        for p in PLAN_PRICING_SEED:
            cols = ', '.join(p.keys())
            phs = ', '.join(['%s'] * len(p))
            cur.execute(f"INSERT INTO plan_pricing ({cols}) VALUES ({phs})", list(p.values()))
        self.conn.commit()
        print(f"   ✅ plan_pricing seeded: {len(PLAN_PRICING_SEED)} piani")

    # ── PLAN CACHE ──

    def _load_plan_cache(self):
        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute("SELECT * FROM plan_pricing WHERE active = TRUE")
        with self._cache_lock:
            self._plan_cache = {r['plan']: dict(r) for r in cur.fetchall()}
        print(f"   ✅ Plan cache: {list(self._plan_cache.keys())}")

    def get_plan_pricing(self, plan):
        with self._cache_lock:
            return self._plan_cache.get(plan.upper() if plan else 'FREE')

    def refresh_plan_cache(self):
        self._load_plan_cache()

    # ── EXCHANGE RATE ──

    def set_usd_eur_rate(self, rate):
        self._usd_eur_rate = Decimal(str(rate))

    def get_usd_eur_rate(self):
        return self._usd_eur_rate

    # ── COST CALCULATION ──

    def calculate_cost(self, org_id, org_plan, op, custom_pricing=None):
        result = CostBreakdown()
        if op.environment == 'sandbox':
            result.is_sandbox = True
            result.pricing_note = "Sandbox: nessun costo"
            return result

        plan = self.get_plan_pricing(org_plan) or self.get_plan_pricing('FREE') or {}

        # Platform cost
        ai_usd = Decimal(str(op.ai_cost_usd or 0))
        if ai_usd > 0:
            result.ai_cost_usd = ai_usd
            result.ai_cost_eur = (ai_usd * self._usd_eur_rate).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
        input_mb = Decimal(str(op.input_bytes or 0)) / Decimal("1048576")
        dur_sec = Decimal(str(op.duration_ms or 0)) / Decimal("1000")
        result.compute_cost_eur = (input_mb * COMPUTE_COST_PER_MB + dur_sec * COMPUTE_COST_PER_SEC).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
        result.platform_cost_eur = result.ai_cost_eur + result.compute_cost_eur

        # Billable
        ot = op.operation
        if custom_pricing and ot in custom_pricing:
            c = custom_pricing[ot]
            result.billable_amount_eur = Decimal(str(c.get('per_unit_eur', 0) if isinstance(c, dict) else c))
            result.pricing_note = f"Custom: €{result.billable_amount_eur}/op"
        else:
            mu = self._get_month_usage_quick(org_id)
            is_t = ot in ('transform', 'validate', 'batch_transform')
            is_ai = ot in ('ai_mapping', 'ai_codegen', 'ai_validate', 'ai_debug')
            inc_t = plan.get('included_transforms', 0)
            inc_ai = plan.get('included_ai_calls', 0)
            cur_t = (mu.get('transforms_count', 0) or 0) + (mu.get('validations_count', 0) or 0)
            cur_ai = mu.get('ai_calls_count', 0) or 0

            if is_t and inc_t > 0 and cur_t < inc_t:
                result.within_inclusion = True
                result.pricing_note = f"Incluso ({cur_t+1}/{inc_t})"
            elif is_ai and inc_ai > 0 and cur_ai < inc_ai:
                result.within_inclusion = True
                result.pricing_note = f"AI inclusa ({cur_ai+1}/{inc_ai})"
            elif is_t:
                pu = Decimal(str(plan.get('per_transform_eur', 0)))
                result.billable_amount_eur = pu
                result.pricing_note = f"Extra: €{pu}/transform"
            elif is_ai:
                pai = Decimal(str(plan.get('per_ai_call_eur', 0)))
                mpct = Decimal(str(plan.get('ai_markup_pct', 30)))
                mk = result.ai_cost_eur * (mpct / Decimal("100"))
                result.billable_amount_eur = (result.ai_cost_eur + mk + pai).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
                result.pricing_note = f"Extra AI: costo×{1+float(mpct)/100:.2f}+€{pai}"

        result.margin_eur = (result.billable_amount_eur - result.platform_cost_eur).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
        return result

    # ── RECORD ──

    def record_cost(self, op, cost):
        try:
            now = datetime.now(timezone.utc)
            cid = str(uuid.uuid4())
            cur = self.conn.cursor()
            cur.execute("""
                INSERT INTO transformation_costs (
                    id, org_id, auth_type, auth_id, auth_name, environment, partner_id, tags, job_id,
                    operation, input_format, output_format, input_bytes, output_bytes, records_count,
                    ai_provider, ai_model, ai_input_tokens, ai_output_tokens,
                    ai_cost_usd, platform_cost_eur, billable_amount_eur, margin_eur,
                    started_at, completed_at, duration_ms, status, error_message, billing_month
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                cid, op.org_id, op.auth_type, op.auth_id, op.auth_name,
                op.environment, op.partner_id, json.dumps(op.tags or {}), op.job_id,
                op.operation, op.input_format, op.output_format,
                op.input_bytes, op.output_bytes, op.records_count,
                op.ai_provider, op.ai_model, op.ai_input_tokens, op.ai_output_tokens,
                str(cost.ai_cost_usd), str(cost.platform_cost_eur),
                str(cost.billable_amount_eur), str(cost.margin_eur),
                op.started_at or now, op.completed_at or now, op.duration_ms,
                op.status, op.error_message, now.strftime('%Y-%m')
            ))
            self.conn.commit()
            return cid
        except Exception as e:
            try: self.conn.rollback()
            except: pass
            print(f"   ❌ record_cost: {e}")
            return None

    # ── QUICK USAGE ──

    def _get_month_usage_quick(self, org_id):
        try:
            m = datetime.now(timezone.utc).strftime('%Y-%m')
            cur = self.conn.cursor(cursor_factory=self.RDC)
            cur.execute("""
                SELECT
                    COALESCE(SUM(CASE WHEN operation IN ('transform','batch_transform') THEN 1 ELSE 0 END),0) as transforms_count,
                    COALESCE(SUM(CASE WHEN operation='validate' THEN 1 ELSE 0 END),0) as validations_count,
                    COALESCE(SUM(CASE WHEN operation IN ('ai_mapping','ai_codegen','ai_validate','ai_debug') THEN 1 ELSE 0 END),0) as ai_calls_count
                FROM transformation_costs WHERE org_id=%s AND billing_month=%s AND environment='live'
            """, (org_id, m))
            r = cur.fetchone()
            return dict(r) if r else {}
        except: return {}

    # ── SANDBOX LIMIT ──

    def check_sandbox_limit(self, org_id):
        try:
            today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
            cur = self.conn.cursor()
            cur.execute("SELECT COUNT(*) FROM transformation_costs WHERE org_id=%s AND environment='sandbox' AND started_at::date=%s::date", (org_id, today))
            c = cur.fetchone()[0]
            return c < SANDBOX_DAILY_LIMIT, max(0, SANDBOX_DAILY_LIMIT - c)
        except: return True, SANDBOX_DAILY_LIMIT

    # ── HARD LIMIT ──

    def check_hard_limit(self, org_id, org_plan, operation_type):
        plan = self.get_plan_pricing(org_plan)
        if not plan: return True, ""
        u = self._get_month_usage_quick(org_id)
        is_t = operation_type in ('transform', 'validate', 'batch_transform')
        is_ai = operation_type in ('ai_mapping', 'ai_codegen', 'ai_validate', 'ai_debug')
        if is_t:
            mx = plan.get('max_transforms_month', 0)
            if mx > 0:
                cur = (u.get('transforms_count',0) or 0) + (u.get('validations_count',0) or 0)
                if cur >= mx: return False, f"Limite mensile: {cur}/{mx} trasformazioni"
        elif is_ai:
            mx = plan.get('max_ai_calls_month', 0)
            if mx > 0:
                cur = u.get('ai_calls_count',0) or 0
                if cur >= mx: return False, f"Limite mensile: {cur}/{mx} AI calls"
        return True, ""

    # ── AGGREGATION ──

    def aggregate_period(self, org_id, period_type, period_key):
        try:
            cur = self.conn.cursor(cursor_factory=self.RDC)
            df = "started_at::date = %s::date" if period_type == 'daily' else "billing_month = %s"
            for auth_filter in ["auth_type, environment", "'all' as auth_type, environment"]:
                gb = "auth_type, environment" if "auth_type," in auth_filter else "environment"
                cur.execute(f"""
                    SELECT {auth_filter},
                        COALESCE(SUM(CASE WHEN operation IN ('transform','batch_transform') THEN 1 ELSE 0 END),0) as transforms_count,
                        COALESCE(SUM(CASE WHEN operation='validate' THEN 1 ELSE 0 END),0) as validations_count,
                        COALESCE(SUM(CASE WHEN operation IN ('ai_mapping','ai_codegen','ai_validate','ai_debug') THEN 1 ELSE 0 END),0) as ai_calls_count,
                        COALESCE(SUM(CASE WHEN operation='ai_codegen' THEN 1 ELSE 0 END),0) as ai_codegen_count,
                        COALESCE(SUM(input_bytes),0) as input_bytes_total,
                        COALESCE(SUM(output_bytes),0) as output_bytes_total,
                        COALESCE(SUM(records_count),0) as records_total,
                        COALESCE(SUM(ai_cost_usd),0) as ai_cost_usd_total,
                        COALESCE(SUM(platform_cost_eur),0) as platform_cost_eur,
                        COALESCE(SUM(billable_amount_eur),0) as billable_eur_total,
                        COALESCE(SUM(margin_eur),0) as margin_eur_total,
                        COALESCE(AVG(duration_ms),0)::INTEGER as avg_duration_ms,
                        COALESCE(SUM(CASE WHEN status='error' THEN 1 ELSE 0 END),0) as error_count,
                        COUNT(DISTINCT CASE WHEN auth_type='user' THEN auth_id END) as unique_users,
                        COUNT(DISTINCT CASE WHEN auth_type='api_token' THEN auth_id END) as unique_tokens,
                        COUNT(DISTINCT partner_id) as unique_partners
                    FROM transformation_costs WHERE org_id=%s AND {df} GROUP BY {gb}
                """, (org_id, period_key))
                for row in cur.fetchall():
                    d = dict(row)
                    cur2 = self.conn.cursor()
                    cur2.execute("""
                        INSERT INTO usage_aggregates (org_id,period_type,period_key,auth_type,environment,
                            transforms_count,validations_count,ai_calls_count,ai_codegen_count,
                            input_bytes_total,output_bytes_total,records_total,
                            ai_cost_usd_total,platform_cost_eur,billable_eur_total,margin_eur_total,
                            avg_duration_ms,error_count,unique_users,unique_tokens,unique_partners,updated_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                        ON CONFLICT (org_id,period_type,period_key,auth_type,environment) DO UPDATE SET
                            transforms_count=EXCLUDED.transforms_count,validations_count=EXCLUDED.validations_count,
                            ai_calls_count=EXCLUDED.ai_calls_count,ai_codegen_count=EXCLUDED.ai_codegen_count,
                            input_bytes_total=EXCLUDED.input_bytes_total,output_bytes_total=EXCLUDED.output_bytes_total,
                            records_total=EXCLUDED.records_total,ai_cost_usd_total=EXCLUDED.ai_cost_usd_total,
                            platform_cost_eur=EXCLUDED.platform_cost_eur,billable_eur_total=EXCLUDED.billable_eur_total,
                            margin_eur_total=EXCLUDED.margin_eur_total,avg_duration_ms=EXCLUDED.avg_duration_ms,
                            error_count=EXCLUDED.error_count,unique_users=EXCLUDED.unique_users,
                            unique_tokens=EXCLUDED.unique_tokens,unique_partners=EXCLUDED.unique_partners,updated_at=NOW()
                    """, (org_id,period_type,period_key,d.get('auth_type','all'),d.get('environment','live'),
                        d.get('transforms_count',0),d.get('validations_count',0),d.get('ai_calls_count',0),d.get('ai_codegen_count',0),
                        d.get('input_bytes_total',0),d.get('output_bytes_total',0),d.get('records_total',0),
                        str(d.get('ai_cost_usd_total',0)),str(d.get('platform_cost_eur',0)),
                        str(d.get('billable_eur_total',0)),str(d.get('margin_eur_total',0)),
                        d.get('avg_duration_ms',0),d.get('error_count',0),
                        d.get('unique_users',0),d.get('unique_tokens',0),d.get('unique_partners',0)))
            self.conn.commit()
        except Exception as e:
            try: self.conn.rollback()
            except: pass
            print(f"   ❌ Aggregation: {e}")

    def aggregate_all_orgs_today(self):
        try:
            today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
            month = datetime.now(timezone.utc).strftime('%Y-%m')
            cur = self.conn.cursor()
            cur.execute("SELECT DISTINCT org_id FROM transformation_costs WHERE started_at::date=%s::date", (today,))
            oids = [str(r[0]) for r in cur.fetchall()]
            for o in oids:
                self.aggregate_period(o, 'daily', today)
                self.aggregate_period(o, 'monthly', month)
            return len(oids)
        except: return 0

    # ── QUERY / REPORTS ──

    def get_org_usage_summary(self, org_id, month=None):
        if not month: month = datetime.now(timezone.utc).strftime('%Y-%m')
        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute("SELECT * FROM usage_aggregates WHERE org_id=%s AND period_type='monthly' AND period_key=%s AND auth_type='all' AND environment='live'", (org_id, month))
        agg = cur.fetchone()
        if not agg:
            u = self._get_month_usage_quick(org_id)
            agg = {'transforms_count':u.get('transforms_count',0),'validations_count':u.get('validations_count',0),'ai_calls_count':u.get('ai_calls_count',0),'ai_cost_usd_total':0,'platform_cost_eur':0,'billable_eur_total':0,'margin_eur_total':0,'error_count':0}
        cur.execute("SELECT plan,name,slug FROM organizations WHERE id=%s", (org_id,))
        org = cur.fetchone()
        op = org['plan'] if org else 'FREE'
        pl = self.get_plan_pricing(op) or {}
        inc_t = pl.get('included_transforms',0); inc_ai = pl.get('included_ai_calls',0)
        ut = (agg.get('transforms_count',0) or 0) + (agg.get('validations_count',0) or 0)
        uai = agg.get('ai_calls_count',0) or 0
        return {'org_id':org_id,'org_name':org.get('name','') if org else '','month':month,'plan':op,
            'monthly_fee_eur':str(pl.get('monthly_fee_eur',0)),
            'transforms_used':ut,'transforms_included':inc_t,'transforms_remaining':max(0,inc_t-ut),
            'ai_calls_used':uai,'ai_calls_included':inc_ai,'ai_calls_remaining':max(0,inc_ai-uai),
            'ai_cost_usd':str(agg.get('ai_cost_usd_total',0)),
            'platform_cost_eur':str(agg.get('platform_cost_eur',0)),
            'billable_eur':str(agg.get('billable_eur_total',0)),
            'margin_eur':str(agg.get('margin_eur_total',0)),
            'error_count':agg.get('error_count',0)}

    def get_costs_by_auth_type(self, org_id, month=None):
        if not month: month = datetime.now(timezone.utc).strftime('%Y-%m')
        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute("SELECT * FROM usage_aggregates WHERE org_id=%s AND period_type='monthly' AND period_key=%s AND auth_type!='all' AND environment='live' ORDER BY auth_type", (org_id, month))
        return [dict(r) for r in cur.fetchall()]

    def get_costs_by_partner(self, org_id, month=None):
        if not month: month = datetime.now(timezone.utc).strftime('%Y-%m')
        cur = self.conn.cursor(cursor_factory=self.RDC)
        # trading_partners potrebbe non esistere ancora (Fase 4), try con JOIN altrimenti fallback
        try:
            cur.execute("""SELECT tc.partner_id,tp.name as partner_name,tp.partner_type,COUNT(*) as operations,
                SUM(tc.ai_cost_usd) as ai_cost_usd,SUM(tc.platform_cost_eur) as platform_cost_eur,
                SUM(tc.billable_amount_eur) as billable_eur,SUM(tc.margin_eur) as margin_eur
                FROM transformation_costs tc LEFT JOIN trading_partners tp ON tc.partner_id=tp.id
                WHERE tc.org_id=%s AND tc.billing_month=%s AND tc.environment='live' AND tc.partner_id IS NOT NULL
                GROUP BY tc.partner_id,tp.name,tp.partner_type ORDER BY billable_eur DESC""", (org_id, month))
            return [dict(r) for r in cur.fetchall()]
        except Exception:
            try: self.conn.rollback()
            except: pass
            # Fallback senza trading_partners JOIN
            cur = self.conn.cursor(cursor_factory=self.RDC)
            cur.execute("""SELECT partner_id,partner_id::text as partner_name,'' as partner_type,COUNT(*) as operations,
                SUM(ai_cost_usd) as ai_cost_usd,SUM(platform_cost_eur) as platform_cost_eur,
                SUM(billable_amount_eur) as billable_eur,SUM(margin_eur) as margin_eur
                FROM transformation_costs
                WHERE org_id=%s AND billing_month=%s AND environment='live' AND partner_id IS NOT NULL
                GROUP BY partner_id ORDER BY billable_eur DESC""", (org_id, month))
            return [dict(r) for r in cur.fetchall()]

    def get_costs_by_tag(self, org_id, tag_key, month=None):
        if not month: month = datetime.now(timezone.utc).strftime('%Y-%m')
        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute("""SELECT tags->>%s as tag_value,COUNT(*) as operations,
            SUM(ai_cost_usd) as ai_cost_usd,SUM(platform_cost_eur) as platform_cost_eur,
            SUM(billable_amount_eur) as billable_eur,SUM(margin_eur) as margin_eur
            FROM transformation_costs WHERE org_id=%s AND billing_month=%s AND environment='live' AND tags ? %s
            GROUP BY tags->>%s ORDER BY billable_eur DESC""", (tag_key, org_id, month, tag_key, tag_key))
        return [dict(r) for r in cur.fetchall()]

    def get_ai_cost_detail(self, org_id, month=None):
        if not month: month = datetime.now(timezone.utc).strftime('%Y-%m')
        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute("""SELECT ai_provider,ai_model,COUNT(*) as calls,
            SUM(ai_input_tokens) as input_tokens,SUM(ai_output_tokens) as output_tokens,
            SUM(ai_cost_usd) as cost_usd,SUM(billable_amount_eur) as billable_eur,
            AVG(duration_ms)::INTEGER as avg_duration_ms
            FROM transformation_costs WHERE org_id=%s AND billing_month=%s AND environment='live'
            AND ai_provider IS NOT NULL AND ai_provider!='' GROUP BY ai_provider,ai_model ORDER BY cost_usd DESC""", (org_id, month))
        return [dict(r) for r in cur.fetchall()]

    def get_daily_trend(self, org_id, month=None):
        if not month: month = datetime.now(timezone.utc).strftime('%Y-%m')
        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute("SELECT * FROM usage_aggregates WHERE org_id=%s AND period_type='daily' AND period_key LIKE %s AND auth_type='all' AND environment='live' ORDER BY period_key", (org_id, month+'%'))
        return [dict(r) for r in cur.fetchall()]

    def get_recent_operations(self, org_id, limit=50, environment=None, auth_type=None, operation=None):
        cur = self.conn.cursor(cursor_factory=self.RDC)
        w = ["tc.org_id=%s"]; p = [org_id]
        if environment: w.append("tc.environment=%s"); p.append(environment)
        if auth_type: w.append("tc.auth_type=%s"); p.append(auth_type)
        if operation: w.append("tc.operation=%s"); p.append(operation)
        p.append(min(limit,200))
        cur.execute(f"""SELECT tc.id,tc.auth_type,tc.auth_id,tc.auth_name,tc.environment,tc.operation,
            tc.input_format,tc.output_format,tc.input_bytes,tc.output_bytes,
            tc.ai_provider,tc.ai_model,tc.ai_cost_usd,tc.platform_cost_eur,tc.billable_amount_eur,tc.margin_eur,
            tc.duration_ms,tc.status,tc.error_message,tc.started_at,tc.billing_month,tc.partner_id,tc.tags
            FROM transformation_costs tc WHERE {' AND '.join(w)} ORDER BY tc.started_at DESC LIMIT %s""", tuple(p))
        return [dict(r) for r in cur.fetchall()]

    def get_platform_profitability(self, month=None):
        if not month: month = datetime.now(timezone.utc).strftime('%Y-%m')
        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute("""SELECT o.id as org_id,o.name as org_name,o.plan,o.org_type,o.parent_org_id,o.revenue_share_pct,
            COALESCE(ua.transforms_count,0)+COALESCE(ua.validations_count,0) as total_ops,
            COALESCE(ua.ai_calls_count,0) as ai_calls,
            COALESCE(ua.ai_cost_usd_total,0) as ai_cost_usd,COALESCE(ua.platform_cost_eur,0) as platform_cost_eur,
            COALESCE(ua.billable_eur_total,0) as billable_eur,COALESCE(ua.margin_eur_total,0) as margin_eur,
            pp.monthly_fee_eur
            FROM organizations o
            LEFT JOIN usage_aggregates ua ON o.id=ua.org_id AND ua.period_type='monthly' AND ua.period_key=%s AND ua.auth_type='all' AND ua.environment='live'
            LEFT JOIN plan_pricing pp ON pp.plan=o.plan AND pp.active=TRUE
            WHERE o.status IN ('active','trial') ORDER BY COALESCE(ua.billable_eur_total,0) DESC""", (month,))
        return [dict(r) for r in cur.fetchall()]

    def list_plans(self):
        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute("SELECT * FROM plan_pricing WHERE active=TRUE ORDER BY monthly_fee_eur")
        return [dict(r) for r in cur.fetchall()]

    def update_plan_pricing(self, plan, updates):
        allowed = {'monthly_fee_eur','yearly_fee_eur','included_transforms','included_ai_calls','included_storage_mb','included_users','included_api_tokens','included_partners','included_groups','per_transform_eur','per_ai_call_eur','per_gb_storage_eur','per_extra_user_eur','ai_markup_pct','max_transforms_month','max_ai_calls_month','max_storage_mb','max_users','max_api_tokens','max_partners','max_groups','max_file_size_mb','max_sub_orgs','max_depth','features'}
        f = {k:v for k,v in updates.items() if k in allowed}
        if not f: return False
        s = ", ".join(f"{k}=%s" for k in f)
        cur = self.conn.cursor()
        cur.execute(f"UPDATE plan_pricing SET {s} WHERE plan=%s AND active=TRUE", list(f.values())+[plan])
        self.conn.commit(); self.refresh_plan_cache()
        return cur.rowcount > 0
