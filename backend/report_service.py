#!/usr/bin/env python3
"""
Buddyliko — Report Service (Phase 5: Report & Cost Analysis)
Report avanzati, CSV export, confronto mesi, revenue share, top operations.
Lavora sopra il CostService esistente (Phase 2).
"""

import csv
import io
import json
import uuid
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import Optional, Dict, List, Any


class ReportService:
    """Genera report avanzati a partire dal CostService."""

    def __init__(self, conn, cursor_factory, cost_service=None):
        self.conn = conn
        self.RDC = cursor_factory
        self.cost_service = cost_service

    # ──────────────────────────────────────────────────────────────
    # HELPER
    # ──────────────────────────────────────────────────────────────

    def _ser_val(self, v):
        if isinstance(v, Decimal): return str(v)
        if isinstance(v, (datetime,)): return v.isoformat()
        if isinstance(v, uuid.UUID): return str(v)
        return v

    def _ser_rows(self, rows):
        return [{k: self._ser_val(v) for k, v in r.items()} for r in rows]

    def _cur_month(self):
        return datetime.now(timezone.utc).strftime('%Y-%m')

    def _prev_month(self, month_str):
        """Dato '2026-02' ritorna '2026-01'."""
        y, m = int(month_str[:4]), int(month_str[5:7])
        m -= 1
        if m < 1:
            m = 12; y -= 1
        return f"{y:04d}-{m:02d}"

    # ──────────────────────────────────────────────────────────────
    # 1. REPORT PANORAMICA (summary KPI + delta vs mese precedente)
    # ──────────────────────────────────────────────────────────────

    def get_summary_report(self, org_id, month=None):
        month = month or self._cur_month()
        prev = self._prev_month(month)

        cur_data = self._get_aggregate(org_id, month)
        prev_data = self._get_aggregate(org_id, prev)

        def delta(cur, pre, key):
            c = float(cur.get(key, 0) or 0)
            p = float(pre.get(key, 0) or 0)
            return {'current': c, 'previous': p,
                    'delta': round(c - p, 4),
                    'delta_pct': round(((c - p) / p * 100) if p else 0, 1)}

        # Org info
        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute("SELECT name, plan, org_type FROM organizations WHERE id=%s", (org_id,))
        org = cur.fetchone()

        plan_pricing = None
        if self.cost_service:
            plan_pricing = self.cost_service.get_plan_pricing((org or {}).get('plan', 'FREE'))

        monthly_fee = float((plan_pricing or {}).get('monthly_fee_eur', 0))

        return {
            'org_id': str(org_id),
            'org_name': (org or {}).get('name', ''),
            'plan': (org or {}).get('plan', 'FREE'),
            'month': month,
            'prev_month': prev,
            'monthly_fee_eur': monthly_fee,
            'transforms': delta(cur_data, prev_data, 'transforms_count'),
            'validations': delta(cur_data, prev_data, 'validations_count'),
            'ai_calls': delta(cur_data, prev_data, 'ai_calls_count'),
            'ai_cost_usd': delta(cur_data, prev_data, 'ai_cost_usd_total'),
            'platform_cost_eur': delta(cur_data, prev_data, 'platform_cost_eur'),
            'billable_eur': delta(cur_data, prev_data, 'billable_eur_total'),
            'margin_eur': delta(cur_data, prev_data, 'margin_eur_total'),
            'error_count': delta(cur_data, prev_data, 'error_count'),
            'unique_users': int(cur_data.get('unique_users', 0) or 0),
            'unique_tokens': int(cur_data.get('unique_tokens', 0) or 0),
            'unique_partners': int(cur_data.get('unique_partners', 0) or 0),
        }

    def _get_aggregate(self, org_id, month):
        try:
            cur = self.conn.cursor(cursor_factory=self.RDC)
            cur.execute("""
                SELECT * FROM usage_aggregates
                WHERE org_id=%s AND period_type='monthly' AND period_key=%s
                  AND auth_type='all' AND environment='live'
            """, (org_id, month))
            r = cur.fetchone()
            return dict(r) if r else {}
        except:
            return {}

    # ──────────────────────────────────────────────────────────────
    # 2. AUTH TYPE BREAKDOWN (umani vs token)
    # ──────────────────────────────────────────────────────────────

    def get_auth_breakdown(self, org_id, month=None):
        month = month or self._cur_month()
        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute("""
            SELECT auth_type,
                COUNT(*) as operations,
                COALESCE(SUM(CASE WHEN operation IN ('transform','batch_transform') THEN 1 ELSE 0 END),0) as transforms,
                COALESCE(SUM(CASE WHEN operation IN ('ai_mapping','ai_codegen','ai_validate','ai_debug') THEN 1 ELSE 0 END),0) as ai_calls,
                COALESCE(SUM(ai_cost_usd),0) as ai_cost_usd,
                COALESCE(SUM(platform_cost_eur),0) as platform_cost_eur,
                COALESCE(SUM(billable_amount_eur),0) as billable_eur,
                COALESCE(SUM(margin_eur),0) as margin_eur,
                COALESCE(AVG(duration_ms),0)::INTEGER as avg_duration_ms,
                COALESCE(SUM(input_bytes),0) as total_bytes,
                COALESCE(SUM(CASE WHEN status='error' THEN 1 ELSE 0 END),0) as errors,
                COUNT(DISTINCT auth_id) as unique_identities
            FROM transformation_costs
            WHERE org_id=%s AND billing_month=%s AND environment='live'
            GROUP BY auth_type ORDER BY billable_eur DESC
        """, (org_id, month))
        rows = [dict(r) for r in cur.fetchall()]

        total_ops = sum(r.get('operations', 0) for r in rows)
        for r in rows:
            r['pct_operations'] = round(r['operations'] / total_ops * 100, 1) if total_ops else 0

        return {'month': month, 'data': self._ser_rows(rows), 'total_operations': total_ops}

    # ──────────────────────────────────────────────────────────────
    # 3. PARTNER BREAKDOWN
    # ──────────────────────────────────────────────────────────────

    def get_partner_breakdown(self, org_id, month=None):
        month = month or self._cur_month()
        cur = self.conn.cursor(cursor_factory=self.RDC)
        try:
            cur.execute("""
                SELECT tc.partner_id, tp.name as partner_name, tp.partner_type, tp.code as partner_code,
                    COUNT(*) as operations,
                    COALESCE(SUM(CASE WHEN tc.operation IN ('transform','batch_transform') THEN 1 ELSE 0 END),0) as transforms,
                    COALESCE(SUM(CASE WHEN tc.operation IN ('ai_mapping','ai_codegen','ai_validate','ai_debug') THEN 1 ELSE 0 END),0) as ai_calls,
                    COALESCE(SUM(tc.ai_cost_usd),0) as ai_cost_usd,
                    COALESCE(SUM(tc.platform_cost_eur),0) as platform_cost_eur,
                    COALESCE(SUM(tc.billable_amount_eur),0) as billable_eur,
                    COALESCE(SUM(tc.margin_eur),0) as margin_eur,
                    COALESCE(AVG(tc.duration_ms),0)::INTEGER as avg_duration_ms,
                    COALESCE(SUM(CASE WHEN tc.status='error' THEN 1 ELSE 0 END),0) as errors
                FROM transformation_costs tc
                LEFT JOIN trading_partners tp ON tc.partner_id = tp.id
                WHERE tc.org_id=%s AND tc.billing_month=%s AND tc.environment='live'
                  AND tc.partner_id IS NOT NULL
                GROUP BY tc.partner_id, tp.name, tp.partner_type, tp.code
                ORDER BY billable_eur DESC
            """, (org_id, month))
            rows = [dict(r) for r in cur.fetchall()]
        except Exception:
            try: self.conn.rollback()
            except: pass
            rows = []

        # Anche operazioni SENZA partner
        try:
            cur2 = self.conn.cursor(cursor_factory=self.RDC)
            cur2.execute("""
                SELECT COUNT(*) as operations,
                    COALESCE(SUM(billable_amount_eur),0) as billable_eur,
                    COALESCE(SUM(margin_eur),0) as margin_eur
                FROM transformation_costs
                WHERE org_id=%s AND billing_month=%s AND environment='live'
                  AND partner_id IS NULL
            """, (org_id, month))
            no_partner = cur2.fetchone()
            no_partner_data = dict(no_partner) if no_partner else {}
        except:
            no_partner_data = {}

        return {
            'month': month,
            'with_partner': self._ser_rows(rows),
            'without_partner': self._ser_rows([no_partner_data]) if no_partner_data.get('operations', 0) else [],
            'total_partners': len(rows)
        }

    # ──────────────────────────────────────────────────────────────
    # 4. AI COST BREAKDOWN (per provider + modello)
    # ──────────────────────────────────────────────────────────────

    def get_ai_breakdown(self, org_id, month=None):
        month = month or self._cur_month()
        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute("""
            SELECT ai_provider, ai_model, operation,
                COUNT(*) as calls,
                COALESCE(SUM(ai_input_tokens),0) as input_tokens,
                COALESCE(SUM(ai_output_tokens),0) as output_tokens,
                COALESCE(SUM(ai_input_tokens + ai_output_tokens),0) as total_tokens,
                COALESCE(SUM(ai_cost_usd),0) as cost_usd,
                COALESCE(SUM(billable_amount_eur),0) as billable_eur,
                COALESCE(SUM(margin_eur),0) as margin_eur,
                COALESCE(AVG(duration_ms),0)::INTEGER as avg_duration_ms,
                MIN(started_at) as first_call,
                MAX(started_at) as last_call
            FROM transformation_costs
            WHERE org_id=%s AND billing_month=%s AND environment='live'
              AND ai_provider IS NOT NULL AND ai_provider != ''
            GROUP BY ai_provider, ai_model, operation
            ORDER BY cost_usd DESC
        """, (org_id, month))
        detail = [dict(r) for r in cur.fetchall()]

        # Aggregato per provider
        cur.execute("""
            SELECT ai_provider,
                COUNT(*) as calls,
                COALESCE(SUM(ai_input_tokens + ai_output_tokens),0) as total_tokens,
                COALESCE(SUM(ai_cost_usd),0) as cost_usd,
                COALESCE(SUM(billable_amount_eur),0) as billable_eur,
                COALESCE(SUM(margin_eur),0) as margin_eur
            FROM transformation_costs
            WHERE org_id=%s AND billing_month=%s AND environment='live'
              AND ai_provider IS NOT NULL AND ai_provider != ''
            GROUP BY ai_provider ORDER BY cost_usd DESC
        """, (org_id, month))
        by_provider = [dict(r) for r in cur.fetchall()]

        return {
            'month': month,
            'by_provider': self._ser_rows(by_provider),
            'detail': self._ser_rows(detail),
            'total_calls': sum(r['calls'] for r in by_provider),
            'total_cost_usd': str(sum(Decimal(str(r.get('cost_usd', 0))) for r in by_provider)),
            'total_billable_eur': str(sum(Decimal(str(r.get('billable_eur', 0))) for r in by_provider))
        }

    # ──────────────────────────────────────────────────────────────
    # 5. TAG BREAKDOWN (centri di costo)
    # ──────────────────────────────────────────────────────────────

    def get_tag_breakdown(self, org_id, month=None, tag_key=None):
        month = month or self._cur_month()
        cur = self.conn.cursor(cursor_factory=self.RDC)

        if tag_key:
            # Breakdown per un singolo tag
            cur.execute("""
                SELECT tags->>%s as tag_value,
                    COUNT(*) as operations,
                    COALESCE(SUM(ai_cost_usd),0) as ai_cost_usd,
                    COALESCE(SUM(platform_cost_eur),0) as platform_cost_eur,
                    COALESCE(SUM(billable_amount_eur),0) as billable_eur,
                    COALESCE(SUM(margin_eur),0) as margin_eur
                FROM transformation_costs
                WHERE org_id=%s AND billing_month=%s AND environment='live' AND tags ? %s
                GROUP BY tags->>%s ORDER BY billable_eur DESC
            """, (tag_key, org_id, month, tag_key, tag_key))
            rows = [dict(r) for r in cur.fetchall()]
            return {'month': month, 'tag_key': tag_key, 'data': self._ser_rows(rows)}

        # Lista di tutti i tag usati
        cur.execute("""
            SELECT DISTINCT jsonb_object_keys(tags) as tag_key
            FROM transformation_costs
            WHERE org_id=%s AND billing_month=%s AND environment='live'
              AND tags != '{}'::jsonb AND tags IS NOT NULL
        """, (org_id, month))
        tag_keys = [r['tag_key'] for r in cur.fetchall()]

        summaries = []
        for tk in tag_keys:
            cur.execute("""
                SELECT COUNT(DISTINCT tags->>%s) as unique_values,
                    COUNT(*) as operations,
                    COALESCE(SUM(billable_amount_eur),0) as billable_eur
                FROM transformation_costs
                WHERE org_id=%s AND billing_month=%s AND environment='live' AND tags ? %s
            """, (tk, org_id, month, tk))
            r = cur.fetchone()
            summaries.append({'tag_key': tk, **(dict(r) if r else {})})

        return {'month': month, 'available_tags': self._ser_rows(summaries)}

    # ──────────────────────────────────────────────────────────────
    # 6. DAILY TREND (con confronto mese precedente)
    # ──────────────────────────────────────────────────────────────

    def get_trend_report(self, org_id, month=None):
        month = month or self._cur_month()
        prev = self._prev_month(month)
        cur = self.conn.cursor(cursor_factory=self.RDC)

        # Mese corrente
        cur.execute("""
            SELECT period_key, transforms_count, validations_count, ai_calls_count,
                ai_cost_usd_total, platform_cost_eur, billable_eur_total, margin_eur_total,
                error_count, unique_users, unique_tokens
            FROM usage_aggregates
            WHERE org_id=%s AND period_type='daily' AND period_key LIKE %s
              AND auth_type='all' AND environment='live'
            ORDER BY period_key
        """, (org_id, month + '%'))
        current = [dict(r) for r in cur.fetchall()]

        # Mese precedente
        cur.execute("""
            SELECT period_key, transforms_count, validations_count, ai_calls_count,
                billable_eur_total, margin_eur_total, error_count
            FROM usage_aggregates
            WHERE org_id=%s AND period_type='daily' AND period_key LIKE %s
              AND auth_type='all' AND environment='live'
            ORDER BY period_key
        """, (org_id, prev + '%'))
        previous = [dict(r) for r in cur.fetchall()]

        return {
            'month': month, 'prev_month': prev,
            'current': self._ser_rows(current),
            'previous': self._ser_rows(previous)
        }

    # ──────────────────────────────────────────────────────────────
    # 7. OPERATIONS LOG (filtri avanzati)
    # ──────────────────────────────────────────────────────────────

    def get_operations_report(self, org_id, month=None, limit=100,
                              auth_type=None, operation=None, environment=None,
                              partner_id=None, status=None, min_cost=None):
        month = month or self._cur_month()
        where = ["tc.org_id=%s", "tc.billing_month=%s"]
        params = [org_id, month]

        if auth_type:
            where.append("tc.auth_type=%s"); params.append(auth_type)
        if operation:
            where.append("tc.operation=%s"); params.append(operation)
        if environment:
            where.append("tc.environment=%s"); params.append(environment)
        else:
            where.append("tc.environment='live'")
        if partner_id:
            where.append("tc.partner_id=%s"); params.append(partner_id)
        if status:
            where.append("tc.status=%s"); params.append(status)
        if min_cost:
            where.append("tc.billable_amount_eur >= %s"); params.append(min_cost)

        params.append(min(int(limit), 500))

        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute(f"""
            SELECT tc.id, tc.auth_type, tc.auth_id, tc.auth_name, tc.environment,
                tc.operation, tc.input_format, tc.output_format,
                tc.input_bytes, tc.output_bytes, tc.records_count,
                tc.ai_provider, tc.ai_model, tc.ai_input_tokens, tc.ai_output_tokens,
                tc.ai_cost_usd, tc.platform_cost_eur, tc.billable_amount_eur, tc.margin_eur,
                tc.duration_ms, tc.status, tc.error_message, tc.started_at,
                tc.partner_id, tc.tags
            FROM transformation_costs tc
            WHERE {' AND '.join(where)}
            ORDER BY tc.started_at DESC LIMIT %s
        """, tuple(params))
        rows = [dict(r) for r in cur.fetchall()]
        return {'month': month, 'count': len(rows), 'data': self._ser_rows(rows)}

    # ──────────────────────────────────────────────────────────────
    # 8. MONTH COMPARISON
    # ──────────────────────────────────────────────────────────────

    def get_month_comparison(self, org_id, month_a=None, month_b=None):
        month_a = month_a or self._cur_month()
        month_b = month_b or self._prev_month(month_a)

        a = self._get_aggregate(org_id, month_a)
        b = self._get_aggregate(org_id, month_b)

        fields = ['transforms_count', 'validations_count', 'ai_calls_count',
                  'ai_cost_usd_total', 'platform_cost_eur', 'billable_eur_total',
                  'margin_eur_total', 'error_count', 'input_bytes_total',
                  'unique_users', 'unique_tokens', 'unique_partners']

        comparison = {}
        for f in fields:
            va = float(a.get(f, 0) or 0)
            vb = float(b.get(f, 0) or 0)
            comparison[f] = {
                'month_a': va, 'month_b': vb,
                'delta': round(va - vb, 4),
                'delta_pct': round(((va - vb) / vb * 100) if vb else 0, 1)
            }

        return {'month_a': month_a, 'month_b': month_b, 'comparison': comparison}

    # ──────────────────────────────────────────────────────────────
    # 9. REVENUE SHARE (per partner org)
    # ──────────────────────────────────────────────────────────────

    def get_revenue_share_report(self, org_id, month=None):
        month = month or self._cur_month()
        cur = self.conn.cursor(cursor_factory=self.RDC)

        # Verifica se l'org è un partner con sub-org
        cur.execute("""
            SELECT o.id, o.name, o.plan, o.org_type, o.revenue_share_pct, o.parent_org_id
            FROM organizations o WHERE o.id = %s
        """, (org_id,))
        org = cur.fetchone()
        if not org:
            return {'error': 'Org not found'}

        org = dict(org)

        # Sub-org di questa org
        cur.execute("""
            SELECT o.id, o.name, o.plan, o.org_type, o.revenue_share_pct
            FROM organizations o WHERE o.parent_org_id = %s AND o.status IN ('active','trial')
            ORDER BY o.name
        """, (org_id,))
        sub_orgs = [dict(r) for r in cur.fetchall()]

        results = []
        total_revenue = Decimal("0")
        total_share = Decimal("0")

        for so in sub_orgs:
            agg = self._get_aggregate(str(so['id']), month)
            billable = Decimal(str(agg.get('billable_eur_total', 0) or 0))
            pct = Decimal(str(so.get('revenue_share_pct') or org.get('revenue_share_pct') or 0))
            share = (billable * pct / Decimal("100")).quantize(Decimal("0.01"))

            # Piano mensile
            pp = self.cost_service.get_plan_pricing(so['plan']) if self.cost_service else None
            fee = Decimal(str((pp or {}).get('monthly_fee_eur', 0)))

            total_revenue += billable + fee
            total_share += share

            results.append({
                'org_id': str(so['id']),
                'org_name': so['name'],
                'plan': so['plan'],
                'monthly_fee_eur': str(fee),
                'usage_billable_eur': str(billable),
                'total_revenue_eur': str(billable + fee),
                'revenue_share_pct': str(pct),
                'revenue_share_eur': str(share),
                'transforms': int(agg.get('transforms_count', 0) or 0),
                'ai_calls': int(agg.get('ai_calls_count', 0) or 0),
            })

        return {
            'month': month,
            'parent_org': {'id': str(org_id), 'name': org.get('name', ''), 'org_type': org.get('org_type', '')},
            'sub_orgs': results,
            'totals': {
                'total_revenue_eur': str(total_revenue),
                'total_share_eur': str(total_share),
                'net_platform_eur': str(total_revenue - total_share),
                'sub_org_count': len(sub_orgs)
            }
        }

    # ──────────────────────────────────────────────────────────────
    # 10. TOP OPERATIONS (operazioni più costose)
    # ──────────────────────────────────────────────────────────────

    def get_top_operations(self, org_id, month=None, limit=20, sort_by='billable_amount_eur'):
        month = month or self._cur_month()
        allowed = {'billable_amount_eur', 'ai_cost_usd', 'duration_ms', 'input_bytes'}
        sort_col = sort_by if sort_by in allowed else 'billable_amount_eur'
        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute(f"""
            SELECT id, auth_type, auth_name, operation, input_format, output_format,
                ai_provider, ai_model, ai_cost_usd, platform_cost_eur,
                billable_amount_eur, margin_eur, duration_ms, input_bytes,
                started_at, partner_id, tags, status
            FROM transformation_costs
            WHERE org_id=%s AND billing_month=%s AND environment='live'
            ORDER BY {sort_col} DESC NULLS LAST
            LIMIT %s
        """, (org_id, month, min(int(limit), 100)))
        return {'month': month, 'sort_by': sort_col, 'data': self._ser_rows([dict(r) for r in cur.fetchall()])}

    # ──────────────────────────────────────────────────────────────
    # 11. PLATFORM PROFITABILITY (admin only, cross-org)
    # ──────────────────────────────────────────────────────────────

    def get_platform_profitability(self, month=None):
        month = month or self._cur_month()
        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute("""
            SELECT o.id as org_id, o.name, o.plan, o.org_type, o.parent_org_id,
                o.revenue_share_pct, o.status,
                COALESCE(ua.transforms_count,0)+COALESCE(ua.validations_count,0) as total_ops,
                COALESCE(ua.ai_calls_count,0) as ai_calls,
                COALESCE(ua.ai_cost_usd_total,0) as ai_cost_usd,
                COALESCE(ua.platform_cost_eur,0) as platform_cost_eur,
                COALESCE(ua.billable_eur_total,0) as billable_eur,
                COALESCE(ua.margin_eur_total,0) as margin_eur,
                COALESCE(ua.unique_users,0) as unique_users,
                COALESCE(ua.unique_tokens,0) as unique_tokens,
                pp.monthly_fee_eur
            FROM organizations o
            LEFT JOIN usage_aggregates ua ON o.id=ua.org_id
                AND ua.period_type='monthly' AND ua.period_key=%s
                AND ua.auth_type='all' AND ua.environment='live'
            LEFT JOIN plan_pricing pp ON pp.plan=o.plan AND pp.active=TRUE
            WHERE o.status IN ('active','trial')
            ORDER BY COALESCE(ua.billable_eur_total,0) DESC
        """, (month,))
        rows = [dict(r) for r in cur.fetchall()]

        total_fees = sum(float(r.get('monthly_fee_eur', 0) or 0) for r in rows)
        total_billable = sum(float(r.get('billable_eur', 0) or 0) for r in rows)
        total_cost = sum(float(r.get('platform_cost_eur', 0) or 0) for r in rows)
        total_margin = sum(float(r.get('margin_eur', 0) or 0) for r in rows)

        return {
            'month': month,
            'orgs': self._ser_rows(rows),
            'totals': {
                'total_orgs': len(rows),
                'total_fees_eur': round(total_fees, 2),
                'total_billable_eur': round(total_billable, 2),
                'total_platform_cost_eur': round(total_cost, 2),
                'total_margin_eur': round(total_margin, 2),
                'total_revenue_eur': round(total_fees + total_billable, 2),
                'gross_profit_eur': round(total_fees + total_margin, 2),
            }
        }

    # ──────────────────────────────────────────────────────────────
    # 12. CSV EXPORT
    # ──────────────────────────────────────────────────────────────

    def export_csv(self, report_type, org_id, month=None, **kwargs):
        """Genera CSV per qualunque tipo di report.
        Ritorna (csv_string, filename)."""
        month = month or self._cur_month()

        if report_type == 'summary':
            data = self.get_summary_report(org_id, month)
            rows = []
            for key in ['transforms', 'validations', 'ai_calls', 'ai_cost_usd',
                        'platform_cost_eur', 'billable_eur', 'margin_eur', 'error_count']:
                d = data.get(key, {})
                rows.append({'Metric': key, 'Current': d.get('current', ''),
                             'Previous': d.get('previous', ''), 'Delta': d.get('delta', ''),
                             'Delta %': d.get('delta_pct', '')})
            return self._rows_to_csv(rows), f"report_summary_{month}.csv"

        elif report_type == 'auth':
            data = self.get_auth_breakdown(org_id, month)
            return self._rows_to_csv(data.get('data', [])), f"report_auth_type_{month}.csv"

        elif report_type == 'partners':
            data = self.get_partner_breakdown(org_id, month)
            return self._rows_to_csv(data.get('with_partner', [])), f"report_partners_{month}.csv"

        elif report_type == 'ai':
            data = self.get_ai_breakdown(org_id, month)
            return self._rows_to_csv(data.get('detail', [])), f"report_ai_{month}.csv"

        elif report_type == 'tags':
            tag_key = kwargs.get('tag_key', '')
            data = self.get_tag_breakdown(org_id, month, tag_key or None)
            csv_data = data.get('data', data.get('available_tags', []))
            return self._rows_to_csv(csv_data), f"report_tags_{tag_key or 'all'}_{month}.csv"

        elif report_type == 'operations':
            data = self.get_operations_report(org_id, month, limit=kwargs.get('limit', 500),
                auth_type=kwargs.get('auth_type'), operation=kwargs.get('operation'),
                partner_id=kwargs.get('partner_id'), status=kwargs.get('status'))
            return self._rows_to_csv(data.get('data', [])), f"report_operations_{month}.csv"

        elif report_type == 'comparison':
            month_b = kwargs.get('month_b') or self._prev_month(month)
            data = self.get_month_comparison(org_id, month, month_b)
            rows = []
            for k, v in data.get('comparison', {}).items():
                rows.append({'Metric': k, f'{month}': v.get('month_a', ''),
                             f'{month_b}': v.get('month_b', ''), 'Delta': v.get('delta', ''),
                             'Delta %': v.get('delta_pct', '')})
            return self._rows_to_csv(rows), f"report_comparison_{month}_vs_{month_b}.csv"

        elif report_type == 'revenue_share':
            data = self.get_revenue_share_report(org_id, month)
            return self._rows_to_csv(data.get('sub_orgs', [])), f"report_revenue_share_{month}.csv"

        elif report_type == 'profitability':
            data = self.get_platform_profitability(month)
            return self._rows_to_csv(data.get('orgs', [])), f"report_profitability_{month}.csv"

        elif report_type == 'top':
            sort_by = kwargs.get('sort_by', 'billable_amount_eur')
            data = self.get_top_operations(org_id, month, limit=kwargs.get('limit', 100), sort_by=sort_by)
            return self._rows_to_csv(data.get('data', [])), f"report_top_operations_{month}.csv"

        else:
            return "", "unknown.csv"

    def _rows_to_csv(self, rows):
        if not rows:
            return ""
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=rows[0].keys(), extrasaction='ignore')
        writer.writeheader()
        for r in rows:
            writer.writerow(r)
        return output.getvalue()
