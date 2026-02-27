#!/usr/bin/env python3
"""
Buddyliko — Partnership Service (Phase 6: Partnership & Gerarchia)
Gerarchia ricorsiva 3+ livelli, revenue share a cascata, dashboard
aggregata, sub-org lifecycle, billing aggregato.

Lavora sopra org_service (Phase 0) e cost_service (Phase 2).
"""

import json
import uuid
import re
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional, Dict, List, Any, Tuple

MAX_HIERARCHY_DEPTH = 5

PARTNERSHIP_MODELS = ('reseller', 'referral', 'hybrid')


class PartnershipService:
    """Business logic per partnership, gerarchia, revenue share."""

    def __init__(self, conn, cursor_factory, org_service=None, cost_service=None):
        self.conn = conn
        self.RDC = cursor_factory
        self.org_service = org_service
        self.cost_service = cost_service

    # ──────────────────────────────────────────────────────────────
    # HELPERS
    # ──────────────────────────────────────────────────────────────

    def _ser(self, v):
        if isinstance(v, Decimal): return str(v)
        if isinstance(v, datetime): return v.isoformat()
        if isinstance(v, uuid.UUID): return str(v)
        return v

    def _ser_row(self, row):
        return {k: self._ser(v) for k, v in row.items()} if row else {}

    def _ser_rows(self, rows):
        return [self._ser_row(r) for r in rows]

    def _cur_month(self):
        return datetime.now(timezone.utc).strftime('%Y-%m')

    def _slugify(self, name):
        s = name.lower().strip()
        s = re.sub(r'[^a-z0-9]+', '-', s).strip('-')
        return s[:100] if s else 'org-' + str(uuid.uuid4())[:8]

    # ──────────────────────────────────────────────────────────────
    # 1. PARTNER DASHBOARD (aggregata su tutte le sub-org)
    # ──────────────────────────────────────────────────────────────

    def get_partner_dashboard(self, org_id, month=None):
        """Dashboard partner: KPI aggregati da tutte le sub-org."""
        month = month or self._cur_month()
        cur = self.conn.cursor(cursor_factory=self.RDC)

        # Info org partner
        cur.execute("""
            SELECT id, name, slug, org_type, plan, partnership_model,
                   revenue_share_pct, status, depth
            FROM organizations WHERE id = %s
        """, (org_id,))
        org = cur.fetchone()
        if not org:
            return {'error': 'Org not found'}
        org = dict(org)

        # Tutte le sub-org dirette e indirette
        cur.execute("""
            SELECT o.id, o.name, o.plan, o.org_type, o.status, o.depth,
                   o.parent_org_id, o.revenue_share_pct
            FROM organizations o
            WHERE o.hierarchy_path LIKE %s AND o.id != %s
            ORDER BY o.depth, o.name
        """, (f"%/{org_id}/%", org_id))
        all_descendants = [dict(r) for r in cur.fetchall()]

        # Sub-org dirette (depth = org.depth + 1)
        direct_children = [d for d in all_descendants if str(d.get('parent_org_id', '')) == str(org_id)]

        # Aggregati per ogni sub-org
        sub_org_stats = []
        totals = {
            'sub_orgs': len(all_descendants),
            'direct_children': len(direct_children),
            'active': 0, 'suspended': 0, 'trial': 0,
            'total_ops': 0, 'total_ai': 0,
            'total_billable_eur': Decimal('0'),
            'total_margin_eur': Decimal('0'),
            'total_revenue_share_eur': Decimal('0'),
            'total_fees_eur': Decimal('0'),
        }

        for so in all_descendants:
            # Status count
            st = so.get('status', 'active')
            if st in totals: totals[st] = totals.get(st, 0) + 1

            # Usage aggregates
            agg = self._get_aggregate(str(so['id']), month)
            ops = int(agg.get('transforms_count', 0) or 0) + int(agg.get('validations_count', 0) or 0)
            ai = int(agg.get('ai_calls_count', 0) or 0)
            billable = Decimal(str(agg.get('billable_eur_total', 0) or 0))
            margin = Decimal(str(agg.get('margin_eur_total', 0) or 0))

            # Plan fee
            pp = self.cost_service.get_plan_pricing(so['plan']) if self.cost_service else None
            fee = Decimal(str((pp or {}).get('monthly_fee_eur', 0)))

            # Revenue share
            pct = Decimal(str(so.get('revenue_share_pct') or org.get('revenue_share_pct') or 0))
            rev_share = ((billable + fee) * pct / Decimal('100')).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

            totals['total_ops'] += ops
            totals['total_ai'] += ai
            totals['total_billable_eur'] += billable
            totals['total_margin_eur'] += margin
            totals['total_revenue_share_eur'] += rev_share
            totals['total_fees_eur'] += fee

            # Solo sub-org dirette nel dettaglio
            if str(so.get('parent_org_id', '')) == str(org_id):
                # Conta sotto-figli di questa sub-org
                sub_children = [d for d in all_descendants if str(d.get('parent_org_id', '')) == str(so['id'])]
                sub_org_stats.append({
                    'org_id': str(so['id']),
                    'name': so['name'],
                    'plan': so['plan'],
                    'org_type': so['org_type'],
                    'status': so['status'],
                    'depth': so['depth'],
                    'operations': ops,
                    'ai_calls': ai,
                    'monthly_fee_eur': str(fee),
                    'billable_eur': str(billable),
                    'margin_eur': str(margin),
                    'revenue_share_pct': str(pct),
                    'revenue_share_eur': str(rev_share),
                    'sub_children_count': len(sub_children),
                })

        return {
            'month': month,
            'partner': self._ser_row(org),
            'sub_orgs': sub_org_stats,
            'totals': {k: str(v) if isinstance(v, Decimal) else v for k, v in totals.items()},
        }

    # ──────────────────────────────────────────────────────────────
    # 2. SUB-ORG MANAGEMENT
    # ──────────────────────────────────────────────────────────────

    def create_sub_org(self, parent_org_id, *, name, owner_user_id,
                       slug=None, org_type='company', plan='FREE',
                       partnership_model=None, revenue_share_pct=None,
                       vat_number=None, country=None, billing_email=None,
                       settings=None):
        """Crea una sub-org sotto parent. Verifica limiti gerarchia."""
        cur = self.conn.cursor(cursor_factory=self.RDC)

        # Verifica parent esiste ed è partner
        cur.execute("SELECT * FROM organizations WHERE id = %s", (parent_org_id,))
        parent = cur.fetchone()
        if not parent:
            raise ValueError("Parent org non trovata")
        parent = dict(parent)

        if parent['org_type'] != 'partner' and parent['depth'] == 0:
            raise ValueError("Solo org di tipo 'partner' possono creare sub-org")

        # Verifica depth limit
        new_depth = parent['depth'] + 1
        if new_depth > MAX_HIERARCHY_DEPTH:
            raise ValueError(f"Profondità massima gerarchia: {MAX_HIERARCHY_DEPTH} (attuale: {parent['depth']})")

        # Verifica max_sub_orgs del piano
        if self.cost_service:
            pp = self.cost_service.get_plan_pricing(parent['plan'])
            max_sub = (pp or {}).get('max_sub_orgs', 0)
            if max_sub > 0:
                cur.execute("SELECT COUNT(*) as cnt FROM organizations WHERE parent_org_id = %s AND status != 'cancelled'", (parent_org_id,))
                current = cur.fetchone()['cnt']
                if current >= max_sub:
                    raise ValueError(f"Limite sub-org raggiunto: {current}/{max_sub}")

        # Crea la sub-org
        slug = slug or self._slugify(name)
        org_id = str(uuid.uuid4())
        hierarchy_path = f"{parent['hierarchy_path']}/{org_id}"

        # Inherit revenue share from parent if not specified
        if revenue_share_pct is None and parent.get('revenue_share_pct'):
            revenue_share_pct = float(parent['revenue_share_pct'])

        cur2 = self.conn.cursor()
        cur2.execute("""
            INSERT INTO organizations
                (id, name, slug, org_type, parent_org_id, depth, hierarchy_path,
                 owner_user_id, plan, billing_email,
                 partnership_model, revenue_share_pct, custom_pricing,
                 vat_number, country, settings, status, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, 'active', NOW(), NOW())
        """, (
            org_id, name, slug, org_type, parent_org_id, new_depth, hierarchy_path,
            owner_user_id, plan, billing_email,
            partnership_model, revenue_share_pct, json.dumps({}),
            vat_number, country, json.dumps(settings or {})
        ))

        # Aggiungi owner come membro
        cur2.execute("""
            INSERT INTO org_members (id, org_id, user_id, role, status, joined_at)
            VALUES (%s, %s, %s, 'owner', 'active', NOW())
        """, (str(uuid.uuid4()), org_id, owner_user_id))

        # Aggiorna default_org_id se l'utente non ne ha uno
        cur2.execute("UPDATE users SET default_org_id = %s WHERE id = %s AND default_org_id IS NULL",
                     (org_id, owner_user_id))

        self.conn.commit()

        cur.execute("SELECT * FROM organizations WHERE id = %s", (org_id,))
        return self._ser_row(dict(cur.fetchone()))

    def get_sub_org_detail(self, parent_org_id, sub_org_id, month=None):
        """Dettaglio di una sub-org con stats."""
        month = month or self._cur_month()
        cur = self.conn.cursor(cursor_factory=self.RDC)

        cur.execute("SELECT * FROM organizations WHERE id = %s", (sub_org_id,))
        org = cur.fetchone()
        if not org:
            raise ValueError("Sub-org non trovata")
        org = dict(org)

        # Verifica che sia effettivamente sotto il parent
        if not (org.get('hierarchy_path') and f"/{parent_org_id}/" in org['hierarchy_path']):
            if str(org.get('parent_org_id', '')) != str(parent_org_id):
                raise ValueError("Questa org non è una sub-org del tuo partner")

        # Members
        cur.execute("""
            SELECT om.user_id, om.role, u.email, u.name as user_name
            FROM org_members om JOIN users u ON om.user_id = u.id
            WHERE om.org_id = %s AND om.status = 'active'
            ORDER BY om.role
        """, (sub_org_id,))
        members = [dict(r) for r in cur.fetchall()]

        # Usage
        agg = self._get_aggregate(sub_org_id, month)

        # Plan pricing
        pp = self.cost_service.get_plan_pricing(org['plan']) if self.cost_service else None
        fee = float((pp or {}).get('monthly_fee_eur', 0))

        # Sub-children count
        cur.execute("SELECT COUNT(*) as cnt FROM organizations WHERE parent_org_id = %s AND status != 'cancelled'", (sub_org_id,))
        children_count = cur.fetchone()['cnt']

        # API tokens count
        try:
            cur.execute("SELECT COUNT(*) as cnt FROM api_tokens WHERE org_id = %s AND status = 'active'", (sub_org_id,))
            tokens_count = cur.fetchone()['cnt']
        except:
            tokens_count = 0

        result = self._ser_row(org)
        result.update({
            'members': self._ser_rows(members),
            'member_count': len(members),
            'children_count': children_count,
            'tokens_count': tokens_count,
            'monthly_fee_eur': fee,
            'usage': {k: str(self._ser(v)) for k, v in agg.items()} if agg else {},
        })
        return result

    def update_sub_org(self, parent_org_id, sub_org_id, data):
        """Aggiorna campi di una sub-org. Il parent verifica proprietà."""
        self._verify_parent_child(parent_org_id, sub_org_id)

        allowed = ['name', 'plan', 'revenue_share_pct', 'partnership_model',
                    'vat_number', 'country', 'billing_email', 'settings',
                    'max_users', 'max_api_tokens', 'max_transforms_month',
                    'max_ai_calls_month', 'max_storage_bytes', 'max_partners']
        fields, values = [], []
        for k, v in data.items():
            if k in allowed:
                if isinstance(v, dict): v = json.dumps(v)
                fields.append(f"{k} = %s")
                values.append(v)
        if not fields:
            return False

        fields.append("updated_at = NOW()")
        values.append(sub_org_id)
        cur = self.conn.cursor()
        cur.execute(f"UPDATE organizations SET {', '.join(fields)} WHERE id = %s", values)
        self.conn.commit()
        return cur.rowcount > 0

    def suspend_sub_org(self, parent_org_id, sub_org_id, reason=None):
        """Sospende una sub-org e tutte le sue sotto-org."""
        self._verify_parent_child(parent_org_id, sub_org_id)
        cur = self.conn.cursor()
        # Sospendi la sub-org
        cur.execute("""
            UPDATE organizations SET status='suspended', suspended_at=NOW(),
                   suspended_reason=%s, updated_at=NOW()
            WHERE id=%s AND status IN ('active','trial')
        """, (reason or 'Sospeso dal partner', sub_org_id))
        affected = cur.rowcount

        # Sospendi anche i discendenti
        cur.execute("""
            UPDATE organizations SET status='suspended', suspended_at=NOW(),
                   suspended_reason=%s, updated_at=NOW()
            WHERE hierarchy_path LIKE %s AND id != %s AND status IN ('active','trial')
        """, (reason or 'Parent sospeso', f"%/{sub_org_id}/%", sub_org_id))
        affected += cur.rowcount
        self.conn.commit()
        return affected

    def reactivate_sub_org(self, parent_org_id, sub_org_id):
        """Riattiva una sub-org sospesa."""
        self._verify_parent_child(parent_org_id, sub_org_id)
        cur = self.conn.cursor()
        cur.execute("""
            UPDATE organizations SET status='active', suspended_at=NULL,
                   suspended_reason=NULL, updated_at=NOW()
            WHERE id=%s AND status='suspended'
        """, (sub_org_id,))
        affected = cur.rowcount
        self.conn.commit()
        return affected > 0

    def transfer_sub_org(self, parent_org_id, sub_org_id, new_owner_user_id):
        """Trasferisce ownership di una sub-org a un nuovo utente."""
        self._verify_parent_child(parent_org_id, sub_org_id)
        cur = self.conn.cursor()
        # Rimuovi owner attuale
        cur.execute("UPDATE org_members SET role='admin' WHERE org_id=%s AND role='owner'", (sub_org_id,))
        # Aggiungi/promuovi nuovo owner
        cur.execute("""
            INSERT INTO org_members (id, org_id, user_id, role, status, joined_at)
            VALUES (%s, %s, %s, 'owner', 'active', NOW())
            ON CONFLICT (org_id, user_id) DO UPDATE SET role='owner'
        """, (str(uuid.uuid4()), sub_org_id, new_owner_user_id))
        # Aggiorna owner_user_id nell'org
        cur.execute("UPDATE organizations SET owner_user_id=%s, updated_at=NOW() WHERE id=%s",
                     (new_owner_user_id, sub_org_id))
        self.conn.commit()
        return True

    def _verify_parent_child(self, parent_org_id, sub_org_id):
        """Verifica che sub_org_id sia sotto parent_org_id."""
        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute("SELECT hierarchy_path, parent_org_id FROM organizations WHERE id=%s", (sub_org_id,))
        org = cur.fetchone()
        if not org:
            raise ValueError("Sub-org non trovata")
        hp = org.get('hierarchy_path', '')
        if f"/{parent_org_id}/" not in hp and str(org.get('parent_org_id', '')) != str(parent_org_id):
            raise ValueError("Questa org non è una sub-org del tuo partner")

    # ──────────────────────────────────────────────────────────────
    # 3. REVENUE SHARE A CASCATA
    # ──────────────────────────────────────────────────────────────

    def calculate_cascading_revenue_share(self, org_id, month=None):
        """Calcola revenue share ricorsiva su tutta la gerarchia.
        Ogni livello trattiene la sua % e passa il resto sopra.
        """
        month = month or self._cur_month()
        cur = self.conn.cursor(cursor_factory=self.RDC)

        # L'org corrente
        cur.execute("SELECT * FROM organizations WHERE id=%s", (org_id,))
        root = cur.fetchone()
        if not root:
            return {'error': 'Org not found'}
        root = dict(root)

        # Tutti i discendenti ordinati per depth (foglie prima)
        cur.execute("""
            SELECT o.id, o.name, o.plan, o.depth, o.parent_org_id,
                   o.revenue_share_pct, o.org_type, o.status
            FROM organizations o
            WHERE o.hierarchy_path LIKE %s AND o.id != %s
            ORDER BY o.depth DESC, o.name
        """, (f"%/{org_id}/%", org_id))
        descendants = [dict(r) for r in cur.fetchall()]

        # Calcola il revenue di ogni nodo
        node_data = {}  # org_id -> {revenue, share_to_parent, net}
        for d in descendants:
            did = str(d['id'])
            agg = self._get_aggregate(did, month)
            billable = Decimal(str(agg.get('billable_eur_total', 0) or 0))

            pp = self.cost_service.get_plan_pricing(d['plan']) if self.cost_service else None
            fee = Decimal(str((pp or {}).get('monthly_fee_eur', 0)))

            direct_revenue = billable + fee

            # Aggiungi revenue share ricevuto dai sotto-figli
            received_from_children = Decimal('0')
            for child_id, child_data in node_data.items():
                # Trova parent di child_id tra i discendenti
                child_node = next((x for x in descendants if str(x['id']) == child_id), None)
                if child_node and str(child_node.get('parent_org_id', '')) == did:
                    received_from_children += Decimal(str(child_data.get('share_to_parent', 0)))

            total_revenue = direct_revenue + received_from_children

            # Quanto va al parent
            pct = Decimal(str(d.get('revenue_share_pct') or root.get('revenue_share_pct') or 0))
            share_to_parent = (total_revenue * pct / Decimal('100')).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            net_retained = total_revenue - share_to_parent

            node_data[did] = {
                'org_id': did,
                'name': d['name'],
                'plan': d['plan'],
                'depth': d['depth'],
                'status': d['status'],
                'parent_org_id': str(d['parent_org_id']) if d.get('parent_org_id') else None,
                'direct_revenue_eur': str(direct_revenue),
                'received_from_children_eur': str(received_from_children),
                'total_revenue_eur': str(total_revenue),
                'revenue_share_pct': str(pct),
                'share_to_parent_eur': str(share_to_parent),
                'net_retained_eur': str(net_retained),
            }

        # Revenue share che arriva alla root org
        root_received = Decimal('0')
        for child_id, child_data in node_data.items():
            child_node = next((x for x in descendants if str(x['id']) == child_id), None)
            if child_node and str(child_node.get('parent_org_id', '')) == str(org_id):
                root_received += Decimal(str(child_data.get('share_to_parent', 0)))

        return {
            'month': month,
            'partner': self._ser_row(root),
            'nodes': list(node_data.values()),
            'total_received_eur': str(root_received),
            'hierarchy_depth': max((d['depth'] for d in descendants), default=0),
        }

    # ──────────────────────────────────────────────────────────────
    # 4. BILLING AGGREGATO (modello reseller)
    # ──────────────────────────────────────────────────────────────

    def get_aggregated_billing(self, org_id, month=None):
        """Per il modello reseller: il partner paga per tutte le sub-org."""
        month = month or self._cur_month()
        cur = self.conn.cursor(cursor_factory=self.RDC)

        cur.execute("SELECT * FROM organizations WHERE id=%s", (org_id,))
        org = cur.fetchone()
        if not org:
            return {'error': 'Org not found'}
        org = dict(org)

        # Tutte le sub-org
        cur.execute("""
            SELECT id, name, plan, status, depth, parent_org_id
            FROM organizations
            WHERE hierarchy_path LIKE %s AND id != %s AND status IN ('active','trial')
            ORDER BY depth, name
        """, (f"%/{org_id}/%", org_id))
        sub_orgs = [dict(r) for r in cur.fetchall()]

        items = []
        total_fees = Decimal('0')
        total_usage = Decimal('0')
        total_ai_cost = Decimal('0')

        for so in sub_orgs:
            agg = self._get_aggregate(str(so['id']), month)
            pp = self.cost_service.get_plan_pricing(so['plan']) if self.cost_service else None
            fee = Decimal(str((pp or {}).get('monthly_fee_eur', 0)))
            billable = Decimal(str(agg.get('billable_eur_total', 0) or 0))
            ai_usd = Decimal(str(agg.get('ai_cost_usd_total', 0) or 0))
            ops = int(agg.get('transforms_count', 0) or 0) + int(agg.get('validations_count', 0) or 0)

            total_fees += fee
            total_usage += billable
            total_ai_cost += ai_usd

            items.append({
                'org_id': str(so['id']),
                'name': so['name'],
                'plan': so['plan'],
                'status': so['status'],
                'monthly_fee_eur': str(fee),
                'usage_billable_eur': str(billable),
                'total_eur': str(fee + billable),
                'ai_cost_usd': str(ai_usd),
                'operations': ops,
            })

        grand_total = total_fees + total_usage

        return {
            'month': month,
            'partner': self._ser_row(org),
            'partnership_model': org.get('partnership_model', 'referral'),
            'items': items,
            'totals': {
                'sub_org_count': len(sub_orgs),
                'total_fees_eur': str(total_fees),
                'total_usage_eur': str(total_usage),
                'grand_total_eur': str(grand_total),
                'total_ai_cost_usd': str(total_ai_cost),
            }
        }

    # ──────────────────────────────────────────────────────────────
    # 5. HIERARCHY TREE (con stats)
    # ──────────────────────────────────────────────────────────────

    def get_hierarchy_tree(self, org_id, month=None):
        """Albero completo con stats per ogni nodo."""
        month = month or self._cur_month()
        cur = self.conn.cursor(cursor_factory=self.RDC)

        # Root
        cur.execute("SELECT * FROM organizations WHERE id=%s", (org_id,))
        root = cur.fetchone()
        if not root:
            return {'error': 'Org not found'}
        root = dict(root)

        # Tutti i nodi
        cur.execute("""
            SELECT o.id, o.name, o.slug, o.plan, o.org_type, o.status,
                   o.depth, o.parent_org_id, o.revenue_share_pct,
                   o.partnership_model, o.created_at,
                   (SELECT COUNT(*) FROM org_members om WHERE om.org_id=o.id AND om.status='active') as members
            FROM organizations o
            WHERE (o.hierarchy_path LIKE %s OR o.id = %s)
              AND o.status != 'cancelled'
            ORDER BY o.depth, o.name
        """, (f"%/{org_id}/%", org_id))
        nodes = [dict(r) for r in cur.fetchall()]

        # Arricchisci con usage
        enriched = []
        for n in nodes:
            agg = self._get_aggregate(str(n['id']), month)
            ops = int(agg.get('transforms_count', 0) or 0) + int(agg.get('validations_count', 0) or 0)
            ai = int(agg.get('ai_calls_count', 0) or 0)
            billable = agg.get('billable_eur_total', 0) or 0

            enriched.append({
                **self._ser_row(n),
                'operations': ops,
                'ai_calls': ai,
                'billable_eur': str(billable),
                'is_root': str(n['id']) == str(org_id),
            })

        return {
            'month': month,
            'root': self._ser_row(root),
            'nodes': enriched,
            'total_nodes': len(nodes),
            'max_depth': max((n['depth'] for n in nodes), default=0),
        }

    # ──────────────────────────────────────────────────────────────
    # 6. LIST SUB-ORGS (con paginazione e filtri)
    # ──────────────────────────────────────────────────────────────

    def list_sub_orgs(self, org_id, status=None, plan=None, direct_only=True):
        """Lista sub-org con filtri."""
        cur = self.conn.cursor(cursor_factory=self.RDC)

        if direct_only:
            where = "o.parent_org_id = %s"
            params = [org_id]
        else:
            where = "o.hierarchy_path LIKE %s AND o.id != %s"
            params = [f"%/{org_id}/%", org_id]

        if status:
            where += " AND o.status = %s"
            params.append(status)
        else:
            where += " AND o.status != 'cancelled'"

        if plan:
            where += " AND o.plan = %s"
            params.append(plan)

        cur.execute(f"""
            SELECT o.*,
                (SELECT COUNT(*) FROM org_members om WHERE om.org_id=o.id AND om.status='active') as member_count,
                (SELECT COUNT(*) FROM organizations sub WHERE sub.parent_org_id=o.id AND sub.status!='cancelled') as children_count
            FROM organizations o
            WHERE {where}
            ORDER BY o.depth, o.name
        """, params)
        return self._ser_rows([dict(r) for r in cur.fetchall()])

    # ──────────────────────────────────────────────────────────────
    # INTERNAL HELPERS
    # ──────────────────────────────────────────────────────────────

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
