#!/usr/bin/env python3
"""
Buddyliko — Organization Service
Fase 0: business logic per organizations e org_members.

Funzionalità:
  - CRUD organizations
  - Gestione membri (add, remove, change role, list)
  - Gerarchia ricorsiva (descendants, ancestors)
  - Org isolation helper
  - Switch org per utente
  - Inviti (crea invito, accetta)
"""

import uuid
import json
import re
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, List, Any, Tuple


# ===========================================================================
# COSTANTI
# ===========================================================================

ORG_TYPES = ('company', 'partner', 'internal')

ORG_ROLES = ('owner', 'admin', 'finance', 'developer', 'operator', 'viewer')

ORG_ROLE_HIERARCHY = {
    'owner': 6,
    'admin': 5,
    'finance': 4,
    'developer': 3,
    'operator': 2,
    'viewer': 1,
}

ORG_STATUSES = ('active', 'trial', 'suspended', 'cancelled', 'pending_setup')

PARTNERSHIP_MODELS = ('reseller', 'referral', 'hybrid')

GROUP_TYPES = ('working', 'discussion', 'project')


def _gen_id() -> str:
    return str(uuid.uuid4())


def _slugify(name: str) -> str:
    """Genera slug URL-safe da nome."""
    s = name.lower().strip()
    s = re.sub(r'[^a-z0-9]+', '-', s)
    s = s.strip('-')
    return s[:100] if s else 'org-' + _gen_id()[:8]


# ===========================================================================
# ORGANIZATION SERVICE
# ===========================================================================

class OrganizationService:
    """
    Servizio completo per la gestione organizations.
    Richiede la connessione psycopg2 e RealDictCursor.
    """

    def __init__(self, conn, RealDictCursor):
        self.conn = conn
        self.RDC = RealDictCursor

    # ── CRUD ORG ──────────────────────────────────────────────────────

    def create_org(self, *,
                   name: str,
                   slug: str = None,
                   org_type: str = 'company',
                   owner_user_id: int,
                   plan: str = 'FREE',
                   parent_org_id: str = None,
                   partnership_model: str = None,
                   revenue_share_pct: float = None,
                   vat_number: str = None,
                   country: str = None,
                   billing_email: str = None,
                   settings: dict = None) -> Dict:
        """
        Crea una nuova organizzazione.
        Se parent_org_id è specificato, la crea come sub-org.
        Aggiunge automaticamente l'owner come membro con ruolo 'owner'.
        """
        if org_type not in ORG_TYPES:
            raise ValueError(f"org_type deve essere uno di {ORG_TYPES}")

        if partnership_model and partnership_model not in PARTNERSHIP_MODELS:
            raise ValueError(f"partnership_model deve essere uno di {PARTNERSHIP_MODELS}")

        slug = slug or _slugify(name)
        org_id = _gen_id()

        # Calcola depth e hierarchy_path
        depth = 0
        hierarchy_path = f"/{org_id}"
        if parent_org_id:
            parent = self.get_org(parent_org_id)
            if not parent:
                raise ValueError("parent_org_id non trovato")
            depth = parent['depth'] + 1
            hierarchy_path = f"{parent['hierarchy_path']}/{org_id}"

        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO organizations
                (id, name, slug, org_type, parent_org_id, depth, hierarchy_path,
                 owner_user_id, plan, billing_email,
                 partnership_model, revenue_share_pct, custom_pricing,
                 vat_number, country, settings, status, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s, %s, NOW(), NOW())
            RETURNING id
        """, (
            org_id, name, slug, org_type, parent_org_id, depth, hierarchy_path,
            owner_user_id, plan, billing_email,
            partnership_model, revenue_share_pct, json.dumps({}),
            vat_number, country, json.dumps(settings or {}), 'active'
        ))

        # Aggiungi owner come membro
        self._add_member_internal(org_id, owner_user_id, 'owner')

        # Se l'utente non ha un default_org_id, imposta questo
        cur.execute("""
            UPDATE users SET default_org_id = %s
            WHERE id = %s AND default_org_id IS NULL
        """, (org_id, owner_user_id))

        self.conn.commit()
        return self.get_org(org_id)

    def get_org(self, org_id: str) -> Optional[Dict]:
        """Ritorna una singola org per id."""
        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute("SELECT * FROM organizations WHERE id = %s", (org_id,))
        row = cur.fetchone()
        return dict(row) if row else None

    def get_org_by_slug(self, slug: str) -> Optional[Dict]:
        """Ritorna una org per slug."""
        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute("SELECT * FROM organizations WHERE slug = %s", (slug,))
        row = cur.fetchone()
        return dict(row) if row else None

    def update_org(self, org_id: str, data: Dict) -> bool:
        """
        Aggiorna campi dell'organizzazione.
        Solo i campi consentiti vengono aggiornati.
        """
        allowed = [
            'name', 'org_type', 'plan', 'billing_email',
            'partnership_model', 'revenue_share_pct', 'custom_pricing',
            'vat_number', 'fiscal_code', 'sdi_code', 'pec_email',
            'country', 'currency', 'industry', 'website', 'logo_url',
            'settings', 'status', 'suspended_reason',
            'max_users', 'max_groups', 'max_api_tokens',
            'max_transforms_month', 'max_ai_calls_month',
            'max_storage_bytes', 'max_partners',
        ]
        fields, values = [], []
        for k, v in data.items():
            if k in allowed:
                if isinstance(v, dict):
                    v = json.dumps(v)
                fields.append(f"{k} = %s")
                values.append(v)

        if not fields:
            return False

        fields.append("updated_at = NOW()")
        values.append(org_id)

        cur = self.conn.cursor()
        cur.execute(
            f"UPDATE organizations SET {', '.join(fields)} WHERE id = %s",
            values
        )
        self.conn.commit()
        return cur.rowcount > 0

    def suspend_org(self, org_id: str, reason: str = None) -> bool:
        """Sospende un'organizzazione."""
        cur = self.conn.cursor()
        cur.execute("""
            UPDATE organizations
            SET status = 'suspended', suspended_at = NOW(), suspended_reason = %s,
                updated_at = NOW()
            WHERE id = %s AND status = 'active'
        """, (reason, org_id))
        self.conn.commit()
        return cur.rowcount > 0

    def reactivate_org(self, org_id: str) -> bool:
        """Riattiva un'organizzazione sospesa."""
        cur = self.conn.cursor()
        cur.execute("""
            UPDATE organizations
            SET status = 'active', suspended_at = NULL, suspended_reason = NULL,
                updated_at = NOW()
            WHERE id = %s AND status = 'suspended'
        """, (org_id,))
        self.conn.commit()
        return cur.rowcount > 0

    # ── MEMBERS ───────────────────────────────────────────────────────

    def _add_member_internal(self, org_id: str, user_id: int, role: str):
        """Aggiunge membro senza commit (usato internamente)."""
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO org_members (org_id, user_id, role, status, joined_at)
            VALUES (%s, %s, %s, 'active', NOW())
            ON CONFLICT (org_id, user_id) DO UPDATE SET role = EXCLUDED.role, status = 'active'
        """, (org_id, user_id, role))

    def add_member(self, org_id: str, user_id: int, role: str = 'operator',
                   invited_by: int = None) -> Dict:
        """Aggiunge un utente all'organizzazione."""
        if role not in ORG_ROLES:
            raise ValueError(f"role deve essere uno di {ORG_ROLES}")

        cur = self.conn.cursor()
        member_id = _gen_id()
        cur.execute("""
            INSERT INTO org_members (id, org_id, user_id, role, status, invited_by, joined_at)
            VALUES (%s, %s, %s, %s, 'active', %s, NOW())
            ON CONFLICT (org_id, user_id) DO UPDATE
                SET role = EXCLUDED.role, status = 'active'
            RETURNING id
        """, (member_id, org_id, user_id, role, invited_by))

        # Se l'utente non ha default_org_id, imposta
        cur.execute("""
            UPDATE users SET default_org_id = %s
            WHERE id = %s AND default_org_id IS NULL
        """, (org_id, user_id))

        self.conn.commit()
        return self.get_member(org_id, user_id)

    def remove_member(self, org_id: str, user_id: int) -> bool:
        """Rimuove un utente dall'organizzazione."""
        cur = self.conn.cursor()

        # Non rimuovere l'ultimo owner
        cur_dict = self.conn.cursor(cursor_factory=self.RDC)
        cur_dict.execute("""
            SELECT COUNT(*) as cnt FROM org_members
            WHERE org_id = %s AND role = 'owner' AND status = 'active'
        """, (org_id,))
        owner_count = cur_dict.fetchone()['cnt']

        cur_dict.execute("""
            SELECT role FROM org_members
            WHERE org_id = %s AND user_id = %s AND status = 'active'
        """, (org_id, user_id))
        member = cur_dict.fetchone()

        if member and member['role'] == 'owner' and owner_count <= 1:
            raise ValueError("Impossibile rimuovere l'ultimo owner dell'organizzazione")

        cur.execute("""
            DELETE FROM org_members WHERE org_id = %s AND user_id = %s
        """, (org_id, user_id))

        # Se era la default_org dell'utente, ricalcola
        cur.execute("""
            UPDATE users SET default_org_id = (
                SELECT org_id FROM org_members
                WHERE user_id = %s AND status = 'active'
                ORDER BY joined_at LIMIT 1
            ) WHERE id = %s AND default_org_id = %s
        """, (user_id, user_id, org_id))

        self.conn.commit()
        return cur.rowcount > 0

    def change_role(self, org_id: str, user_id: int, new_role: str) -> bool:
        """Cambia il ruolo di un membro."""
        if new_role not in ORG_ROLES:
            raise ValueError(f"role deve essere uno di {ORG_ROLES}")

        cur = self.conn.cursor()
        cur.execute("""
            UPDATE org_members SET role = %s
            WHERE org_id = %s AND user_id = %s AND status = 'active'
        """, (new_role, org_id, user_id))
        self.conn.commit()
        return cur.rowcount > 0

    def get_member(self, org_id: str, user_id: int) -> Optional[Dict]:
        """Ritorna un singolo membro."""
        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute("""
            SELECT om.*, u.email, u.name as user_name, u.role as platform_role
            FROM org_members om
            JOIN users u ON om.user_id = u.id
            WHERE om.org_id = %s AND om.user_id = %s
        """, (org_id, user_id))
        row = cur.fetchone()
        return dict(row) if row else None

    def list_members(self, org_id: str, status: str = 'active') -> List[Dict]:
        """Lista tutti i membri di un'organizzazione."""
        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute("""
            SELECT om.*, u.email, u.name as user_name, u.role as platform_role,
                   u.status as user_status, u.plan as user_plan
            FROM org_members om
            JOIN users u ON om.user_id = u.id
            WHERE om.org_id = %s AND om.status = %s
            ORDER BY
                CASE om.role
                    WHEN 'owner' THEN 1
                    WHEN 'admin' THEN 2
                    WHEN 'finance' THEN 3
                    WHEN 'developer' THEN 4
                    WHEN 'operator' THEN 5
                    WHEN 'viewer' THEN 6
                END,
                u.name
        """, (org_id, status))
        return [dict(r) for r in cur.fetchall()]

    def get_user_orgs(self, user_id: int) -> List[Dict]:
        """Ritorna tutte le org a cui appartiene un utente (per switch)."""
        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute("""
            SELECT o.id, o.name, o.slug, o.org_type, o.plan, o.status,
                   o.logo_url, om.role as my_role
            FROM organizations o
            JOIN org_members om ON o.id = om.org_id
            WHERE om.user_id = %s AND om.status = 'active' AND o.status != 'cancelled'
            ORDER BY o.name
        """, (user_id,))
        return [dict(r) for r in cur.fetchall()]

    def user_has_role(self, org_id: str, user_id: int, min_role: str) -> bool:
        """Controlla se l'utente ha almeno il ruolo indicato nell'org."""
        member = self.get_member(org_id, user_id)
        if not member or member.get('status') != 'active':
            return False
        min_level = ORG_ROLE_HIERARCHY.get(min_role, 0)
        user_level = ORG_ROLE_HIERARCHY.get(member['role'], 0)
        return user_level >= min_level

    # ── SWITCH ORG ────────────────────────────────────────────────────

    def switch_org(self, user_id: int, org_id: str) -> Dict:
        """
        Cambia l'org attiva dell'utente.
        Ritorna i dati necessari per generare un nuovo JWT.
        """
        member = self.get_member(org_id, user_id)
        if not member or member.get('status') != 'active':
            raise ValueError("Non sei membro di questa organizzazione")

        org = self.get_org(org_id)
        if not org or org['status'] == 'cancelled':
            raise ValueError("Organizzazione non disponibile")

        # Aggiorna default_org_id
        cur = self.conn.cursor()
        cur.execute(
            "UPDATE users SET default_org_id = %s WHERE id = %s",
            (org_id, user_id)
        )
        self.conn.commit()

        return {
            'org_id': org_id,
            'org_name': org['name'],
            'org_slug': org['slug'],
            'org_type': org['org_type'],
            'org_plan': org['plan'],
            'org_role': member['role'],
            'org_status': org['status'],
        }

    # ── GERARCHIA ─────────────────────────────────────────────────────

    def get_descendants(self, org_id: str, include_self: bool = False) -> List[Dict]:
        """
        Ritorna tutti i discendenti di un'org (sub-org, sub-sub-org, etc.)
        Usa hierarchy_path per query veloce.
        """
        org = self.get_org(org_id)
        if not org:
            return []

        cur = self.conn.cursor(cursor_factory=self.RDC)
        if include_self:
            cur.execute("""
                SELECT * FROM organizations
                WHERE hierarchy_path LIKE %s OR id = %s
                ORDER BY depth, name
            """, (f"{org['hierarchy_path']}/%", org_id))
        else:
            cur.execute("""
                SELECT * FROM organizations
                WHERE hierarchy_path LIKE %s AND id != %s
                ORDER BY depth, name
            """, (f"{org['hierarchy_path']}/%", org_id))
        return [dict(r) for r in cur.fetchall()]

    def get_ancestors(self, org_id: str) -> List[Dict]:
        """Risale la catena fino al root."""
        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute("""
            WITH RECURSIVE ancestors AS (
                SELECT * FROM organizations WHERE id = %s
                UNION ALL
                SELECT o.* FROM organizations o
                JOIN ancestors a ON o.id = a.parent_org_id
            )
            SELECT * FROM ancestors ORDER BY depth
        """, (org_id,))
        return [dict(r) for r in cur.fetchall()]

    def get_children(self, org_id: str) -> List[Dict]:
        """Ritorna solo i figli diretti (depth + 1)."""
        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute("""
            SELECT * FROM organizations
            WHERE parent_org_id = %s AND status != 'cancelled'
            ORDER BY name
        """, (org_id,))
        return [dict(r) for r in cur.fetchall()]

    # ── ORG SUMMARY ───────────────────────────────────────────────────

    def get_org_summary(self, org_id: str) -> Dict:
        """Ritorna un riepilogo dell'org: contatori, stato, piano."""
        cur = self.conn.cursor(cursor_factory=self.RDC)

        summary = {'org_id': org_id}

        # Conteggio membri
        cur.execute("""
            SELECT COUNT(*) as total,
                   COUNT(*) FILTER (WHERE role = 'owner') as owners,
                   COUNT(*) FILTER (WHERE role = 'admin') as admins
            FROM org_members WHERE org_id = %s AND status = 'active'
        """, (org_id,))
        members = cur.fetchone()
        summary['members'] = dict(members) if members else {}

        # Conteggio gruppi
        cur.execute("""
            SELECT COUNT(*) as total FROM groups WHERE org_id = %s
        """, (org_id,))
        summary['groups_count'] = cur.fetchone()['total']

        # Conteggio file
        cur.execute("""
            SELECT COUNT(*) as total,
                   COALESCE(SUM(file_size), 0) as total_bytes
            FROM files WHERE org_id = %s
        """, (org_id,))
        files = cur.fetchone()
        summary['files'] = dict(files) if files else {}

        # Sub-org (se è partner)
        cur.execute("""
            SELECT COUNT(*) as total FROM organizations
            WHERE parent_org_id = %s AND status != 'cancelled'
        """, (org_id,))
        summary['sub_orgs_count'] = cur.fetchone()['total']

        # Usage del mese corrente
        month = datetime.now(timezone.utc).strftime('%Y-%m')
        cur.execute("""
            SELECT COALESCE(SUM(transforms_count), 0) as transforms,
                   COALESCE(SUM(api_calls_count), 0) as api_calls,
                   COALESCE(SUM(bytes_processed), 0) as bytes_processed
            FROM usage_counters
            WHERE org_id = %s AND month = %s
        """, (org_id, month))
        usage = cur.fetchone()
        summary['usage_this_month'] = dict(usage) if usage else {}

        return summary

    # ── ISOLAMENTO ────────────────────────────────────────────────────

    def verify_org_access(self, org_id: str, resource_org_id: str) -> bool:
        """
        Verifica che resource_org_id appartenga a org_id.
        Un'org può accedere:
          - ai propri dati (org_id == resource_org_id)
          - ai dati dei propri discendenti (se è partner)
        """
        if org_id == resource_org_id:
            return True

        # Controlla se resource_org è un discendente
        resource_org = self.get_org(resource_org_id)
        if resource_org and resource_org.get('hierarchy_path', ''):
            return f"/{org_id}/" in resource_org['hierarchy_path']

        return False
