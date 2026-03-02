#!/usr/bin/env python3
"""
Buddyliko — Permission Service (Phase 5: RBAC Granulare)
════════════════════════════════════════════════════════
Gestisce:
- Risoluzione permessi per utente (role_template + custom overrides)
- CRUD role templates (preset + custom per org)
- Custom user permission overrides
- Cache in-memory con invalidazione

Tabelle:
- permissions (scope, action)
- role_templates (name, is_system, org_id)
- role_permissions (M2M)
- custom_user_permissions (override per utente)
- org_members.role_template_id (FK)
"""

import json
import time
from typing import Optional, Dict, List, Set, Tuple
from functools import lru_cache


class PermissionService:
    """RBAC permission resolution engine."""

    # Cache TTL in secondi
    CACHE_TTL = 300  # 5 minuti

    def __init__(self, conn, cursor_factory):
        self.conn = conn
        self.cursor_factory = cursor_factory
        self._perm_cache: Dict[str, Tuple[Set[str], float]] = {}  # user:org → (perms, ts)
        self._role_cache: Dict[int, Tuple[List[dict], float]] = {}  # template_id → (perms, ts)
        self._all_perms_cache: Optional[Tuple[List[dict], float]] = None

    # ══════════════════════════════════════════════════════════════════
    # PERMISSION RESOLUTION (core)
    # ══════════════════════════════════════════════════════════════════

    def get_user_permissions(self, user_id: int, org_id: str) -> Set[str]:
        """
        Risolvi tutti i permessi effettivi per un utente in un'org.
        Formato: "scope:action" (es. "transforms:execute", "billing:manage").
        
        Logica:
        1. Prendi role_template_id da org_members
        2. Prendi permessi dal role template
        3. Applica custom overrides (grant/revoke per utente)
        4. Ritorna set finale
        """
        cache_key = f"{user_id}:{org_id}"
        cached = self._perm_cache.get(cache_key)
        if cached and (time.time() - cached[1]) < self.CACHE_TTL:
            return cached[0]

        perms = set()
        try:
            cur = self.conn.cursor(cursor_factory=self.cursor_factory)

            # 1. Ottieni role_template_id dal membro
            cur.execute("""
                SELECT role_template_id, role FROM org_members
                WHERE org_id = %s AND user_id = %s
            """, (org_id, user_id))
            member = cur.fetchone()
            if not member:
                return perms

            template_id = member.get('role_template_id')

            # 2. Permessi dal role template
            if template_id:
                cur.execute("""
                    SELECT p.scope, p.action
                    FROM role_permissions rp
                    JOIN permissions p ON p.id = rp.permission_id
                    WHERE rp.role_template_id = %s
                """, (template_id,))
                for row in cur.fetchall():
                    perms.add(f"{row['scope']}:{row['action']}")
            else:
                # Fallback: vecchio ruolo stringa → mappa a template
                perms = self._legacy_role_permissions(member.get('role', 'viewer'))

            # 3. Custom overrides
            cur.execute("""
                SELECT p.scope, p.action, cup.granted
                FROM custom_user_permissions cup
                JOIN permissions p ON p.id = cup.permission_id
                WHERE cup.user_id = %s AND cup.org_id = %s
            """, (user_id, org_id))
            for row in cur.fetchall():
                perm_str = f"{row['scope']}:{row['action']}"
                if row['granted']:
                    perms.add(perm_str)
                else:
                    perms.discard(perm_str)

            cur.close()
        except Exception as e:
            print(f"⚠️  PermissionService.get_user_permissions error: {e}")

        # Cache
        self._perm_cache[cache_key] = (perms, time.time())
        return perms

    def has_permission(self, user_id: int, org_id: str, scope: str, action: str) -> bool:
        """Check se un utente ha un permesso specifico."""
        perms = self.get_user_permissions(user_id, org_id)
        return f"{scope}:{action}" in perms

    def has_any_permission(self, user_id: int, org_id: str, checks: List[str]) -> bool:
        """Check se un utente ha ALMENO UNO dei permessi (OR)."""
        perms = self.get_user_permissions(user_id, org_id)
        return any(c in perms for c in checks)

    def has_all_permissions(self, user_id: int, org_id: str, checks: List[str]) -> bool:
        """Check se un utente ha TUTTI i permessi (AND)."""
        perms = self.get_user_permissions(user_id, org_id)
        return all(c in perms for c in checks)

    def _legacy_role_permissions(self, role: str) -> Set[str]:
        """Mappa vecchi ruoli stringa a set di permessi (backward compat)."""
        LEGACY_MAP = {
            'owner': None,  # all
            'admin': None,  # all
            'finance': {'billing:view', 'billing:manage', 'members:view', 'audit:view',
                        'reports:view', 'settings:view'},
            'developer': {'schemas:view', 'schemas:edit', 'schemas:delete',
                          'transforms:view', 'transforms:execute',
                          'mappings:view', 'mappings:edit', 'mappings:delete',
                          'workspace:view', 'workspace:edit', 'workspace:delete',
                          'dbconn:view', 'dbconn:manage', 'dbconn:execute',
                          'audit:view', 'reports:view', 'members:view',
                          'tokens:view', 'settings:view'},
            'operator': {'schemas:view', 'transforms:view', 'transforms:execute',
                         'mappings:view', 'mappings:edit',
                         'workspace:view', 'workspace:edit', 'dbconn:view'},
            'viewer': {'schemas:view', 'transforms:view', 'mappings:view',
                       'workspace:view', 'reports:view'},
        }
        mapped = LEGACY_MAP.get(role)
        if mapped is None:
            # owner/admin → tutti i permessi
            return self._get_all_permission_strings()
        return mapped

    def _get_all_permission_strings(self) -> Set[str]:
        """Ritorna tutti i permessi disponibili (per owner)."""
        if self._all_perms_cache and (time.time() - self._all_perms_cache[1]) < self.CACHE_TTL:
            return {f"{p['scope']}:{p['action']}" for p in self._all_perms_cache[0]}
        try:
            cur = self.conn.cursor(cursor_factory=self.cursor_factory)
            cur.execute("SELECT scope, action FROM permissions")
            rows = cur.fetchall()
            cur.close()
            self._all_perms_cache = (rows, time.time())
            return {f"{r['scope']}:{r['action']}" for r in rows}
        except Exception:
            return set()

    # ══════════════════════════════════════════════════════════════════
    # PERMISSION CATALOG
    # ══════════════════════════════════════════════════════════════════

    def list_permissions(self) -> List[dict]:
        """Ritorna tutti i permessi disponibili, raggruppati per scope."""
        try:
            cur = self.conn.cursor(cursor_factory=self.cursor_factory)
            cur.execute("SELECT id, scope, action, description FROM permissions ORDER BY scope, action")
            rows = cur.fetchall()
            cur.close()
            return [dict(r) for r in rows]
        except Exception as e:
            print(f"⚠️  list_permissions error: {e}")
            return []

    def list_permissions_grouped(self) -> Dict[str, List[dict]]:
        """Permessi raggruppati per scope."""
        perms = self.list_permissions()
        grouped = {}
        for p in perms:
            scope = p['scope']
            if scope not in grouped:
                grouped[scope] = []
            grouped[scope].append(p)
        return grouped

    # ══════════════════════════════════════════════════════════════════
    # ROLE TEMPLATES
    # ══════════════════════════════════════════════════════════════════

    def list_role_templates(self, org_id: Optional[str] = None) -> List[dict]:
        """
        Lista role template disponibili per un'org.
        Include: system templates + custom templates dell'org.
        """
        try:
            cur = self.conn.cursor(cursor_factory=self.cursor_factory)
            if org_id:
                cur.execute("""
                    SELECT rt.*, 
                           COALESCE(
                               (SELECT json_agg(json_build_object('scope', p.scope, 'action', p.action))
                                FROM role_permissions rp JOIN permissions p ON p.id = rp.permission_id
                                WHERE rp.role_template_id = rt.id), '[]'
                           ) as permissions,
                           (SELECT COUNT(*) FROM org_members om WHERE om.role_template_id = rt.id AND om.org_id = %s) as member_count
                    FROM role_templates rt
                    WHERE rt.is_system = TRUE OR rt.org_id = %s
                    ORDER BY rt.is_system DESC, rt.name
                """, (org_id, org_id))
            else:
                cur.execute("""
                    SELECT rt.*,
                           COALESCE(
                               (SELECT json_agg(json_build_object('scope', p.scope, 'action', p.action))
                                FROM role_permissions rp JOIN permissions p ON p.id = rp.permission_id
                                WHERE rp.role_template_id = rt.id), '[]'
                           ) as permissions
                    FROM role_templates rt
                    WHERE rt.is_system = TRUE
                    ORDER BY rt.name
                """)
            rows = cur.fetchall()
            cur.close()
            result = []
            for r in rows:
                d = dict(r)
                # Parse permissions JSON se stringa
                if isinstance(d.get('permissions'), str):
                    d['permissions'] = json.loads(d['permissions'])
                result.append(d)
            return result
        except Exception as e:
            print(f"⚠️  list_role_templates error: {e}")
            return []

    def get_role_template(self, template_id: int) -> Optional[dict]:
        """Dettaglio di un role template con permessi."""
        try:
            cur = self.conn.cursor(cursor_factory=self.cursor_factory)
            cur.execute("""
                SELECT rt.*,
                       COALESCE(
                           (SELECT json_agg(json_build_object('id', p.id, 'scope', p.scope, 'action', p.action, 'description', p.description))
                            FROM role_permissions rp JOIN permissions p ON p.id = rp.permission_id
                            WHERE rp.role_template_id = rt.id), '[]'
                       ) as permissions
                FROM role_templates rt
                WHERE rt.id = %s
            """, (template_id,))
            row = cur.fetchone()
            cur.close()
            if not row:
                return None
            d = dict(row)
            if isinstance(d.get('permissions'), str):
                d['permissions'] = json.loads(d['permissions'])
            return d
        except Exception as e:
            print(f"⚠️  get_role_template error: {e}")
            return None

    def create_role_template(self, org_id: str, name: str, label: str,
                             description: str, permission_ids: List[int],
                             created_by: int) -> dict:
        """Crea un custom role template per un'org."""
        try:
            cur = self.conn.cursor(cursor_factory=self.cursor_factory)

            # Verifica nome univoco in org
            cur.execute("""
                SELECT id FROM role_templates WHERE name = %s AND (org_id = %s OR is_system = TRUE)
            """, (name, org_id))
            if cur.fetchone():
                raise ValueError(f"Nome ruolo '{name}' già esistente")

            cur.execute("""
                INSERT INTO role_templates (name, label, description, is_system, org_id, created_by)
                VALUES (%s, %s, %s, FALSE, %s, %s)
                RETURNING id
            """, (name, label, description, org_id, created_by))
            template_id = cur.fetchone()['id']

            # Associa permessi
            for pid in permission_ids:
                cur.execute("""
                    INSERT INTO role_permissions (role_template_id, permission_id)
                    VALUES (%s, %s) ON CONFLICT DO NOTHING
                """, (template_id, pid))

            self.conn.commit()
            cur.close()
            self._invalidate_cache()
            return self.get_role_template(template_id)
        except ValueError:
            raise
        except Exception as e:
            self.conn.rollback()
            raise ValueError(f"Errore creazione ruolo: {e}")

    def update_role_template(self, template_id: int, org_id: str,
                             label: str = None, description: str = None,
                             permission_ids: List[int] = None) -> dict:
        """Aggiorna un custom role template (non system)."""
        try:
            cur = self.conn.cursor(cursor_factory=self.cursor_factory)

            # Verifica che non sia system
            cur.execute("SELECT is_system, org_id FROM role_templates WHERE id = %s", (template_id,))
            rt = cur.fetchone()
            if not rt:
                raise ValueError("Role template non trovato")
            if rt['is_system']:
                raise ValueError("Non puoi modificare un ruolo di sistema")
            if str(rt['org_id']) != str(org_id):
                raise ValueError("Role template non appartiene a questa org")

            if label is not None:
                cur.execute("UPDATE role_templates SET label = %s WHERE id = %s", (label, template_id))
            if description is not None:
                cur.execute("UPDATE role_templates SET description = %s WHERE id = %s", (description, template_id))

            if permission_ids is not None:
                cur.execute("DELETE FROM role_permissions WHERE role_template_id = %s", (template_id,))
                for pid in permission_ids:
                    cur.execute("""
                        INSERT INTO role_permissions (role_template_id, permission_id)
                        VALUES (%s, %s) ON CONFLICT DO NOTHING
                    """, (template_id, pid))

            self.conn.commit()
            cur.close()
            self._invalidate_cache()
            return self.get_role_template(template_id)
        except ValueError:
            raise
        except Exception as e:
            self.conn.rollback()
            raise ValueError(f"Errore aggiornamento ruolo: {e}")

    def delete_role_template(self, template_id: int, org_id: str) -> bool:
        """Elimina un custom role template. Sposta membri a 'viewer'."""
        try:
            cur = self.conn.cursor(cursor_factory=self.cursor_factory)

            cur.execute("SELECT is_system, org_id FROM role_templates WHERE id = %s", (template_id,))
            rt = cur.fetchone()
            if not rt:
                raise ValueError("Role template non trovato")
            if rt['is_system']:
                raise ValueError("Non puoi eliminare un ruolo di sistema")
            if str(rt['org_id']) != str(org_id):
                raise ValueError("Role template non appartiene a questa org")

            # Sposta membri con questo template a viewer
            cur.execute("""
                UPDATE org_members SET role_template_id = (
                    SELECT id FROM role_templates WHERE name = 'viewer' AND is_system = TRUE
                ), role = 'viewer'
                WHERE role_template_id = %s AND org_id = %s
            """, (template_id, org_id))

            cur.execute("DELETE FROM role_permissions WHERE role_template_id = %s", (template_id,))
            cur.execute("DELETE FROM role_templates WHERE id = %s", (template_id,))

            self.conn.commit()
            cur.close()
            self._invalidate_cache()
            return True
        except ValueError:
            raise
        except Exception as e:
            self.conn.rollback()
            raise ValueError(f"Errore eliminazione ruolo: {e}")

    # ══════════════════════════════════════════════════════════════════
    # MEMBER ROLE ASSIGNMENT
    # ══════════════════════════════════════════════════════════════════

    def assign_role(self, org_id: str, user_id: int, template_id: int,
                    assigned_by: int) -> dict:
        """Assegna un role template a un membro dell'org."""
        try:
            cur = self.conn.cursor(cursor_factory=self.cursor_factory)

            # Verifica template esiste e appartiene a org (o è system)
            cur.execute("""
                SELECT name FROM role_templates WHERE id = %s AND (is_system = TRUE OR org_id = %s)
            """, (template_id, org_id))
            rt = cur.fetchone()
            if not rt:
                raise ValueError("Role template non trovato o non disponibile per questa org")

            # Non permettere di cambiare ruolo dell'owner
            cur.execute("SELECT role FROM org_members WHERE org_id = %s AND user_id = %s", (org_id, user_id))
            member = cur.fetchone()
            if not member:
                raise ValueError("Utente non è membro dell'org")
            if member['role'] == 'owner' and rt['name'] != 'owner':
                raise ValueError("Non puoi cambiare ruolo all'owner. Trasferisci prima la proprietà.")

            # Aggiorna
            cur.execute("""
                UPDATE org_members SET role_template_id = %s, role = %s
                WHERE org_id = %s AND user_id = %s
            """, (template_id, rt['name'], org_id, user_id))

            self.conn.commit()
            cur.close()
            self._invalidate_cache(user_id, org_id)
            return {"success": True, "role": rt['name'], "template_id": template_id}
        except ValueError:
            raise
        except Exception as e:
            self.conn.rollback()
            raise ValueError(f"Errore assegnazione ruolo: {e}")

    # ══════════════════════════════════════════════════════════════════
    # CUSTOM USER PERMISSION OVERRIDES
    # ══════════════════════════════════════════════════════════════════

    def get_user_overrides(self, user_id: int, org_id: str) -> List[dict]:
        """Ritorna le override custom di un utente nell'org."""
        try:
            cur = self.conn.cursor(cursor_factory=self.cursor_factory)
            cur.execute("""
                SELECT p.scope, p.action, p.description, cup.granted, cup.granted_at
                FROM custom_user_permissions cup
                JOIN permissions p ON p.id = cup.permission_id
                WHERE cup.user_id = %s AND cup.org_id = %s
                ORDER BY p.scope, p.action
            """, (user_id, org_id))
            rows = cur.fetchall()
            cur.close()
            return [dict(r) for r in rows]
        except Exception:
            return []

    def set_user_override(self, user_id: int, org_id: str,
                          permission_id: int, granted: bool,
                          granted_by: int) -> dict:
        """Imposta un override custom per un utente."""
        try:
            cur = self.conn.cursor(cursor_factory=self.cursor_factory)
            cur.execute("""
                INSERT INTO custom_user_permissions (user_id, org_id, permission_id, granted, granted_by)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (user_id, org_id, permission_id) DO UPDATE SET
                    granted = EXCLUDED.granted,
                    granted_by = EXCLUDED.granted_by,
                    granted_at = NOW()
            """, (user_id, org_id, permission_id, granted, granted_by))
            self.conn.commit()
            cur.close()
            self._invalidate_cache(user_id, org_id)
            return {"success": True}
        except Exception as e:
            self.conn.rollback()
            raise ValueError(f"Errore impostazione override: {e}")

    def remove_user_override(self, user_id: int, org_id: str, permission_id: int) -> bool:
        """Rimuovi un override custom (torna al default del ruolo)."""
        try:
            cur = self.conn.cursor(cursor_factory=self.cursor_factory)
            cur.execute("""
                DELETE FROM custom_user_permissions
                WHERE user_id = %s AND org_id = %s AND permission_id = %s
            """, (user_id, org_id, permission_id))
            self.conn.commit()
            cur.close()
            self._invalidate_cache(user_id, org_id)
            return True
        except Exception:
            self.conn.rollback()
            return False

    def clear_user_overrides(self, user_id: int, org_id: str) -> int:
        """Rimuovi tutte le override custom di un utente."""
        try:
            cur = self.conn.cursor(cursor_factory=self.cursor_factory)
            cur.execute("""
                DELETE FROM custom_user_permissions WHERE user_id = %s AND org_id = %s
            """, (user_id, org_id))
            count = cur.rowcount
            self.conn.commit()
            cur.close()
            self._invalidate_cache(user_id, org_id)
            return count
        except Exception:
            self.conn.rollback()
            return 0

    # ══════════════════════════════════════════════════════════════════
    # MEMBER DETAILS (con permessi risolti)
    # ══════════════════════════════════════════════════════════════════

    def get_member_with_permissions(self, org_id: str, user_id: int) -> Optional[dict]:
        """Ritorna un membro con i suoi permessi effettivi risolti."""
        try:
            cur = self.conn.cursor(cursor_factory=self.cursor_factory)
            cur.execute("""
                SELECT om.user_id, om.org_id, om.role, om.role_template_id, om.joined_at,
                       u.email, u.name,
                       rt.name as template_name, rt.label as template_label
                FROM org_members om
                JOIN users u ON u.id = om.user_id
                LEFT JOIN role_templates rt ON rt.id = om.role_template_id
                WHERE om.org_id = %s AND om.user_id = %s
            """, (org_id, user_id))
            row = cur.fetchone()
            cur.close()
            if not row:
                return None
            d = dict(row)
            d['permissions'] = sorted(self.get_user_permissions(user_id, org_id))
            d['overrides'] = self.get_user_overrides(user_id, org_id)
            return d
        except Exception:
            return None

    # ══════════════════════════════════════════════════════════════════
    # CACHE MANAGEMENT
    # ══════════════════════════════════════════════════════════════════

    def _invalidate_cache(self, user_id: int = None, org_id: str = None):
        """Invalida cache. Se user_id+org_id forniti, solo quella entry."""
        if user_id and org_id:
            self._perm_cache.pop(f"{user_id}:{org_id}", None)
        else:
            self._perm_cache.clear()
        self._role_cache.clear()

    def invalidate_all(self):
        """Forza invalidazione totale (utile dopo migration)."""
        self._perm_cache.clear()
        self._role_cache.clear()
        self._all_perms_cache = None
