#!/usr/bin/env python3
"""
Buddyliko — Organization API Endpoints
Fase 0: REST API per gestione organizations.
Fase 1: Context switch (personal ↔ org) per multi-tenant.

Registrare in api.py con:
    from org_api import register_org_api
    register_org_api(app, org_service, get_auth_context, require_org_role, auth_manager)

Endpoint:
    GET    /api/org                     — Info org corrente + summary
    PUT    /api/org                     — Aggiorna org settings
    GET    /api/org/members             — Lista membri dell'org
    POST   /api/org/members             — Aggiungi membro (invita)
    PUT    /api/org/members/{uid}/role  — Cambia ruolo membro
    DELETE /api/org/members/{uid}       — Rimuovi membro
    GET    /api/orgs                    — Le mie org (per switch)
    POST   /api/orgs                    — Crea nuova org
    POST   /api/auth/switch-org         — Switch org attiva
    POST   /api/auth/switch-context     — Switch personal ↔ org (Fase 1)
    GET    /api/auth/contexts           — Lista contesti disponibili (Fase 1)
    GET    /api/org/children            — Sub-org (per partner)
    POST   /api/org/children            — Crea sub-org
    GET    /api/platform/orgs           — [platform admin] tutte le org
"""

from fastapi import HTTPException
from pydantic import BaseModel, Field
from typing import Optional, List


# ===========================================================================
# PYDANTIC MODELS
# ===========================================================================

class OrgUpdate(BaseModel):
    name: Optional[str] = None
    vat_number: Optional[str] = None
    fiscal_code: Optional[str] = None
    sdi_code: Optional[str] = None
    pec_email: Optional[str] = None
    country: Optional[str] = None
    currency: Optional[str] = None
    industry: Optional[str] = None
    website: Optional[str] = None
    billing_email: Optional[str] = None
    settings: Optional[dict] = None

class MemberAdd(BaseModel):
    user_id: Optional[int] = None
    email: Optional[str] = None     # Cerca per email se user_id non dato
    role: str = 'operator'

class RoleChange(BaseModel):
    role: str

class OrgCreate(BaseModel):
    name: str
    slug: Optional[str] = None
    org_type: str = 'company'
    plan: str = 'FREE'
    vat_number: Optional[str] = None
    country: Optional[str] = None
    billing_email: Optional[str] = None

class SubOrgCreate(BaseModel):
    name: str
    slug: Optional[str] = None
    org_type: str = 'company'
    plan: str = 'FREE'
    partnership_model: Optional[str] = None
    revenue_share_pct: Optional[float] = None
    owner_email: Optional[str] = None   # Email del proprietario della sub-org

class SwitchOrg(BaseModel):
    org_id: str
    environment: str = 'live'

class SwitchContext(BaseModel):
    context: str = 'personal'    # 'personal' | 'org'
    org_id: Optional[str] = None # se context='org' e si vuole specificare quale
    environment: str = 'live'

class InviteUser(BaseModel):
    email: str
    name: Optional[str] = None
    org_id: Optional[str] = None
    role: str = 'operator'
    plan: str = 'FREE'

class MoveUserToOrg(BaseModel):
    org_id: str
    role: str = 'operator'

class CreateUserDirect(BaseModel):
    email: str
    name: str
    role: str = 'USER'
    plan: str = 'FREE'
    org_id: Optional[str] = None
    org_role: str = 'operator'
    password: Optional[str] = None


# ===========================================================================
# REGISTER ENDPOINTS
# ===========================================================================

def register_org_api(app, org_service, get_auth_context, require_org_role,
                     auth_manager, storage):
    """Registra tutti gli endpoint org sull'app FastAPI."""

    from fastapi import Depends
    from org_middleware import OrgContext

    # ── ORG INFO ──────────────────────────────────────────────────────

    @app.get("/api/org")
    async def get_current_org(ctx: OrgContext = Depends(get_auth_context)):
        """Info dell'organizzazione corrente + summary."""
        if not ctx.org_id:
            raise HTTPException(404, "Nessuna organizzazione attiva. Creane una o accetta un invito.")

        org = org_service.get_org(ctx.org_id)
        if not org:
            raise HTTPException(404, "Organizzazione non trovata")

        summary = org_service.get_org_summary(ctx.org_id)

        # Rimuovi campi sensibili per non-admin
        result = {
            'id': str(org['id']),
            'name': org['name'],
            'slug': org['slug'],
            'org_type': org['org_type'],
            'plan': org['plan'],
            'status': org['status'],
            'country': org['country'],
            'currency': org['currency'],
            'logo_url': org.get('logo_url'),
            'my_role': ctx.org_role,
            'summary': summary,
        }

        # Solo admin vedono dati fiscali e billing
        if ctx.is_org_admin or ctx.org_role == 'finance':
            result.update({
                'vat_number': org.get('vat_number'),
                'fiscal_code': org.get('fiscal_code'),
                'sdi_code': org.get('sdi_code'),
                'pec_email': org.get('pec_email'),
                'billing_email': org.get('billing_email'),
                'industry': org.get('industry'),
                'website': org.get('website'),
                'settings': org.get('settings', {}),
                'stripe_customer_id': org.get('stripe_customer_id'),
                'partnership_model': org.get('partnership_model'),
                'revenue_share_pct': float(org['revenue_share_pct']) if org.get('revenue_share_pct') else None,
                'parent_org_id': str(org['parent_org_id']) if org.get('parent_org_id') else None,
                'created_at': org['created_at'].isoformat() if org.get('created_at') else None,
            })

        return result

    @app.put("/api/org")
    async def update_current_org(
        data: OrgUpdate,
        ctx: OrgContext = Depends(require_org_role('admin'))
    ):
        """Aggiorna info dell'organizzazione corrente. Richiede ruolo admin+."""
        update_data = data.dict(exclude_none=True)
        if not update_data:
            raise HTTPException(400, "Nessun campo da aggiornare")

        ok = org_service.update_org(ctx.org_id, update_data)
        if not ok:
            raise HTTPException(500, "Aggiornamento fallito")

        return {"success": True, "message": "Organizzazione aggiornata"}

    # ── MEMBERS ───────────────────────────────────────────────────────

    @app.get("/api/org/members")
    async def list_org_members(ctx: OrgContext = Depends(require_org_role('viewer'))):
        """Lista membri dell'organizzazione."""
        members = org_service.list_members(ctx.org_id)
        return {
            "org_id": ctx.org_id,
            "members": [
                {
                    'user_id': m['user_id'],
                    'email': m['email'],
                    'name': m.get('user_name'),
                    'role': m['role'],
                    'status': m.get('status', 'active'),
                    'platform_role': m.get('platform_role'),
                    'joined_at': m['joined_at'].isoformat() if m.get('joined_at') else None,
                }
                for m in members
            ]
        }

    @app.post("/api/org/members")
    async def add_org_member(
        data: MemberAdd,
        ctx: OrgContext = Depends(require_org_role('admin'))
    ):
        """Aggiunge un utente all'organizzazione."""
        user_id = data.user_id

        # Se email fornita al posto di user_id, cerca l'utente
        if not user_id and data.email:
            user = storage.get_user_by_email(data.email)
            if not user:
                raise HTTPException(404, f"Utente con email {data.email} non trovato")
            user_id = user['id']

        if not user_id:
            raise HTTPException(400, "Fornire user_id o email")

        # Non puoi assegnare un ruolo superiore al tuo
        from org_service import ORG_ROLE_HIERARCHY
        if not ctx.is_platform_admin:
            requester_level = ORG_ROLE_HIERARCHY.get(ctx.org_role, 0)
            target_level = ORG_ROLE_HIERARCHY.get(data.role, 0)
            if target_level > requester_level:
                raise HTTPException(403, f"Non puoi assegnare il ruolo {data.role}")

        try:
            member = org_service.add_member(
                ctx.org_id, user_id, data.role,
                invited_by=ctx.user_id
            )
            return {"success": True, "member": member}
        except ValueError as e:
            raise HTTPException(400, str(e))

    @app.put("/api/org/members/{user_id}/role")
    async def change_member_role(
        user_id: int,
        data: RoleChange,
        ctx: OrgContext = Depends(require_org_role('admin'))
    ):
        """Cambia il ruolo di un membro."""
        from org_service import ORG_ROLE_HIERARCHY

        # Non puoi promuovere qualcuno a un ruolo superiore al tuo
        if not ctx.is_platform_admin:
            requester_level = ORG_ROLE_HIERARCHY.get(ctx.org_role, 0)
            target_level = ORG_ROLE_HIERARCHY.get(data.role, 0)
            if target_level > requester_level:
                raise HTTPException(403, f"Non puoi assegnare il ruolo {data.role}")

            # Non puoi modificare il ruolo di qualcuno con ruolo >= il tuo
            target_member = org_service.get_member(ctx.org_id, user_id)
            if target_member:
                current_level = ORG_ROLE_HIERARCHY.get(target_member['role'], 0)
                if current_level >= requester_level and ctx.user_id != user_id:
                    raise HTTPException(403, "Non puoi modificare il ruolo di questo utente")

        try:
            ok = org_service.change_role(ctx.org_id, user_id, data.role)
            if not ok:
                raise HTTPException(404, "Membro non trovato")
            return {"success": True}
        except ValueError as e:
            raise HTTPException(400, str(e))

    @app.delete("/api/org/members/{user_id}")
    async def remove_org_member(
        user_id: int,
        ctx: OrgContext = Depends(require_org_role('admin'))
    ):
        """Rimuove un membro dall'organizzazione."""
        if user_id == ctx.user_id:
            raise HTTPException(400, "Non puoi rimuovere te stesso. Usa 'Lascia organizzazione'.")

        try:
            ok = org_service.remove_member(ctx.org_id, user_id)
            if not ok:
                raise HTTPException(404, "Membro non trovato")
            return {"success": True}
        except ValueError as e:
            raise HTTPException(400, str(e))

    # ── MY ORGS + SWITCH ──────────────────────────────────────────────

    @app.get("/api/orgs")
    async def list_my_orgs(ctx: OrgContext = Depends(get_auth_context)):
        """Lista le organizzazioni a cui appartengo (per switch)."""
        orgs = org_service.get_user_orgs(ctx.user_id)
        return {
            "current_org_id": ctx.org_id,
            "organizations": [
                {
                    'id': str(o['id']),
                    'name': o['name'],
                    'slug': o['slug'],
                    'org_type': o['org_type'],
                    'plan': o['plan'],
                    'status': o['status'],
                    'logo_url': o.get('logo_url'),
                    'my_role': o['my_role'],
                    'is_current': str(o['id']) == ctx.org_id,
                }
                for o in orgs
            ]
        }

    @app.post("/api/auth/switch-org")
    async def switch_org(data: SwitchOrg, ctx: OrgContext = Depends(get_auth_context)):
        """
        Switch org attiva. Genera un nuovo JWT con il nuovo org context.
        """
        try:
            switch_result = org_service.switch_org(ctx.user_id, data.org_id)
        except ValueError as e:
            raise HTTPException(403, str(e))

        # Genera nuovo JWT con org_id incluso
        new_payload = {
            'id': str(ctx.user_id),
            'user_id': str(ctx.user_id),
            'email': ctx.email,
            'name': ctx.name,
            'role': ctx.platform_role,
            'status': 'APPROVED',
            'plan': switch_result['org_plan'],
            'org_id': data.org_id,
            'org_role': switch_result['org_role'],
            'environment': data.environment,
        }

        # Usa _generate_token dell'auth_manager (accede al private method)
        import jwt as pyjwt
        from datetime import datetime, timedelta
        new_payload['exp'] = datetime.utcnow() + timedelta(hours=auth_manager.token_expiry_hours)
        new_payload['iat'] = datetime.utcnow()
        new_token = pyjwt.encode(new_payload, auth_manager.secret_key, algorithm='HS256')

        return {
            "success": True,
            "token": new_token,
            "org": switch_result,
        }

    # ── SWITCH CONTEXT (personal ↔ org) — Fase 1 multi-tenant ────────

    @app.post("/api/auth/switch-context")
    async def switch_context(data: SwitchContext, ctx: OrgContext = Depends(get_auth_context)):
        """
        Switch tra contesto personale e org.
        - context='personal' → JWT senza org_id, utente lavora nel suo spazio
        - context='org' → JWT con org_id, utente lavora nello spazio org
        """
        import jwt as pyjwt
        from datetime import datetime, timedelta

        if data.context == 'personal':
            # Contesto personale: nessun org_id nel JWT
            new_payload = {
                'id': str(ctx.user_id),
                'user_id': str(ctx.user_id),
                'email': ctx.email,
                'name': ctx.name,
                'role': ctx.platform_role,
                'status': 'APPROVED',
                'plan': ctx.org_plan or 'FREE',
                'context': 'personal',
                'environment': data.environment,
                'exp': datetime.utcnow() + timedelta(hours=auth_manager.token_expiry_hours),
                'iat': datetime.utcnow(),
            }
            # Prendi il piano personale dell'utente
            db_user = storage.get_user(str(ctx.user_id)) if storage else None
            if db_user:
                new_payload['plan'] = db_user.get('plan', 'FREE')

            new_token = pyjwt.encode(new_payload, auth_manager.secret_key, algorithm='HS256')
            return {
                "success": True,
                "token": new_token,
                "context": "personal",
                "org": None,
            }

        elif data.context == 'org':
            # Determina quale org
            target_org_id = data.org_id or ctx.org_id
            if not target_org_id:
                # Prova la prima org dell'utente
                user_orgs = org_service.get_user_orgs(ctx.user_id)
                if not user_orgs:
                    raise HTTPException(400, "Non sei membro di nessuna organizzazione.")
                target_org_id = str(user_orgs[0]['id'])

            try:
                switch_result = org_service.switch_org(ctx.user_id, target_org_id)
            except ValueError as e:
                raise HTTPException(403, str(e))

            new_payload = {
                'id': str(ctx.user_id),
                'user_id': str(ctx.user_id),
                'email': ctx.email,
                'name': ctx.name,
                'role': ctx.platform_role,
                'status': 'APPROVED',
                'plan': switch_result['org_plan'],
                'org_id': target_org_id,
                'org_role': switch_result['org_role'],
                'context': 'org',
                'environment': data.environment,
                'exp': datetime.utcnow() + timedelta(hours=auth_manager.token_expiry_hours),
                'iat': datetime.utcnow(),
            }
            new_token = pyjwt.encode(new_payload, auth_manager.secret_key, algorithm='HS256')
            return {
                "success": True,
                "token": new_token,
                "context": "org",
                "org": switch_result,
            }
        else:
            raise HTTPException(400, f"Contesto non valido: {data.context}. Usa 'personal' o 'org'.")

    @app.get("/api/auth/contexts")
    async def list_contexts(ctx: OrgContext = Depends(get_auth_context)):
        """
        Lista contesti disponibili per l'utente corrente.
        Ritorna spazio personale + eventuali org di cui è membro.
        """
        contexts = [{
            "type": "personal",
            "name": "Spazio personale",
            "active": ctx.context == 'personal',
        }]

        user_orgs = org_service.get_user_orgs(ctx.user_id)
        for org in user_orgs:
            contexts.append({
                "type": "org",
                "org_id": str(org['id']),
                "name": org.get('name', ''),
                "slug": org.get('slug', ''),
                "plan": org.get('plan', 'FREE'),
                "role": org.get('my_role', 'viewer'),
                "active": ctx.context == 'org' and str(org['id']) == ctx.org_id,
            })

        return {
            "current_context": ctx.context,
            "current_org_id": ctx.org_id,
            "contexts": contexts,
        }

    # ── CREATE ORG ────────────────────────────────────────────────────

    @app.post("/api/orgs")
    async def create_org(data: OrgCreate, ctx: OrgContext = Depends(get_auth_context)):
        """
        Crea una nuova organizzazione.
        Utenti normali possono creare org (diventano owner).
        Solo platform admin possono creare org di tipo 'partner' o 'internal'.
        """
        if data.org_type in ('partner', 'internal') and not ctx.is_platform_admin:
            raise HTTPException(403, "Solo platform admin possono creare org partner/internal")

        try:
            org = org_service.create_org(
                name=data.name,
                slug=data.slug,
                org_type=data.org_type,
                owner_user_id=ctx.user_id,
                plan=data.plan,
                vat_number=data.vat_number,
                country=data.country,
                billing_email=data.billing_email,
            )
            return {"success": True, "org": {
                'id': str(org['id']),
                'name': org['name'],
                'slug': org['slug'],
            }}
        except Exception as e:
            raise HTTPException(400, str(e))

    # ── SUB-ORG (PARTNER) ────────────────────────────────────────────

    @app.get("/api/org/children")
    async def list_children(ctx: OrgContext = Depends(require_org_role('admin'))):
        """Lista le sub-org dirette (per partner)."""
        children = org_service.get_children(ctx.org_id)
        return {
            "parent_org_id": ctx.org_id,
            "children": [
                {
                    'id': str(c['id']),
                    'name': c['name'],
                    'slug': c['slug'],
                    'org_type': c['org_type'],
                    'plan': c['plan'],
                    'status': c['status'],
                    'depth': c['depth'],
                    'created_at': c['created_at'].isoformat() if c.get('created_at') else None,
                }
                for c in children
            ]
        }

    @app.post("/api/org/children")
    async def create_sub_org(
        data: SubOrgCreate,
        ctx: OrgContext = Depends(require_org_role('admin'))
    ):
        """
        Crea una sub-organizzazione.
        L'org corrente diventa parent.
        """
        if not ctx.org_id:
            raise HTTPException(400, "Seleziona prima un'organizzazione (POST /api/orgs per crearne una)")

        # Solo org partner possono creare sub-org (o platform admin)
        current_org = org_service.get_org(ctx.org_id)
        if not current_org:
            raise HTTPException(404, "Org non trovata")

        if current_org['org_type'] != 'partner' and not ctx.is_platform_admin:
            raise HTTPException(403, "Solo organizzazioni partner possono creare sub-org")

        # Determina l'owner della sub-org
        owner_id = ctx.user_id
        if data.owner_email:
            owner = storage.get_user_by_email(data.owner_email)
            if not owner:
                raise HTTPException(404, f"Utente {data.owner_email} non trovato")
            owner_id = owner['id']

        try:
            org = org_service.create_org(
                name=data.name,
                slug=data.slug,
                org_type=data.org_type,
                owner_user_id=owner_id,
                plan=data.plan,
                parent_org_id=ctx.org_id,
                partnership_model=data.partnership_model,
                revenue_share_pct=data.revenue_share_pct,
            )
            return {"success": True, "org": {
                'id': str(org['id']),
                'name': org['name'],
                'slug': org['slug'],
                'parent_org_id': ctx.org_id,
                'depth': org['depth'],
            }}
        except Exception as e:
            raise HTTPException(400, str(e))

    # ── GERARCHIA ─────────────────────────────────────────────────────

    @app.get("/api/org/hierarchy")
    async def get_hierarchy(ctx: OrgContext = Depends(require_org_role('admin'))):
        """Ritorna tutto l'albero sotto l'org corrente."""
        descendants = org_service.get_descendants(ctx.org_id, include_self=True)
        return {
            "root_org_id": ctx.org_id,
            "tree": [
                {
                    'id': str(d['id']),
                    'name': d['name'],
                    'slug': d['slug'],
                    'org_type': d['org_type'],
                    'plan': d['plan'],
                    'status': d['status'],
                    'depth': d['depth'],
                    'parent_org_id': str(d['parent_org_id']) if d.get('parent_org_id') else None,
                }
                for d in descendants
            ]
        }

    @app.get("/api/org/ancestors")
    async def get_ancestors(ctx: OrgContext = Depends(require_org_role('viewer'))):
        """Risale la catena fino al root (per breadcrumb)."""
        ancestors = org_service.get_ancestors(ctx.org_id)
        return {
            "org_id": ctx.org_id,
            "ancestors": [
                {
                    'id': str(a['id']),
                    'name': a['name'],
                    'slug': a['slug'],
                    'depth': a['depth'],
                }
                for a in ancestors
            ]
        }

    # ── PLATFORM ADMIN ────────────────────────────────────────────────

    @app.get("/api/platform/orgs")
    async def platform_list_orgs(ctx: OrgContext = Depends(get_auth_context)):
        """[Platform admin] Lista tutte le organizzazioni."""
        if not ctx.is_platform_admin:
            raise HTTPException(403, "Solo platform admin")

        cur = org_service.conn.cursor(cursor_factory=org_service.RDC)
        cur.execute("""
            SELECT o.*,
                   (SELECT COUNT(*) FROM org_members om
                    WHERE om.org_id = o.id AND om.status = 'active') as member_count,
                   (SELECT COUNT(*) FROM organizations sub
                    WHERE sub.parent_org_id = o.id) as children_count
            FROM organizations o
            ORDER BY o.depth, o.name
        """)
        orgs = [dict(r) for r in cur.fetchall()]

        return {
            "total": len(orgs),
            "organizations": [
                {
                    'id': str(o['id']),
                    'name': o['name'],
                    'slug': o['slug'],
                    'org_type': o['org_type'],
                    'plan': o['plan'],
                    'status': o['status'],
                    'depth': o['depth'],
                    'parent_org_id': str(o['parent_org_id']) if o.get('parent_org_id') else None,
                    'member_count': o['member_count'],
                    'children_count': o['children_count'],
                    'country': o.get('country'),
                    'created_at': o['created_at'].isoformat() if o.get('created_at') else None,
                }
                for o in orgs
            ]
        }

    @app.put("/api/platform/orgs/{org_id}")
    async def platform_update_org(
        org_id: str,
        data: OrgUpdate,
        ctx: OrgContext = Depends(get_auth_context)
    ):
        """[Platform admin] Modifica qualsiasi org."""
        if not ctx.is_platform_admin:
            raise HTTPException(403, "Solo platform admin")

        update_data = data.dict(exclude_none=True)
        ok = org_service.update_org(org_id, update_data)
        if not ok:
            raise HTTPException(404, "Org non trovata")
        return {"success": True}

    @app.post("/api/platform/orgs/{org_id}/suspend")
    async def platform_suspend_org(
        org_id: str,
        ctx: OrgContext = Depends(get_auth_context)
    ):
        """[Platform admin] Sospende un'org."""
        if not ctx.is_platform_admin:
            raise HTTPException(403, "Solo platform admin")
        ok = org_service.suspend_org(org_id, reason="Sospeso da platform admin")
        if not ok:
            raise HTTPException(400, "Impossibile sospendere (già sospesa o non trovata)")
        return {"success": True}

    @app.post("/api/platform/orgs/{org_id}/reactivate")
    async def platform_reactivate_org(
        org_id: str,
        ctx: OrgContext = Depends(get_auth_context)
    ):
        """[Platform admin] Riattiva un'org sospesa."""
        if not ctx.is_platform_admin:
            raise HTTPException(403, "Solo platform admin")
        ok = org_service.reactivate_org(org_id)
        if not ok:
            raise HTTPException(400, "Impossibile riattivare")
        return {"success": True}

    # ── LEAVE ORG ─────────────────────────────────────────────────────

    @app.post("/api/org/leave")
    async def leave_org(ctx: OrgContext = Depends(get_auth_context)):
        """L'utente lascia l'org corrente."""
        if not ctx.org_id:
            raise HTTPException(400, "Nessuna org attiva")

        try:
            org_service.remove_member(ctx.org_id, ctx.user_id)
            return {"success": True, "message": "Hai lasciato l'organizzazione"}
        except ValueError as e:
            raise HTTPException(400, str(e))

    print("   ✅ Organization API endpoints registered (17 endpoints)")

    # ==================================================================
    # FASE 2 — Admin User Management (inviti, spostamento, dettaglio)
    # ==================================================================

    @app.post("/api/admin/users/invite")
    async def invite_user(data: InviteUser, ctx: OrgContext = Depends(get_auth_context)):
        """
        Invita un utente via email. Se già registrato, lo aggiunge all'org.
        Se nuovo, crea un invito con token e manda email.
        """
        if not ctx.is_platform_admin and not ctx.is_org_admin:
            raise HTTPException(403, "Solo admin possono invitare utenti")

        import secrets
        from datetime import datetime, timedelta

        target_org_id = data.org_id or ctx.org_id
        target_org_name = ''
        if target_org_id:
            org_data = org_service.get_org(target_org_id)
            if org_data:
                target_org_name = org_data.get('name', '')

        # Controlla se l'utente esiste già
        cur = org_service.conn.cursor(cursor_factory=org_service.RDC)
        cur.execute("SELECT id, name, email FROM users WHERE email = %s", (data.email,))
        existing_user = cur.fetchone()

        if existing_user and target_org_id:
            # Utente esiste → aggiungilo all'org direttamente
            user_id = existing_user['id']
            try:
                org_service.add_member(target_org_id, user_id, data.role)
            except Exception as e:
                if 'already' in str(e).lower() or 'duplicate' in str(e).lower():
                    return {"success": True, "action": "already_member",
                            "message": f"{data.email} è già membro dell'organizzazione"}
                raise HTTPException(400, str(e))

            # Notifica via email
            try:
                import email_service
                email_service.send_user_added_to_org(
                    data.email, existing_user.get('name', ''), target_org_name, data.role)
            except Exception as e:
                print(f"[INVITE] Errore invio email notifica: {e}")

            return {"success": True, "action": "added_to_org",
                    "user_id": user_id, "org_id": target_org_id,
                    "message": f"{data.email} aggiunto a {target_org_name}"}

        # Utente non esiste → crea invito
        invite_token = secrets.token_urlsafe(32)
        expires_at = datetime.utcnow() + timedelta(hours=72)

        cur = org_service.conn.cursor()
        cur.execute("""
            INSERT INTO user_invitations (email, name, org_id, role, plan, invite_token,
                                          invited_by, expires_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (data.email, data.name, target_org_id, data.role, data.plan,
              invite_token, ctx.user_id, expires_at))
        invite_id = cur.fetchone()[0]
        org_service.conn.commit()

        # Manda email
        invite_url = f"https://buddyliko.com/login.html?invite={invite_token}"
        try:
            import email_service
            email_service.send_invite_email(
                data.email, data.name or '', ctx.name or 'Admin',
                target_org_name or 'Buddyliko', invite_url, data.role)
        except Exception as e:
            print(f"[INVITE] Errore invio email invito: {e}")

        return {"success": True, "action": "invited",
                "invite_id": str(invite_id), "invite_token": invite_token,
                "message": f"Invito inviato a {data.email}"}

    @app.get("/api/admin/users/{user_id}/detail")
    async def get_user_detail(user_id: str, ctx: OrgContext = Depends(get_auth_context)):
        """Dettaglio completo di un utente: org, usage, ruoli."""
        if not ctx.is_platform_admin:
            raise HTTPException(403, "Solo platform admin")

        from datetime import datetime, timezone

        db_user = storage.get_user(user_id) if storage else None
        if not db_user:
            raise HTTPException(404, "Utente non trovato")

        # Org dell'utente
        user_orgs = org_service.get_user_orgs(int(user_id))

        # Usage del mese corrente
        month = datetime.now(timezone.utc).strftime('%Y-%m')
        cur = org_service.conn.cursor(cursor_factory=org_service.RDC)
        cur.execute("""
            SELECT transforms_count, api_calls_count, bytes_processed, codegen_count, org_id
            FROM usage_counters WHERE user_id = %s AND month = %s
        """, (user_id, month))
        usage_rows = [dict(r) for r in cur.fetchall()]

        personal_usage = next((r for r in usage_rows if not r.get('org_id')),
                              {"transforms_count": 0, "api_calls_count": 0, "bytes_processed": 0})
        org_usage = [r for r in usage_rows if r.get('org_id')]

        return {
            "user": {
                "id": str(db_user.get('id')),
                "email": db_user.get('email'),
                "name": db_user.get('name'),
                "role": db_user.get('role'),
                "status": db_user.get('status'),
                "plan": db_user.get('plan'),
                "created_at": str(db_user.get('created_at', '')),
                "active_org_id": db_user.get('active_org_id'),
            },
            "organizations": [
                {"id": str(o['id']), "name": o['name'], "role": o['my_role'],
                 "plan": o.get('plan'), "status": o.get('status')}
                for o in user_orgs
            ],
            "usage": {
                "month": month,
                "personal": personal_usage,
                "org": org_usage,
            },
        }

    @app.put("/api/admin/users/{user_id}/org")
    async def move_user_to_org(
        user_id: str, data: MoveUserToOrg,
        ctx: OrgContext = Depends(get_auth_context)
    ):
        """Sposta un utente in un'org (rimuove da quella vecchia se c'era)."""
        if not ctx.is_platform_admin:
            raise HTTPException(403, "Solo platform admin")

        uid = int(user_id)

        # Rimuovi da eventuali org precedenti
        current_orgs = org_service.get_user_orgs(uid)
        for old_org in current_orgs:
            try:
                org_service.remove_member(str(old_org['id']), uid)
            except Exception:
                pass

        # Aggiungi alla nuova org
        try:
            org_service.add_member(data.org_id, uid, data.role)
        except Exception as e:
            raise HTTPException(400, str(e))

        # Aggiorna active_org_id
        cur = org_service.conn.cursor()
        cur.execute("UPDATE users SET active_org_id = %s WHERE id = %s", (data.org_id, uid))
        org_service.conn.commit()

        org_data = org_service.get_org(data.org_id)
        return {"success": True,
                "message": f"Utente spostato in {org_data.get('name', data.org_id)}"}

    @app.delete("/api/admin/users/{user_id}/org")
    async def remove_user_from_org(user_id: str, ctx: OrgContext = Depends(get_auth_context)):
        """Rimuovi un utente dalla sua org (torna standalone)."""
        if not ctx.is_platform_admin:
            raise HTTPException(403, "Solo platform admin")

        uid = int(user_id)
        current_orgs = org_service.get_user_orgs(uid)

        for org in current_orgs:
            try:
                org_service.remove_member(str(org['id']), uid)
            except Exception:
                pass

        cur = org_service.conn.cursor()
        cur.execute("UPDATE users SET active_org_id = NULL WHERE id = %s", (uid,))
        org_service.conn.commit()

        return {"success": True, "message": "Utente rimosso dall'organizzazione"}

    @app.get("/api/admin/invitations")
    async def list_invitations(ctx: OrgContext = Depends(get_auth_context)):
        """Lista inviti pendenti."""
        if not ctx.is_platform_admin and not ctx.is_org_admin:
            raise HTTPException(403, "Solo admin")

        cur = org_service.conn.cursor(cursor_factory=org_service.RDC)
        query = """
            SELECT i.*, o.name as org_name, u.name as inviter_name
            FROM user_invitations i
            LEFT JOIN organizations o ON o.id = i.org_id
            LEFT JOIN users u ON u.id = i.invited_by
            WHERE i.status = 'pending'
            ORDER BY i.created_at DESC
        """
        cur.execute(query)
        invitations = [dict(r) for r in cur.fetchall()]

        return {"invitations": [
            {
                "id": str(inv['id']),
                "email": inv['email'],
                "name": inv.get('name'),
                "org_name": inv.get('org_name'),
                "role": inv['role'],
                "inviter_name": inv.get('inviter_name'),
                "status": inv['status'],
                "created_at": inv['created_at'].isoformat() if inv.get('created_at') else None,
                "expires_at": inv['expires_at'].isoformat() if inv.get('expires_at') else None,
            }
            for inv in invitations
        ]}

    @app.delete("/api/admin/invitations/{invite_id}")
    async def cancel_invitation(invite_id: str, ctx: OrgContext = Depends(get_auth_context)):
        """Cancella un invito pendente."""
        if not ctx.is_platform_admin and not ctx.is_org_admin:
            raise HTTPException(403, "Solo admin")

        cur = org_service.conn.cursor()
        cur.execute("""
            UPDATE user_invitations SET status = 'cancelled'
            WHERE id = %s AND status = 'pending'
        """, (invite_id,))
        org_service.conn.commit()
        if cur.rowcount == 0:
            raise HTTPException(404, "Invito non trovato o già processato")
        return {"success": True}

    print("   ✅ Phase 2 — Admin user management endpoints registered")
