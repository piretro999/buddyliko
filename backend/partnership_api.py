#!/usr/bin/env python3
"""
Buddyliko — Partnership API (Phase 6) — 14 endpoint REST
Tutti sotto /api/partnership/
"""
import uuid as _uuid
from decimal import Decimal
from datetime import datetime, date
from pydantic import BaseModel, Field
from typing import Optional

class SubOrgCreateModel(BaseModel):
    name: str
    slug: Optional[str] = None
    org_type: str = 'company'
    plan: str = 'FREE'
    partnership_model: Optional[str] = None
    revenue_share_pct: Optional[float] = None
    owner_email: Optional[str] = None
    vat_number: Optional[str] = None
    country: Optional[str] = None
    billing_email: Optional[str] = None

class SubOrgUpdateModel(BaseModel):
    name: Optional[str] = None
    plan: Optional[str] = None
    revenue_share_pct: Optional[float] = None
    vat_number: Optional[str] = None
    country: Optional[str] = None
    billing_email: Optional[str] = None
    max_users: Optional[int] = None
    max_api_tokens: Optional[int] = None
    max_transforms_month: Optional[int] = None

class TransferModel(BaseModel):
    new_owner_email: str


def register_partnership_endpoints(app, get_auth_context, require_org_role,
                                   partnership_service, storage):
    from fastapi import Depends, HTTPException, Query

    def _require_partner(ctx):
        """Verifica che l'org corrente sia di tipo partner."""
        if not ctx.org_id:
            raise HTTPException(403, "Nessuna org attiva")
        if not ctx.has_min_role('admin'):
            raise HTTPException(403, "Ruolo minimo: admin")
        # Platform admin può sempre
        if ctx.is_platform_admin:
            return ctx
        if ctx.org_type != 'partner':
            raise HTTPException(403, "Questa funzionalità è riservata alle organizzazioni partner")
        return ctx

    # ── 1. Partner Dashboard ──
    @app.get("/api/partnership/dashboard")
    def partnership_dashboard(month: str = Query(None), ctx=Depends(get_auth_context)):
        ctx = _require_partner(ctx)
        return partnership_service.get_partner_dashboard(ctx.org_id, month)

    # ── 2. List sub-orgs ──
    @app.get("/api/partnership/sub-orgs")
    def list_sub_orgs(
        status: str = Query(None), plan: str = Query(None),
        direct_only: bool = Query(True),
        ctx=Depends(get_auth_context)
    ):
        ctx = _require_partner(ctx)
        orgs = partnership_service.list_sub_orgs(ctx.org_id, status, plan, direct_only)
        return {"parent_org_id": ctx.org_id, "count": len(orgs), "sub_orgs": orgs}

    # ── 3. Create sub-org ──
    @app.post("/api/partnership/sub-orgs")
    def create_sub_org(data: SubOrgCreateModel, ctx=Depends(get_auth_context)):
        ctx = _require_partner(ctx)

        owner_id = ctx.user_id
        if data.owner_email:
            user = storage.get_user_by_email(data.owner_email)
            if not user:
                raise HTTPException(404, f"Utente {data.owner_email} non trovato. "
                                    "L'utente deve registrarsi prima.")
            owner_id = user['id']

        try:
            org = partnership_service.create_sub_org(
                ctx.org_id, name=data.name, owner_user_id=owner_id,
                slug=data.slug, org_type=data.org_type, plan=data.plan,
                partnership_model=data.partnership_model,
                revenue_share_pct=data.revenue_share_pct,
                vat_number=data.vat_number, country=data.country,
                billing_email=data.billing_email)
            return {"success": True, "org": org}
        except ValueError as e:
            raise HTTPException(400, str(e))

    # ── 4. Sub-org detail ──
    @app.get("/api/partnership/sub-orgs/{sub_org_id}")
    def get_sub_org(sub_org_id: str, month: str = Query(None), ctx=Depends(get_auth_context)):
        ctx = _require_partner(ctx)
        try:
            return partnership_service.get_sub_org_detail(ctx.org_id, sub_org_id, month)
        except ValueError as e:
            raise HTTPException(404, str(e))

    # ── 5. Update sub-org ──
    @app.put("/api/partnership/sub-orgs/{sub_org_id}")
    def update_sub_org(sub_org_id: str, data: SubOrgUpdateModel, ctx=Depends(get_auth_context)):
        ctx = _require_partner(ctx)
        update = data.dict(exclude_none=True)
        if not update:
            raise HTTPException(400, "Nessun campo da aggiornare")
        try:
            ok = partnership_service.update_sub_org(ctx.org_id, sub_org_id, update)
            if not ok:
                raise HTTPException(500, "Aggiornamento fallito")
            return {"success": True}
        except ValueError as e:
            raise HTTPException(400, str(e))

    # ── 6. Suspend sub-org ──
    @app.post("/api/partnership/sub-orgs/{sub_org_id}/suspend")
    def suspend_sub_org(sub_org_id: str, ctx=Depends(get_auth_context)):
        ctx = _require_partner(ctx)
        try:
            affected = partnership_service.suspend_sub_org(ctx.org_id, sub_org_id)
            return {"success": True, "affected_orgs": affected}
        except ValueError as e:
            raise HTTPException(400, str(e))

    # ── 7. Reactivate sub-org ──
    @app.post("/api/partnership/sub-orgs/{sub_org_id}/reactivate")
    def reactivate_sub_org(sub_org_id: str, ctx=Depends(get_auth_context)):
        ctx = _require_partner(ctx)
        try:
            ok = partnership_service.reactivate_sub_org(ctx.org_id, sub_org_id)
            if not ok:
                raise HTTPException(400, "Impossibile riattivare (non sospesa?)")
            return {"success": True}
        except ValueError as e:
            raise HTTPException(400, str(e))

    # ── 8. Transfer ownership ──
    @app.post("/api/partnership/sub-orgs/{sub_org_id}/transfer")
    def transfer_sub_org(sub_org_id: str, data: TransferModel, ctx=Depends(get_auth_context)):
        ctx = _require_partner(ctx)
        user = storage.get_user_by_email(data.new_owner_email)
        if not user:
            raise HTTPException(404, f"Utente {data.new_owner_email} non trovato")
        try:
            partnership_service.transfer_sub_org(ctx.org_id, sub_org_id, user['id'])
            return {"success": True, "new_owner": data.new_owner_email}
        except ValueError as e:
            raise HTTPException(400, str(e))

    # ── 9. Revenue share cascata ──
    @app.get("/api/partnership/revenue-share")
    def revenue_share(month: str = Query(None), ctx=Depends(get_auth_context)):
        ctx = _require_partner(ctx)
        return partnership_service.calculate_cascading_revenue_share(ctx.org_id, month)

    # ── 10. Billing aggregato ──
    @app.get("/api/partnership/billing")
    def aggregated_billing(month: str = Query(None), ctx=Depends(get_auth_context)):
        ctx = _require_partner(ctx)
        return partnership_service.get_aggregated_billing(ctx.org_id, month)

    # ── 11. Hierarchy tree ──
    @app.get("/api/partnership/hierarchy")
    def hierarchy_tree(month: str = Query(None), ctx=Depends(get_auth_context)):
        ctx = _require_partner(ctx)
        return partnership_service.get_hierarchy_tree(ctx.org_id, month)

    # ── 12. Partner info (check if current org is partner) ──
    @app.get("/api/partnership/info")
    def partnership_info(ctx=Depends(get_auth_context)):
        if not ctx.org_id:
            raise HTTPException(403, "Nessuna org attiva")
        is_partner = ctx.org_type == 'partner' or ctx.is_platform_admin
        from psycopg2.extras import RealDictCursor
        sub_count = 0
        if is_partner:
            try:
                cur = partnership_service.conn.cursor(cursor_factory=RealDictCursor)
                cur.execute("SELECT COUNT(*) as cnt FROM organizations WHERE parent_org_id=%s AND status!='cancelled'", (ctx.org_id,))
                sub_count = cur.fetchone()['cnt']
            except: pass
        return {
            "org_id": ctx.org_id,
            "org_type": ctx.org_type,
            "is_partner": is_partner,
            "sub_org_count": sub_count,
        }

    # ── 13. Clone settings to sub-org ──
    @app.post("/api/partnership/sub-orgs/{sub_org_id}/clone-settings")
    def clone_settings(sub_org_id: str, ctx=Depends(get_auth_context)):
        """Copia le impostazioni del partner sulla sub-org."""
        ctx = _require_partner(ctx)
        try:
            partnership_service._verify_parent_child(ctx.org_id, sub_org_id)
        except ValueError as e:
            raise HTTPException(400, str(e))

        from psycopg2.extras import RealDictCursor
        cur = partnership_service.conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT settings FROM organizations WHERE id=%s", (ctx.org_id,))
        parent_settings = cur.fetchone()
        if parent_settings and parent_settings.get('settings'):
            import json
            cur2 = partnership_service.conn.cursor()
            cur2.execute("UPDATE organizations SET settings=%s, updated_at=NOW() WHERE id=%s",
                         (json.dumps(parent_settings['settings']) if isinstance(parent_settings['settings'], dict)
                          else parent_settings['settings'], sub_org_id))
            partnership_service.conn.commit()
        return {"success": True}

    # ── 14. Upgrade plan for sub-org ──
    @app.post("/api/partnership/sub-orgs/{sub_org_id}/upgrade")
    def upgrade_sub_org(sub_org_id: str, plan: str = Query(...), ctx=Depends(get_auth_context)):
        ctx = _require_partner(ctx)
        valid_plans = ('FREE', 'PRO', 'ENTERPRISE')
        if plan.upper() not in valid_plans:
            raise HTTPException(400, f"Piano non valido. Valori: {valid_plans}")
        try:
            ok = partnership_service.update_sub_org(ctx.org_id, sub_org_id, {'plan': plan.upper()})
            if not ok:
                raise HTTPException(500, "Upgrade fallito")
            return {"success": True, "new_plan": plan.upper()}
        except ValueError as e:
            raise HTTPException(400, str(e))

    print("   ✅ Partnership API: 14 endpoints registered")
