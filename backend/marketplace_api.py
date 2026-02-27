#!/usr/bin/env python3
"""
Buddyliko — Marketplace API (Phase 7) — 16 endpoint REST
Tutti sotto /api/marketplace/
"""
from pydantic import BaseModel, Field
from typing import Optional, List

class TemplateCreate(BaseModel):
    name: str
    description: Optional[str] = ''
    long_description: Optional[str] = ''
    category: str = 'other'
    input_standard: Optional[str] = ''
    output_standard: Optional[str] = ''
    input_format: Optional[str] = ''
    output_format: Optional[str] = ''
    mapping_data: Optional[dict] = {}
    sample_input: Optional[str] = ''
    sample_output: Optional[str] = ''
    availability: str = 'private'
    price_eur: Optional[float] = 0
    price_type: Optional[str] = 'one_time'
    version: Optional[str] = '1.0.0'
    tags: Optional[list] = []
    icon: Optional[str] = '📄'

class TemplateUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    long_description: Optional[str] = None
    category: Optional[str] = None
    input_standard: Optional[str] = None
    output_standard: Optional[str] = None
    input_format: Optional[str] = None
    output_format: Optional[str] = None
    mapping_data: Optional[dict] = None
    sample_input: Optional[str] = None
    sample_output: Optional[str] = None
    availability: Optional[str] = None
    price_eur: Optional[float] = None
    price_type: Optional[str] = None
    status: Optional[str] = None
    version: Optional[str] = None
    tags: Optional[list] = None
    icon: Optional[str] = None
    featured: Optional[bool] = None

class ReviewCreate(BaseModel):
    rating: int = Field(ge=1, le=5)
    title: Optional[str] = ''
    body: Optional[str] = ''

class CloneModel(BaseModel):
    new_name: Optional[str] = None


def register_marketplace_endpoints(app, get_auth_context, require_org_role,
                                   marketplace_service):
    from fastapi import Depends, HTTPException, Query

    # ── 1. Browse marketplace ──
    @app.get("/api/marketplace/browse")
    def marketplace_browse(
        q: str = Query(None), category: str = Query(None),
        input_format: str = Query(None), output_format: str = Query(None),
        availability: str = Query(None), price_max: float = Query(None),
        sort_by: str = Query('downloads'), page: int = Query(1), per_page: int = Query(24),
        ctx=Depends(get_auth_context)
    ):
        return marketplace_service.browse(
            org_id=ctx.org_id, org_plan=ctx.org_plan or 'FREE',
            q=q, category=category, input_format=input_format,
            output_format=output_format, availability=availability,
            price_max=price_max, sort_by=sort_by, page=page, per_page=min(per_page, 50))

    # ── 2. Featured templates ──
    @app.get("/api/marketplace/featured")
    def marketplace_featured(limit: int = Query(8, ge=1, le=20), ctx=Depends(get_auth_context)):
        return {"templates": marketplace_service.get_featured(limit)}

    # ── 3. Categories ──
    @app.get("/api/marketplace/categories")
    def marketplace_categories(ctx=Depends(get_auth_context)):
        return {"categories": marketplace_service.get_categories()}

    # ── 4. Template detail ──
    @app.get("/api/marketplace/templates/{template_id}")
    def get_template(template_id: str, ctx=Depends(get_auth_context)):
        include_mapping = ctx.org_id is not None  # Only logged-in users see mapping
        tpl = marketplace_service.get_template(template_id, ctx.org_id, include_mapping)
        if not tpl:
            raise HTTPException(404, "Template non trovato")
        return tpl

    # ── 5. Publish template ──
    @app.post("/api/marketplace/templates")
    def create_template(data: TemplateCreate, ctx=Depends(get_auth_context)):
        if not ctx.org_id:
            raise HTTPException(403, "Nessuna org attiva")
        if not ctx.has_min_role('developer'):
            raise HTTPException(403, "Ruolo minimo: developer")

        d = data.dict()
        # Only admin can create builtin
        if d.get('availability') == 'builtin' and not ctx.is_platform_admin:
            d['availability'] = 'private'

        try:
            tpl = marketplace_service.create_template(ctx.org_id, ctx.user_id, d)
            return {"success": True, "template": tpl}
        except ValueError as e:
            raise HTTPException(400, str(e))

    # ── 6. Update template ──
    @app.put("/api/marketplace/templates/{template_id}")
    def update_template(template_id: str, data: TemplateUpdate, ctx=Depends(get_auth_context)):
        if not ctx.org_id:
            raise HTTPException(403, "Nessuna org attiva")
        if not ctx.has_min_role('developer'):
            raise HTTPException(403, "Ruolo minimo: developer")

        d = data.dict(exclude_none=True)
        # featured only by admin
        if 'featured' in d and not ctx.is_platform_admin:
            del d['featured']
        if d.get('availability') == 'builtin' and not ctx.is_platform_admin:
            del d['availability']

        try:
            ok = marketplace_service.update_template(template_id, ctx.org_id, ctx.user_id, d, ctx.is_platform_admin)
            if not ok:
                raise HTTPException(500, "Aggiornamento fallito")
            return {"success": True}
        except ValueError as e:
            raise HTTPException(400, str(e))

    # ── 7. Deprecate template ──
    @app.delete("/api/marketplace/templates/{template_id}")
    def deprecate_template(template_id: str, ctx=Depends(get_auth_context)):
        if not ctx.org_id:
            raise HTTPException(403, "Nessuna org attiva")
        try:
            marketplace_service.update_template(
                template_id, ctx.org_id, ctx.user_id,
                {'status': 'deprecated'}, ctx.is_platform_admin)
            return {"success": True}
        except ValueError as e:
            raise HTTPException(400, str(e))

    # ── 8. Purchase / Install ──
    @app.post("/api/marketplace/templates/{template_id}/purchase")
    def purchase_template(template_id: str, ctx=Depends(get_auth_context)):
        if not ctx.org_id:
            raise HTTPException(403, "Nessuna org attiva")
        if not ctx.has_min_role('operator'):
            raise HTTPException(403, "Ruolo minimo: operator")
        try:
            result = marketplace_service.purchase_template(template_id, ctx.org_id, ctx.user_id)
            return {"success": True, **result}
        except ValueError as e:
            raise HTTPException(400, str(e))

    # ── 9. Add review ──
    @app.post("/api/marketplace/templates/{template_id}/review")
    def add_review(template_id: str, data: ReviewCreate, ctx=Depends(get_auth_context)):
        if not ctx.org_id:
            raise HTTPException(403, "Nessuna org attiva")
        try:
            marketplace_service.add_review(
                template_id, ctx.org_id, ctx.user_id,
                data.rating, data.title, data.body)
            return {"success": True}
        except ValueError as e:
            raise HTTPException(400, str(e))

    # ── 10. Get reviews ──
    @app.get("/api/marketplace/templates/{template_id}/reviews")
    def get_reviews(template_id: str, page: int = Query(1), ctx=Depends(get_auth_context)):
        return {"reviews": marketplace_service.get_reviews(template_id, page)}

    # ── 11. Clone template ──
    @app.post("/api/marketplace/templates/{template_id}/clone")
    def clone_template(template_id: str, data: CloneModel = None, ctx=Depends(get_auth_context)):
        if not ctx.org_id:
            raise HTTPException(403, "Nessuna org attiva")
        if not ctx.has_min_role('developer'):
            raise HTTPException(403, "Ruolo minimo: developer")
        try:
            new_name = data.new_name if data else None
            tpl = marketplace_service.clone_template(template_id, ctx.org_id, ctx.user_id, new_name)
            return {"success": True, "template": tpl}
        except ValueError as e:
            raise HTTPException(400, str(e))

    # ── 12. My templates ──
    @app.get("/api/marketplace/my-templates")
    def my_templates(status: str = Query(None), ctx=Depends(get_auth_context)):
        if not ctx.org_id:
            raise HTTPException(403, "Nessuna org attiva")
        return {"templates": marketplace_service.get_my_templates(ctx.org_id, status)}

    # ── 13. My purchases ──
    @app.get("/api/marketplace/my-purchases")
    def my_purchases(ctx=Depends(get_auth_context)):
        if not ctx.org_id:
            raise HTTPException(403, "Nessuna org attiva")
        return {"purchases": marketplace_service.get_my_purchases(ctx.org_id)}

    # ── 14. Marketplace stats (admin) ──
    @app.get("/api/marketplace/stats")
    def marketplace_stats(ctx=Depends(get_auth_context)):
        if not ctx.is_platform_admin:
            raise HTTPException(403, "Solo Platform Admin")
        return marketplace_service.get_marketplace_stats()

    # ── 15. Seed builtin (admin) ──
    @app.post("/api/marketplace/seed-builtin")
    def seed_builtin(ctx=Depends(get_auth_context)):
        if not ctx.is_platform_admin:
            raise HTTPException(403, "Solo Platform Admin")
        count = marketplace_service.seed_builtin()
        return {"success": True, "seeded": count}

    # ── 16. Get mapping data (for installed/owned) ──
    @app.get("/api/marketplace/templates/{template_id}/mapping")
    def get_mapping_data(template_id: str, ctx=Depends(get_auth_context)):
        if not ctx.org_id:
            raise HTTPException(403, "Nessuna org attiva")
        tpl = marketplace_service.get_template(template_id, ctx.org_id, include_mapping=True)
        if not tpl:
            raise HTTPException(404, "Template non trovato")
        if not tpl.get('can_use', False):
            raise HTTPException(403, "Devi acquistare/installare questo template per usarlo")
        return {"mapping_data": tpl.get('mapping_data', {}), "name": tpl.get('name', ''), "version": tpl.get('version', '')}

    print("   ✅ Marketplace API: 16 endpoints registered")
