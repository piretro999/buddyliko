#!/usr/bin/env python3
"""
Buddyliko — Partner API (Phase 4) — 12 endpoint REST
"""
import uuid as _uuid
from decimal import Decimal
from datetime import datetime, date

def _ser(obj):
    if isinstance(obj, list):
        return [_ser(i) for i in obj]
    if isinstance(obj, dict):
        return {k: _ser(v) for k, v in obj.items()}
    if isinstance(obj, Decimal): return str(obj)
    if isinstance(obj, (datetime, date)): return obj.isoformat()
    if isinstance(obj, _uuid.UUID): return str(obj)
    return obj

def register_partner_endpoints(app, get_auth_context, require_org_role, partner_service):
    from fastapi import Depends, HTTPException, Query, UploadFile, File

    @app.get("/api/partners")
    async def list_partners(
        partner_type: str = Query(None), status: str = Query(None),
        search: str = Query(None), limit: int = Query(200, ge=1, le=500),
        offset: int = Query(0, ge=0),
        ctx=Depends(require_org_role('viewer'))
    ):
        partners = partner_service.list(ctx.org_id, partner_type, status, search, limit, offset)
        return {"org_id": ctx.org_id, "count": len(partners), "data": _ser(partners)}

    @app.post("/api/partners")
    async def create_partner(body: dict, ctx=Depends(require_org_role('operator'))):
        try:
            p = partner_service.create(ctx.org_id, body)
            return {"success": True, "partner": _ser(p)}
        except ValueError as e:
            raise HTTPException(400, str(e))

    @app.get("/api/partners/stats")
    async def partner_stats(ctx=Depends(require_org_role('viewer'))):
        return _ser(partner_service.stats(ctx.org_id))

    @app.get("/api/partners/{partner_id}")
    async def get_partner(partner_id: str, ctx=Depends(require_org_role('viewer'))):
        p = partner_service.get(ctx.org_id, partner_id)
        if not p: raise HTTPException(404, "Partner non trovato")
        return _ser(p)

    @app.put("/api/partners/{partner_id}")
    async def update_partner(partner_id: str, body: dict, ctx=Depends(require_org_role('operator'))):
        ok = partner_service.update(ctx.org_id, partner_id, body)
        if not ok: raise HTTPException(404, "Partner non trovato o nessuna modifica")
        return {"success": True}

    @app.delete("/api/partners/{partner_id}")
    async def delete_partner(partner_id: str, ctx=Depends(require_org_role('admin'))):
        ok = partner_service.delete(ctx.org_id, partner_id)
        if not ok: raise HTTPException(404, "Partner non trovato")
        return {"success": True}

    @app.post("/api/partners/import-csv")
    async def import_csv(file: UploadFile = File(...), ctx=Depends(require_org_role('admin'))):
        content = (await file.read()).decode('utf-8', errors='replace')
        created, errors = partner_service.import_csv(ctx.org_id, content)
        return {"success": True, "created": created, "errors": errors}

    @app.get("/api/partners/{partner_id}/tokens")
    async def partner_tokens(partner_id: str, ctx=Depends(require_org_role('admin'))):
        p = partner_service.get(ctx.org_id, partner_id)
        if not p: raise HTTPException(404, "Partner non trovato")
        tokens = partner_service.get_partner_tokens(ctx.org_id, partner_id)
        return {"partner_id": partner_id, "tokens": _ser(tokens)}

    @app.put("/api/partners/{partner_id}/tokens/{token_id}")
    async def link_token(partner_id: str, token_id: str, ctx=Depends(require_org_role('admin'))):
        try:
            ok = partner_service.link_token(ctx.org_id, partner_id, token_id)
            if not ok: raise HTTPException(404, "Token non trovato")
            return {"success": True}
        except ValueError as e:
            raise HTTPException(400, str(e))

    @app.delete("/api/partners/{partner_id}/tokens/{token_id}")
    async def unlink_token(partner_id: str, token_id: str, ctx=Depends(require_org_role('admin'))):
        ok = partner_service.unlink_token(ctx.org_id, partner_id, token_id)
        if not ok: raise HTTPException(404, "Non trovato")
        return {"success": True}

    @app.get("/api/partners/{partner_id}/costs")
    async def partner_costs(partner_id: str, month: str = Query(None), ctx=Depends(require_org_role('finance'))):
        """Costi sostenuti per questo partner (dalla Phase 2)."""
        try:
            from cost_service import CostService
            # Get cost_service from app state
            cur = partner_service.conn.cursor(cursor_factory=partner_service.RDC)
            if not month:
                from datetime import datetime as dt, timezone as tz
                month = dt.now(tz.utc).strftime('%Y-%m')
            cur.execute("""SELECT COUNT(*) as operations,
                COALESCE(SUM(ai_cost_usd),0) as ai_cost_usd,
                COALESCE(SUM(platform_cost_eur),0) as platform_cost_eur,
                COALESCE(SUM(billable_amount_eur),0) as billable_eur,
                COALESCE(SUM(margin_eur),0) as margin_eur
                FROM transformation_costs
                WHERE org_id=%s AND partner_id=%s AND billing_month=%s""",
                (ctx.org_id, partner_id, month))
            r = cur.fetchone()
            return _ser({"partner_id": partner_id, "month": month, **(dict(r) if r else {})})
        except Exception:
            return {"partner_id": partner_id, "month": month, "operations": 0}

    print("   ✅ Partner API: 12 endpoints registered")
