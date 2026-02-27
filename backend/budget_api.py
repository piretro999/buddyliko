#!/usr/bin/env python3
"""
Buddyliko — Budget API (Phase 8B) — 8 endpoint REST
"""
from pydantic import BaseModel, Field
from typing import Optional, List

class BudgetSet(BaseModel):
    budget_eur: float = Field(ge=0)
    auto_block: bool = False
    alert_pct: Optional[List[int]] = None
    block_message: Optional[str] = None


def register_budget_endpoints(app, get_auth_context, require_org_role, budget_service):
    from fastapi import Depends, HTTPException, Query

    # 1. Get budget status
    @app.get("/api/budget")
    def get_budget(ctx=Depends(require_org_role('finance'))):
        return budget_service.get_budget(ctx.org_id)

    # 2. Set/update budget
    @app.put("/api/budget")
    def set_budget(data: BudgetSet, ctx=Depends(require_org_role('admin'))):
        try:
            result = budget_service.set_budget(
                ctx.org_id, data.budget_eur, data.auto_block,
                data.alert_pct, data.block_message)
            return {"success": True, "budget": result}
        except ValueError as e:
            raise HTTPException(400, str(e))

    # 3. Remove budget
    @app.delete("/api/budget")
    def remove_budget(ctx=Depends(require_org_role('admin'))):
        ok = budget_service.remove_budget(ctx.org_id)
        return {"success": True, "removed": ok}

    # 4. Check budget (can be called before operations)
    @app.get("/api/budget/check")
    def check_budget(ctx=Depends(require_org_role('viewer'))):
        allowed, msg = budget_service.check_budget(ctx.org_id)
        return {"allowed": allowed, "message": msg}

    # 5. Unblock manually
    @app.post("/api/budget/unblock")
    def unblock(ctx=Depends(require_org_role('admin'))):
        ok = budget_service.unblock(ctx.org_id)
        if not ok: raise HTTPException(400, "Org non bloccata")
        return {"success": True}

    # 6. Budget alerts log
    @app.get("/api/budget/alerts")
    def budget_alerts(month: str = Query(None), limit: int = Query(50),
                      ctx=Depends(require_org_role('finance'))):
        return {"alerts": budget_service.get_alerts(ctx.org_id, month, limit)}

    # 7. Platform budget overview (admin)
    @app.get("/api/budget/platform")
    def platform_budgets(ctx=Depends(get_auth_context)):
        if not ctx.is_platform_admin:
            raise HTTPException(403, "Solo Platform Admin")
        return {"budgets": budget_service.get_platform_budgets()}

    # 8. Set budget for sub-org (partner)
    @app.put("/api/budget/{sub_org_id}")
    def set_sub_org_budget(sub_org_id: str, data: BudgetSet, ctx=Depends(require_org_role('admin'))):
        # Verify parent-child relationship
        from psycopg2.extras import RealDictCursor
        cur = budget_service.conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT hierarchy_path, parent_org_id FROM organizations WHERE id=%s", (sub_org_id,))
        org = cur.fetchone()
        if not org:
            raise HTTPException(404, "Org non trovata")
        hp = org.get('hierarchy_path', '')
        if f"/{ctx.org_id}/" not in hp and str(org.get('parent_org_id', '')) != str(ctx.org_id):
            if not ctx.is_platform_admin:
                raise HTTPException(403, "Non sei il parent di questa org")
        try:
            result = budget_service.set_budget(sub_org_id, data.budget_eur, data.auto_block, data.alert_pct, data.block_message)
            return {"success": True, "budget": result}
        except ValueError as e:
            raise HTTPException(400, str(e))

    print("   ✅ Budget API: 8 endpoints registered")
