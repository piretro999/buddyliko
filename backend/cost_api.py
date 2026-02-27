#!/usr/bin/env python3
"""
Buddyliko — Cost API (Phase 2) — 14 endpoint REST
"""
import uuid as _uuid
from decimal import Decimal
from datetime import datetime, date

def _ser(rows):
    result = []
    for row in rows:
        c = {}
        for k,v in row.items():
            if isinstance(v, Decimal): c[k] = str(v)
            elif isinstance(v, (datetime, date)): c[k] = v.isoformat()
            elif isinstance(v, _uuid.UUID): c[k] = str(v)
            else: c[k] = v
        result.append(c)
    return result

def register_cost_endpoints(app, get_auth_context, require_org_role, cost_service):
    from fastapi import Depends, HTTPException, Query
    from datetime import datetime as dt, timezone as tz

    @app.get("/api/costs/summary")
    def costs_summary(month:str=Query(None), ctx=Depends(get_auth_context)):
        if not ctx.org_id: raise HTTPException(403,"Nessuna org attiva")
        if not ctx.has_min_role('viewer'): raise HTTPException(403,"Ruolo minimo: viewer")
        return cost_service.get_org_usage_summary(ctx.org_id, month)

    @app.get("/api/costs/by-auth-type")
    def costs_by_auth_type(month:str=Query(None), ctx=Depends(get_auth_context)):
        if not ctx.org_id: raise HTTPException(403,"Nessuna org attiva")
        if not ctx.has_min_role('finance'): raise HTTPException(403,"Ruolo minimo: finance")
        return {"org_id":ctx.org_id,"month":month,"data":_ser(cost_service.get_costs_by_auth_type(ctx.org_id,month))}

    @app.get("/api/costs/by-partner")
    def costs_by_partner(month:str=Query(None), ctx=Depends(get_auth_context)):
        if not ctx.org_id: raise HTTPException(403,"Nessuna org attiva")
        if not ctx.has_min_role('finance'): raise HTTPException(403,"Ruolo minimo: finance")
        return {"org_id":ctx.org_id,"month":month,"data":_ser(cost_service.get_costs_by_partner(ctx.org_id,month))}

    @app.get("/api/costs/by-tag")
    def costs_by_tag(tag_key:str=Query(...), month:str=Query(None), ctx=Depends(get_auth_context)):
        if not ctx.org_id: raise HTTPException(403,"Nessuna org attiva")
        if not ctx.has_min_role('finance'): raise HTTPException(403,"Ruolo minimo: finance")
        return {"org_id":ctx.org_id,"tag_key":tag_key,"month":month,"data":_ser(cost_service.get_costs_by_tag(ctx.org_id,tag_key,month))}

    @app.get("/api/costs/ai-detail")
    def costs_ai_detail(month:str=Query(None), ctx=Depends(get_auth_context)):
        if not ctx.org_id: raise HTTPException(403,"Nessuna org attiva")
        if not ctx.has_min_role('developer'): raise HTTPException(403,"Ruolo minimo: developer")
        return {"org_id":ctx.org_id,"month":month,"data":_ser(cost_service.get_ai_cost_detail(ctx.org_id,month))}

    @app.get("/api/costs/daily-trend")
    def costs_daily_trend(month:str=Query(None), ctx=Depends(get_auth_context)):
        if not ctx.org_id: raise HTTPException(403,"Nessuna org attiva")
        if not ctx.has_min_role('viewer'): raise HTTPException(403,"Ruolo minimo: viewer")
        return {"org_id":ctx.org_id,"month":month,"data":_ser(cost_service.get_daily_trend(ctx.org_id,month))}

    @app.get("/api/costs/recent")
    def costs_recent(limit:int=Query(50,ge=1,le=200), environment:str=Query(None), auth_type:str=Query(None), operation:str=Query(None), ctx=Depends(get_auth_context)):
        if not ctx.org_id: raise HTTPException(403,"Nessuna org attiva")
        if not ctx.has_min_role('viewer'): raise HTTPException(403,"Ruolo minimo: viewer")
        rows = cost_service.get_recent_operations(ctx.org_id,limit,environment,auth_type,operation)
        return {"org_id":ctx.org_id,"count":len(rows),"data":_ser(rows)}

    @app.get("/api/costs/profitability")
    def costs_profitability(month:str=Query(None), ctx=Depends(get_auth_context)):
        if not ctx.is_platform_admin: raise HTTPException(403,"Solo Platform Admin")
        return {"month":month,"data":_ser(cost_service.get_platform_profitability(month))}

    @app.get("/api/pricing/plans")
    def pricing_plans(ctx=Depends(get_auth_context)):
        return {"plans":_ser(cost_service.list_plans())}

    @app.put("/api/pricing/plans/{plan}")
    def update_plan(plan:str, body:dict, ctx=Depends(get_auth_context)):
        if not ctx.is_platform_admin: raise HTTPException(403,"Solo Platform Admin")
        ok = cost_service.update_plan_pricing(plan.upper(), body)
        if not ok: raise HTTPException(404,f"Piano '{plan}' non trovato")
        return {"ok":True,"plan":plan.upper()}

    @app.post("/api/costs/aggregate-now")
    def force_aggregate(ctx=Depends(get_auth_context)):
        if not ctx.is_org_admin and not ctx.is_platform_admin: raise HTTPException(403,"Solo admin")
        return {"ok":True,"orgs_aggregated":cost_service.aggregate_all_orgs_today()}

    @app.post("/api/costs/aggregate-org")
    def force_aggregate_org(month:str=Query(None), ctx=Depends(get_auth_context)):
        if not ctx.org_id: raise HTTPException(403,"Nessuna org attiva")
        if not ctx.is_org_admin and not ctx.is_platform_admin: raise HTTPException(403,"Solo admin")
        if not month: month = dt.now(tz.utc).strftime('%Y-%m')
        today = dt.now(tz.utc).strftime('%Y-%m-%d')
        cost_service.aggregate_period(ctx.org_id,'daily',today)
        cost_service.aggregate_period(ctx.org_id,'monthly',month)
        return {"ok":True,"org_id":ctx.org_id,"month":month}

    @app.get("/api/costs/exchange-rate")
    def get_exchange_rate(ctx=Depends(get_auth_context)):
        return {"usd_eur":str(cost_service.get_usd_eur_rate())}

    @app.put("/api/costs/exchange-rate")
    def set_exchange_rate(body:dict, ctx=Depends(get_auth_context)):
        if not ctx.is_platform_admin: raise HTTPException(403,"Solo Platform Admin")
        r = body.get('rate')
        if not r or float(r)<=0: raise HTTPException(400,"Rate non valido")
        cost_service.set_usd_eur_rate(float(r))
        return {"ok":True,"usd_eur":str(cost_service.get_usd_eur_rate())}

    print("   ✅ Cost API: 14 endpoints registered")
