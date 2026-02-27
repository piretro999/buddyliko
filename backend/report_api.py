#!/usr/bin/env python3
"""
Buddyliko — Report API (Phase 5) — 16 endpoint REST
Tutti sotto /api/reports/
"""
import uuid as _uuid
from decimal import Decimal
from datetime import datetime, date

def _ser(rows):
    if isinstance(rows, dict):
        return {k: _ser(v) if isinstance(v, (dict, list)) else _sv(v) for k, v in rows.items()}
    if isinstance(rows, list):
        return [_ser(r) if isinstance(r, (dict, list)) else _sv(r) for r in rows]
    return _sv(rows)

def _sv(v):
    if isinstance(v, Decimal): return str(v)
    if isinstance(v, (datetime, date)): return v.isoformat()
    if isinstance(v, _uuid.UUID): return str(v)
    return v

def register_report_endpoints(app, get_auth_context, require_org_role, report_service):
    from fastapi import Depends, HTTPException, Query
    from fastapi.responses import Response

    # ── 1. Summary KPI ──
    @app.get("/api/reports/summary")
    def report_summary(month: str = Query(None), ctx=Depends(get_auth_context)):
        if not ctx.org_id: raise HTTPException(403, "Nessuna org attiva")
        if not ctx.has_min_role('viewer'): raise HTTPException(403, "Ruolo minimo: viewer")
        return _ser(report_service.get_summary_report(ctx.org_id, month))

    # ── 2. Auth type breakdown ──
    @app.get("/api/reports/auth-breakdown")
    def report_auth(month: str = Query(None), ctx=Depends(get_auth_context)):
        if not ctx.org_id: raise HTTPException(403, "Nessuna org attiva")
        if not ctx.has_min_role('finance'): raise HTTPException(403, "Ruolo minimo: finance")
        return _ser(report_service.get_auth_breakdown(ctx.org_id, month))

    # ── 3. Partner breakdown ──
    @app.get("/api/reports/partner-breakdown")
    def report_partners(month: str = Query(None), ctx=Depends(get_auth_context)):
        if not ctx.org_id: raise HTTPException(403, "Nessuna org attiva")
        if not ctx.has_min_role('finance'): raise HTTPException(403, "Ruolo minimo: finance")
        return _ser(report_service.get_partner_breakdown(ctx.org_id, month))

    # ── 4. AI cost breakdown ──
    @app.get("/api/reports/ai-breakdown")
    def report_ai(month: str = Query(None), ctx=Depends(get_auth_context)):
        if not ctx.org_id: raise HTTPException(403, "Nessuna org attiva")
        if not ctx.has_min_role('developer'): raise HTTPException(403, "Ruolo minimo: developer")
        return _ser(report_service.get_ai_breakdown(ctx.org_id, month))

    # ── 5. Tag breakdown ──
    @app.get("/api/reports/tag-breakdown")
    def report_tags(tag_key: str = Query(None), month: str = Query(None), ctx=Depends(get_auth_context)):
        if not ctx.org_id: raise HTTPException(403, "Nessuna org attiva")
        if not ctx.has_min_role('finance'): raise HTTPException(403, "Ruolo minimo: finance")
        return _ser(report_service.get_tag_breakdown(ctx.org_id, month, tag_key))

    # ── 6. Daily trend ──
    @app.get("/api/reports/trend")
    def report_trend(month: str = Query(None), ctx=Depends(get_auth_context)):
        if not ctx.org_id: raise HTTPException(403, "Nessuna org attiva")
        if not ctx.has_min_role('viewer'): raise HTTPException(403, "Ruolo minimo: viewer")
        return _ser(report_service.get_trend_report(ctx.org_id, month))

    # ── 7. Operations log ──
    @app.get("/api/reports/operations")
    def report_operations(
        month: str = Query(None), limit: int = Query(100, ge=1, le=500),
        auth_type: str = Query(None), operation: str = Query(None),
        environment: str = Query(None), partner_id: str = Query(None),
        status: str = Query(None), min_cost: float = Query(None),
        ctx=Depends(get_auth_context)
    ):
        if not ctx.org_id: raise HTTPException(403, "Nessuna org attiva")
        if not ctx.has_min_role('viewer'): raise HTTPException(403, "Ruolo minimo: viewer")
        return _ser(report_service.get_operations_report(
            ctx.org_id, month, limit, auth_type, operation, environment,
            partner_id, status, min_cost))

    # ── 8. Month comparison ──
    @app.get("/api/reports/comparison")
    def report_comparison(month_a: str = Query(None), month_b: str = Query(None), ctx=Depends(get_auth_context)):
        if not ctx.org_id: raise HTTPException(403, "Nessuna org attiva")
        if not ctx.has_min_role('finance'): raise HTTPException(403, "Ruolo minimo: finance")
        return _ser(report_service.get_month_comparison(ctx.org_id, month_a, month_b))

    # ── 9. Revenue share ──
    @app.get("/api/reports/revenue-share")
    def report_revenue_share(month: str = Query(None), ctx=Depends(get_auth_context)):
        if not ctx.org_id: raise HTTPException(403, "Nessuna org attiva")
        if not ctx.has_min_role('owner'): raise HTTPException(403, "Ruolo minimo: owner")
        return _ser(report_service.get_revenue_share_report(ctx.org_id, month))

    # ── 10. Top operations ──
    @app.get("/api/reports/top-operations")
    def report_top(month: str = Query(None), limit: int = Query(20, ge=1, le=100),
                   sort_by: str = Query('billable_amount_eur'), ctx=Depends(get_auth_context)):
        if not ctx.org_id: raise HTTPException(403, "Nessuna org attiva")
        if not ctx.has_min_role('finance'): raise HTTPException(403, "Ruolo minimo: finance")
        return _ser(report_service.get_top_operations(ctx.org_id, month, limit, sort_by))

    # ── 11. Platform profitability (admin only) ──
    @app.get("/api/reports/profitability")
    def report_profitability(month: str = Query(None), ctx=Depends(get_auth_context)):
        if not ctx.is_platform_admin: raise HTTPException(403, "Solo Platform Admin")
        return _ser(report_service.get_platform_profitability(month))

    # ── 12. CSV export ──
    @app.get("/api/reports/export/csv")
    def report_export_csv(
        report: str = Query(..., description="summary|auth|partners|ai|tags|operations|comparison|revenue_share|profitability|top"),
        month: str = Query(None),
        tag_key: str = Query(None),
        month_b: str = Query(None),
        auth_type: str = Query(None),
        operation: str = Query(None),
        partner_id: str = Query(None),
        status: str = Query(None),
        sort_by: str = Query(None),
        limit: int = Query(500),
        ctx=Depends(get_auth_context)
    ):
        if report == 'profitability':
            if not ctx.is_platform_admin: raise HTTPException(403, "Solo Platform Admin")
        elif report == 'revenue_share':
            if not ctx.org_id: raise HTTPException(403, "Nessuna org attiva")
            if not ctx.has_min_role('owner'): raise HTTPException(403, "Ruolo minimo: owner")
        else:
            if not ctx.org_id: raise HTTPException(403, "Nessuna org attiva")
            if not ctx.has_min_role('viewer'): raise HTTPException(403, "Ruolo minimo: viewer")

        org_id = ctx.org_id or 'platform'
        csv_content, filename = report_service.export_csv(
            report, org_id, month,
            tag_key=tag_key, month_b=month_b,
            auth_type=auth_type, operation=operation,
            partner_id=partner_id, status=status,
            sort_by=sort_by or 'billable_amount_eur', limit=limit)

        if not csv_content:
            raise HTTPException(404, "Nessun dato per questo report")

        return Response(
            content=csv_content,
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'}
        )

    # ── 13. Available months ──
    @app.get("/api/reports/available-months")
    def report_months(ctx=Depends(get_auth_context)):
        if not ctx.org_id: raise HTTPException(403, "Nessuna org attiva")
        try:
            from psycopg2.extras import RealDictCursor
            cur = report_service.conn.cursor(cursor_factory=RealDictCursor)
        except:
            cur = report_service.conn.cursor(cursor_factory=report_service.RDC)
        cur.execute("""
            SELECT DISTINCT billing_month FROM transformation_costs
            WHERE org_id=%s ORDER BY billing_month DESC LIMIT 24
        """, (ctx.org_id,))
        months = [r['billing_month'] for r in cur.fetchall()]
        return {"months": months}

    # ── 14. Operation types distribution ──
    @app.get("/api/reports/operation-types")
    def report_op_types(month: str = Query(None), ctx=Depends(get_auth_context)):
        if not ctx.org_id: raise HTTPException(403, "Nessuna org attiva")
        if not ctx.has_min_role('viewer'): raise HTTPException(403, "Ruolo minimo: viewer")
        from datetime import datetime as dt, timezone as tz
        month = month or dt.now(tz.utc).strftime('%Y-%m')
        try:
            from psycopg2.extras import RealDictCursor
            cur = report_service.conn.cursor(cursor_factory=RealDictCursor)
        except:
            cur = report_service.conn.cursor(cursor_factory=report_service.RDC)
        cur.execute("""
            SELECT operation, COUNT(*) as count,
                COALESCE(SUM(billable_amount_eur),0) as billable_eur,
                COALESCE(SUM(ai_cost_usd),0) as ai_cost_usd,
                COALESCE(AVG(duration_ms),0)::INTEGER as avg_ms
            FROM transformation_costs
            WHERE org_id=%s AND billing_month=%s AND environment='live'
            GROUP BY operation ORDER BY count DESC
        """, (ctx.org_id, month))
        return _ser({"month": month, "data": [dict(r) for r in cur.fetchall()]})

    # ── 15. Force re-aggregate ──
    @app.post("/api/reports/refresh")
    def report_refresh(month: str = Query(None), ctx=Depends(get_auth_context)):
        if not ctx.org_id: raise HTTPException(403, "Nessuna org attiva")
        if not ctx.has_min_role('admin'): raise HTTPException(403, "Ruolo minimo: admin")
        from datetime import datetime as dt, timezone as tz
        if report_service.cost_service:
            month = month or dt.now(tz.utc).strftime('%Y-%m')
            today = dt.now(tz.utc).strftime('%Y-%m-%d')
            report_service.cost_service.aggregate_period(ctx.org_id, 'daily', today)
            report_service.cost_service.aggregate_period(ctx.org_id, 'monthly', month)
            return {"ok": True, "month": month}
        raise HTTPException(500, "Cost service non disponibile")

    # ── 16. Exchange rate info ──
    @app.get("/api/reports/exchange-rate")
    def report_exchange_rate(ctx=Depends(get_auth_context)):
        rate = "0.92"
        if report_service.cost_service:
            rate = str(report_service.cost_service.get_usd_eur_rate())
        return {"usd_eur": rate}

    print("   ✅ Report API: 16 endpoints registered")
