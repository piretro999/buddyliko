#!/usr/bin/env python3
"""
Buddyliko — Webhook API (Phase 8A) — 10 endpoint REST
"""
from pydantic import BaseModel, Field
from typing import Optional, List

class WebhookCreate(BaseModel):
    name: str
    url: str
    events: List[str]
    secret: Optional[str] = None
    headers: Optional[dict] = None

class WebhookUpdate(BaseModel):
    name: Optional[str] = None
    url: Optional[str] = None
    events: Optional[List[str]] = None
    secret: Optional[str] = None
    headers: Optional[dict] = None
    is_active: Optional[bool] = None


def register_webhook_endpoints(app, get_auth_context, require_org_role, webhook_service):
    from fastapi import Depends, HTTPException, Query

    # 1. List webhooks
    @app.get("/api/webhooks")
    def list_webhooks(ctx=Depends(require_org_role('admin'))):
        return {"webhooks": webhook_service.list_webhooks(ctx.org_id)}

    # 2. Create webhook
    @app.post("/api/webhooks")
    def create_webhook(data: WebhookCreate, ctx=Depends(require_org_role('admin'))):
        try:
            wh = webhook_service.create_webhook(
                ctx.org_id, ctx.user_id, data.name, data.url,
                data.events, data.secret, data.headers)
            return {"success": True, "webhook": wh}
        except ValueError as e:
            raise HTTPException(400, str(e))

    # 3. Get webhook detail
    @app.get("/api/webhooks/{webhook_id}")
    def get_webhook(webhook_id: str, ctx=Depends(require_org_role('admin'))):
        wh = webhook_service.get_webhook(webhook_id)
        if not wh or str(wh.get('org_id', '')) != str(ctx.org_id):
            raise HTTPException(404, "Webhook non trovato")
        return wh

    # 4. Update webhook
    @app.put("/api/webhooks/{webhook_id}")
    def update_webhook(webhook_id: str, data: WebhookUpdate, ctx=Depends(require_org_role('admin'))):
        d = data.dict(exclude_none=True)
        if not d: raise HTTPException(400, "Nessun campo")
        ok = webhook_service.update_webhook(webhook_id, ctx.org_id, d)
        if not ok: raise HTTPException(404, "Webhook non trovato")
        return {"success": True}

    # 5. Delete webhook
    @app.delete("/api/webhooks/{webhook_id}")
    def delete_webhook(webhook_id: str, ctx=Depends(require_org_role('admin'))):
        ok = webhook_service.delete_webhook(webhook_id, ctx.org_id)
        if not ok: raise HTTPException(404, "Webhook non trovato")
        return {"success": True}

    # 6. Test webhook
    @app.post("/api/webhooks/{webhook_id}/test")
    def test_webhook(webhook_id: str, ctx=Depends(require_org_role('admin'))):
        try:
            webhook_service.test_webhook(webhook_id, ctx.org_id)
            return {"success": True, "message": "Test event inviato"}
        except ValueError as e:
            raise HTTPException(400, str(e))

    # 7. Delivery log (per webhook)
    @app.get("/api/webhooks/{webhook_id}/deliveries")
    def webhook_deliveries(webhook_id: str, limit: int = Query(50, ge=1, le=200),
                           ctx=Depends(require_org_role('admin'))):
        return {"deliveries": webhook_service.get_deliveries(ctx.org_id, webhook_id, limit)}

    # 8. All deliveries (org-wide)
    @app.get("/api/webhooks/deliveries/all")
    def all_deliveries(limit: int = Query(50, ge=1, le=200),
                       ctx=Depends(require_org_role('admin'))):
        return {"deliveries": webhook_service.get_deliveries(ctx.org_id, None, limit)}

    # 9. Available events
    @app.get("/api/webhooks/events/list")
    def available_events(ctx=Depends(require_org_role('admin'))):
        return {"events": webhook_service.get_available_events()}

    # 10. Fire event (admin/internal testing)
    @app.post("/api/webhooks/fire-test")
    def fire_test_event(ctx=Depends(require_org_role('admin'))):
        webhook_service.fire_event(ctx.org_id, 'test.ping', {
            'message': 'Manual test event', 'source': 'api'})
        return {"success": True}

    print("   ✅ Webhook API: 10 endpoints registered")
