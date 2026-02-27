#!/usr/bin/env python3
"""
Buddyliko — Schedule API (Phase 8 Part 2B) — 10 endpoint REST
"""
from pydantic import BaseModel, Field
from typing import Optional

class ScheduleCreate(BaseModel):
    name: str
    schedule_type: str = 'transform'
    cron_expr: str
    config: Optional[dict] = None
    timezone: str = 'Europe/Rome'
    max_runs: int = 0
    description: Optional[str] = None

class ScheduleUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    cron_expr: Optional[str] = None
    config: Optional[dict] = None
    timezone: Optional[str] = None
    max_runs: Optional[int] = None
    status: Optional[str] = None


def register_schedule_endpoints(app, get_auth_context, require_org_role, schedule_service):
    from fastapi import Depends, HTTPException, Query

    # 1. List schedules
    @app.get("/api/schedules")
    def list_schedules(status: str = Query(None), ctx=Depends(require_org_role('operator'))):
        return {"schedules": schedule_service.list_schedules(ctx.org_id, status)}

    # 2. Create schedule
    @app.post("/api/schedules")
    def create_schedule(data: ScheduleCreate, ctx=Depends(require_org_role('admin'))):
        try:
            sch = schedule_service.create_schedule(
                ctx.org_id, ctx.user_id, data.name, data.schedule_type,
                data.cron_expr, data.config, data.timezone, data.max_runs)
            return {"success": True, "schedule": sch}
        except ValueError as e:
            raise HTTPException(400, str(e))

    # 3. Get schedule detail
    @app.get("/api/schedules/{schedule_id}")
    def get_schedule(schedule_id: str, ctx=Depends(require_org_role('viewer'))):
        sch = schedule_service.get_schedule(schedule_id)
        if not sch:
            raise HTTPException(404, "Schedule non trovato")
        return sch

    # 4. Update schedule
    @app.put("/api/schedules/{schedule_id}")
    def update_schedule(schedule_id: str, data: ScheduleUpdate, ctx=Depends(require_org_role('admin'))):
        d = data.dict(exclude_none=True)
        if not d: raise HTTPException(400, "Nessun campo")
        ok = schedule_service.update_schedule(schedule_id, ctx.org_id, d)
        if not ok: raise HTTPException(404, "Schedule non trovato")
        return {"success": True}

    # 5. Delete schedule
    @app.delete("/api/schedules/{schedule_id}")
    def delete_schedule(schedule_id: str, ctx=Depends(require_org_role('admin'))):
        ok = schedule_service.delete_schedule(schedule_id, ctx.org_id)
        if not ok: raise HTTPException(404, "Schedule non trovato")
        return {"success": True}

    # 6. Pause
    @app.post("/api/schedules/{schedule_id}/pause")
    def pause_schedule(schedule_id: str, ctx=Depends(require_org_role('admin'))):
        ok = schedule_service.pause_schedule(schedule_id, ctx.org_id)
        if not ok: raise HTTPException(400, "Non pausabile")
        return {"success": True}

    # 7. Resume
    @app.post("/api/schedules/{schedule_id}/resume")
    def resume_schedule(schedule_id: str, ctx=Depends(require_org_role('admin'))):
        ok = schedule_service.resume_schedule(schedule_id, ctx.org_id)
        if not ok: raise HTTPException(400, "Non ripristinabile")
        return {"success": True}

    # 8. Trigger now
    @app.post("/api/schedules/{schedule_id}/trigger")
    def trigger_schedule(schedule_id: str, ctx=Depends(require_org_role('admin'))):
        try:
            schedule_service.trigger_now(schedule_id, ctx.org_id)
            return {"success": True, "message": "Esecuzione avviata"}
        except ValueError as e:
            raise HTTPException(400, str(e))

    # 9. Run history
    @app.get("/api/schedules/{schedule_id}/runs")
    def schedule_runs(schedule_id: str, limit: int = Query(50),
                      ctx=Depends(require_org_role('viewer'))):
        return {"runs": schedule_service.get_runs(schedule_id, limit)}

    # 10. Cron presets
    @app.get("/api/schedules/presets/list")
    def cron_presets(ctx=Depends(require_org_role('viewer'))):
        return {"presets": schedule_service.get_presets()}

    print("   ✅ Schedule API: 10 endpoints registered")
