#!/usr/bin/env python3
"""Buddyliko — Standards Library API — 8 endpoint REST"""

def register_standards_endpoints(app, get_auth_context, standards_service):
    from fastapi import Depends, HTTPException, Query

    @app.get("/api/standards/browse")
    def browse(q:str=Query(None),domain:str=Query(None),region:str=Query(None),
               format_type:str=Query(None),sort_by:str=Query('popularity'),
               page:int=Query(1),per_page:int=Query(30),ctx=Depends(get_auth_context)):
        return standards_service.browse(q,domain,region,format_type,sort_by,page,min(per_page,50))

    @app.get("/api/standards/domains")
    def domains(ctx=Depends(get_auth_context)):
        return {"domains": standards_service.get_domains()}

    @app.get("/api/standards/regions")
    def regions(ctx=Depends(get_auth_context)):
        return {"regions": standards_service.get_regions()}

    @app.get("/api/standards/stats")
    def stats(ctx=Depends(get_auth_context)):
        return standards_service.get_stats()

    @app.get("/api/standards/{slug_or_id}")
    def get_standard(slug_or_id:str,ctx=Depends(get_auth_context)):
        s=standards_service.get_standard(slug_or_id)
        if not s: raise HTTPException(404,"Standard non trovato")
        return s

    @app.get("/api/standards/{slug_or_id}/related")
    def related(slug_or_id:str,ctx=Depends(get_auth_context)):
        return {"related": standards_service.get_related(slug_or_id)}

    @app.get("/api/standards/domain/{domain}")
    def by_domain(domain:str,ctx=Depends(get_auth_context)):
        return {"standards": standards_service.get_by_domain(domain)}

    @app.post("/api/standards/seed")
    def seed(ctx=Depends(get_auth_context)):
        if not ctx.is_platform_admin:
            raise HTTPException(403,"Solo admin")
        return {"seeded": standards_service.seed_standards()}

    print("   ✅ Standards API: 8 endpoints registered")
