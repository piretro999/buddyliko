#!/usr/bin/env python3
"""
Buddyliko — Token API Endpoints
Fase 1: REST API per gestione API token.

Endpoint:
    POST   /api/tokens                  — Crea token (ritorna valore UNA VOLTA)
    GET    /api/tokens                  — Lista token dell'org
    GET    /api/tokens/{id}             — Dettaglio token
    DELETE /api/tokens/{id}             — Revoca token
    POST   /api/tokens/{id}/rotate      — Ruota token
    GET    /api/tokens/{id}/audit       — Audit log del token
    GET    /api/tokens/audit            — Audit log di tutti i token dell'org
"""

from fastapi import Depends, HTTPException, Query
from pydantic import BaseModel, Field
from typing import Optional, List


class TokenCreate(BaseModel):
    name: str
    description: Optional[str] = None
    environment: str = 'live'
    scopes: Optional[List[str]] = None
    rate_limit_rpm: Optional[int] = None
    rate_limit_rph: Optional[int] = None
    rate_limit_rpd: Optional[int] = None
    allowed_ips: Optional[List[str]] = None
    partner_id: Optional[str] = None
    tags: Optional[dict] = None
    expires_in_days: Optional[int] = None

class TokenRevoke(BaseModel):
    reason: Optional[str] = None


def _serialize_token(t: dict) -> dict:
    """Serializza un token record per JSON response."""
    import json
    result = {}
    for k, v in t.items():
        if k == 'token_hash':
            continue  # Mai esporre l'hash
        if hasattr(v, 'isoformat'):
            result[k] = v.isoformat()
        elif isinstance(v, str) and k in ('scopes', 'tags', 'allowed_ips'):
            try:
                result[k] = json.loads(v)
            except (json.JSONDecodeError, TypeError):
                result[k] = v
        else:
            result[k] = str(v) if v is not None and not isinstance(v, (str, int, float, bool, list, dict)) else v
    return result


def register_token_api(app, token_service, get_auth_context, require_org_role):
    """Registra endpoint token sull'app FastAPI."""

    from org_middleware import OrgContext

    @app.post("/api/tokens")
    async def create_token(
        data: TokenCreate,
        ctx: OrgContext = Depends(require_org_role('admin'))
    ):
        """
        Crea un nuovo API token.
        ⚠️ Il valore completo del token viene mostrato UNA SOLA VOLTA in questa response.
        """
        try:
            full_token, record = token_service.create_token(
                org_id=ctx.org_id,
                name=data.name,
                description=data.description,
                environment=data.environment,
                created_by=ctx.user_id,
                scopes=data.scopes,
                rate_limit_rpm=data.rate_limit_rpm,
                rate_limit_rph=data.rate_limit_rph,
                rate_limit_rpd=data.rate_limit_rpd,
                allowed_ips=data.allowed_ips,
                partner_id=data.partner_id,
                tags=data.tags,
                expires_in_days=data.expires_in_days,
            )
            return {
                "success": True,
                "token_value": full_token,
                "warning": "Copia questo token ora. Non potrai rivederlo.",
                "token": _serialize_token(record),
            }
        except ValueError as e:
            raise HTTPException(400, str(e))

    @app.get("/api/tokens")
    async def list_tokens(
        include_revoked: bool = Query(False),
        ctx: OrgContext = Depends(require_org_role('developer'))
    ):
        """Lista tutti i token dell'organizzazione."""
        tokens = token_service.list_tokens(ctx.org_id, include_revoked=include_revoked)
        return {
            "org_id": ctx.org_id,
            "tokens": [_serialize_token(t) for t in tokens],
        }

    @app.get("/api/tokens/{token_id}")
    async def get_token(
        token_id: str,
        ctx: OrgContext = Depends(require_org_role('developer'))
    ):
        """Dettaglio di un singolo token."""
        t = token_service.get_token(token_id)
        if not t:
            raise HTTPException(404, "Token non trovato")
        if str(t['org_id']) != ctx.org_id and not ctx.is_platform_admin:
            raise HTTPException(403, "Token non appartiene alla tua org")
        return {"token": _serialize_token(t)}

    @app.delete("/api/tokens/{token_id}")
    async def revoke_token(
        token_id: str,
        data: TokenRevoke = None,
        ctx: OrgContext = Depends(require_org_role('admin'))
    ):
        """Revoca un token."""
        # Verifica che il token appartenga all'org
        t = token_service.get_token(token_id)
        if not t:
            raise HTTPException(404, "Token non trovato")
        if str(t['org_id']) != ctx.org_id and not ctx.is_platform_admin:
            raise HTTPException(403, "Token non appartiene alla tua org")

        reason = data.reason if data else None
        ok = token_service.revoke_token(token_id, ctx.user_id, reason)
        if not ok:
            raise HTTPException(400, "Token già revocato o non trovato")
        return {"success": True, "message": "Token revocato"}

    @app.post("/api/tokens/{token_id}/rotate")
    async def rotate_token(
        token_id: str,
        ctx: OrgContext = Depends(require_org_role('admin'))
    ):
        """
        Ruota un token: revoca il vecchio, crea uno nuovo con stesse configurazioni.
        ⚠️ Il nuovo valore viene mostrato UNA SOLA VOLTA.
        """
        t = token_service.get_token(token_id)
        if not t:
            raise HTTPException(404, "Token non trovato")
        if str(t['org_id']) != ctx.org_id and not ctx.is_platform_admin:
            raise HTTPException(403, "Token non appartiene alla tua org")

        try:
            new_token, new_record = token_service.rotate_token(token_id, ctx.user_id)
            return {
                "success": True,
                "token_value": new_token,
                "warning": "Copia questo nuovo token ora. Non potrai rivederlo.",
                "token": _serialize_token(new_record),
                "old_token_id": token_id,
            }
        except ValueError as e:
            raise HTTPException(400, str(e))

    @app.get("/api/tokens/{token_id}/audit")
    async def get_token_audit(
        token_id: str,
        limit: int = Query(50, le=500),
        ctx: OrgContext = Depends(require_org_role('admin'))
    ):
        """Audit log di un singolo token."""
        t = token_service.get_token(token_id)
        if not t:
            raise HTTPException(404, "Token non trovato")
        if str(t['org_id']) != ctx.org_id and not ctx.is_platform_admin:
            raise HTTPException(403, "Token non appartiene alla tua org")

        logs = token_service.get_audit_log(ctx.org_id, token_id=token_id, limit=limit)
        return {
            "token_id": token_id,
            "audit_log": [
                {
                    'event_type': l['event_type'],
                    'ip_address': l.get('ip_address'),
                    'endpoint': l.get('endpoint'),
                    'actor_user_id': l.get('actor_user_id'),
                    'details': l.get('details', {}),
                    'created_at': l['created_at'].isoformat() if l.get('created_at') else None,
                }
                for l in logs
            ],
        }

    @app.get("/api/tokens/audit")
    async def get_org_token_audit(
        limit: int = Query(50, le=500),
        ctx: OrgContext = Depends(require_org_role('admin'))
    ):
        """Audit log di tutti i token dell'org."""
        logs = token_service.get_audit_log(ctx.org_id, limit=limit)
        return {
            "org_id": ctx.org_id,
            "audit_log": [
                {
                    'token_id': str(l['token_id']),
                    'event_type': l['event_type'],
                    'ip_address': l.get('ip_address'),
                    'endpoint': l.get('endpoint'),
                    'details': l.get('details', {}),
                    'created_at': l['created_at'].isoformat() if l.get('created_at') else None,
                }
                for l in logs
            ],
        }

    print("   ✅ Token API endpoints registered (7 endpoints)")
