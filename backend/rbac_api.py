#!/usr/bin/env python3
"""
Buddyliko — RBAC & Approval API (Phase 5)
═════════════════════════════════════════
Endpoint per gestione permessi granulari e workflow di approvazione.

RBAC Endpoints:
    GET    /api/permissions                      — Catalogo permessi
    GET    /api/roles                            — Role templates disponibili
    GET    /api/roles/{id}                       — Dettaglio role template
    POST   /api/roles                            — Crea custom role template
    PUT    /api/roles/{id}                       — Modifica custom role template
    DELETE /api/roles/{id}                       — Elimina custom role template
    PUT    /api/org/members/{uid}/role-template   — Assegna role template a membro
    GET    /api/org/members/{uid}/permissions     — Permessi risolti per membro
    POST   /api/org/members/{uid}/overrides       — Imposta override permesso
    DELETE /api/org/members/{uid}/overrides/{pid}  — Rimuovi override
    GET    /api/auth/my-permissions               — I miei permessi nell'org corrente

Approval Endpoints:
    GET    /api/approvals                        — Lista richieste (tutte/pending)
    POST   /api/approvals                        — Crea richiesta (manuale)
    GET    /api/approvals/{id}                   — Dettaglio richiesta
    POST   /api/approvals/{id}/approve            — Approva
    POST   /api/approvals/{id}/reject             — Rigetta
    GET    /api/approvals/stats                   — Statistiche
    GET    /api/approvals/pending-count            — Conteggio pending (per badge topbar)
"""

from fastapi import HTTPException
from pydantic import BaseModel, Field
from typing import Optional, List


# ══════════════════════════════════════════════════════════════════
# PYDANTIC MODELS
# ══════════════════════════════════════════════════════════════════

class CreateRoleTemplate(BaseModel):
    name: str
    label: str
    description: Optional[str] = None
    permission_ids: List[int]

class UpdateRoleTemplate(BaseModel):
    label: Optional[str] = None
    description: Optional[str] = None
    permission_ids: Optional[List[int]] = None

class AssignRole(BaseModel):
    role_template_id: int

class SetOverride(BaseModel):
    permission_id: int
    granted: bool

class CreateApproval(BaseModel):
    operation: str
    payload: dict = {}

class RejectApproval(BaseModel):
    note: Optional[str] = None


# ══════════════════════════════════════════════════════════════════
# REGISTER
# ══════════════════════════════════════════════════════════════════

def register_rbac_api(app, permission_service, approval_service,
                      get_auth_context, require_org_role, require_permission):
    """Registra endpoint RBAC + Approval sull'app FastAPI."""

    from fastapi import Depends
    from org_middleware import OrgContext

    # ══════════════════════════════════════════════════════════════
    # RBAC: PERMISSIONS CATALOG
    # ══════════════════════════════════════════════════════════════

    @app.get("/api/permissions")
    async def list_permissions(ctx: OrgContext = Depends(get_auth_context)):
        """Catalogo di tutti i permessi disponibili, raggruppati per scope."""
        if not ctx.org_id and not ctx.is_platform_admin:
            raise HTTPException(403, "Richiede contesto org")
        return {
            "permissions": permission_service.list_permissions(),
            "grouped": permission_service.list_permissions_grouped()
        }

    # ══════════════════════════════════════════════════════════════
    # RBAC: ROLE TEMPLATES
    # ══════════════════════════════════════════════════════════════

    @app.get("/api/roles")
    async def list_roles(ctx: OrgContext = Depends(require_org_role('viewer'))):
        """Lista role templates (system + custom org)."""
        return {
            "roles": permission_service.list_role_templates(ctx.org_id)
        }

    @app.get("/api/roles/{template_id}")
    async def get_role(template_id: int, ctx: OrgContext = Depends(require_org_role('viewer'))):
        """Dettaglio role template con permessi."""
        rt = permission_service.get_role_template(template_id)
        if not rt:
            raise HTTPException(404, "Role template non trovato")
        return {"role": rt}

    @app.post("/api/roles")
    async def create_role(data: CreateRoleTemplate,
                          ctx: OrgContext = Depends(require_permission('settings', 'manage'))):
        """Crea un custom role template per l'org."""
        try:
            rt = permission_service.create_role_template(
                org_id=ctx.org_id,
                name=data.name,
                label=data.label,
                description=data.description,
                permission_ids=data.permission_ids,
                created_by=ctx.user_id
            )
            return {"success": True, "role": rt}
        except ValueError as e:
            raise HTTPException(400, str(e))

    @app.put("/api/roles/{template_id}")
    async def update_role(template_id: int, data: UpdateRoleTemplate,
                          ctx: OrgContext = Depends(require_permission('settings', 'manage'))):
        """Modifica un custom role template."""
        try:
            rt = permission_service.update_role_template(
                template_id=template_id,
                org_id=ctx.org_id,
                label=data.label,
                description=data.description,
                permission_ids=data.permission_ids
            )
            return {"success": True, "role": rt}
        except ValueError as e:
            raise HTTPException(400, str(e))

    @app.delete("/api/roles/{template_id}")
    async def delete_role(template_id: int,
                          ctx: OrgContext = Depends(require_permission('settings', 'manage'))):
        """Elimina un custom role template. Membri downgraded a viewer."""
        try:
            permission_service.delete_role_template(template_id, ctx.org_id)
            return {"success": True, "message": "Ruolo eliminato. Membri spostati a Viewer."}
        except ValueError as e:
            raise HTTPException(400, str(e))

    # ══════════════════════════════════════════════════════════════
    # RBAC: MEMBER PERMISSIONS
    # ══════════════════════════════════════════════════════════════

    @app.put("/api/org/members/{user_id}/role-template")
    async def assign_member_role(user_id: int, data: AssignRole,
                                 ctx: OrgContext = Depends(require_permission('members', 'manage'))):
        """Assegna un role template a un membro."""
        try:
            result = permission_service.assign_role(
                org_id=ctx.org_id,
                user_id=user_id,
                template_id=data.role_template_id,
                assigned_by=ctx.user_id
            )
            return result
        except ValueError as e:
            raise HTTPException(400, str(e))

    @app.get("/api/org/members/{user_id}/permissions")
    async def get_member_permissions(user_id: int,
                                     ctx: OrgContext = Depends(require_permission('members', 'view'))):
        """Ritorna i permessi risolti di un membro (template + overrides)."""
        member = permission_service.get_member_with_permissions(ctx.org_id, user_id)
        if not member:
            raise HTTPException(404, "Membro non trovato nell'org")
        return {"member": member}

    @app.post("/api/org/members/{user_id}/overrides")
    async def set_member_override(user_id: int, data: SetOverride,
                                  ctx: OrgContext = Depends(require_permission('members', 'manage'))):
        """Imposta un override permesso per un utente specifico."""
        try:
            result = permission_service.set_user_override(
                user_id=user_id,
                org_id=ctx.org_id,
                permission_id=data.permission_id,
                granted=data.granted,
                granted_by=ctx.user_id
            )
            return result
        except ValueError as e:
            raise HTTPException(400, str(e))

    @app.delete("/api/org/members/{user_id}/overrides/{permission_id}")
    async def remove_member_override(user_id: int, permission_id: int,
                                     ctx: OrgContext = Depends(require_permission('members', 'manage'))):
        """Rimuovi un override specifico (torna al default del ruolo)."""
        removed = permission_service.remove_user_override(user_id, ctx.org_id, permission_id)
        if not removed:
            raise HTTPException(400, "Override non trovato o errore")
        return {"success": True}

    @app.get("/api/auth/my-permissions")
    async def my_permissions(ctx: OrgContext = Depends(get_auth_context)):
        """I miei permessi nell'org corrente."""
        if not ctx.org_id:
            return {"permissions": [], "role": None, "context": "personal"}

        perms = sorted(ctx.permissions)
        return {
            "permissions": perms,
            "role": ctx.org_role,
            "org_id": ctx.org_id,
            "is_owner": ctx.is_org_owner,
            "is_platform_admin": ctx.is_platform_admin,
        }

    # ══════════════════════════════════════════════════════════════
    # APPROVAL WORKFLOW
    # ══════════════════════════════════════════════════════════════

    @app.get("/api/approvals")
    async def list_approvals(status: str = 'all',
                             ctx: OrgContext = Depends(require_org_role('viewer'))):
        """
        Lista richieste di approvazione.
        ?status=pending → solo pending (che l'utente può approvare)
        ?status=all → tutte (richiede admin/owner)
        """
        if status == 'pending':
            items = approval_service.list_pending(ctx.org_id, ctx.user_id)
        else:
            if not ctx.has_permission('settings', 'manage') and not ctx.is_org_admin:
                raise HTTPException(403, "Solo admin/owner possono vedere tutte le richieste")
            items = approval_service.list_all(ctx.org_id)
        return {"approvals": items}

    @app.post("/api/approvals")
    async def create_approval(data: CreateApproval,
                              ctx: OrgContext = Depends(require_org_role('viewer'))):
        """Crea una richiesta di approvazione manuale."""
        try:
            result = approval_service.create_request(
                org_id=ctx.org_id,
                requested_by=ctx.user_id,
                operation=data.operation,
                payload=data.payload
            )
            return {"success": True, "request": result}
        except ValueError as e:
            raise HTTPException(400, str(e))

    @app.get("/api/approvals/stats")
    async def approval_stats(ctx: OrgContext = Depends(require_org_role('viewer'))):
        """Statistiche richieste di approvazione."""
        stats = approval_service.get_stats(ctx.org_id)
        return stats

    @app.get("/api/approvals/pending-count")
    async def pending_count(ctx: OrgContext = Depends(get_auth_context)):
        """Conteggio rapido pending (per badge topbar). Funziona anche senza org per non bloccare."""
        if not ctx.org_id:
            return {"count": 0}
        count = approval_service.get_pending_count(ctx.org_id)
        return {"count": count}

    @app.get("/api/approvals/{request_id}")
    async def get_approval(request_id: str,
                           ctx: OrgContext = Depends(require_org_role('viewer'))):
        """Dettaglio singola richiesta."""
        req = approval_service.get_request(request_id)
        if not req:
            raise HTTPException(404, "Richiesta non trovata")
        if str(req['org_id']) != str(ctx.org_id) and not ctx.is_platform_admin:
            raise HTTPException(403, "Richiesta non appartiene a questa org")
        return {"request": req}

    @app.post("/api/approvals/{request_id}/approve")
    async def approve_request(request_id: str,
                              ctx: OrgContext = Depends(require_org_role('viewer'))):
        """Approva una richiesta pending."""
        try:
            result = approval_service.approve(
                request_id=request_id,
                approved_by=ctx.user_id,
                org_id=ctx.org_id
            )
            return {"success": True, "request": result}
        except ValueError as e:
            raise HTTPException(400, str(e))

    @app.post("/api/approvals/{request_id}/reject")
    async def reject_request(request_id: str, data: RejectApproval,
                             ctx: OrgContext = Depends(require_org_role('viewer'))):
        """Rigetta una richiesta pending."""
        try:
            result = approval_service.reject(
                request_id=request_id,
                rejected_by=ctx.user_id,
                org_id=ctx.org_id,
                note=data.note
            )
            return {"success": True, "request": result}
        except ValueError as e:
            raise HTTPException(400, str(e))
