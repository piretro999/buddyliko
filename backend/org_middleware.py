#!/usr/bin/env python3
"""
Buddyliko — Organization Auth Middleware (v3 — Phase 5: RBAC Granulare)
Gestisce autenticazione duale: JWT utenti + API token (blk_...).
Aggiunge: permission resolution via PermissionService.

Cambiamenti vs v2:
- OrgContext.permissions: set di permessi risolti (lazy)
- has_permission(scope, action): check granulare
- require_permission(scope, action): dependency FastAPI
- Backward compat: has_min_role() funziona ancora
"""

from dataclasses import dataclass, field
from typing import Optional, List, Set
from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials


@dataclass
class OrgContext:
    """Contesto di autenticazione con org info + permessi granulari."""
    auth_type: str = 'user'
    user_id: Optional[int] = None
    email: Optional[str] = None
    name: Optional[str] = None
    platform_role: Optional[str] = None
    org_id: Optional[str] = None
    org_name: Optional[str] = None
    org_slug: Optional[str] = None
    org_type: Optional[str] = None
    org_role: Optional[str] = None
    org_plan: Optional[str] = None
    org_status: Optional[str] = None
    environment: str = 'live'
    context: str = 'personal'       # 'personal' | 'org'
    token_id: Optional[str] = None
    token_name: Optional[str] = None
    scopes: List[str] = field(default_factory=list)
    partner_id: Optional[str] = None
    tags: dict = field(default_factory=dict)
    # Phase 5: permessi granulari (risolti lazy da PermissionService)
    _permissions: Optional[Set[str]] = field(default=None, repr=False)
    _permission_service: object = field(default=None, repr=False)

    @property
    def permissions(self) -> Set[str]:
        """Lazy-load permessi granulari."""
        if self._permissions is not None:
            return self._permissions
        if self._permission_service and self.user_id and self.org_id:
            self._permissions = self._permission_service.get_user_permissions(
                self.user_id, self.org_id
            )
        elif self.is_platform_admin:
            self._permissions = set()  # platform admin bypassa tutto
        else:
            self._permissions = set()
        return self._permissions

    @property
    def is_org_admin(self) -> bool:
        return self.org_role in ('owner', 'admin')

    @property
    def is_org_owner(self) -> bool:
        return self.org_role == 'owner'

    @property
    def is_platform_admin(self) -> bool:
        return self.platform_role in ('MASTER', 'ADMIN')

    @property
    def is_sandbox(self) -> bool:
        return self.environment == 'sandbox'

    def has_scope(self, scope: str) -> bool:
        """Check scope per API token."""
        if self.auth_type == 'user':
            return True
        return scope in self.scopes

    def has_min_role(self, min_role: str) -> bool:
        """Backward compat: check ruolo gerarchico."""
        from org_service import ORG_ROLE_HIERARCHY
        user_level = ORG_ROLE_HIERARCHY.get(self.org_role, 0)
        min_level = ORG_ROLE_HIERARCHY.get(min_role, 0)
        return user_level >= min_level

    def has_permission(self, scope: str, action: str) -> bool:
        """Check permesso granulare. Platform admin bypassa."""
        if self.is_platform_admin:
            return True
        if self.org_role == 'owner':
            return True  # owner ha tutto
        return f"{scope}:{action}" in self.permissions

    def has_any_permission(self, checks: List[str]) -> bool:
        """Check se ha almeno uno dei permessi (OR). checks: ["scope:action", ...]"""
        if self.is_platform_admin or self.org_role == 'owner':
            return True
        return any(c in self.permissions for c in checks)


def create_auth_dependencies(auth_manager, storage, org_service,
                             token_service=None, permission_service=None):
    """
    Crea le dependencies FastAPI per autenticazione con org context.
    Se token_service è fornito, supporta anche autenticazione via API token.
    Se permission_service è fornito, risolve permessi granulari.
    """
    import json

    security = HTTPBearer(auto_error=False)
    AUTH_ENABLED = auth_manager is not None

    def get_auth_context(
        credentials: HTTPAuthorizationCredentials = Depends(security),
        request: Request = None
    ) -> OrgContext:

        if not AUTH_ENABLED:
            return OrgContext(
                auth_type='user', user_id=0, email='anonymous@local',
                name='Anonymous', platform_role='MASTER', org_id=None,
                org_role='owner', org_plan='ENTERPRISE', environment='live'
            )

        if not credentials:
            raise HTTPException(401, "Authentication required")

        token = credentials.credentials

        # ── API TOKEN (blk_...) ──
        if token.startswith('blk_'):
            if not token_service:
                raise HTTPException(501, "API token non ancora configurato")

            ip = None
            ua = None
            ep = None
            if request:
                ip = request.client.host if request.client else None
                ua = request.headers.get('user-agent', '')[:500]
                ep = f"{request.method} {request.url.path}"

            try:
                rec = token_service.verify_token(
                    token, ip_address=ip, user_agent=ua, endpoint=ep
                )
            except ValueError as e:
                raise HTTPException(401, str(e))

            scopes = rec.get('scopes', [])
            if isinstance(scopes, str):
                scopes = json.loads(scopes)

            tags = rec.get('tags', {})
            if isinstance(tags, str):
                tags = json.loads(tags)

            env = rec.get('environment', 'live')

            return OrgContext(
                auth_type='api_token',
                org_id=str(rec['org_id']),
                org_name=rec.get('org_name'),
                org_slug=rec.get('org_slug'),
                org_type=rec.get('org_type'),
                org_role='developer',  # Token hanno ruolo developer di default
                org_plan=rec.get('org_plan', 'FREE'),
                org_status=rec.get('org_status', 'active'),
                environment=env,
                token_id=str(rec['id']),
                token_name=rec.get('name'),
                scopes=scopes,
                partner_id=str(rec['partner_id']) if rec.get('partner_id') else None,
                tags=tags,
            )

        # ── JWT (utente umano) ──
        valid, payload = auth_manager.verify_token(token)
        if not valid:
            raise HTTPException(401, "Invalid or expired token")

        user_id = payload.get('user_id') or payload.get('id')
        if not user_id:
            raise HTTPException(401, "Token senza user_id")
        user_id = int(user_id)

        db_user = storage.get_user(str(user_id))
        if not db_user:
            raise HTTPException(401, "Utente non trovato")
        if db_user.get('status') in ('SUSPENDED', 'BLOCKED'):
            raise HTTPException(403, "Account sospeso o bloccato")

        # ── Contesto personale vs org (Fase 1 multi-tenant) ──
        requested_context = None
        if request:
            requested_context = request.headers.get('x-context', '').lower().strip() or None
        if not requested_context:
            requested_context = payload.get('context')

        org_id = None
        org_role = None
        org_data = {}
        context = 'personal'

        if requested_context == 'personal':
            org_id = None
            context = 'personal'
        else:
            jwt_org_id = payload.get('org_id')
            if jwt_org_id:
                org_id = jwt_org_id
            if not org_id:
                org_id = db_user.get('default_org_id')
            if not org_id:
                if requested_context != 'org':
                    user_orgs = org_service.get_user_orgs(user_id)
                    if user_orgs:
                        org_id = str(user_orgs[0]['id'])

            if org_id:
                org_id = str(org_id)
                org_data = org_service.get_org(org_id) or {}
                member = org_service.get_member(org_id, user_id)
                if member:
                    org_role = member.get('role', 'viewer')
                    context = 'org'
                else:
                    user_orgs = org_service.get_user_orgs(user_id)
                    if user_orgs:
                        org_id = str(user_orgs[0]['id'])
                        org_data = org_service.get_org(org_id) or {}
                        org_role = user_orgs[0].get('my_role', 'viewer')
                        context = 'org'
                    else:
                        org_id = None
                        org_data = {}
                        org_role = None

        return OrgContext(
            auth_type='user',
            user_id=user_id,
            email=db_user.get('email'),
            name=db_user.get('name'),
            platform_role=db_user.get('role', 'USER'),
            org_id=org_id,
            org_name=org_data.get('name'),
            org_slug=org_data.get('slug'),
            org_type=org_data.get('org_type'),
            org_role=org_role,
            org_plan=org_data.get('plan', 'FREE'),
            org_status=org_data.get('status', 'active'),
            environment=payload.get('environment', 'live'),
            context=context,
            _permission_service=permission_service,
        )

    def require_org_role(min_role: str):
        """Backward compat: require ruolo gerarchico minimo."""
        def _check(ctx: OrgContext = Depends(get_auth_context)) -> OrgContext:
            if ctx.is_platform_admin:
                return ctx
            if not ctx.org_id:
                raise HTTPException(403, "Nessuna organizzazione attiva. Creane una o accetta un invito.")
            if not ctx.has_min_role(min_role):
                raise HTTPException(
                    403, f"Ruolo minimo richiesto: {min_role}, il tuo: {ctx.org_role}"
                )
            return ctx
        return _check

    def require_scope(scope: str):
        """Require API token scope."""
        def _check(ctx: OrgContext = Depends(get_auth_context)) -> OrgContext:
            if not ctx.has_scope(scope):
                raise HTTPException(403, f"Scope richiesto: {scope}")
            return ctx
        return _check

    def require_permission(scope: str, action: str):
        """
        Phase 5: require permesso granulare.
        Usa PermissionService per risolvere role_template + custom overrides.
        Fallback: se PermissionService non disponibile, usa require_org_role.
        """
        def _check(ctx: OrgContext = Depends(get_auth_context)) -> OrgContext:
            if ctx.is_platform_admin:
                return ctx
            if not ctx.org_id:
                raise HTTPException(403, "Nessuna organizzazione attiva.")
            if not ctx.has_permission(scope, action):
                raise HTTPException(
                    403,
                    f"Permesso richiesto: {scope}:{action}. "
                    f"Il tuo ruolo ({ctx.org_role}) non include questo permesso."
                )
            return ctx
        return _check

    def get_current_user_compat(
        credentials: HTTPAuthorizationCredentials = Depends(security)
    ) -> dict:
        ctx = get_auth_context(credentials)
        return {
            'id': str(ctx.user_id or ''),
            'user_id': str(ctx.user_id or ''),
            'email': ctx.email,
            'name': ctx.name,
            'role': ctx.platform_role,
            'status': 'APPROVED',
            'plan': ctx.org_plan,
            'org_id': ctx.org_id,
            'org_role': ctx.org_role,
            'org_plan': ctx.org_plan,
            'org_type': ctx.org_type,
            'environment': ctx.environment,
            'context': ctx.context,
            '_auth_type': ctx.auth_type,
            '_partner_id': ctx.partner_id,
            '_tags': ctx.tags,
            '_token_id': ctx.token_id,
        }

    return get_auth_context, require_org_role, require_scope, get_current_user_compat, require_permission
