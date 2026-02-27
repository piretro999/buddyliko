#!/usr/bin/env python3
"""
Buddyliko - Groups & Files API Endpoints
Chiamare register_groups_api(app, ...) da api.py dopo la definizione di get_current_user.
"""

from fastapi import HTTPException, Depends, UploadFile, File, Form, Query
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import List, Optional, Dict
from datetime import datetime, timedelta
import os
import uuid
import json


# ===========================================================================
# PYDANTIC MODELS
# ===========================================================================

class GroupCreate(BaseModel):
    name: str
    description: Optional[str] = ""
    parent_id: Optional[str] = None

class GroupUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None

class MemberAdd(BaseModel):
    user_id: str
    role: str = "member"  # owner / admin / member / viewer

class MemberRoleUpdate(BaseModel):
    role: str

class PermissionSet(BaseModel):
    file_id: str
    user_id: Optional[str] = None
    group_id: Optional[str] = None
    can_view: bool = True
    can_download: bool = False
    can_copy: bool = False
    can_edit: bool = False
    can_delete: bool = False

class ShareLinkCreate(BaseModel):
    file_id: str
    expires_hours: int = 168
    max_uses: Optional[int] = None
    note: Optional[str] = ""

class FileUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    is_common: Optional[bool] = None
    is_public: Optional[bool] = None
    group_id: Optional[str] = None

class WorkspaceProjectSave(BaseModel):
    name: str
    description: Optional[str] = ""
    group_id: Optional[str] = None
    project_data: dict  # full JSON project from app.html

class InvitationCreate(BaseModel):
    email: str
    role: str = "member"
    message: Optional[str] = ""

class GroupSettingsUpdate(BaseModel):
    naming_rule: Optional[str] = None
    default_output_schema: Optional[str] = None
    shared_schemas: Optional[List[str]] = None
    shared_formulas: Optional[List[dict]] = None
    max_members: Optional[int] = None


# ===========================================================================
# MAIN REGISTRATION FUNCTION
# ===========================================================================

def register_groups_api(app, group_storage, permission_checker,
                        get_current_user, get_optional_user,
                        UPLOAD_DIR: str, BASE_URL: str):
    """Registra tutte le route groups/files sull'app FastAPI principale."""

    os.makedirs(UPLOAD_DIR, exist_ok=True)

    # ------------------------------------------------------------------
    # HELPER: verifica ruolo minimo in un gruppo
    # ------------------------------------------------------------------
    def _require_group_role(group_id: str, user: Dict, min_role: str) -> bool:
        if user.get('role') in ('MASTER', 'ADMIN'):
            return True
        members = group_storage.get_group_members(group_id)
        hierarchy = ['viewer', 'member', 'admin', 'owner']
        for m in members:
            if str(m['user_id']) == str(user['id']):
                try:
                    return hierarchy.index(m['role']) >= hierarchy.index(min_role)
                except ValueError:
                    return False
        return False

    def _is_admin(user: Dict) -> bool:
        return user.get('role') in ('MASTER', 'ADMIN')

    # ==================================================================
    # GROUPS
    # ==================================================================

    @app.get("/api/groups")
    async def list_groups(
        parent_id: Optional[str] = None,
        tree: bool = False,
        user=Depends(get_current_user)
    ):
        uid = str(user['id'])
        admin = _is_admin(user)
        if tree:
            all_groups = group_storage.get_group_tree()
            if not admin:
                accessible = set(group_storage.get_user_accessible_group_ids(uid))
                all_groups = [g for g in all_groups if g['id'] in accessible]
            return {"groups": _build_tree(all_groups)}
        groups = group_storage.list_groups(parent_id=parent_id)
        if not admin:
            accessible = set(group_storage.get_user_accessible_group_ids(uid))
            groups = [g for g in groups if g['id'] in accessible]
        return {"groups": groups}

    @app.post("/api/groups")
    async def create_group(data: GroupCreate, user=Depends(get_current_user)):
        uid = str(user['id'])
        admin = _is_admin(user)
        if data.parent_id is None and not admin:
            raise HTTPException(403, "Solo gli admin possono creare gruppi radice")
        if data.parent_id and not _require_group_role(data.parent_id, user, 'admin'):
            raise HTTPException(403, "Serve ruolo admin nel gruppo padre per creare sottogruppi")
        gid = group_storage.create_group({
            'name': data.name,
            'description': data.description,
            'parent_id': data.parent_id,
            'owner_id': uid
        })
        # Auto-add creator as owner member
        group_storage.add_group_member(gid, uid, 'owner', invited_by=uid)
        return {"success": True, "group_id": gid}

    @app.get("/api/groups/{group_id}")
    async def get_group(group_id: str, user=Depends(get_current_user)):
        group = group_storage.get_group(group_id)
        if not group:
            raise HTTPException(404, "Gruppo non trovato")
        uid = str(user['id'])
        if not _is_admin(user):
            accessible = set(group_storage.get_user_accessible_group_ids(uid))
            if group_id not in accessible:
                raise HTTPException(403, "Accesso negato")
        return {"group": group}

    @app.put("/api/groups/{group_id}")
    async def update_group(group_id: str, data: GroupUpdate, user=Depends(get_current_user)):
        if not _require_group_role(group_id, user, 'admin'):
            raise HTTPException(403, "Serve ruolo admin")
        updates = {k: v for k, v in data.dict().items() if v is not None}
        ok = group_storage.update_group(group_id, updates)
        return {"success": ok}

    @app.delete("/api/groups/{group_id}")
    async def delete_group(group_id: str, user=Depends(get_current_user)):
        if not _require_group_role(group_id, user, 'owner'):
            raise HTTPException(403, "Serve ruolo owner per eliminare il gruppo")
        ok = group_storage.delete_group(group_id)
        return {"success": ok}

    # ==================================================================
    # MEMBERS
    # ==================================================================

    @app.get("/api/groups/{group_id}/members")
    async def get_members(group_id: str, user=Depends(get_current_user)):
        uid = str(user['id'])
        if not _is_admin(user):
            accessible = set(group_storage.get_user_accessible_group_ids(uid))
            if group_id not in accessible:
                raise HTTPException(403, "Accesso negato")
        members = group_storage.get_group_members(group_id)
        return {"members": members}

    @app.post("/api/groups/{group_id}/members")
    async def add_member(group_id: str, data: MemberAdd, user=Depends(get_current_user)):
        if not _require_group_role(group_id, user, 'admin'):
            raise HTTPException(403, "Serve ruolo admin per aggiungere membri")
        ok = group_storage.add_group_member(
            group_id, data.user_id, data.role, invited_by=str(user['id'])
        )
        return {"success": ok}

    @app.put("/api/groups/{group_id}/members/{user_id}")
    async def update_member_role(
        group_id: str, user_id: str,
        data: MemberRoleUpdate, user=Depends(get_current_user)
    ):
        if not _require_group_role(group_id, user, 'admin'):
            raise HTTPException(403, "Serve ruolo admin")
        ok = group_storage.update_member_role(group_id, user_id, data.role)
        return {"success": ok}

    @app.delete("/api/groups/{group_id}/members/{user_id}")
    async def remove_member(group_id: str, user_id: str, user=Depends(get_current_user)):
        uid = str(user['id'])
        # Ognuno può rimuovere se stesso; admin può rimuovere altri
        if uid != user_id and not _require_group_role(group_id, user, 'admin'):
            raise HTTPException(403, "Serve ruolo admin per rimuovere altri membri")
        ok = group_storage.remove_group_member(group_id, user_id)
        return {"success": ok}

    @app.get("/api/users/me/groups")
    async def my_groups(user=Depends(get_current_user)):
        groups = group_storage.get_user_groups(str(user['id']))
        return {"groups": groups}

    # ==================================================================
    # FILES - LIST / UPLOAD / GET / UPDATE / DELETE
    # ==================================================================

    @app.get("/api/workspace/files")
    async def list_workspace_files(
        file_type: Optional[str] = None,
        scope: str = "all",
        group_id: Optional[str] = None,
        user=Depends(get_current_user)
    ):
        uid = str(user['id'])
        admin = _is_admin(user)

        if scope == "common":
            files = group_storage.list_files(is_common=True, file_type=file_type)
        elif scope == "mine":
            files = group_storage.list_files(owner_id=uid, file_type=file_type)
        elif scope == "group" and group_id:
            if not admin:
                accessible = set(group_storage.get_user_accessible_group_ids(uid))
                if group_id not in accessible:
                    raise HTTPException(403, "Accesso negato a questo gruppo")
            files = group_storage.list_files(group_id=group_id, file_type=file_type)
        else:
            files = permission_checker.get_visible_files(uid, admin, file_type)

        # Aggiungi info permessi per ogni file
        user_group_ids = group_storage.get_user_accessible_group_ids(uid)
        for f in files:
            if admin or str(f.get('owner_id')) == uid:
                f['permissions'] = {
                    'can_view': True, 'can_download': True,
                    'can_copy': True, 'can_edit': True, 'can_delete': True
                }
            elif f.get('is_common'):
                f['permissions'] = {
                    'can_view': True, 'can_download': False,
                    'can_copy': True, 'can_edit': False, 'can_delete': False
                }
            else:
                f['permissions'] = group_storage.get_effective_permission(
                    f['id'], uid, user_group_ids
                )
        return {"files": files, "count": len(files)}

    @app.post("/api/workspace/upload")
    async def upload_file(
        file: UploadFile = File(...),
        file_type: str = Form(...),
        group_id: Optional[str] = Form(None),
        description: Optional[str] = Form(""),
        is_common: bool = Form(False),
        user=Depends(get_current_user)
    ):
        uid = str(user['id'])
        admin = _is_admin(user)
        if is_common and not admin:
            raise HTTPException(403, "Solo gli admin possono caricare file comuni")

        valid_types = ['schema', 'project', 'csv', 'example']
        if file_type not in valid_types:
            raise HTTPException(400, f"file_type deve essere uno di: {valid_types}")

        # Salva su disco
        file_id = str(uuid.uuid4())
        original_name = file.filename or file_id
        ext = os.path.splitext(original_name)[1]
        type_dir = os.path.join(UPLOAD_DIR, file_type)
        os.makedirs(type_dir, exist_ok=True)
        storage_path = os.path.join(type_dir, f"{file_id}{ext}")

        content = await file.read()
        with open(storage_path, 'wb') as fout:
            fout.write(content)
        file_size = len(content)

        # Salva metadati nel DB
        fid = group_storage.create_file({
            'name': original_name,
            'description': description or '',
            'file_type': file_type,
            'owner_id': uid,
            'group_id': group_id or None,
            'is_common': is_common,
            'storage_path': storage_path,
            'file_size': file_size,
            'mime_type': file.content_type or '',
        })

        # Permesso automatico sul gruppo se specificato
        if group_id:
            group_storage.set_permission({
                'file_id': fid, 'group_id': group_id,
                'can_view': True, 'can_download': True,
                'can_copy': True, 'can_edit': False, 'can_delete': False
            })

        return {"success": True, "file_id": fid, "name": original_name, "size": file_size}

    @app.get("/api/workspace/files/{file_id}")
    async def get_file_info(file_id: str, user=Depends(get_current_user)):
        file = group_storage.get_file(file_id)
        if not file:
            raise HTTPException(404, "File non trovato")
        uid = str(user['id'])
        if not permission_checker.check(file_id, uid, 'view', _is_admin(user)):
            raise HTTPException(403, "Accesso negato")
        return {"file": file}

    @app.put("/api/workspace/files/{file_id}")
    async def update_file(file_id: str, data: FileUpdate, user=Depends(get_current_user)):
        uid = str(user['id'])
        if not permission_checker.check(file_id, uid, 'edit', _is_admin(user)):
            raise HTTPException(403, "Permesso di modifica non disponibile")
        updates = {k: v for k, v in data.dict().items() if v is not None}
        ok = group_storage.update_file(file_id, updates)
        return {"success": ok}

    @app.delete("/api/workspace/files/{file_id}")
    async def delete_file(file_id: str, user=Depends(get_current_user)):
        uid = str(user['id'])
        if not permission_checker.check(file_id, uid, 'delete', _is_admin(user)):
            raise HTTPException(403, "Permesso di eliminazione non disponibile")
        file = group_storage.get_file(file_id)
        # Elimina dal disco
        if file and file.get('storage_path') and os.path.exists(file['storage_path']):
            try:
                os.remove(file['storage_path'])
            except OSError:
                pass
        ok = group_storage.delete_file(file_id)
        return {"success": ok}

    @app.get("/api/workspace/files/{file_id}/download")
    async def download_file(file_id: str, user=Depends(get_current_user)):
        uid = str(user['id'])
        if not permission_checker.check(file_id, uid, 'download', _is_admin(user)):
            raise HTTPException(403, "Permesso di download non disponibile")
        file = group_storage.get_file(file_id)
        if not file:
            raise HTTPException(404, "File non trovato")
        path = file.get('storage_path', '')
        if not path or not os.path.exists(path):
            raise HTTPException(404, "File non trovato su disco")
        return FileResponse(
            path=path,
            filename=file['name'],
            media_type=file.get('mime_type') or 'application/octet-stream'
        )

    @app.post("/api/workspace/files/{file_id}/copy")
    async def copy_file(
        file_id: str,
        group_id: Optional[str] = None,
        user=Depends(get_current_user)
    ):
        uid = str(user['id'])
        if not permission_checker.check(file_id, uid, 'copy', _is_admin(user)):
            raise HTTPException(403, "Permesso di copia non disponibile")

        original = group_storage.get_file(file_id)
        if not original:
            raise HTTPException(404, "File non trovato")

        # Copia fisica su disco
        new_fid = str(uuid.uuid4())
        orig_path = original.get('storage_path', '')
        new_path = ''
        if orig_path and os.path.exists(orig_path):
            ext = os.path.splitext(orig_path)[1]
            type_dir = os.path.join(UPLOAD_DIR, original['file_type'])
            os.makedirs(type_dir, exist_ok=True)
            new_path = os.path.join(type_dir, f"{new_fid}{ext}")
            import shutil
            shutil.copy2(orig_path, new_path)

        # Nuovo record DB
        new_file = {
            'name': original['name'] + ' (copia)',
            'description': original.get('description', ''),
            'file_type': original['file_type'],
            'owner_id': uid,
            'group_id': group_id,
            'is_common': False,
            'storage_path': new_path,
            'file_size': original.get('file_size', 0),
            'mime_type': original.get('mime_type', ''),
        }
        created_fid = group_storage.create_file(new_file)
        return {"success": True, "new_file_id": created_fid, "message": "File copiato nel tuo workspace"}

    # ==================================================================
    # WORKSPACE PROJECTS (save/load dal mapper app.html)
    # ==================================================================

    @app.post("/api/workspace/projects/save")
    async def save_workspace_project(data: WorkspaceProjectSave, user=Depends(get_current_user)):
        """Salva il progetto dal mapper come file JSON nel workspace."""
        uid = str(user['id'])
        project_json = json.dumps(data.project_data, ensure_ascii=False, indent=2)
        content_bytes = project_json.encode('utf-8')

        file_id = str(uuid.uuid4())
        type_dir = os.path.join(UPLOAD_DIR, 'project')
        os.makedirs(type_dir, exist_ok=True)
        safe_name = (data.name or 'project').replace('/', '_').replace('\\', '_')
        filename = f"{safe_name}_{file_id[:8]}.json"
        storage_path = os.path.join(type_dir, filename)

        with open(storage_path, 'wb') as f:
            f.write(content_bytes)

        fid = group_storage.create_file({
            'name': filename,
            'description': data.description or '',
            'file_type': 'project',
            'owner_id': uid,
            'group_id': data.group_id,
            'is_common': False,
            'storage_path': storage_path,
            'file_size': len(content_bytes),
            'mime_type': 'application/json',
        })

        if data.group_id:
            group_storage.set_permission({
                'file_id': fid, 'group_id': data.group_id,
                'can_view': True, 'can_download': True,
                'can_copy': True, 'can_edit': False, 'can_delete': False
            })

        return {"success": True, "file_id": fid, "name": filename}

    @app.get("/api/workspace/projects/{file_id}/load")
    async def load_workspace_project(file_id: str, user=Depends(get_current_user)):
        """Carica il JSON di un progetto dal workspace per il mapper."""
        uid = str(user['id'])
        if not permission_checker.check(file_id, uid, 'view', _is_admin(user)):
            raise HTTPException(403, "Accesso negato")
        file = group_storage.get_file(file_id)
        if not file:
            raise HTTPException(404, "Progetto non trovato")
        if file['file_type'] not in ('project', 'transform'):
            raise HTTPException(400, "Il file non è un progetto")
        path = file.get('storage_path', '')
        if not path or not os.path.exists(path):
            raise HTTPException(404, "File non trovato su disco")
        with open(path, 'r', encoding='utf-8') as f:
            project_data = json.load(f)
        return {"success": True, "project": project_data, "name": file['name']}

    # ==================================================================
    # PERMISSIONS
    # ==================================================================

    @app.get("/api/workspace/files/{file_id}/permissions")
    async def get_permissions(file_id: str, user=Depends(get_current_user)):
        uid = str(user['id'])
        file = group_storage.get_file(file_id)
        if not file:
            raise HTTPException(404, "File non trovato")
        if str(file.get('owner_id')) != uid and not _is_admin(user):
            raise HTTPException(403, "Solo il proprietario o un admin possono vedere i permessi")
        perms = group_storage.get_file_permissions(file_id)
        return {"permissions": perms}

    @app.post("/api/workspace/files/{file_id}/permissions")
    async def set_permission(file_id: str, data: PermissionSet, user=Depends(get_current_user)):
        uid = str(user['id'])
        file = group_storage.get_file(file_id)
        if not file:
            raise HTTPException(404, "File non trovato")
        if str(file.get('owner_id')) != uid and not _is_admin(user):
            raise HTTPException(403, "Solo il proprietario o un admin possono impostare i permessi")
        perm = data.dict()
        perm['file_id'] = file_id
        ok = group_storage.set_permission(perm)
        return {"success": ok}

    @app.delete("/api/workspace/files/{file_id}/permissions/{perm_id}")
    async def delete_permission(file_id: str, perm_id: str, user=Depends(get_current_user)):
        uid = str(user['id'])
        file = group_storage.get_file(file_id)
        if not file:
            raise HTTPException(404, "File non trovato")
        if str(file.get('owner_id')) != uid and not _is_admin(user):
            raise HTTPException(403, "Solo il proprietario o un admin possono rimuovere i permessi")
        ok = group_storage.delete_permission(perm_id)
        return {"success": ok}

    # ==================================================================
    # SHARE LINKS
    # ==================================================================

    @app.post("/api/workspace/files/{file_id}/share")
    async def create_share(file_id: str, data: ShareLinkCreate, user=Depends(get_current_user)):
        uid = str(user['id'])
        if not permission_checker.check(file_id, uid, 'view', _is_admin(user)):
            raise HTTPException(403, "Accesso negato")
        token = group_storage.create_share_link({
            'file_id': file_id,
            'created_by': uid,
            'expires_at': datetime.now() + timedelta(hours=data.expires_hours),
            'max_uses': data.max_uses,
            'note': data.note or ''
        })
        share_url = f"{BASE_URL}/share/{token}"
        return {"success": True, "token": token, "url": share_url,
                "expires_in_hours": data.expires_hours}

    @app.get("/api/workspace/files/{file_id}/share")
    async def list_shares(file_id: str, user=Depends(get_current_user)):
        uid = str(user['id'])
        file = group_storage.get_file(file_id)
        if not file:
            raise HTTPException(404, "File non trovato")
        if str(file.get('owner_id')) != uid and not _is_admin(user):
            raise HTTPException(403, "Solo il proprietario può vedere i link di condivisione")
        links = group_storage.list_share_links(file_id)
        return {"links": links}

    @app.delete("/api/workspace/files/{file_id}/share/{link_id}")
    async def delete_share(file_id: str, link_id: str, user=Depends(get_current_user)):
        uid = str(user['id'])
        file = group_storage.get_file(file_id)
        if not file:
            raise HTTPException(404, "File non trovato")
        if str(file.get('owner_id')) != uid and not _is_admin(user):
            raise HTTPException(403, "Solo il proprietario può eliminare i link di condivisione")
        ok = group_storage.delete_share_link(link_id)
        return {"success": ok}

    @app.get("/share/{token}")
    async def access_share(token: str):
        """Endpoint pubblico - accesso via link di condivisione (sola lettura)."""
        link = group_storage.get_share_link(token)
        if not link:
            raise HTTPException(404, "Link non trovato o scaduto")
        group_storage.use_share_link(token)
        file = group_storage.get_file(link['file_id'])
        if not file:
            raise HTTPException(404, "File non trovato")
        return {
            "file": {
                "id": file['id'],
                "name": file['name'],
                "file_type": file['file_type'],
                "description": file.get('description', ''),
            },
            "expires_at": str(link['expires_at']),
            "note": link.get('note', ''),
            "download_url": f"{BASE_URL}/share/{token}/download"
        }

    @app.get("/share/{token}/download")
    async def download_share(token: str):
        """Download pubblico via share link."""
        link = group_storage.get_share_link(token)
        if not link:
            raise HTTPException(404, "Link non trovato o scaduto")
        file = group_storage.get_file(link['file_id'])
        if not file:
            raise HTTPException(404, "File non trovato")
        path = file.get('storage_path', '')
        if not path or not os.path.exists(path):
            raise HTTPException(404, "File non trovato su disco")
        group_storage.use_share_link(token)
        return FileResponse(
            path=path,
            filename=file['name'],
            media_type=file.get('mime_type') or 'application/octet-stream'
        )

    # ==================================================================
    # ADMIN ENDPOINTS
    # ==================================================================

    @app.get("/api/admin/users")
    async def admin_list_users(
        status: Optional[str] = None,
        role: Optional[str] = None,
        user=Depends(get_current_user)
    ):
        if not _is_admin(user):
            raise HTTPException(403, "Accesso admin richiesto")
        cur = group_storage.conn.cursor(cursor_factory=group_storage.RealDictCursor)
        sql = "SELECT id, email, name, role, status, plan, created_at FROM users"
        conditions, vals = [], []
        if status:
            conditions.append("status = %s")
            vals.append(status)
        if role:
            conditions.append("role = %s")
            vals.append(role)
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
        sql += " ORDER BY created_at DESC NULLS LAST"
        cur.execute(sql, vals)
        users = [dict(r) for r in cur.fetchall()]
        return {"users": users, "count": len(users)}

    @app.put("/api/admin/users/{target_user_id}/status")
    async def admin_update_user_status(
        target_user_id: str,
        status: str = Query(..., regex="^(APPROVED|SUSPENDED|BLOCKED|PENDING)$"),
        user=Depends(get_current_user)
    ):
        if not _is_admin(user):
            raise HTTPException(403, "Accesso admin richiesto")
        cur = group_storage.conn.cursor()
        cur.execute("UPDATE users SET status = %s WHERE id = %s", (status, target_user_id))
        group_storage.conn.commit()
        return {"success": cur.rowcount > 0}

    @app.put("/api/admin/users/{target_user_id}/role")
    async def admin_update_user_role(
        target_user_id: str,
        role: str = Query(..., regex="^(MASTER|ADMIN|USER)$"),
        user=Depends(get_current_user)
    ):
        if user.get('role') != 'MASTER':
            raise HTTPException(403, "Solo il MASTER può cambiare i ruoli")
        cur = group_storage.conn.cursor()
        cur.execute("UPDATE users SET role = %s WHERE id = %s", (role, target_user_id))
        group_storage.conn.commit()
        return {"success": cur.rowcount > 0}

    # ==================================================================
    # INVITATIONS
    # ==================================================================

    @app.post("/api/groups/{group_id}/invite")
    async def invite_to_group(group_id: str, data: InvitationCreate, user=Depends(get_current_user)):
        """Invite someone by email. If they're already registered, auto-add. Otherwise send invitation."""
        if not _require_group_role(group_id, user, 'admin'):
            raise HTTPException(403, "Serve ruolo admin per invitare")
        uid = str(user['id'])
        email = data.email.lower().strip()
        if not email or '@' not in email:
            raise HTTPException(400, "Email non valida")

        try:
            # Check if user already exists
            cur = group_storage.conn.cursor(cursor_factory=group_storage.RealDictCursor)
            cur.execute("SELECT id, email, name FROM users WHERE LOWER(email) = %s", (email,))
            existing_user = cur.fetchone()

            if existing_user:
                # User exists — add directly
                group_storage.add_group_member(group_id, str(existing_user['id']), data.role, invited_by=uid)
                return {"success": True, "action": "added", "email_sent": False,
                        "message": f"Utente {email} aggiunto direttamente al gruppo"}

            # User doesn't exist — create invitation + send email
            inv = group_storage.create_invitation(group_id, email, data.role, uid, data.message or '')

            # Send invitation email
            email_sent = False
            try:
                import email_service
                group = group_storage.get_group(group_id)
                group_name = group['name'] if group else 'un gruppo'
                inviter_name = user.get('name', user.get('email', 'Qualcuno'))
                email_sent = bool(email_service.send_group_invitation_email(
                    email, inviter_name, group_name, inv['token'], data.message or ''
                ))
            except Exception as e:
                print(f"[INVITE] Errore invio email: {e}")

            if email_sent:
                msg = f"Invito inviato via email a {email}"
            else:
                msg = f"Invito creato per {email} (email non inviata — SMTP non configurato). Link: /app.html?invite={inv['token']}"

            return {"success": True, "action": "invited", "token": inv['token'],
                    "email_sent": email_sent, "message": msg}
        except HTTPException:
            raise
        except Exception as e:
            print(f"[INVITE] Eccezione: {e}")
            import traceback; traceback.print_exc()
            raise HTTPException(500, f"Errore interno durante l'invito: {str(e)}")

    @app.get("/api/invitations/{token}")
    async def get_invitation(token: str):
        """Check invitation details (public — no auth needed for accepting)"""
        inv = group_storage.get_invitation_by_token(token)
        if not inv:
            raise HTTPException(404, "Invito non trovato o scaduto")
        if inv['status'] != 'pending':
            return {"invitation": inv, "valid": False, "reason": f"Invito già {inv['status']}"}
        if inv.get('expires_at') and inv['expires_at'] < datetime.now():
            return {"invitation": inv, "valid": False, "reason": "Invito scaduto"}
        return {"invitation": inv, "valid": True}

    @app.post("/api/invitations/{token}/accept")
    async def accept_invitation(token: str, user=Depends(get_current_user)):
        """Accept a group invitation (must be logged in)"""
        uid = str(user['id'])
        ok = group_storage.accept_invitation(token, uid)
        if not ok:
            raise HTTPException(400, "Impossibile accettare l'invito (scaduto, già accettato, o non trovato)")
        inv = group_storage.get_invitation_by_token(token)
        return {"success": True, "group_id": inv['group_id'] if inv else None,
                "message": "Sei entrato nel gruppo!"}

    @app.get("/api/users/me/invitations")
    async def my_pending_invitations(user=Depends(get_current_user)):
        """Get pending invitations for current user's email"""
        email = user.get('email', '')
        if not email:
            return {"invitations": []}
        invitations = group_storage.get_pending_invitations_for_email(email)
        return {"invitations": invitations}

    @app.get("/api/groups/{group_id}/invitations")
    async def list_group_invitations(group_id: str, user=Depends(get_current_user)):
        """List all invitations for a group (admin only)"""
        if not _require_group_role(group_id, user, 'admin'):
            raise HTTPException(403, "Serve ruolo admin")
        invitations = group_storage.get_group_invitations(group_id)
        return {"invitations": invitations}

    @app.delete("/api/groups/{group_id}/invitations/{invitation_id}")
    async def cancel_group_invitation(group_id: str, invitation_id: str, user=Depends(get_current_user)):
        """Cancel a pending invitation"""
        if not _require_group_role(group_id, user, 'admin'):
            raise HTTPException(403, "Serve ruolo admin")
        ok = group_storage.cancel_invitation(invitation_id)
        return {"success": ok}

    # ==================================================================
    # GROUP SETTINGS
    # ==================================================================

    @app.get("/api/groups/{group_id}/settings")
    async def get_group_settings(group_id: str, user=Depends(get_current_user)):
        uid = str(user['id'])
        if not _is_admin(user):
            accessible = set(group_storage.get_user_accessible_group_ids(uid))
            if group_id not in accessible:
                raise HTTPException(403, "Accesso negato")
        settings = group_storage.get_group_settings(group_id)
        return {"settings": settings}

    @app.put("/api/groups/{group_id}/settings")
    async def update_group_settings(group_id: str, data: GroupSettingsUpdate, user=Depends(get_current_user)):
        if not _require_group_role(group_id, user, 'admin'):
            raise HTTPException(403, "Serve ruolo admin per modificare i settings")
        # Build settings dict from non-None fields
        settings = {k: v for k, v in data.dict().items() if v is not None}
        if not settings:
            raise HTTPException(400, "Nessun setting da aggiornare")
        ok = group_storage.update_group_settings(group_id, settings)
        return {"success": ok, "settings": group_storage.get_group_settings(group_id)}

    # ==================================================================
    # ADMIN STATS
    # ==================================================================

    @app.get("/api/admin/stats")
    async def admin_stats(user=Depends(get_current_user)):
        if not _is_admin(user):
            raise HTTPException(403, "Accesso admin richiesto")
        cur = group_storage.conn.cursor(cursor_factory=group_storage.RealDictCursor)
        cur.execute("""
            SELECT
                COUNT(*) as total_users,
                COUNT(*) FILTER (WHERE status = 'APPROVED') as active_users,
                COUNT(*) FILTER (WHERE status = 'PENDING') as pending_users,
                COUNT(*) FILTER (WHERE plan = 'FREE') as free_users,
                COUNT(*) FILTER (WHERE plan = 'PRO') as pro_users,
                COUNT(*) FILTER (WHERE plan = 'ENTERPRISE') as enterprise_users
            FROM users
        """)
        user_stats = dict(cur.fetchone())
        cur.execute("SELECT COUNT(*) as total_groups FROM groups")
        group_stats = dict(cur.fetchone())
        cur.execute("SELECT COUNT(*) as total_files, COALESCE(SUM(file_size),0) as total_size FROM files")
        file_stats = dict(cur.fetchone())
        return {"users": user_stats, "groups": group_stats, "files": file_stats}

    print("✅ Groups & Files API registrate")


# ===========================================================================
# HELPER
# ===========================================================================

def _build_tree(flat_groups):
    """Converte lista piatta con parent_id in albero annidato."""
    by_id = {g['id']: {**g, 'children': []} for g in flat_groups}
    roots = []
    for g in by_id.values():
        pid = g.get('parent_id')
        if pid and pid in by_id:
            by_id[pid]['children'].append(g)
        else:
            roots.append(g)
    return roots
