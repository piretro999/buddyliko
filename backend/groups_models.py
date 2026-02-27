#!/usr/bin/env python3
"""
Buddyliko - Groups, Files, Permissions Models
Extends storage_layer.py with group hierarchy and file management
"""

from abc import abstractmethod
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import json
import os
import uuid


# ===========================================================================
# ABSTRACT INTERFACE EXTENSIONS
# ===========================================================================

class GroupStorageInterface:
    """Mixin interface for group and file storage"""

    # --- GROUPS ---
    @abstractmethod
    def create_group(self, group: Dict) -> str: pass

    @abstractmethod
    def get_group(self, group_id: str) -> Optional[Dict]: pass

    @abstractmethod
    def update_group(self, group_id: str, data: Dict) -> bool: pass

    @abstractmethod
    def delete_group(self, group_id: str) -> bool: pass

    @abstractmethod
    def list_groups(self, parent_id: Optional[str] = None) -> List[Dict]: pass

    @abstractmethod
    def get_group_tree(self, root_id: Optional[str] = None) -> List[Dict]: pass

    # --- GROUP MEMBERS ---
    @abstractmethod
    def add_group_member(self, group_id: str, user_id: str, role: str) -> bool: pass

    @abstractmethod
    def remove_group_member(self, group_id: str, user_id: str) -> bool: pass

    @abstractmethod
    def update_member_role(self, group_id: str, user_id: str, role: str) -> bool: pass

    @abstractmethod
    def get_group_members(self, group_id: str) -> List[Dict]: pass

    @abstractmethod
    def get_user_groups(self, user_id: str) -> List[Dict]: pass

    # --- FILES ---
    @abstractmethod
    def create_file(self, file_obj: Dict) -> str: pass

    @abstractmethod
    def get_file(self, file_id: str) -> Optional[Dict]: pass

    @abstractmethod
    def update_file(self, file_id: str, data: Dict) -> bool: pass

    @abstractmethod
    def delete_file(self, file_id: str) -> bool: pass

    @abstractmethod
    def list_files(self, owner_id: Optional[str] = None,
                   group_id: Optional[str] = None,
                   file_type: Optional[str] = None,
                   is_common: Optional[bool] = None) -> List[Dict]: pass

    # --- PERMISSIONS ---
    @abstractmethod
    def set_permission(self, perm: Dict) -> bool: pass

    @abstractmethod
    def get_permission(self, file_id: str, user_id: Optional[str] = None,
                       group_id: Optional[str] = None) -> Optional[Dict]: pass

    @abstractmethod
    def get_file_permissions(self, file_id: str) -> List[Dict]: pass

    @abstractmethod
    def delete_permission(self, perm_id: str) -> bool: pass

    # --- SHARE LINKS ---
    @abstractmethod
    def create_share_link(self, link: Dict) -> str: pass

    @abstractmethod
    def get_share_link(self, token: str) -> Optional[Dict]: pass

    @abstractmethod
    def delete_share_link(self, link_id: str) -> bool: pass

    @abstractmethod
    def list_share_links(self, file_id: str) -> List[Dict]: pass

    # --- INVITATIONS ---
    @abstractmethod
    def create_invitation(self, group_id: str, email: str, role: str = 'member', invited_by: str = '', message: str = '') -> Dict: pass

    @abstractmethod
    def get_invitation_by_token(self, token: str) -> Optional[Dict]: pass

    @abstractmethod
    def accept_invitation(self, token: str, user_id: str) -> bool: pass

    @abstractmethod
    def get_pending_invitations_for_email(self, email: str) -> List[Dict]: pass

    @abstractmethod
    def auto_accept_pending_invitations(self, user_id: str, email: str) -> List[str]: pass

    # --- GROUP SETTINGS ---
    @abstractmethod
    def update_group_settings(self, group_id: str, settings: Dict) -> bool: pass

    @abstractmethod
    def get_group_settings(self, group_id: str) -> Dict: pass


# ===========================================================================
# POSTGRESQL IMPLEMENTATION
# ===========================================================================

class PostgreSQLGroupStorage(GroupStorageInterface):
    """PostgreSQL implementation for groups and files"""

    def __init__(self, conn, RealDictCursor):
        self.conn = conn
        self.RealDictCursor = RealDictCursor
        self._init_tables()

    def _init_tables(self):
        cur = self.conn.cursor()

        # Groups - hierarchical with parent_id (null = root)
        cur.execute('''
            CREATE TABLE IF NOT EXISTS groups (
                id VARCHAR(36) PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                description TEXT,
                parent_id VARCHAR(36) REFERENCES groups(id) ON DELETE CASCADE,
                owner_id VARCHAR(255) NOT NULL,
                settings JSONB DEFAULT '{}',
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        ''')

        # Group members - user can be in multiple groups
        cur.execute('''
            CREATE TABLE IF NOT EXISTS group_members (
                id VARCHAR(36) PRIMARY KEY,
                group_id VARCHAR(36) REFERENCES groups(id) ON DELETE CASCADE,
                user_id VARCHAR(255) NOT NULL,
                role VARCHAR(50) DEFAULT 'member',
                invited_by VARCHAR(255),
                joined_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(group_id, user_id)
            )
        ''')

        # Files - type: schema, project, csv, example, transform
        cur.execute('''
            CREATE TABLE IF NOT EXISTS files (
                id VARCHAR(36) PRIMARY KEY,
                name VARCHAR(500) NOT NULL,
                description TEXT,
                file_type VARCHAR(50) NOT NULL,
                owner_id VARCHAR(255),
                group_id VARCHAR(36) REFERENCES groups(id) ON DELETE SET NULL,
                is_common BOOLEAN DEFAULT FALSE,
                is_public BOOLEAN DEFAULT FALSE,
                storage_path VARCHAR(1000),
                file_size BIGINT DEFAULT 0,
                mime_type VARCHAR(255),
                metadata JSONB DEFAULT '{}',
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        ''')

        # Permissions - per file, per user or per group
        cur.execute('''
            CREATE TABLE IF NOT EXISTS file_permissions (
                id VARCHAR(36) PRIMARY KEY,
                file_id VARCHAR(36) REFERENCES files(id) ON DELETE CASCADE,
                user_id VARCHAR(255),
                group_id VARCHAR(36) REFERENCES groups(id) ON DELETE CASCADE,
                can_view BOOLEAN DEFAULT TRUE,
                can_download BOOLEAN DEFAULT FALSE,
                can_copy BOOLEAN DEFAULT FALSE,
                can_edit BOOLEAN DEFAULT FALSE,
                can_delete BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT NOW(),
                UNIQUE NULLS NOT DISTINCT (file_id, user_id, group_id)
            )
        ''')

        # Share links - temporary external sharing
        cur.execute('''
            CREATE TABLE IF NOT EXISTS share_links (
                id VARCHAR(36) PRIMARY KEY,
                file_id VARCHAR(36) REFERENCES files(id) ON DELETE CASCADE,
                token VARCHAR(255) UNIQUE NOT NULL,
                created_by VARCHAR(255) NOT NULL,
                expires_at TIMESTAMP NOT NULL,
                max_uses INTEGER,
                uses INTEGER DEFAULT 0,
                note TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            )
        ''')

        cur.execute('CREATE INDEX IF NOT EXISTS idx_group_members_user ON group_members(user_id)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_group_members_group ON group_members(group_id)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_files_owner ON files(owner_id)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_files_group ON files(group_id)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_files_type ON files(file_type)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_perms_file ON file_permissions(file_id)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_share_token ON share_links(token)')

        # Group invitations — pending invites by email
        cur.execute('''
            CREATE TABLE IF NOT EXISTS group_invitations (
                id VARCHAR(36) PRIMARY KEY,
                group_id VARCHAR(36) REFERENCES groups(id) ON DELETE CASCADE,
                email VARCHAR(255) NOT NULL,
                role VARCHAR(50) DEFAULT 'member',
                token VARCHAR(255) UNIQUE NOT NULL,
                invited_by VARCHAR(255) NOT NULL,
                status VARCHAR(20) DEFAULT 'pending',
                accepted_by_user_id VARCHAR(255),
                message TEXT,
                created_at TIMESTAMP DEFAULT NOW(),
                expires_at TIMESTAMP DEFAULT NOW() + INTERVAL '30 days',
                accepted_at TIMESTAMP
            )
        ''')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_invitations_email ON group_invitations(email)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_invitations_token ON group_invitations(token)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_invitations_group ON group_invitations(group_id)')

        self.conn.commit()
        print("✅ Group/File tables initialized")

    def _gen_id(self):
        return str(uuid.uuid4())

    # --- GROUPS ---

    def create_group(self, group: Dict) -> str:
        gid = self._gen_id()
        cur = self.conn.cursor()
        cur.execute('''
            INSERT INTO groups (id, name, description, parent_id, owner_id, settings)
            VALUES (%s, %s, %s, %s, %s, %s)
        ''', (
            gid,
            group['name'],
            group.get('description', ''),
            group.get('parent_id'),
            group['owner_id'],
            json.dumps(group.get('settings', {}))
        ))
        # Auto-add owner as group owner member
        self.add_group_member(gid, group['owner_id'], 'owner')
        self.conn.commit()
        return gid

    def get_group(self, group_id: str) -> Optional[Dict]:
        cur = self.conn.cursor(cursor_factory=self.RealDictCursor)
        cur.execute('SELECT * FROM groups WHERE id = %s', (group_id,))
        row = cur.fetchone()
        return dict(row) if row else None

    def update_group(self, group_id: str, data: Dict) -> bool:
        cur = self.conn.cursor()
        fields = []
        values = []
        allowed = ['name', 'description', 'settings']
        for k, v in data.items():
            if k in allowed:
                fields.append(f"{k} = %s")
                values.append(json.dumps(v) if isinstance(v, dict) else v)
        if not fields:
            return False
        fields.append("updated_at = NOW()")
        values.append(group_id)
        cur.execute(f"UPDATE groups SET {', '.join(fields)} WHERE id = %s", values)
        self.conn.commit()
        return cur.rowcount > 0

    def delete_group(self, group_id: str) -> bool:
        cur = self.conn.cursor()
        cur.execute('DELETE FROM groups WHERE id = %s', (group_id,))
        self.conn.commit()
        return cur.rowcount > 0

    def list_groups(self, parent_id: Optional[str] = None) -> List[Dict]:
        cur = self.conn.cursor(cursor_factory=self.RealDictCursor)
        if parent_id is None:
            # No filter → return ALL groups (for tree building)
            cur.execute('SELECT * FROM groups ORDER BY name')
        elif parent_id == '':
            # Explicit root only
            cur.execute('SELECT * FROM groups WHERE parent_id IS NULL ORDER BY name')
        else:
            cur.execute('SELECT * FROM groups WHERE parent_id = %s ORDER BY name', (parent_id,))
        return [dict(r) for r in cur.fetchall()]

    def get_group_tree(self, root_id: Optional[str] = None) -> List[Dict]:
        """Recursive group tree using WITH RECURSIVE"""
        cur = self.conn.cursor(cursor_factory=self.RealDictCursor)
        if root_id:
            cur.execute('''
                WITH RECURSIVE tree AS (
                    SELECT *, 0 as depth FROM groups WHERE id = %s
                    UNION ALL
                    SELECT g.*, t.depth + 1 FROM groups g
                    JOIN tree t ON g.parent_id = t.id
                )
                SELECT * FROM tree ORDER BY depth, name
            ''', (root_id,))
        else:
            cur.execute('''
                WITH RECURSIVE tree AS (
                    SELECT *, 0 as depth FROM groups WHERE parent_id IS NULL
                    UNION ALL
                    SELECT g.*, t.depth + 1 FROM groups g
                    JOIN tree t ON g.parent_id = t.id
                )
                SELECT * FROM tree ORDER BY depth, name
            ''')
        return [dict(r) for r in cur.fetchall()]

    def get_user_accessible_group_ids(self, user_id: str) -> List[str]:
        """Get all group IDs a user has access to, including parent groups they're member of"""
        cur = self.conn.cursor()
        # Direct memberships
        cur.execute('SELECT group_id FROM group_members WHERE user_id = %s', (user_id,))
        direct = [r[0] for r in cur.fetchall()]
        if not direct:
            return []
        # For each direct group, also include all subgroups (parent sees children)
        all_ids = set(direct)
        for gid in direct:
            subtree = self.get_group_tree(gid)
            all_ids.update(g['id'] for g in subtree)
        return list(all_ids)

    # --- GROUP MEMBERS ---

    def add_group_member(self, group_id: str, user_id: str, role: str = 'member',
                         invited_by: Optional[str] = None) -> bool:
        cur = self.conn.cursor()
        mid = self._gen_id()
        cur.execute('''
            INSERT INTO group_members (id, group_id, user_id, role, invited_by)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (group_id, user_id) DO UPDATE SET role = EXCLUDED.role
        ''', (mid, group_id, user_id, role, invited_by))
        self.conn.commit()
        return True

    def remove_group_member(self, group_id: str, user_id: str) -> bool:
        cur = self.conn.cursor()
        cur.execute('DELETE FROM group_members WHERE group_id = %s AND user_id = %s',
                    (group_id, user_id))
        self.conn.commit()
        return cur.rowcount > 0

    def update_member_role(self, group_id: str, user_id: str, role: str) -> bool:
        cur = self.conn.cursor()
        cur.execute('UPDATE group_members SET role = %s WHERE group_id = %s AND user_id = %s',
                    (role, group_id, user_id))
        self.conn.commit()
        return cur.rowcount > 0

    def get_group_members(self, group_id: str) -> List[Dict]:
        cur = self.conn.cursor(cursor_factory=self.RealDictCursor)
        cur.execute('''
            SELECT gm.*, u.email, u.name as user_name
            FROM group_members gm
            LEFT JOIN users u ON gm.user_id = u.id::text
            WHERE gm.group_id = %s
            ORDER BY gm.role, u.name
        ''', (group_id,))
        return [dict(r) for r in cur.fetchall()]

    def get_user_groups(self, user_id: str) -> List[Dict]:
        cur = self.conn.cursor(cursor_factory=self.RealDictCursor)
        cur.execute('''
            SELECT g.*, gm.role as member_role
            FROM groups g
            JOIN group_members gm ON g.id = gm.group_id
            WHERE gm.user_id = %s
            ORDER BY g.name
        ''', (user_id,))
        return [dict(r) for r in cur.fetchall()]

    # --- FILES ---

    def create_file(self, file_obj: Dict) -> str:
        fid = self._gen_id()
        cur = self.conn.cursor()
        cur.execute('''
            INSERT INTO files (id, name, description, file_type, owner_id, group_id,
                               is_common, is_public, storage_path, file_size, mime_type, metadata)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', (
            fid,
            file_obj['name'],
            file_obj.get('description', ''),
            file_obj['file_type'],
            file_obj.get('owner_id'),
            file_obj.get('group_id'),
            file_obj.get('is_common', False),
            file_obj.get('is_public', False),
            file_obj.get('storage_path', ''),
            file_obj.get('file_size', 0),
            file_obj.get('mime_type', ''),
            json.dumps(file_obj.get('metadata', {}))
        ))
        self.conn.commit()
        return fid

    def get_file(self, file_id: str) -> Optional[Dict]:
        cur = self.conn.cursor(cursor_factory=self.RealDictCursor)
        cur.execute('SELECT * FROM files WHERE id = %s', (file_id,))
        row = cur.fetchone()
        return dict(row) if row else None

    def update_file(self, file_id: str, data: Dict) -> bool:
        cur = self.conn.cursor()
        allowed = ['name', 'description', 'is_common', 'is_public', 'group_id', 'metadata']
        fields, values = [], []
        for k, v in data.items():
            if k in allowed:
                fields.append(f"{k} = %s")
                values.append(json.dumps(v) if isinstance(v, dict) else v)
        if not fields:
            return False
        fields.append("updated_at = NOW()")
        values.append(file_id)
        cur.execute(f"UPDATE files SET {', '.join(fields)} WHERE id = %s", values)
        self.conn.commit()
        return cur.rowcount > 0

    def delete_file(self, file_id: str) -> bool:
        cur = self.conn.cursor()
        cur.execute('DELETE FROM files WHERE id = %s', (file_id,))
        self.conn.commit()
        return cur.rowcount > 0

    def list_files(self, owner_id: Optional[str] = None,
                   group_id: Optional[str] = None,
                   file_type: Optional[str] = None,
                   is_common: Optional[bool] = None,
                   group_ids: Optional[List[str]] = None) -> List[Dict]:
        cur = self.conn.cursor(cursor_factory=self.RealDictCursor)
        where, vals = [], []

        if is_common is True:
            where.append("is_common = TRUE")
        else:
            if owner_id:
                where.append("owner_id = %s")
                vals.append(owner_id)
            if group_id:
                where.append("group_id = %s")
                vals.append(group_id)
            if group_ids:
                placeholders = ','.join(['%s'] * len(group_ids))
                where.append(f"group_id IN ({placeholders})")
                vals.extend(group_ids)

        if file_type:
            where.append("file_type = %s")
            vals.append(file_type)

        sql = "SELECT * FROM files"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY created_at DESC"

        cur.execute(sql, vals)
        return [dict(r) for r in cur.fetchall()]

    def copy_file(self, file_id: str, new_owner_id: str, new_group_id: Optional[str] = None) -> str:
        """Copy a file - copy becomes private to new owner"""
        original = self.get_file(file_id)
        if not original:
            raise ValueError("File not found")
        new_file = {
            'name': original['name'] + ' (copy)',
            'description': original.get('description', ''),
            'file_type': original['file_type'],
            'owner_id': new_owner_id,
            'group_id': new_group_id,
            'is_common': False,
            'is_public': False,
            'storage_path': original.get('storage_path', ''),
            'file_size': original.get('file_size', 0),
            'mime_type': original.get('mime_type', ''),
            'metadata': original.get('metadata', {})
        }
        return self.create_file(new_file)

    # --- PERMISSIONS ---

    def set_permission(self, perm: Dict) -> bool:
        pid = self._gen_id()
        cur = self.conn.cursor()
        cur.execute('''
            INSERT INTO file_permissions
                (id, file_id, user_id, group_id, can_view, can_download, can_copy, can_edit, can_delete)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (file_id, user_id, group_id) DO UPDATE SET
                can_view = EXCLUDED.can_view,
                can_download = EXCLUDED.can_download,
                can_copy = EXCLUDED.can_copy,
                can_edit = EXCLUDED.can_edit,
                can_delete = EXCLUDED.can_delete
        ''', (
            pid,
            perm['file_id'],
            perm.get('user_id'),
            perm.get('group_id'),
            perm.get('can_view', True),
            perm.get('can_download', False),
            perm.get('can_copy', False),
            perm.get('can_edit', False),
            perm.get('can_delete', False)
        ))
        self.conn.commit()
        return True

    def get_effective_permission(self, file_id: str, user_id: str,
                                  user_group_ids: List[str]) -> Dict:
        """Get most permissive combination of all applicable permissions"""
        cur = self.conn.cursor(cursor_factory=self.RealDictCursor)
        placeholders = ','.join(['%s'] * len(user_group_ids)) if user_group_ids else "''"
        params = [file_id, user_id] + user_group_ids

        query = f'''
            SELECT
                bool_or(can_view) as can_view,
                bool_or(can_download) as can_download,
                bool_or(can_copy) as can_copy,
                bool_or(can_edit) as can_edit,
                bool_or(can_delete) as can_delete
            FROM file_permissions
            WHERE file_id = %s AND (
                user_id = %s
                {("OR group_id IN (" + placeholders + ")") if user_group_ids else ""}
            )
        '''
        cur.execute(query, params)
        row = cur.fetchone()
        if row and row['can_view'] is not None:
            return dict(row)
        return {'can_view': False, 'can_download': False,
                'can_copy': False, 'can_edit': False, 'can_delete': False}

    def get_file_permissions(self, file_id: str) -> List[Dict]:
        cur = self.conn.cursor(cursor_factory=self.RealDictCursor)
        cur.execute('SELECT * FROM file_permissions WHERE file_id = %s', (file_id,))
        return [dict(r) for r in cur.fetchall()]

    def get_permission(self, file_id: str, user_id=None, group_id=None):
        cur = self.conn.cursor(cursor_factory=self.RealDictCursor)
        cur.execute('''
            SELECT * FROM file_permissions
            WHERE file_id = %s AND user_id = %s AND group_id IS NOT DISTINCT FROM %s
        ''', (file_id, user_id, group_id))
        row = cur.fetchone()
        return dict(row) if row else None

    def delete_permission(self, perm_id: str) -> bool:
        cur = self.conn.cursor()
        cur.execute('DELETE FROM file_permissions WHERE id = %s', (perm_id,))
        self.conn.commit()
        return cur.rowcount > 0

    # --- SHARE LINKS ---

    def create_share_link(self, link: Dict) -> str:
        lid = self._gen_id()
        token = str(uuid.uuid4()).replace('-', '')
        cur = self.conn.cursor()
        cur.execute('''
            INSERT INTO share_links (id, file_id, token, created_by, expires_at, max_uses, note)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        ''', (
            lid,
            link['file_id'],
            token,
            link['created_by'],
            link.get('expires_at', datetime.now() + timedelta(days=7)),
            link.get('max_uses'),
            link.get('note', '')
        ))
        self.conn.commit()
        return token

    def get_share_link(self, token: str) -> Optional[Dict]:
        cur = self.conn.cursor(cursor_factory=self.RealDictCursor)
        cur.execute('''
            SELECT sl.*, f.name as file_name, f.file_type
            FROM share_links sl
            JOIN files f ON sl.file_id = f.id
            WHERE sl.token = %s AND sl.expires_at > NOW()
            AND (sl.max_uses IS NULL OR sl.uses < sl.max_uses)
        ''', (token,))
        row = cur.fetchone()
        return dict(row) if row else None

    def use_share_link(self, token: str) -> bool:
        cur = self.conn.cursor()
        cur.execute('UPDATE share_links SET uses = uses + 1 WHERE token = %s', (token,))
        self.conn.commit()
        return cur.rowcount > 0

    def delete_share_link(self, link_id: str) -> bool:
        cur = self.conn.cursor()
        cur.execute('DELETE FROM share_links WHERE id = %s', (link_id,))
        self.conn.commit()
        return cur.rowcount > 0

    def list_share_links(self, file_id: str) -> List[Dict]:
        cur = self.conn.cursor(cursor_factory=self.RealDictCursor)
        cur.execute('SELECT * FROM share_links WHERE file_id = %s ORDER BY created_at DESC',
                    (file_id,))
        return [dict(r) for r in cur.fetchall()]

    # --- GROUP INVITATIONS ---

    def create_invitation(self, group_id: str, email: str, role: str = 'member',
                          invited_by: str = '', message: str = '') -> Dict:
        """Create a group invitation. Returns invitation dict with token."""
        import secrets
        cur = self.conn.cursor(cursor_factory=self.RealDictCursor)
        inv_id = self._gen_id()
        token = secrets.token_urlsafe(32)
        cur.execute('''
            INSERT INTO group_invitations (id, group_id, email, role, token, invited_by, message)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING *
        ''', (inv_id, group_id, email.lower().strip(), role, token, invited_by, message))
        result = dict(cur.fetchone())
        self.conn.commit()
        return result

    def get_invitation_by_token(self, token: str) -> Optional[Dict]:
        cur = self.conn.cursor(cursor_factory=self.RealDictCursor)
        cur.execute('''
            SELECT gi.*, g.name as group_name, u.name as inviter_name, u.email as inviter_email
            FROM group_invitations gi
            JOIN groups g ON gi.group_id = g.id
            LEFT JOIN users u ON gi.invited_by = u.id::text
            WHERE gi.token = %s
        ''', (token,))
        row = cur.fetchone()
        return dict(row) if row else None

    def accept_invitation(self, token: str, user_id: str) -> bool:
        """Accept invitation: add user to group, mark invitation as accepted."""
        inv = self.get_invitation_by_token(token)
        if not inv:
            return False
        if inv['status'] != 'pending':
            return False
        if inv['expires_at'] and inv['expires_at'] < datetime.now():
            return False
        # Add to group
        self.add_group_member(inv['group_id'], user_id, inv['role'], invited_by=inv['invited_by'])
        # Mark accepted
        cur = self.conn.cursor()
        cur.execute('''
            UPDATE group_invitations
            SET status = 'accepted', accepted_by_user_id = %s, accepted_at = NOW()
            WHERE token = %s
        ''', (user_id, token))
        self.conn.commit()
        return True

    def get_pending_invitations_for_email(self, email: str) -> List[Dict]:
        """Get all pending invitations for an email address."""
        cur = self.conn.cursor(cursor_factory=self.RealDictCursor)
        cur.execute('''
            SELECT gi.*, g.name as group_name
            FROM group_invitations gi
            JOIN groups g ON gi.group_id = g.id
            WHERE gi.email = %s AND gi.status = 'pending' AND gi.expires_at > NOW()
            ORDER BY gi.created_at DESC
        ''', (email.lower().strip(),))
        return [dict(r) for r in cur.fetchall()]

    def get_group_invitations(self, group_id: str) -> List[Dict]:
        """Get all invitations for a group (any status)."""
        cur = self.conn.cursor(cursor_factory=self.RealDictCursor)
        cur.execute('''
            SELECT gi.*, u.name as inviter_name
            FROM group_invitations gi
            LEFT JOIN users u ON gi.invited_by = u.id::text
            WHERE gi.group_id = %s
            ORDER BY gi.created_at DESC
        ''', (group_id,))
        return [dict(r) for r in cur.fetchall()]

    def cancel_invitation(self, invitation_id: str) -> bool:
        cur = self.conn.cursor()
        cur.execute('''
            UPDATE group_invitations SET status = 'cancelled'
            WHERE id = %s AND status = 'pending'
        ''', (invitation_id,))
        self.conn.commit()
        return cur.rowcount > 0

    def auto_accept_pending_invitations(self, user_id: str, email: str) -> List[str]:
        """Accept all pending invitations for this email. Returns list of group names joined."""
        pending = self.get_pending_invitations_for_email(email)
        joined = []
        for inv in pending:
            if self.accept_invitation(inv['token'], user_id):
                joined.append(inv['group_name'])
        return joined

    # --- GROUP SETTINGS ---

    def update_group_settings(self, group_id: str, settings: Dict) -> bool:
        """Merge new settings into existing group settings JSONB."""
        cur = self.conn.cursor()
        cur.execute('''
            UPDATE groups
            SET settings = COALESCE(settings, '{}'::jsonb) || %s::jsonb,
                updated_at = NOW()
            WHERE id = %s
        ''', (json.dumps(settings), group_id))
        self.conn.commit()
        return cur.rowcount > 0

    def get_group_settings(self, group_id: str) -> Dict:
        cur = self.conn.cursor(cursor_factory=self.RealDictCursor)
        cur.execute('SELECT settings FROM groups WHERE id = %s', (group_id,))
        row = cur.fetchone()
        if row and row.get('settings'):
            return row['settings'] if isinstance(row['settings'], dict) else json.loads(row['settings'])
        return {}


# ===========================================================================
# PERMISSION CHECKER HELPER
# ===========================================================================

class PermissionChecker:
    """Helper to check permissions consistently across the app"""

    def __init__(self, group_storage: PostgreSQLGroupStorage):
        self.gs = group_storage

    def check(self, file_id: str, user_id: str, action: str,
              is_admin: bool = False) -> bool:
        """
        Check if user can perform action on file.
        action: 'view' | 'download' | 'copy' | 'edit' | 'delete'
        """
        if is_admin:
            return True

        file = self.gs.get_file(file_id)
        if not file:
            return False

        # Owner has full access
        if str(file.get('owner_id')) == str(user_id):
            return True

        # Common files: everyone can view and copy, nobody can edit/delete (unless admin)
        if file.get('is_common'):
            if action in ('view',):
                return True
            if action == 'copy':
                return True
            return False

        # Public files: everyone can view
        if file.get('is_public') and action == 'view':
            return True

        # Check permissions table
        user_group_ids = self.gs.get_user_accessible_group_ids(user_id)
        effective = self.gs.get_effective_permission(file_id, user_id, user_group_ids)

        action_map = {
            'view': 'can_view',
            'download': 'can_download',
            'copy': 'can_copy',
            'edit': 'can_edit',
            'delete': 'can_delete'
        }
        field = action_map.get(action, 'can_view')
        return bool(effective.get(field, False))

    def get_visible_files(self, user_id: str, is_admin: bool = False,
                          file_type: Optional[str] = None) -> List[Dict]:
        """Get all files visible to a user"""
        if is_admin:
            return self.gs.list_files(file_type=file_type)

        user_group_ids = self.gs.get_user_accessible_group_ids(user_id)

        # Common files
        common = self.gs.list_files(is_common=True, file_type=file_type)

        # Own files
        own = self.gs.list_files(owner_id=user_id, file_type=file_type)

        # Group files
        group_files = []
        if user_group_ids:
            group_files = self.gs.list_files(group_ids=user_group_ids, file_type=file_type)

        # Merge deduplicating by id
        seen = set()
        result = []
        for f in common + own + group_files:
            if f['id'] not in seen:
                seen.add(f['id'])
                result.append(f)

        return result
