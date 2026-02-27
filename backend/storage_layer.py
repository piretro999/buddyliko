#!/usr/bin/env python3
"""
Multi-DB Storage Layer with Automatic Fallback
Supports: TinyDB (JSON) → SQLite → PostgreSQL → MySQL

Usage:
    db = StorageFactory.get_storage()
    db.save_schema(schema_data)
    schemas = db.list_schemas()
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from datetime import datetime
import json
import os


class StorageInterface(ABC):
    """Abstract storage interface - all DB implementations must follow"""
    
    @abstractmethod
    def save_schema(self, schema: Dict) -> str:
        """Save schema, return ID"""
        pass
    
    @abstractmethod
    def get_schema(self, schema_id: str) -> Optional[Dict]:
        """Get schema by ID"""
        pass
    
    @abstractmethod
    def list_schemas(self, user_id: Optional[str] = None) -> List[Dict]:
        """List all schemas (optionally filtered by user)"""
        pass
    
    @abstractmethod
    def delete_schema(self, schema_id: str) -> bool:
        """Delete schema"""
        pass
    
    @abstractmethod
    def save_project(self, project: Dict) -> str:
        """Save project, return ID"""
        pass
    
    @abstractmethod
    def get_project(self, project_id: str) -> Optional[Dict]:
        """Get project by ID"""
        pass
    
    @abstractmethod
    def list_projects(self, user_id: Optional[str] = None) -> List[Dict]:
        """List projects"""
        pass
    
    @abstractmethod
    def save_user(self, user: Dict) -> str:
        """Save user, return ID"""
        pass
    
    @abstractmethod
    def get_user(self, user_id: str) -> Optional[Dict]:
        """Get user by ID"""
        pass
    
    @abstractmethod
    def get_user_by_email(self, email: str) -> Optional[Dict]:
        """Get user by email"""
        pass

    @abstractmethod
    def update_user(self, user_id: str, data: Dict) -> bool:
        """Update user fields"""
        pass


# ===========================================================================
# TINYDB IMPLEMENTATION (JSON Local File)
# ===========================================================================

class TinyDBStorage(StorageInterface):
    """JSON file-based storage using TinyDB"""
    
    def __init__(self, db_path: str = "data/database.json"):
        try:
            from tinydb import TinyDB, Query
            self.db_available = True
            
            os.makedirs(os.path.dirname(db_path), exist_ok=True)
            self.db = TinyDB(db_path)
            self.schemas = self.db.table('schemas')
            self.projects = self.db.table('projects')
            self.users = self.db.table('users')
            self.Query = Query
            
            print(f"✅ TinyDB initialized: {db_path}")
        except ImportError:
            self.db_available = False
            print("⚠️ TinyDB not installed: pip install tinydb")
    
    def save_schema(self, schema: Dict) -> str:
        schema['created_at'] = datetime.now().isoformat()
        schema['updated_at'] = datetime.now().isoformat()
        doc_id = self.schemas.insert(schema)
        return str(doc_id)
    
    def get_schema(self, schema_id: str) -> Optional[Dict]:
        result = self.schemas.get(doc_id=int(schema_id))
        if result:
            result['id'] = str(result.doc_id)
        return result
    
    def list_schemas(self, user_id: Optional[str] = None) -> List[Dict]:
        if user_id:
            results = self.schemas.search(self.Query().user_id == user_id)
        else:
            results = self.schemas.all()
        
        for r in results:
            r['id'] = str(r.doc_id)
        return results
    
    def delete_schema(self, schema_id: str) -> bool:
        return self.schemas.remove(doc_ids=[int(schema_id)]) is not None
    
    def save_project(self, project: Dict) -> str:
        project['created_at'] = datetime.now().isoformat()
        project['updated_at'] = datetime.now().isoformat()
        doc_id = self.projects.insert(project)
        return str(doc_id)
    
    def get_project(self, project_id: str) -> Optional[Dict]:
        result = self.projects.get(doc_id=int(project_id))
        if result:
            result['id'] = str(result.doc_id)
        return result
    
    def list_projects(self, user_id: Optional[str] = None) -> List[Dict]:
        if user_id:
            results = self.projects.search(self.Query().user_id == user_id)
        else:
            results = self.projects.all()
        
        for r in results:
            r['id'] = str(r.doc_id)
        return results
    
    def save_user(self, user: Dict) -> str:
        user['created_at'] = datetime.now().isoformat()
        doc_id = self.users.insert(user)
        return str(doc_id)
    
    def get_user(self, user_id: str) -> Optional[Dict]:
        result = self.users.get(doc_id=int(user_id))
        if result:
            result['id'] = str(result.doc_id)
        return result
    
    def get_user_by_email(self, email: str) -> Optional[Dict]:
        results = self.users.search(self.Query().email == email)
        if results:
            result = results[0]
            result['id'] = str(result.doc_id)
            return result
        return None

    def update_user(self, user_id: str, data: Dict) -> bool:
        try:
            self.users.update(data, doc_ids=[int(user_id)])
            return True
        except Exception:
            return False


# ===========================================================================
# SQLITE IMPLEMENTATION
# ===========================================================================

class SQLiteStorage(StorageInterface):
    """SQLite file-based storage"""
    
    def __init__(self, db_path: str = "data/database.sqlite"):
        import sqlite3
        
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._init_tables()
        
        print(f"✅ SQLite initialized: {db_path}")
    
    def _init_tables(self):
        """Create tables if not exist"""
        cursor = self.conn.cursor()
        
        # Schemas table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS schemas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT,
                name TEXT NOT NULL,
                description TEXT,
                format TEXT,
                schema_data TEXT,
                created_at TEXT,
                updated_at TEXT
            )
        ''')
        
        # Projects table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS projects (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT,
                name TEXT NOT NULL,
                project_data TEXT,
                created_at TEXT,
                updated_at TEXT
            )
        ''')
        
        # Users table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT UNIQUE NOT NULL,
                password_hash TEXT,
                name TEXT,
                role TEXT DEFAULT 'USER',
                status TEXT DEFAULT 'APPROVED',
                plan TEXT DEFAULT 'FREE',
                auth_provider TEXT,
                auth_provider_id TEXT,
                created_at TEXT
            )
        ''')
        
        self.conn.commit()
    
    def save_schema(self, schema: Dict) -> str:
        cursor = self.conn.cursor()
        now = datetime.now().isoformat()
        
        cursor.execute('''
            INSERT INTO schemas (user_id, name, description, format, schema_data, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            schema.get('user_id'),
            schema.get('name'),
            schema.get('description'),
            schema.get('format'),
            json.dumps(schema),
            now,
            now
        ))
        
        self.conn.commit()
        return str(cursor.lastrowid)
    
    def get_schema(self, schema_id: str) -> Optional[Dict]:
        cursor = self.conn.cursor()
        cursor.execute('SELECT * FROM schemas WHERE id = ?', (schema_id,))
        row = cursor.fetchone()
        
        if row:
            return json.loads(row['schema_data'])
        return None
    
    def list_schemas(self, user_id: Optional[str] = None) -> List[Dict]:
        cursor = self.conn.cursor()
        
        if user_id:
            cursor.execute('SELECT * FROM schemas WHERE user_id = ? ORDER BY created_at DESC', (user_id,))
        else:
            cursor.execute('SELECT * FROM schemas ORDER BY created_at DESC')
        
        rows = cursor.fetchall()
        return [json.loads(row['schema_data']) for row in rows]
    
    def delete_schema(self, schema_id: str) -> bool:
        cursor = self.conn.cursor()
        cursor.execute('DELETE FROM schemas WHERE id = ?', (schema_id,))
        self.conn.commit()
        return cursor.rowcount > 0
    
    def save_project(self, project: Dict) -> str:
        cursor = self.conn.cursor()
        now = datetime.now().isoformat()
        
        cursor.execute('''
            INSERT INTO projects (user_id, name, project_data, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?)
        ''', (
            project.get('user_id'),
            project.get('projectName'),
            json.dumps(project),
            now,
            now
        ))
        
        self.conn.commit()
        return str(cursor.lastrowid)
    
    def get_project(self, project_id: str) -> Optional[Dict]:
        cursor = self.conn.cursor()
        cursor.execute('SELECT * FROM projects WHERE id = ?', (project_id,))
        row = cursor.fetchone()
        
        if row:
            return json.loads(row['project_data'])
        return None
    
    def list_projects(self, user_id: Optional[str] = None) -> List[Dict]:
        cursor = self.conn.cursor()
        
        if user_id:
            cursor.execute('SELECT * FROM projects WHERE user_id = ? ORDER BY created_at DESC', (user_id,))
        else:
            cursor.execute('SELECT * FROM projects ORDER BY created_at DESC')
        
        rows = cursor.fetchall()
        return [json.loads(row['project_data']) for row in rows]
    
    def save_user(self, user: Dict) -> str:
        cursor = self.conn.cursor()
        
        cursor.execute('''
            INSERT INTO users (email, password_hash, name, role, status, plan, auth_provider, auth_provider_id, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            user['email'],
            user.get('password_hash'),
            user.get('name'),
            user.get('role', 'USER'),
            user.get('status', 'APPROVED'),
            user.get('plan', 'FREE'),
            user.get('auth_provider'),
            user.get('auth_provider_id'),
            datetime.now().isoformat()
        ))
        
        self.conn.commit()
        return str(cursor.lastrowid)
    
    def get_user(self, user_id: str) -> Optional[Dict]:
        cursor = self.conn.cursor()
        cursor.execute('SELECT * FROM users WHERE id = ?', (user_id,))
        row = cursor.fetchone()
        
        if row:
            return dict(row)
        return None
    
    def get_user_by_email(self, email: str) -> Optional[Dict]:
        cursor = self.conn.cursor()
        cursor.execute('SELECT * FROM users WHERE email = ?', (email,))
        row = cursor.fetchone()
        
        if row:
            return dict(row)
        return None

    def update_user(self, user_id: str, data: Dict) -> bool:
        cursor = self.conn.cursor()
        allowed = ['name', 'role', 'status', 'plan', 'password_hash']
        fields, values = [], []
        for k, v in data.items():
            if k in allowed:
                fields.append(f"{k} = ?")
                values.append(v)
        if not fields:
            return False
        values.append(user_id)
        cursor.execute(f"UPDATE users SET {', '.join(fields)} WHERE id = ?", values)
        self.conn.commit()
        return cursor.rowcount > 0


# ===========================================================================
# POSTGRESQL IMPLEMENTATION
# ===========================================================================

class PostgreSQLStorage(StorageInterface):
    """PostgreSQL storage for production"""
    
    def __init__(self, connection_string: str):
        try:
            import psycopg2
            from psycopg2.extras import RealDictCursor
            
            self.conn = psycopg2.connect(connection_string)
            self.conn.autocommit = True
            self.RealDictCursor = RealDictCursor
            self._init_tables()
            
            print(f"✅ PostgreSQL connected")
        except ImportError:
            raise ImportError("PostgreSQL support requires: pip install psycopg2-binary")
    
    def _init_tables(self):
        """Create tables if not exist"""
        cursor = self.conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS schemas (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(255),
                name VARCHAR(255) NOT NULL,
                description TEXT,
                format VARCHAR(50),
                schema_data JSONB,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS projects (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(255),
                name VARCHAR(255) NOT NULL,
                project_data JSONB,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                email VARCHAR(255) UNIQUE NOT NULL,
                password_hash VARCHAR(255),
                name VARCHAR(255),
                role VARCHAR(50) DEFAULT 'USER',
                status VARCHAR(50) DEFAULT 'APPROVED',
                plan VARCHAR(50) DEFAULT 'FREE',
                auth_provider VARCHAR(50),
                auth_provider_id VARCHAR(255),
                email_verified BOOLEAN DEFAULT FALSE,
                mfa_enabled BOOLEAN DEFAULT FALSE,
                mfa_method VARCHAR(50),
                mfa_secret VARCHAR(255),
                mfa_totp_pending VARCHAR(255),
                created_at TIMESTAMP
            )
        ''')
    
    def save_schema(self, schema: Dict) -> str:
        cursor = self.conn.cursor()
        now = datetime.now()
        
        cursor.execute('''
            INSERT INTO schemas (user_id, name, description, format, schema_data, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        ''', (
            schema.get('user_id'),
            schema.get('name'),
            schema.get('description'),
            schema.get('format'),
            json.dumps(schema),
            now,
            now
        ))
        
        return str(cursor.fetchone()[0])
    
    def get_schema(self, schema_id: str) -> Optional[Dict]:
        cursor = self.conn.cursor(cursor_factory=self.RealDictCursor)
        cursor.execute('SELECT schema_data FROM schemas WHERE id = %s', (schema_id,))
        row = cursor.fetchone()
        
        if row:
            return row['schema_data']
        return None
    
    def list_schemas(self, user_id: Optional[str] = None) -> List[Dict]:
        cursor = self.conn.cursor(cursor_factory=self.RealDictCursor)
        
        if user_id:
            cursor.execute('SELECT schema_data FROM schemas WHERE user_id = %s ORDER BY created_at DESC', (user_id,))
        else:
            cursor.execute('SELECT schema_data FROM schemas ORDER BY created_at DESC')
        
        return [row['schema_data'] for row in cursor.fetchall()]
    
    def delete_schema(self, schema_id: str) -> bool:
        cursor = self.conn.cursor()
        cursor.execute('DELETE FROM schemas WHERE id = %s', (schema_id,))
        return cursor.rowcount > 0

    def save_project(self, project: Dict) -> str:
        cursor = self.conn.cursor()
        now = datetime.now()
        cursor.execute('''
            INSERT INTO projects (user_id, name, project_data, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id
        ''', (
            project.get('user_id'),
            project.get('projectName') or project.get('name', 'Unnamed'),
            json.dumps(project),
            now,
            now
        ))
        return str(cursor.fetchone()[0])

    def get_project(self, project_id: str) -> Optional[Dict]:
        cursor = self.conn.cursor(cursor_factory=self.RealDictCursor)
        cursor.execute('SELECT project_data FROM projects WHERE id = %s', (project_id,))
        row = cursor.fetchone()
        if row:
            return row['project_data']
        return None

    def list_projects(self, user_id: Optional[str] = None) -> List[Dict]:
        cursor = self.conn.cursor(cursor_factory=self.RealDictCursor)
        if user_id:
            cursor.execute('SELECT project_data FROM projects WHERE user_id = %s ORDER BY created_at DESC', (user_id,))
        else:
            cursor.execute('SELECT project_data FROM projects ORDER BY created_at DESC')
        return [row['project_data'] for row in cursor.fetchall()]

    def save_user(self, user: Dict) -> str:
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT INTO users (email, password_hash, name, role, status, plan, auth_provider, auth_provider_id, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (email) DO UPDATE SET
                password_hash = COALESCE(EXCLUDED.password_hash, users.password_hash),
                name = COALESCE(EXCLUDED.name, users.name)
            RETURNING id
        ''', (
            user['email'],
            user.get('password_hash'),
            user.get('name'),
            user.get('role', 'USER'),
            user.get('status', 'APPROVED'),
            user.get('plan', 'FREE'),
            user.get('auth_provider'),
            user.get('auth_provider_id'),
            datetime.now()
        ))
        return str(cursor.fetchone()[0])

    def get_user(self, user_id: str) -> Optional[Dict]:
        cursor = self.conn.cursor(cursor_factory=self.RealDictCursor)
        cursor.execute('SELECT * FROM users WHERE id = %s', (user_id,))
        row = cursor.fetchone()
        return dict(row) if row else None

    def get_user_by_email(self, email: str) -> Optional[Dict]:
        cursor = self.conn.cursor(cursor_factory=self.RealDictCursor)
        cursor.execute('SELECT * FROM users WHERE email = %s', (email,))
        row = cursor.fetchone()
        return dict(row) if row else None

    def update_user(self, user_id: str, data: Dict) -> bool:
        cursor = self.conn.cursor()
        allowed = ['name', 'role', 'status', 'plan', 'password_hash',
                    'email_verified', 'mfa_enabled', 'mfa_method', 'mfa_secret', 'mfa_totp_pending']
        fields, values = [], []
        for k, v in data.items():
            if k in allowed:
                fields.append(f"{k} = %s")
                values.append(v)
        if not fields:
            return False
        values.append(user_id)
        cursor.execute(f"UPDATE users SET {', '.join(fields)} WHERE id = %s", values)
        return cursor.rowcount > 0


# ===========================================================================
# STORAGE FACTORY with AUTO-FALLBACK
# ===========================================================================

class StorageFactory:
    """Factory with automatic fallback"""
    
    @staticmethod
    def get_storage(config: Optional[Dict] = None) -> StorageInterface:
        """
        Get storage with automatic fallback
        
        Priority:
        1. PostgreSQL (if configured)
        2. SQLite (always available)
        3. TinyDB (if installed)
        4. In-memory (fallback)
        """
        if config is None:
            config = {}
        
        # Try PostgreSQL
        if config.get('postgresql'):
            try:
                pg = config['postgresql']
                if isinstance(pg, str):
                    conn_str = pg
                else:
                    # Build connection string from dict
                    conn_str = (
                        f"postgresql://{pg.get('user', 'postgres')}:"
                        f"{pg.get('password', '')}@"
                        f"{pg.get('host', 'localhost')}:"
                        f"{pg.get('port', 5432)}/"
                        f"{pg.get('database', 'buddyliko')}"
                    )
                return PostgreSQLStorage(conn_str)
            except Exception as e:
                print(f"⚠️ PostgreSQL failed: {e}")
        
        # Try SQLite
        try:
            db_path = config.get('sqlite_path', 'data/database.sqlite')
            return SQLiteStorage(db_path)
        except Exception as e:
            print(f"⚠️ SQLite failed: {e}")
        
        # Try TinyDB
        try:
            db_path = config.get('tinydb_path', 'data/database.json')
            storage = TinyDBStorage(db_path)
            if storage.db_available:
                return storage
        except Exception as e:
            print(f"⚠️ TinyDB failed: {e}")
        
        # Fallback to in-memory (current implementation)
        print("⚠️ Using in-memory storage (no persistence)")
        return InMemoryStorage()


class InMemoryStorage(StorageInterface):
    """Fallback in-memory storage (current implementation)"""
    
    def __init__(self):
        self.schemas = {}
        self.projects = {}
        self.users = {}
        self._counter = 0
    
    def _gen_id(self):
        self._counter += 1
        return str(self._counter)
    
    def save_schema(self, schema: Dict) -> str:
        schema_id = self._gen_id()
        schema['id'] = schema_id
        self.schemas[schema_id] = schema
        return schema_id
    
    def get_schema(self, schema_id: str) -> Optional[Dict]:
        return self.schemas.get(schema_id)
    
    def list_schemas(self, user_id: Optional[str] = None) -> List[Dict]:
        if user_id:
            return [s for s in self.schemas.values() if s.get('user_id') == user_id]
        return list(self.schemas.values())
    
    def delete_schema(self, schema_id: str) -> bool:
        if schema_id in self.schemas:
            del self.schemas[schema_id]
            return True
        return False
    
    def save_project(self, project: Dict) -> str:
        project_id = self._gen_id()
        project['id'] = project_id
        self.projects[project_id] = project
        return project_id
    
    def get_project(self, project_id: str) -> Optional[Dict]:
        return self.projects.get(project_id)
    
    def list_projects(self, user_id: Optional[str] = None) -> List[Dict]:
        if user_id:
            return [p for p in self.projects.values() if p.get('user_id') == user_id]
        return list(self.projects.values())
    
    def save_user(self, user: Dict) -> str:
        user_id = self._gen_id()
        user['id'] = user_id
        self.users[user_id] = user
        return user_id
    
    def get_user(self, user_id: str) -> Optional[Dict]:
        return self.users.get(user_id)
    
    def get_user_by_email(self, email: str) -> Optional[Dict]:
        for user in self.users.values():
            if user.get('email') == email:
                return user
        return None

    def update_user(self, user_id: str, data: Dict) -> bool:
        if user_id in self.users:
            self.users[user_id].update(data)
            return True
        return False


# ===========================================================================
# USAGE EXAMPLE
# ===========================================================================

if __name__ == '__main__':
    # Auto-fallback: Will use SQLite by default
    db = StorageFactory.get_storage()
    
    # Save schema
    schema_id = db.save_schema({
        'name': 'Test Schema',
        'format': 'xml',
        'description': 'Test'
    })
    
    # List schemas
    schemas = db.list_schemas()
    print(f"Found {len(schemas)} schemas")
    
    # With PostgreSQL config:
    # db = StorageFactory.get_storage({
    #     'postgresql': 'postgresql://user:pass@localhost/dbname'
    # })
