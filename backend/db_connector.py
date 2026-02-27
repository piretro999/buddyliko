#!/usr/bin/env python3
"""
Buddyliko - Database Connector
Permette di usare database relazionali come sorgente e destinazione delle trasformazioni.

Sorgente: legge righe da una tabella/query SQL → le espone come schema nel mapper
Destinazione: il mapper genera INSERT/UPDATE verso la tabella target

Supporto:
  - PostgreSQL  (pip install psycopg2-binary)
  - MySQL       (pip install pymysql)
  - SQL Server  (pip install pyodbc)
  - SQLite      (built-in Python)

Endpoint:
  POST /api/dbconn/test         → testa connessione
  POST /api/dbconn/save         → salva configurazione connessione (cifrata)
  GET  /api/dbconn/list         → lista connessioni salvate
  DELETE /api/dbconn/{id}       → rimuove connessione
  GET  /api/dbconn/{id}/tables  → lista tabelle del DB
  GET  /api/dbconn/{id}/schema/{table} → schema colonne come Buddyliko schema
  POST /api/dbconn/{id}/preview → anteprima dati (prime 50 righe)
  POST /api/dbconn/{id}/execute-write → scrivi risultato trasformazione nel DB
"""

import json
import uuid
import base64
import hashlib
import re
from datetime import datetime, timezone
from typing import Optional, Dict, List, Any, Tuple


# ===========================================================================
# TIPO MAPPING  SQL → Buddyliko
# ===========================================================================

_SQL_TYPE_MAP = {
    # numerici
    'integer': 'number', 'int': 'number', 'bigint': 'number',
    'smallint': 'number', 'tinyint': 'number', 'serial': 'number',
    'bigserial': 'number', 'numeric': 'number', 'decimal': 'number',
    'float': 'number', 'double': 'number', 'real': 'number',
    'double precision': 'number', 'money': 'number',
    # testo
    'varchar': 'string', 'char': 'string', 'text': 'string',
    'nvarchar': 'string', 'nchar': 'string', 'ntext': 'string',
    'character varying': 'string', 'character': 'string',
    # booleano
    'boolean': 'boolean', 'bool': 'boolean', 'bit': 'boolean',
    # data/ora
    'date': 'date', 'time': 'time', 'datetime': 'datetime',
    'timestamp': 'datetime', 'timestamptz': 'datetime',
    'timestamp with time zone': 'datetime',
    'timestamp without time zone': 'datetime',
    # json
    'json': 'object', 'jsonb': 'object', 'xml': 'string',
    # uuid
    'uuid': 'string',
}


def _sql_to_buddyliko_type(sql_type: str) -> str:
    cleaned = sql_type.lower().split('(')[0].strip()
    return _SQL_TYPE_MAP.get(cleaned, 'string')


# ===========================================================================
# ENCRYPTION (semplice, per non salvare password in chiaro nel DB)
# ===========================================================================

def _encrypt_conn_string(conn_str: str, secret: str) -> str:
    """Cifra la connection string con XOR + base64. Non è crittografia forte,
    serve solo a non salvare le credenziali in chiaro nel DB."""
    key = hashlib.sha256(secret.encode()).digest()
    data = conn_str.encode()
    encrypted = bytes(b ^ key[i % len(key)] for i, b in enumerate(data))
    return base64.b64encode(encrypted).decode()


def _decrypt_conn_string(encrypted: str, secret: str) -> str:
    key = hashlib.sha256(secret.encode()).digest()
    data = base64.b64decode(encrypted.encode())
    decrypted = bytes(b ^ key[i % len(key)] for i, b in enumerate(data))
    return decrypted.decode()


# ===========================================================================
# DB CONNECTOR MANAGER
# ===========================================================================

class DBConnectorManager:
    """
    Gestisce connessioni ai database esterni.
    Richiede tabella db_connections nel DB Buddyliko.
    """

    def __init__(self, conn, RealDictCursor, secret_key: str = 'buddyliko-dbconn-secret'):
        self.conn = conn
        self.RealDictCursor = RealDictCursor
        self.secret_key = secret_key
        self._init_tables()

    def _init_tables(self):
        cur = self.conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS db_connections (
                id VARCHAR(36) PRIMARY KEY,
                user_id VARCHAR(255) NOT NULL,
                name VARCHAR(255) NOT NULL,
                db_type VARCHAR(50) NOT NULL,
                -- 'postgresql' | 'mysql' | 'sqlserver' | 'sqlite'
                connection_string_enc TEXT NOT NULL,  -- cifrata
                default_schema VARCHAR(255),
                created_at TIMESTAMPTZ DEFAULT NOW(),
                last_used_at TIMESTAMPTZ,
                last_test_status VARCHAR(50) DEFAULT 'unknown',
                -- 'ok' | 'error' | 'unknown'
                last_test_message TEXT,
                metadata JSONB DEFAULT '{}'
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_dbconn_user ON db_connections(user_id)")
        print("✅ DB Connector tables initialized")

    # ------------------------------------------------------------------
    # CONNECTION MANAGEMENT
    # ------------------------------------------------------------------

    def save_connection(self, user_id: str, name: str, db_type: str,
                        connection_params: Dict) -> str:
        """
        Salva una connessione. connection_params può essere:
        - { 'connection_string': 'postgresql://user:pass@host/db' }  (diretta)
        - { 'host': ..., 'port': ..., 'database': ..., 'user': ..., 'password': ... }
        """
        conn_str = self._build_connection_string(db_type, connection_params)
        encrypted = _encrypt_conn_string(conn_str, self.secret_key)
        conn_id = str(uuid.uuid4())
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO db_connections (id, user_id, name, db_type, connection_string_enc,
                                         default_schema)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (conn_id, str(user_id), name, db_type.lower(), encrypted,
              connection_params.get('schema', 'public')))
        return conn_id

    def list_connections(self, user_id: str) -> List[Dict]:
        cur = self.conn.cursor(cursor_factory=self.RealDictCursor)
        cur.execute("""
            SELECT id, name, db_type, default_schema, created_at,
                   last_used_at, last_test_status, last_test_message
            FROM db_connections WHERE user_id = %s ORDER BY name
        """, (str(user_id),))
        return [dict(r) for r in cur.fetchall()]

    def delete_connection(self, conn_id: str, user_id: str) -> bool:
        cur = self.conn.cursor()
        cur.execute("""
            DELETE FROM db_connections WHERE id = %s AND user_id = %s
        """, (conn_id, str(user_id)))
        return cur.rowcount > 0

    def _get_connection_record(self, conn_id: str, user_id: str) -> Optional[Dict]:
        cur = self.conn.cursor(cursor_factory=self.RealDictCursor)
        cur.execute("""
            SELECT * FROM db_connections WHERE id = %s AND user_id = %s
        """, (conn_id, str(user_id)))
        return dict(cur.fetchone()) if cur.fetchone() else None

    def _get_connection_record_any(self, conn_id: str, user_id: str) -> Optional[Dict]:
        """Versione che non consuma il cursore due volte."""
        cur = self.conn.cursor()
        cur.execute("""
            SELECT id, name, db_type, connection_string_enc, default_schema
            FROM db_connections WHERE id = %s AND user_id = %s
        """, (conn_id, str(user_id)))
        row = cur.fetchone()
        if not row:
            return None
        return {
            'id': row[0], 'name': row[1], 'db_type': row[2],
            'connection_string_enc': row[3], 'default_schema': row[4] or 'public'
        }

    def _get_external_conn(self, conn_id: str, user_id: str):
        """Apre e ritorna una connessione al DB esterno."""
        record = self._get_connection_record_any(conn_id, user_id)
        if not record:
            raise ValueError(f"Connection {conn_id} not found")
        conn_str = _decrypt_conn_string(record['connection_string_enc'], self.secret_key)
        db_type = record['db_type']
        return self._open_external(db_type, conn_str), record

    def _open_external(self, db_type: str, conn_str: str):
        if db_type == 'postgresql':
            import psycopg2
            return psycopg2.connect(conn_str)
        elif db_type == 'mysql':
            import pymysql
            # pymysql accetta conn_str come mysql://user:pass@host/db
            from sqlalchemy import create_engine
            engine = create_engine(conn_str.replace('mysql://', 'mysql+pymysql://'))
            return engine.connect()
        elif db_type == 'sqlserver':
            import pyodbc
            # conn_str format: mssql://user:pass@host/db o ODBC connection string
            if conn_str.startswith('mssql://') or conn_str.startswith('mssql+pyodbc://'):
                from sqlalchemy import create_engine
                engine = create_engine(conn_str if 'pyodbc' in conn_str
                                       else conn_str.replace('mssql://', 'mssql+pyodbc://'))
                return engine.connect()
            else:
                return pyodbc.connect(conn_str)
        elif db_type == 'sqlite':
            import sqlite3
            path = conn_str.replace('sqlite:///', '')
            return sqlite3.connect(path)
        else:
            raise ValueError(f"DB type non supportato: {db_type}")

    # ------------------------------------------------------------------
    # TEST CONNECTION
    # ------------------------------------------------------------------

    def test_connection(self, conn_id: str, user_id: str) -> Dict:
        try:
            ext_conn, record = self._get_external_conn(conn_id, user_id)
            db_type = record['db_type']

            test_query = {
                'postgresql': 'SELECT version()',
                'mysql': 'SELECT version()',
                'sqlserver': 'SELECT @@VERSION',
                'sqlite': 'SELECT sqlite_version()',
            }.get(db_type, 'SELECT 1')

            cur = ext_conn.cursor()
            cur.execute(test_query)
            version = str(cur.fetchone()[0])[:100]
            ext_conn.close()

            # Aggiorna last_test_status
            self._update_test_status(conn_id, 'ok', f"OK — {version}")
            return {"success": True, "message": f"Connessione riuscita: {version}"}

        except Exception as e:
            self._update_test_status(conn_id, 'error', str(e)[:255])
            return {"success": False, "message": str(e)}

    def _update_test_status(self, conn_id: str, status: str, message: str):
        cur = self.conn.cursor()
        cur.execute("""
            UPDATE db_connections
            SET last_test_status = %s, last_test_message = %s,
                last_used_at = NOW()
            WHERE id = %s
        """, (status, message[:255], conn_id))

    # ------------------------------------------------------------------
    # TABLE LISTING
    # ------------------------------------------------------------------

    def list_tables(self, conn_id: str, user_id: str) -> List[str]:
        ext_conn, record = self._get_external_conn(conn_id, user_id)
        db_type = record['db_type']
        schema = record.get('default_schema', 'public')

        try:
            cur = ext_conn.cursor()
            if db_type == 'postgresql':
                cur.execute("""
                    SELECT table_name FROM information_schema.tables
                    WHERE table_schema = %s AND table_type = 'BASE TABLE'
                    ORDER BY table_name
                """, (schema,))
            elif db_type == 'mysql':
                cur.execute("SHOW TABLES")
            elif db_type == 'sqlserver':
                cur.execute("""
                    SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME
                """)
            elif db_type == 'sqlite':
                cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            else:
                return []
            tables = [row[0] for row in cur.fetchall()]
            ext_conn.close()
            return tables
        except Exception as e:
            try: ext_conn.close()
            except: pass
            raise

    # ------------------------------------------------------------------
    # SCHEMA EXTRACTION → Buddyliko schema format
    # ------------------------------------------------------------------

    def get_table_schema(self, conn_id: str, user_id: str, table_name: str) -> Dict:
        """
        Ritorna lo schema di una tabella in formato Buddyliko:
        {
          "name": "orders",
          "source": "db",
          "db_connection_id": "...",
          "db_table": "orders",
          "fields": [
            { "id": "...", "name": "id", "type": "number", "path": "orders/id",
              "xml_path": "orders/id", "nullable": false, "primary_key": true }
          ]
        }
        """
        # Sanitize table name (previene SQL injection)
        if not re.match(r'^[a-zA-Z0-9_\.]+$', table_name):
            raise ValueError(f"Table name non valido: {table_name}")

        ext_conn, record = self._get_external_conn(conn_id, user_id)
        db_type = record['db_type']
        schema_name = record.get('default_schema', 'public')

        try:
            cur = ext_conn.cursor()
            if db_type == 'postgresql':
                cur.execute("""
                    SELECT
                        c.column_name,
                        c.data_type,
                        c.is_nullable,
                        c.column_default,
                        CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END as is_pk
                    FROM information_schema.columns c
                    LEFT JOIN (
                        SELECT ku.column_name
                        FROM information_schema.table_constraints tc
                        JOIN information_schema.key_column_usage ku
                            ON tc.constraint_name = ku.constraint_name
                            AND tc.table_schema = ku.table_schema
                        WHERE tc.constraint_type = 'PRIMARY KEY'
                            AND tc.table_name = %s
                            AND tc.table_schema = %s
                    ) pk ON c.column_name = pk.column_name
                    WHERE c.table_name = %s AND c.table_schema = %s
                    ORDER BY c.ordinal_position
                """, (table_name, schema_name, table_name, schema_name))

            elif db_type == 'mysql':
                cur.execute(f"""
                    SELECT COLUMN_NAME, DATA_TYPE,
                           IF(IS_NULLABLE='YES','YES','NO'),
                           COLUMN_DEFAULT,
                           IF(COLUMN_KEY='PRI',1,0)
                    FROM information_schema.columns
                    WHERE table_name = %s
                    ORDER BY ordinal_position
                """, (table_name,))

            elif db_type == 'sqlserver':
                cur.execute("""
                    SELECT
                        c.COLUMN_NAME, c.DATA_TYPE,
                        c.IS_NULLABLE, c.COLUMN_DEFAULT,
                        CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END
                    FROM INFORMATION_SCHEMA.COLUMNS c
                    LEFT JOIN (
                        SELECT ku.COLUMN_NAME
                        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
                            ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
                        WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY' AND tc.TABLE_NAME = ?
                    ) pk ON c.COLUMN_NAME = pk.COLUMN_NAME
                    WHERE c.TABLE_NAME = ? ORDER BY c.ORDINAL_POSITION
                """, (table_name, table_name))

            elif db_type == 'sqlite':
                cur.execute(f"PRAGMA table_info({table_name})")
                rows = cur.fetchall()
                ext_conn.close()
                fields = []
                for r in rows:
                    cid, name, typ, notnull, dflt, pk = r
                    fields.append({
                        "id": str(uuid.uuid4()),
                        "name": name,
                        "type": _sql_to_buddyliko_type(typ or 'text'),
                        "sql_type": typ,
                        "path": f"{table_name}/{name}",
                        "xml_path": f"{table_name}/{name}",
                        "nullable": not notnull,
                        "primary_key": bool(pk),
                    })
                return self._build_schema(conn_id, table_name, fields)

            rows = cur.fetchall()
            ext_conn.close()

            fields = []
            for row in rows:
                col_name, data_type, is_nullable, _, is_pk = row
                fields.append({
                    "id": str(uuid.uuid4()),
                    "name": col_name,
                    "type": _sql_to_buddyliko_type(data_type or 'varchar'),
                    "sql_type": data_type,
                    "path": f"{table_name}/{col_name}",
                    "xml_path": f"{table_name}/{col_name}",
                    "nullable": (str(is_nullable).upper() in ('YES', '1', 'TRUE')),
                    "primary_key": bool(is_pk),
                })

            return self._build_schema(conn_id, table_name, fields)

        except Exception as e:
            try: ext_conn.close()
            except: pass
            raise

    def _build_schema(self, conn_id: str, table_name: str, fields: List[Dict]) -> Dict:
        return {
            "id": str(uuid.uuid4()),
            "name": table_name,
            "source": "db",
            "db_connection_id": conn_id,
            "db_table": table_name,
            "fields": fields,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

    # ------------------------------------------------------------------
    # DATA PREVIEW
    # ------------------------------------------------------------------

    def preview_data(self, conn_id: str, user_id: str,
                     table_name: str, limit: int = 50,
                     where_clause: str = None) -> Dict:
        """
        Ritorna le prime N righe della tabella come lista di dict.
        Usato per anteprima nel mapper.
        """
        # Sanitize
        if not re.match(r'^[a-zA-Z0-9_\.]+$', table_name):
            raise ValueError(f"Table name non valido: {table_name}")
        limit = min(int(limit), 500)

        ext_conn, record = self._get_external_conn(conn_id, user_id)
        db_type = record['db_type']

        try:
            cur = ext_conn.cursor()
            # Costruisci query
            if db_type in ('postgresql', 'sqlite'):
                q = f"SELECT * FROM {table_name}"
                if where_clause:
                    q += f" WHERE {where_clause}"
                q += f" LIMIT {limit}"
                cur.execute(q)
            elif db_type == 'mysql':
                q = f"SELECT * FROM `{table_name}`"
                if where_clause:
                    q += f" WHERE {where_clause}"
                q += f" LIMIT {limit}"
                cur.execute(q)
            elif db_type == 'sqlserver':
                q = f"SELECT TOP {limit} * FROM [{table_name}]"
                if where_clause:
                    q += f" WHERE {where_clause}"
                cur.execute(q)

            cols = [desc[0] for desc in cur.description]
            rows = []
            for row in cur.fetchall():
                rows.append({cols[i]: (str(v) if v is not None else None)
                             for i, v in enumerate(row)})
            ext_conn.close()
            return {"columns": cols, "rows": rows, "count": len(rows)}

        except Exception as e:
            try: ext_conn.close()
            except: pass
            raise

    # ------------------------------------------------------------------
    # WRITE (INSERT / UPDATE)
    # ------------------------------------------------------------------

    def execute_write(self, conn_id: str, user_id: str,
                      table_name: str, rows: List[Dict],
                      mode: str = 'insert',
                      pk_columns: List[str] = None) -> Dict:
        """
        Scrive righe nel DB di destinazione.
        mode:
          'insert'       → INSERT INTO, errore se duplicato
          'insert_ignore'→ INSERT IGNORE (MySQL) / ON CONFLICT DO NOTHING (PG)
          'upsert'       → ON CONFLICT (pk_columns) DO UPDATE (PG) /
                           INSERT ... ON DUPLICATE KEY UPDATE (MySQL)
          'update'       → UPDATE WHERE pk_columns
        """
        if not re.match(r'^[a-zA-Z0-9_\.]+$', table_name):
            raise ValueError(f"Table name non valido: {table_name}")
        if not rows:
            return {"success": True, "inserted": 0, "updated": 0, "errors": []}

        ext_conn, record = self._get_external_conn(conn_id, user_id)
        db_type = record['db_type']
        pk_cols = pk_columns or []

        inserted = 0
        updated = 0
        errors = []

        try:
            cur = ext_conn.cursor()
            for i, row in enumerate(rows):
                try:
                    cols = list(row.keys())
                    vals = list(row.values())

                    if mode == 'insert':
                        sql, params = self._build_insert(db_type, table_name, cols, vals)
                        cur.execute(sql, params)
                        inserted += 1

                    elif mode == 'insert_ignore':
                        sql, params = self._build_insert_ignore(db_type, table_name, cols, vals)
                        cur.execute(sql, params)
                        inserted += 1

                    elif mode == 'upsert':
                        sql, params = self._build_upsert(db_type, table_name, cols, vals, pk_cols)
                        cur.execute(sql, params)
                        inserted += 1  # upsert conta come insert

                    elif mode == 'update':
                        if not pk_cols:
                            raise ValueError("update mode richiede pk_columns")
                        sql, params = self._build_update(db_type, table_name, cols, vals, pk_cols, row)
                        cur.execute(sql, params)
                        updated += cur.rowcount

                except Exception as row_err:
                    errors.append({"row": i, "error": str(row_err)[:200]})
                    if len(errors) >= 50:
                        break  # stop dopo 50 errori

            if db_type != 'postgresql':  # PG è autocommit
                ext_conn.commit()
            ext_conn.close()
            return {"success": True, "inserted": inserted, "updated": updated, "errors": errors}

        except Exception as e:
            try:
                ext_conn.rollback()
                ext_conn.close()
            except: pass
            return {"success": False, "inserted": inserted, "updated": updated,
                    "errors": errors, "fatal_error": str(e)}

    def _build_insert(self, db_type: str, table: str, cols: List[str], vals: list) -> Tuple:
        if db_type == 'sqlserver':
            ph = ','.join(['?' for _ in cols])
            col_str = ','.join([f'[{c}]' for c in cols])
            return f"INSERT INTO [{table}] ({col_str}) VALUES ({ph})", vals
        elif db_type == 'mysql':
            ph = ','.join(['%s' for _ in cols])
            col_str = ','.join([f'`{c}`' for c in cols])
            return f"INSERT INTO `{table}` ({col_str}) VALUES ({ph})", vals
        else:  # postgresql, sqlite
            ph = ','.join(['%s' if db_type == 'postgresql' else '?' for _ in cols])
            col_str = ','.join([f'"{c}"' for c in cols])
            return f'INSERT INTO "{table}" ({col_str}) VALUES ({ph})', vals

    def _build_insert_ignore(self, db_type: str, table: str, cols: List[str], vals: list) -> Tuple:
        base_sql, params = self._build_insert(db_type, table, cols, vals)
        if db_type == 'postgresql':
            return base_sql + " ON CONFLICT DO NOTHING", params
        elif db_type == 'mysql':
            return base_sql.replace('INSERT INTO', 'INSERT IGNORE INTO'), params
        elif db_type == 'sqlite':
            return base_sql.replace('INSERT INTO', 'INSERT OR IGNORE INTO'), params
        return base_sql, params

    def _build_upsert(self, db_type: str, table: str, cols: List[str],
                      vals: list, pk_cols: List[str]) -> Tuple:
        if db_type == 'postgresql':
            ph = ','.join(['%s' for _ in cols])
            col_str = ','.join([f'"{c}"' for c in cols])
            pk_str = ','.join([f'"{c}"' for c in pk_cols])
            update_str = ','.join([f'"{c}" = EXCLUDED."{c}"'
                                   for c in cols if c not in pk_cols])
            sql = (f'INSERT INTO "{table}" ({col_str}) VALUES ({ph}) '
                   f'ON CONFLICT ({pk_str}) DO UPDATE SET {update_str}')
            return sql, vals

        elif db_type == 'mysql':
            ph = ','.join(['%s' for _ in cols])
            col_str = ','.join([f'`{c}`' for c in cols])
            update_str = ','.join([f'`{c}` = VALUES(`{c}`)'
                                   for c in cols if c not in pk_cols])
            sql = (f'INSERT INTO `{table}` ({col_str}) VALUES ({ph}) '
                   f'ON DUPLICATE KEY UPDATE {update_str}')
            return sql, vals

        elif db_type == 'sqlite':
            ph = ','.join(['?' for _ in cols])
            col_str = ','.join([f'"{c}"' for c in cols])
            sql = f'INSERT OR REPLACE INTO "{table}" ({col_str}) VALUES ({ph})'
            return sql, vals

        # SQLServer: usa MERGE
        pk_match = ' AND '.join([f'target.[{c}] = source.[{c}]' for c in pk_cols])
        set_str = ','.join([f'target.[{c}] = source.[{c}]'
                            for c in cols if c not in pk_cols])
        col_str = ','.join([f'[{c}]' for c in cols])
        val_str = ','.join(['?' for _ in cols])
        sql = (f"MERGE [{table}] AS target "
               f"USING (SELECT {', '.join([f'? AS [{c}]' for c in cols])}) AS source "
               f"ON {pk_match} "
               f"WHEN MATCHED THEN UPDATE SET {set_str} "
               f"WHEN NOT MATCHED THEN INSERT ({col_str}) VALUES ({val_str})")
        return sql, vals * 2 + vals  # params ripetuti per MERGE

    def _build_update(self, db_type: str, table: str, cols: List[str],
                      vals: list, pk_cols: List[str], row: Dict) -> Tuple:
        non_pk = [c for c in cols if c not in pk_cols]
        if not non_pk:
            raise ValueError("Nessuna colonna da aggiornare (tutte sono PK)")

        if db_type == 'sqlserver':
            set_str = ','.join([f'[{c}] = ?' for c in non_pk])
            where_str = ' AND '.join([f'[{c}] = ?' for c in pk_cols])
            sql = f'UPDATE [{table}] SET {set_str} WHERE {where_str}'
        elif db_type == 'mysql':
            set_str = ','.join([f'`{c}` = %s' for c in non_pk])
            where_str = ' AND '.join([f'`{c}` = %s' for c in pk_cols])
            sql = f'UPDATE `{table}` SET {set_str} WHERE {where_str}'
        else:
            ph = '%s' if db_type == 'postgresql' else '?'
            set_str = ','.join([f'"{c}" = {ph}' for c in non_pk])
            where_str = ' AND '.join([f'"{c}" = {ph}' for c in pk_cols])
            sql = f'UPDATE "{table}" SET {set_str} WHERE {where_str}'

        params = [row[c] for c in non_pk] + [row[c] for c in pk_cols]
        return sql, params

    # ------------------------------------------------------------------
    # HELPERS
    # ------------------------------------------------------------------

    @staticmethod
    def _build_connection_string(db_type: str, params: Dict) -> str:
        if 'connection_string' in params:
            return params['connection_string']
        host = params.get('host', 'localhost')
        port = params.get('port', '')
        database = params.get('database', '')
        user = params.get('user', params.get('username', ''))
        password = params.get('password', '')
        schema = params.get('schema', '')

        if db_type == 'postgresql':
            port = port or 5432
            return f"postgresql://{user}:{password}@{host}:{port}/{database}"
        elif db_type == 'mysql':
            port = port or 3306
            return f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
        elif db_type == 'sqlserver':
            port = port or 1433
            return (f"mssql+pyodbc://{user}:{password}@{host}:{port}/{database}"
                    f"?driver=ODBC+Driver+17+for+SQL+Server")
        elif db_type == 'sqlite':
            return f"sqlite:///{database}"
        else:
            raise ValueError(f"DB type non supportato: {db_type}")
