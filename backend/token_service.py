#!/usr/bin/env python3
"""
Buddyliko — Token Service
Fase 1: Gestione API token per accesso machine-to-machine.

Responsabilità:
  - Generazione token sicuri (blk_live_... / blk_test_...)
  - Hashing e verifica (bcrypt via passlib)
  - Rate limiting in-memory (sliding window)
  - Audit log per ogni evento token
  - CRUD: create, list, get, revoke, rotate
"""

import secrets
import time
import json
import uuid
import threading
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, List, Tuple, Any
from collections import defaultdict

try:
    from passlib.hash import bcrypt as passlib_bcrypt
except ImportError:
    passlib_bcrypt = None
    import hashlib


# ===========================================================================
# TOKEN FORMAT
# ===========================================================================
# Full:   blk_live_a3f8x9kLmN2pQ5rT7vW0yZ1234567890abcdef
# Prefix: blk_live_a3f8x9kL  (17 chars, stored for lookup)
# Hash:   bcrypt(full_token)   (stored for verification)

TOKEN_RANDOM_LENGTH = 32    # chars di parte random
PREFIX_RANDOM_LENGTH = 8    # chars random nel prefix

VALID_SCOPES = [
    'transform:execute', 'transform:read',
    'mapping:read', 'mapping:write',
    'partner:read', 'partner:write',
    'file:read', 'file:write',
    'ai:generate',
    'report:read',
]


# ===========================================================================
# IN-MEMORY RATE LIMITER
# ===========================================================================

class RateLimiter:
    """Sliding window rate limiter. Thread-safe, in-memory."""

    def __init__(self):
        self._hits: Dict[str, List[float]] = defaultdict(list)
        self._lock = threading.Lock()

    def check(self, key: str, limit: int, window_secs: int) -> Tuple[bool, int]:
        """
        Check rate limit. Returns (allowed, remaining).
        """
        if limit <= 0:
            return True, 999999  # 0 = unlimited

        now = time.time()
        cutoff = now - window_secs

        with self._lock:
            hits = self._hits[key]
            # Prune old entries
            self._hits[key] = [t for t in hits if t > cutoff]
            hits = self._hits[key]

            if len(hits) >= limit:
                return False, 0

            hits.append(now)
            return True, limit - len(hits)

    def cleanup(self, max_age_secs: int = 3600):
        """Remove stale entries."""
        cutoff = time.time() - max_age_secs
        with self._lock:
            empty_keys = []
            for k, hits in self._hits.items():
                self._hits[k] = [t for t in hits if t > cutoff]
                if not self._hits[k]:
                    empty_keys.append(k)
            for k in empty_keys:
                del self._hits[k]


# ===========================================================================
# TOKEN SERVICE
# ===========================================================================

class TokenService:
    """Servizio completo per gestione API token."""

    # Rate limit defaults (se non specificati su token o piano)
    DEFAULT_RPM = 60
    DEFAULT_RPH = 1000
    DEFAULT_RPD = 10000

    def __init__(self, conn, RealDictCursor):
        self.conn = conn
        self.RDC = RealDictCursor
        self.rate_limiter = RateLimiter()
        self._init_tables()

    def _init_tables(self):
        cur = self.conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS api_tokens (
                id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                org_id          UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
                name            VARCHAR(255) NOT NULL,
                description     TEXT,
                token_hash      VARCHAR(255) NOT NULL,
                token_prefix    VARCHAR(20) NOT NULL,
                environment     VARCHAR(10) NOT NULL DEFAULT 'live',
                created_by      INTEGER NOT NULL REFERENCES users(id),
                scopes          JSONB NOT NULL DEFAULT '["transform:execute"]',
                rate_limit_rpm  INTEGER,
                rate_limit_rph  INTEGER,
                rate_limit_rpd  INTEGER,
                allowed_ips     JSONB,
                partner_id      UUID,
                tags            JSONB DEFAULT '{}',
                expires_at      TIMESTAMPTZ,
                status          VARCHAR(20) NOT NULL DEFAULT 'active',
                revoked_at      TIMESTAMPTZ,
                revoked_by      INTEGER REFERENCES users(id),
                revoke_reason   TEXT,
                last_used_at    TIMESTAMPTZ,
                last_used_ip    VARCHAR(45),
                use_count       BIGINT DEFAULT 0,
                created_at      TIMESTAMPTZ DEFAULT NOW()
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS token_audit_log (
                id              BIGSERIAL PRIMARY KEY,
                token_id        UUID NOT NULL,
                org_id          UUID NOT NULL,
                event_type      VARCHAR(30) NOT NULL,
                ip_address      VARCHAR(45),
                user_agent      VARCHAR(500),
                endpoint        VARCHAR(200),
                http_status     INTEGER,
                actor_user_id   INTEGER,
                details         JSONB DEFAULT '{}',
                created_at      TIMESTAMPTZ DEFAULT NOW()
            )
        """)

        for idx in [
            "CREATE INDEX IF NOT EXISTS idx_at_org ON api_tokens(org_id, status)",
            "CREATE INDEX IF NOT EXISTS idx_at_prefix ON api_tokens(token_prefix)",
            "CREATE INDEX IF NOT EXISTS idx_at_env ON api_tokens(org_id, environment)",
            "CREATE INDEX IF NOT EXISTS idx_at_partner ON api_tokens(partner_id) WHERE partner_id IS NOT NULL",
            "CREATE INDEX IF NOT EXISTS idx_at_tags ON api_tokens USING gin(tags)",
            "CREATE INDEX IF NOT EXISTS idx_tal_token ON token_audit_log(token_id, created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_tal_org ON token_audit_log(org_id, created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_tal_event ON token_audit_log(event_type, created_at DESC)",
        ]:
            cur.execute(idx)

        self.conn.commit()
        print("   ✅ API Token tables initialized")

    # ── HASHING ───────────────────────────────────────────────────────

    def _hash_token(self, token_value: str) -> str:
        if passlib_bcrypt:
            return passlib_bcrypt.hash(token_value)
        # Fallback: SHA256 (less secure but works without passlib)
        return 'sha256:' + hashlib.sha256(token_value.encode()).hexdigest()

    def _verify_hash(self, token_value: str, token_hash: str) -> bool:
        if token_hash.startswith('sha256:'):
            return token_hash == 'sha256:' + hashlib.sha256(token_value.encode()).hexdigest()
        if passlib_bcrypt:
            return passlib_bcrypt.verify(token_value, token_hash)
        return False

    # ── GENERATE ──────────────────────────────────────────────────────

    def _generate_token_value(self, environment: str) -> Tuple[str, str]:
        """
        Genera token e prefix.
        Returns: (full_token, prefix)
        """
        env_tag = 'live' if environment == 'live' else 'test'
        random_part = secrets.token_urlsafe(TOKEN_RANDOM_LENGTH)
        full_token = f"blk_{env_tag}_{random_part}"
        prefix = f"blk_{env_tag}_{random_part[:PREFIX_RANDOM_LENGTH]}"
        return full_token, prefix

    # ── CREATE ────────────────────────────────────────────────────────

    def create_token(self, *,
                     org_id: str,
                     name: str,
                     created_by: int,
                     description: str = None,
                     environment: str = 'live',
                     scopes: List[str] = None,
                     rate_limit_rpm: int = None,
                     rate_limit_rph: int = None,
                     rate_limit_rpd: int = None,
                     allowed_ips: List[str] = None,
                     partner_id: str = None,
                     tags: dict = None,
                     expires_in_days: int = None) -> Tuple[str, Dict]:
        """
        Crea un nuovo API token.
        Returns: (full_token_value, token_record)
        Il full_token_value viene mostrato UNA SOLA VOLTA.
        """
        if scopes:
            invalid = [s for s in scopes if s not in VALID_SCOPES]
            if invalid:
                raise ValueError(f"Scope non validi: {invalid}. Validi: {VALID_SCOPES}")
        else:
            scopes = ['transform:execute']

        if environment not in ('live', 'sandbox'):
            raise ValueError("environment deve essere 'live' o 'sandbox'")

        full_token, prefix = self._generate_token_value(environment)
        token_hash = self._hash_token(full_token)
        token_id = str(uuid.uuid4())

        expires_at = None
        if expires_in_days:
            expires_at = datetime.now(timezone.utc) + timedelta(days=expires_in_days)

        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO api_tokens
                (id, org_id, name, description, token_hash, token_prefix,
                 environment, created_by, scopes,
                 rate_limit_rpm, rate_limit_rph, rate_limit_rpd,
                 allowed_ips, partner_id, tags, expires_at, status)
            VALUES (%s, %s, %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s, %s, 'active')
        """, (
            token_id, org_id, name, description, token_hash, prefix,
            environment, created_by, json.dumps(scopes),
            rate_limit_rpm, rate_limit_rph, rate_limit_rpd,
            json.dumps(allowed_ips) if allowed_ips else None,
            partner_id, json.dumps(tags or {}), expires_at
        ))

        self._audit(token_id, org_id, 'created', actor_user_id=created_by,
                    details={'name': name, 'scopes': scopes, 'environment': environment})

        self.conn.commit()

        record = self.get_token(token_id)
        return full_token, record

    # ── VERIFY (chiamato ad ogni richiesta API) ───────────────────────

    def verify_token(self, token_value: str, ip_address: str = None,
                     user_agent: str = None, endpoint: str = None) -> Dict:
        """
        Verifica un token API.
        Controlla: esistenza, hash, status, scadenza, IP, rate limit.
        Aggiorna: last_used_at, use_count.
        Returns: token_record dict.
        Raises: ValueError con messaggio specifico.
        """
        # Extract prefix per lookup
        parts = token_value.split('_', 2)
        if len(parts) < 3 or parts[0] != 'blk':
            raise ValueError("Formato token non valido")

        env_tag = parts[1]
        random_part = parts[2]
        prefix = f"blk_{env_tag}_{random_part[:PREFIX_RANDOM_LENGTH]}"

        # Lookup per prefix
        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute("""
            SELECT t.*, o.status as org_status, o.plan as org_plan,
                   o.name as org_name, o.slug as org_slug, o.org_type
            FROM api_tokens t
            JOIN organizations o ON t.org_id = o.id
            WHERE t.token_prefix = %s
        """, (prefix,))

        rows = cur.fetchall()
        if not rows:
            raise ValueError("Token non trovato")

        # Verifica hash (potrebbe esserci collision di prefix, improbabile)
        token_record = None
        for row in rows:
            if self._verify_hash(token_value, row['token_hash']):
                token_record = dict(row)
                break

        if not token_record:
            raise ValueError("Token non valido")

        token_id = str(token_record['id'])
        org_id = str(token_record['org_id'])

        # Check status
        if token_record['status'] != 'active':
            self._audit(token_id, org_id, 'rejected',
                       ip_address=ip_address, details={'reason': f"status={token_record['status']}"})
            raise ValueError(f"Token {token_record['status']}")

        # Check org status
        if token_record.get('org_status') not in ('active', 'trial'):
            raise ValueError("Organizzazione non attiva")

        # Check expiry
        if token_record.get('expires_at'):
            if token_record['expires_at'].replace(tzinfo=timezone.utc) < datetime.now(timezone.utc):
                # Auto-expire
                cur2 = self.conn.cursor()
                cur2.execute(
                    "UPDATE api_tokens SET status = 'expired' WHERE id = %s",
                    (token_id,)
                )
                self.conn.commit()
                self._audit(token_id, org_id, 'expired', ip_address=ip_address)
                raise ValueError("Token scaduto")

        # Check IP whitelist
        if token_record.get('allowed_ips') and ip_address:
            ips = token_record['allowed_ips']
            if isinstance(ips, str):
                ips = json.loads(ips)
            if ips and ip_address not in ips:
                # Basic CIDR check would go here
                self._audit(token_id, org_id, 'ip_rejected',
                           ip_address=ip_address, details={'allowed': ips})
                raise ValueError("IP non autorizzato")

        # Rate limiting
        rpm = token_record.get('rate_limit_rpm') or self.DEFAULT_RPM
        rph = token_record.get('rate_limit_rph') or self.DEFAULT_RPH
        rpd = token_record.get('rate_limit_rpd') or self.DEFAULT_RPD

        ok_m, rem_m = self.rate_limiter.check(f"rpm:{token_id}", rpm, 60)
        if not ok_m:
            self._audit(token_id, org_id, 'rate_limited',
                       ip_address=ip_address, details={'type': 'rpm', 'limit': rpm})
            raise ValueError(f"Rate limit superato ({rpm}/min)")

        ok_h, _ = self.rate_limiter.check(f"rph:{token_id}", rph, 3600)
        if not ok_h:
            self._audit(token_id, org_id, 'rate_limited',
                       ip_address=ip_address, details={'type': 'rph', 'limit': rph})
            raise ValueError(f"Rate limit superato ({rph}/ora)")

        ok_d, _ = self.rate_limiter.check(f"rpd:{token_id}", rpd, 86400)
        if not ok_d:
            self._audit(token_id, org_id, 'rate_limited',
                       ip_address=ip_address, details={'type': 'rpd', 'limit': rpd})
            raise ValueError(f"Rate limit superato ({rpd}/giorno)")

        # Update last used
        cur2 = self.conn.cursor()
        cur2.execute("""
            UPDATE api_tokens
            SET last_used_at = NOW(), last_used_ip = %s, use_count = use_count + 1
            WHERE id = %s
        """, (ip_address, token_id))
        self.conn.commit()

        return token_record

    # ── LIST / GET ────────────────────────────────────────────────────

    def list_tokens(self, org_id: str, include_revoked: bool = False) -> List[Dict]:
        """Lista token di un'org (senza hash)."""
        cur = self.conn.cursor(cursor_factory=self.RDC)
        status_filter = "" if include_revoked else "AND t.status = 'active'"
        cur.execute(f"""
            SELECT t.id, t.name, t.description, t.token_prefix, t.environment,
                   t.scopes, t.status, t.created_by, t.created_at,
                   t.expires_at, t.last_used_at, t.last_used_ip, t.use_count,
                   t.rate_limit_rpm, t.rate_limit_rph, t.rate_limit_rpd,
                   t.allowed_ips, t.partner_id, t.tags,
                   t.revoked_at, t.revoke_reason,
                   u.email as created_by_email, u.name as created_by_name
            FROM api_tokens t
            JOIN users u ON t.created_by = u.id
            WHERE t.org_id = %s {status_filter}
            ORDER BY t.created_at DESC
        """, (org_id,))
        return [dict(r) for r in cur.fetchall()]

    def get_token(self, token_id: str) -> Optional[Dict]:
        """Singolo token per id (senza hash)."""
        cur = self.conn.cursor(cursor_factory=self.RDC)
        cur.execute("""
            SELECT t.id, t.org_id, t.name, t.description, t.token_prefix, t.environment,
                   t.scopes, t.status, t.created_by, t.created_at,
                   t.expires_at, t.last_used_at, t.last_used_ip, t.use_count,
                   t.rate_limit_rpm, t.rate_limit_rph, t.rate_limit_rpd,
                   t.allowed_ips, t.partner_id, t.tags,
                   t.revoked_at, t.revoked_by, t.revoke_reason,
                   u.email as created_by_email
            FROM api_tokens t
            JOIN users u ON t.created_by = u.id
            WHERE t.id = %s
        """, (token_id,))
        row = cur.fetchone()
        return dict(row) if row else None

    # ── REVOKE ────────────────────────────────────────────────────────

    def revoke_token(self, token_id: str, revoked_by: int, reason: str = None) -> bool:
        cur = self.conn.cursor()
        cur.execute("""
            UPDATE api_tokens
            SET status = 'revoked', revoked_at = NOW(), revoked_by = %s, revoke_reason = %s
            WHERE id = %s AND status = 'active'
            RETURNING org_id
        """, (revoked_by, reason, token_id))
        row = cur.fetchone()
        if not row:
            return False

        self._audit(token_id, str(row[0]), 'revoked',
                   actor_user_id=revoked_by, details={'reason': reason})
        self.conn.commit()
        return True

    # ── ROTATE ────────────────────────────────────────────────────────

    def rotate_token(self, token_id: str, rotated_by: int) -> Tuple[str, Dict]:
        """
        Genera un nuovo token mantenendo le stesse configurazioni.
        Il vecchio viene revocato, il nuovo viene creato.
        Returns: (new_full_token, new_token_record)
        """
        old = self.get_token(token_id)
        if not old:
            raise ValueError("Token non trovato")
        if old['status'] != 'active':
            raise ValueError("Solo token attivi possono essere ruotati")

        # Revoca il vecchio
        self.revoke_token(token_id, rotated_by, reason="Rotated")

        # Crea il nuovo con le stesse config
        scopes = old.get('scopes', ['transform:execute'])
        if isinstance(scopes, str):
            scopes = json.loads(scopes)
        tags = old.get('tags', {})
        if isinstance(tags, str):
            tags = json.loads(tags)
        allowed_ips = old.get('allowed_ips')
        if isinstance(allowed_ips, str):
            allowed_ips = json.loads(allowed_ips)

        new_token, new_record = self.create_token(
            org_id=str(old['org_id']),
            name=old['name'],
            description=old.get('description'),
            environment=old.get('environment', 'live'),
            created_by=rotated_by,
            scopes=scopes,
            rate_limit_rpm=old.get('rate_limit_rpm'),
            rate_limit_rph=old.get('rate_limit_rph'),
            rate_limit_rpd=old.get('rate_limit_rpd'),
            allowed_ips=allowed_ips,
            partner_id=str(old['partner_id']) if old.get('partner_id') else None,
            tags=tags,
        )

        self._audit(str(new_record['id']), str(old['org_id']), 'rotated',
                   actor_user_id=rotated_by,
                   details={'old_token_id': token_id})
        return new_token, new_record

    # ── AUDIT LOG ─────────────────────────────────────────────────────

    def _audit(self, token_id: str, org_id: str, event_type: str,
               ip_address: str = None, user_agent: str = None,
               endpoint: str = None, http_status: int = None,
               actor_user_id: int = None, details: dict = None):
        try:
            cur = self.conn.cursor()
            cur.execute("""
                INSERT INTO token_audit_log
                    (token_id, org_id, event_type, ip_address, user_agent,
                     endpoint, http_status, actor_user_id, details)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                token_id, org_id, event_type, ip_address, user_agent,
                endpoint, http_status, actor_user_id,
                json.dumps(details or {})
            ))
        except Exception:
            pass  # Audit failure should never break the request

    def get_audit_log(self, org_id: str, token_id: str = None,
                      limit: int = 50) -> List[Dict]:
        cur = self.conn.cursor(cursor_factory=self.RDC)
        if token_id:
            cur.execute("""
                SELECT * FROM token_audit_log
                WHERE org_id = %s AND token_id = %s
                ORDER BY created_at DESC LIMIT %s
            """, (org_id, token_id, limit))
        else:
            cur.execute("""
                SELECT * FROM token_audit_log
                WHERE org_id = %s
                ORDER BY created_at DESC LIMIT %s
            """, (org_id, limit))
        return [dict(r) for r in cur.fetchall()]
