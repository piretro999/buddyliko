#!/usr/bin/env python3
"""
Authentication System with OAuth Support
Supports: Local auth, Google, Facebook, GitHub, Microsoft

Usage:
    auth = AuthManager(storage, secret_key)
    user = auth.register_user(email, password)
    user = auth.login(email, password)
    user = auth.oauth_login(provider, token)
"""

import os
import secrets
import hashlib
import hmac
import base64
import struct
import time
from typing import Optional
from datetime import datetime, timedelta
import jwt
from passlib.hash import bcrypt


class AuthManager:
    """Complete authentication system"""
    
    def __init__(self, storage, secret_key: str, token_expiry_hours: int = 24):
        self.storage = storage
        self.secret_key = secret_key
        self.token_expiry_hours = token_expiry_hours
    
    # ===================================================================
    # LOCAL AUTHENTICATION
    # ===================================================================
    
    def register_user(self, email: str, password: str, name: str = "") -> tuple[bool, str, dict | None]:
        """
        Register new user with email/password
        Returns: (success, message, user_data)
        """
        # Check if user exists
        existing = self.storage.get_user_by_email(email)
        if existing:
            return False, "Email already registered", None
        
        # Hash password
        password_hash = bcrypt.hash(password)
        
        # Create user
        user = {
            'email': email,
            'password_hash': password_hash,
            'name': name or email.split('@')[0],
            'role': 'USER',
            'status': 'PENDING',
            'plan': 'FREE',
            'auth_provider': 'local',
            'auth_provider_id': None,
            'created_at': datetime.now().isoformat()
        }
        
        user_id = self.storage.save_user(user)
        user['id'] = user_id
        
        # Remove password hash from returned data
        user_data = {k: v for k, v in user.items() if k != 'password_hash'}
        
        return True, "User registered successfully", user_data
    
    def login(self, email: str, password: str) -> tuple[bool, str, dict | None]:
        """
        Login with email/password
        Returns: (success, message, user_data_with_token)
        """
        # Get user
        user = self.storage.get_user_by_email(email)
        if not user:
            return False, "Invalid credentials", None
        
        # Check password
        if not user.get('password_hash'):
            return False, "This account uses OAuth login", None
        
        if not bcrypt.verify(password, user['password_hash']):
            return False, "Invalid credentials", None
        
        # Check account status
        status = user.get('status', 'APPROVED')
        if status == 'PENDING':
            return False, "Account in attesa di approvazione. Contatta l'amministratore.", None
        if status in ('SUSPENDED', 'BLOCKED'):
            return False, "Account sospeso o bloccato. Contatta l'amministratore.", None
        
        # Generate JWT token
        token = self._generate_token(user)
        
        # Return user data with token
        user_data = {k: v for k, v in user.items() if k != 'password_hash'}
        user_data['token'] = token
        
        return True, "Login successful", user_data
    
    # ===================================================================
    # OAUTH AUTHENTICATION
    # ===================================================================
    
    def oauth_login(self, provider: str, oauth_data: dict) -> tuple[bool, str, dict | None]:
        """
        OAuth login (Google, Facebook, GitHub, Microsoft)
        
        oauth_data should contain:
        - provider_id: Unique ID from provider
        - email: User email
        - name: User name
        - access_token: OAuth access token (optional, for verification)
        
        Returns: (success, message, user_data_with_token)
        """
        provider_id = oauth_data.get('provider_id')
        email = oauth_data.get('email')
        name = oauth_data.get('name', '')
        
        if not provider_id or not email:
            return False, "Invalid OAuth data", None
        
        # Check if user exists with this provider
        user = self.storage.get_user_by_email(email)
        
        if user:
            # User exists - update if needed
            if user.get('auth_provider') != provider:
                # User registered with different method
                return False, f"Email already registered with {user.get('auth_provider')}", None
            
            # Update provider ID if changed
            if user.get('auth_provider_id') != provider_id:
                user['auth_provider_id'] = provider_id
                # Note: storage update method needed here
        else:
            # Create new user
            user = {
                'email': email,
                'password_hash': None,  # OAuth users don't have passwords
                'name': name,
                'role': 'USER',
                'status': 'APPROVED',
                'plan': 'FREE',
                'auth_provider': provider,
                'auth_provider_id': provider_id,
                'created_at': datetime.now().isoformat()
            }
            
            user_id = self.storage.save_user(user)
            user['id'] = user_id
        
        # Generate JWT token
        token = self._generate_token(user)
        
        # Return user data
        user_data = {k: v for k, v in user.items() if k != 'password_hash'}
        user_data['token'] = token
        
        return True, "OAuth login successful", user_data
    
    # ===================================================================
    # TOKEN MANAGEMENT
    # ===================================================================
    
    def _generate_token(self, user: dict) -> str:
        """Generate JWT token"""
        payload = {
            'id': str(user['id']),
            'user_id': str(user['id']),
            'email': user.get('email', ''),
            'name': user.get('name', ''),
            'role': user.get('role', 'USER'),
            'status': user.get('status', 'APPROVED'),
            'plan': user.get('plan', 'FREE'),
            'exp': datetime.utcnow() + timedelta(hours=self.token_expiry_hours),
            'iat': datetime.utcnow()
        }
        return jwt.encode(payload, self.secret_key, algorithm='HS256')
    
    def verify_token(self, token: str) -> tuple[bool, dict | None]:
        """
        Verify JWT token
        Returns: (valid, payload)
        """
        try:
            payload = jwt.decode(token, self.secret_key, algorithms=['HS256'])
            return True, payload
        except jwt.ExpiredSignatureError:
            return False, {'error': 'Token expired'}
        except jwt.InvalidTokenError:
            return False, {'error': 'Invalid token'}
    
    def refresh_token(self, old_token: str) -> tuple[bool, str | None]:
        """
        Refresh token if still valid
        Returns: (success, new_token)
        """
        valid, payload = self.verify_token(old_token)
        if not valid:
            return False, None
        
        # Get user
        user = self.storage.get_user(payload['user_id'])
        if not user:
            return False, None
        
        # Generate new token
        new_token = self._generate_token(user)
        return True, new_token
    
    # ===================================================================
    # PASSWORD MANAGEMENT
    # ===================================================================
    
    def change_password(self, user_id: str, old_password: str, new_password: str) -> tuple[bool, str]:
        """Change user password"""
        user = self.storage.get_user(user_id)
        if not user:
            return False, "User not found"
        
        if user.get('auth_provider') != 'local':
            return False, "OAuth users cannot change password"
        
        # Verify old password
        if not bcrypt.verify(old_password, user['password_hash']):
            return False, "Invalid current password"
        
        # Update password
        user['password_hash'] = bcrypt.hash(new_password)
        # Note: storage update method needed here
        
        return True, "Password changed successfully"
    
    def reset_password_request(self, email: str) -> tuple[bool, str, str | None]:
        """
        Generate password reset token
        Returns: (success, message, reset_token)
        """
        user = self.storage.get_user_by_email(email)
        if not user:
            # Don't reveal if email exists
            return True, "If email exists, reset link sent", None
        
        if not user.get('password_hash'):
            # Utente senza password (solo OAuth) — non può fare reset
            return False, "Questo account usa solo accesso OAuth", None
        
        # Generate reset token (valid for 1 hour)
        reset_payload = {
            'user_id': user['id'],
            'email': email,
            'type': 'password_reset',
            'exp': datetime.utcnow() + timedelta(hours=1),
            'iat': datetime.utcnow()
        }
        
        reset_token = jwt.encode(reset_payload, self.secret_key, algorithm='HS256')
        
        return True, "Reset token generated", reset_token
    
    def reset_password(self, reset_token: str, new_password: str) -> tuple[bool, str]:
        """Reset password with token — PATCHED by 007"""
        try:
            payload = jwt.decode(reset_token, self.secret_key, algorithms=['HS256'])

            if payload.get('type') != 'password_reset':
                return False, "Token di reset non valido"

            user_id = str(payload.get('user_id', ''))
            user = self.storage.get_user(user_id)
            if not user:
                return False, "Utente non trovato"

            # Hash della nuova password
            try:
                import bcrypt as _bcrypt
                new_hash = _bcrypt.hashpw(new_password.encode('utf-8'), _bcrypt.gensalt()).decode('utf-8')
            except ImportError:
                import hashlib
                new_hash = hashlib.sha256(new_password.encode('utf-8')).hexdigest()

            # Salva nel database
            self.storage.update_user(user_id, {"password_hash": new_hash})

            return True, "Password reimpostata con successo"

        except jwt.ExpiredSignatureError:
            return False, "Il link di reset è scaduto (valido 1 ora)"
        except jwt.InvalidTokenError:
            return False, "Token di reset non valido"

    # ==================================================================
    # EMAIL VERIFICATION
    # ==================================================================

    def generate_email_verification_token(self, user_id: str, email: str) -> str:
        """Generate a JWT token for email verification (24h expiry)."""
        payload = {
            'sub': str(user_id),
            'email': email,
            'purpose': 'email_verify',
            'exp': datetime.utcnow() + timedelta(hours=24)
        }
        return jwt.encode(payload, self.secret_key, algorithm='HS256')

    def verify_email_token(self, token: str) -> tuple[bool, str, str]:
        """Verify email token. Returns (success, message, user_id)."""
        try:
            payload = jwt.decode(token, self.secret_key, algorithms=['HS256'])
            if payload.get('purpose') != 'email_verify':
                return False, "Token non valido", ""
            user_id = payload['sub']
            # Mark email as verified in DB
            self.storage.update_user(user_id, {'email_verified': True})
            return True, "Email verificata con successo!", user_id
        except jwt.ExpiredSignatureError:
            return False, "Link di verifica scaduto. Richiedi un nuovo link.", ""
        except jwt.InvalidTokenError:
            return False, "Link di verifica non valido.", ""

    # ==================================================================
    # MFA — TOTP (Google Authenticator / Authy)
    # ==================================================================

    @staticmethod
    def generate_totp_secret() -> str:
        """Generate a new TOTP secret (base32 encoded)."""
        return base64.b32encode(secrets.token_bytes(20)).decode('ascii')

    @staticmethod
    def get_totp_uri(secret: str, email: str, issuer: str = 'Buddyliko') -> str:
        """Generate otpauth:// URI for QR code."""
        return f"otpauth://totp/{issuer}:{email}?secret={secret}&issuer={issuer}&digits=6&period=30"

    @staticmethod
    def verify_totp(secret: str, code: str, window: int = 1) -> bool:
        """Verify a TOTP code (allows ±window*30s drift)."""
        try:
            key = base64.b32decode(secret.upper())
            now = int(time.time())
            for offset in range(-window, window + 1):
                counter = (now // 30) + offset
                # HOTP calculation (RFC 4226)
                msg = struct.pack('>Q', counter)
                h = hmac.new(key, msg, hashlib.sha1).digest()
                o = h[-1] & 0x0F
                otp = (struct.unpack('>I', h[o:o+4])[0] & 0x7FFFFFFF) % 1000000
                if str(otp).zfill(6) == code.strip():
                    return True
            return False
        except Exception:
            return False

    def setup_mfa_totp(self, user_id: str) -> dict:
        """Start TOTP setup — returns secret + URI for QR code."""
        secret = self.generate_totp_secret()
        user = self.storage.get_user(user_id) if hasattr(self.storage, 'get_user') else None
        email = user.get('email', '') if user else ''
        uri = self.get_totp_uri(secret, email)
        # Store pending secret (not yet confirmed)
        self.storage.update_user(user_id, {'mfa_totp_pending': secret})
        return {'secret': secret, 'uri': uri, 'email': email}

    def confirm_mfa_totp(self, user_id: str, code: str) -> tuple[bool, str]:
        """Confirm TOTP setup by verifying first code."""
        user = self.storage.get_user(user_id) if hasattr(self.storage, 'get_user') else None
        if not user:
            return False, "Utente non trovato"
        pending = user.get('mfa_totp_pending')
        if not pending:
            return False, "Nessun setup TOTP in corso"
        if self.verify_totp(pending, code):
            self.storage.update_user(user_id, {
                'mfa_secret': pending,
                'mfa_method': user.get('mfa_method', '') + ',totp' if user.get('mfa_method') else 'totp',
                'mfa_enabled': True,
                'mfa_totp_pending': None
            })
            return True, "MFA TOTP attivato!"
        return False, "Codice non valido. Riprova."

    # ==================================================================
    # MFA — EMAIL CODE
    # ==================================================================

    def generate_mfa_email_code(self, user_id: str) -> tuple[str, str]:
        """Generate a 6-digit MFA code for email. Returns (code, expiry_token)."""
        code = str(secrets.randbelow(900000) + 100000)  # 6 digits
        payload = {
            'sub': str(user_id),
            'code': code,
            'purpose': 'mfa_email',
            'exp': datetime.utcnow() + timedelta(minutes=10)
        }
        token = jwt.encode(payload, self.secret_key, algorithm='HS256')
        return code, token

    def verify_mfa_email_code(self, token: str, code: str) -> tuple[bool, str]:
        """Verify MFA email code."""
        try:
            payload = jwt.decode(token, self.secret_key, algorithms=['HS256'])
            if payload.get('purpose') != 'mfa_email':
                return False, ""
            if payload.get('code') == code.strip():
                return True, payload['sub']
            return False, ""
        except (jwt.ExpiredSignatureError, jwt.InvalidTokenError):
            return False, ""

    def enable_mfa_email(self, user_id: str) -> tuple[bool, str]:
        """Enable email-based MFA for user."""
        user = self.storage.get_user(user_id) if hasattr(self.storage, 'get_user') else None
        if not user:
            return False, "Utente non trovato"
        methods = user.get('mfa_method', '') or ''
        if 'email' not in methods:
            methods = (methods + ',email').strip(',')
        self.storage.update_user(user_id, {'mfa_method': methods, 'mfa_enabled': True})
        return True, "MFA via email attivato!"

    def disable_mfa(self, user_id: str) -> tuple[bool, str]:
        """Disable all MFA for user."""
        self.storage.update_user(user_id, {
            'mfa_enabled': False,
            'mfa_method': None,
            'mfa_secret': None,
            'mfa_totp_pending': None
        })
        return True, "MFA disattivato"

    def get_mfa_status(self, user_id: str) -> dict:
        """Get MFA status for user."""
        user = self.storage.get_user(user_id) if hasattr(self.storage, 'get_user') else None
        if not user:
            return {'enabled': False, 'methods': []}
        methods = [m for m in (user.get('mfa_method') or '').split(',') if m]
        return {
            'enabled': bool(user.get('mfa_enabled')),
            'methods': methods,
            'email_verified': bool(user.get('email_verified'))
        }

    # ==================================================================
    # LOGIN WITH MFA SUPPORT
    # ==================================================================

    def login_step1(self, email: str, password: str) -> tuple[bool, str, dict | None]:
        """
        Step 1 of login: verify credentials.
        If MFA enabled, returns mfa_required=True + partial_token.
        If no MFA, returns full token (backward compatible).
        """
        user = self.storage.get_user_by_email(email)
        if not user:
            return False, "Invalid credentials", None
        if not user.get('password_hash'):
            return False, "This account uses OAuth login", None
        if not bcrypt.verify(password, user['password_hash']):
            return False, "Invalid credentials", None

        status = user.get('status', 'APPROVED')
        if status == 'PENDING':
            return False, "Account in attesa di approvazione. Contatta l'amministratore.", None
        if status in ('SUSPENDED', 'BLOCKED'):
            return False, "Account sospeso o bloccato. Contatta l'amministratore.", None

        # Check if email is verified
        if not user.get('email_verified') and user.get('auth_provider') == 'local':
            return False, "Email non verificata. Controlla la tua casella di posta.", None

        # Check MFA
        if user.get('mfa_enabled'):
            methods = [m for m in (user.get('mfa_method') or '').split(',') if m]
            # Generate partial token (not a full login token)
            partial = jwt.encode({
                'sub': str(user['id']),
                'purpose': 'mfa_partial',
                'exp': datetime.utcnow() + timedelta(minutes=10)
            }, self.secret_key, algorithm='HS256')
            return True, "MFA required", {
                'mfa_required': True,
                'mfa_methods': methods,
                'mfa_token': partial,
                'user_email': user['email'],
                'user_name': user.get('name', '')
            }

        # No MFA — return full token
        token = self._generate_token(user)
        user_data = {k: v for k, v in user.items() if k != 'password_hash'}
        user_data['token'] = token
        return True, "Login successful", user_data

    def login_step2_mfa(self, partial_token: str, code: str, method: str = 'totp',
                         email_mfa_token: str = None) -> tuple[bool, str, dict | None]:
        """
        Step 2: verify MFA code and return full token.
        method: 'totp' or 'email'
        """
        try:
            payload = jwt.decode(partial_token, self.secret_key, algorithms=['HS256'])
            if payload.get('purpose') != 'mfa_partial':
                return False, "Token non valido", None
            user_id = payload['sub']
        except (jwt.ExpiredSignatureError, jwt.InvalidTokenError):
            return False, "Sessione scaduta, effettua di nuovo il login", None

        user = self.storage.get_user(user_id) if hasattr(self.storage, 'get_user') else None
        if not user:
            return False, "Utente non trovato", None

        verified = False
        if method == 'totp':
            secret = user.get('mfa_secret')
            if secret and self.verify_totp(secret, code):
                verified = True
        elif method == 'email' and email_mfa_token:
            ok, uid = self.verify_mfa_email_code(email_mfa_token, code)
            if ok and uid == user_id:
                verified = True

        if not verified:
            return False, "Codice non valido", None

        # MFA verified — issue full token
        token = self._generate_token(user)
        user_data = {k: v for k, v in user.items() if k != 'password_hash'}
        user_data['token'] = token
        return True, "Login successful", user_data


# ===========================================================================
# OAUTH PROVIDERS INTEGRATION
# ===========================================================================

class OAuthProviders:
    """OAuth provider configurations"""
    
    @staticmethod
    def get_google_config(client_id: str, client_secret: str, redirect_uri: str) -> dict:
        """Google OAuth configuration"""
        return {
            'provider': 'google',
            'auth_url': 'https://accounts.google.com/o/oauth2/v2/auth',
            'token_url': 'https://oauth2.googleapis.com/token',
            'userinfo_url': 'https://www.googleapis.com/oauth2/v2/userinfo',
            'client_id': client_id,
            'client_secret': client_secret,
            'redirect_uri': redirect_uri,
            'scope': 'openid email profile'
        }
    
    @staticmethod
    def get_facebook_config(app_id: str, app_secret: str, redirect_uri: str) -> dict:
        """Facebook OAuth configuration"""
        return {
            'provider': 'facebook',
            'auth_url': 'https://www.facebook.com/v12.0/dialog/oauth',
            'token_url': 'https://graph.facebook.com/v12.0/oauth/access_token',
            'userinfo_url': 'https://graph.facebook.com/me',
            'app_id': app_id,
            'app_secret': app_secret,
            'redirect_uri': redirect_uri,
            'scope': 'email public_profile'
        }
    
    @staticmethod
    def get_github_config(client_id: str, client_secret: str, redirect_uri: str) -> dict:
        """GitHub OAuth configuration"""
        return {
            'provider': 'github',
            'auth_url': 'https://github.com/login/oauth/authorize',
            'token_url': 'https://github.com/login/oauth/access_token',
            'userinfo_url': 'https://api.github.com/user',
            'client_id': client_id,
            'client_secret': client_secret,
            'redirect_uri': redirect_uri,
            'scope': 'user:email'
        }
    
    @staticmethod
    def get_microsoft_config(client_id: str, client_secret: str, redirect_uri: str) -> dict:
        """Microsoft OAuth configuration"""
        return {
            'provider': 'microsoft',
            'auth_url': 'https://login.microsoftonline.com/common/oauth2/v2.0/authorize',
            'token_url': 'https://login.microsoftonline.com/common/oauth2/v2.0/token',
            'userinfo_url': 'https://graph.microsoft.com/v1.0/me',
            'client_id': client_id,
            'client_secret': client_secret,
            'redirect_uri': redirect_uri,
            'scope': 'openid email profile'
        }


# ===========================================================================
# MIDDLEWARE FOR FASTAPI
# ===========================================================================

from fastapi import Request, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

security = HTTPBearer()


# ===========================================================================
# FEDERATED IDENTITY MANAGER
# ===========================================================================

class FederatedIdentityManager:
    """
    Gestisce la federazione di più provider OAuth per lo stesso account.
    Un utente con email+password può collegare anche Google, GitHub, Facebook
    e viceversa — e scollegarli quando vuole.

    Tabella: user_auth_providers
      (user_id, provider, provider_id, email, name, linked_at)

    Flusso link:
      1. Utente loggato → POST /api/auth/link/{provider} con token OAuth
      2. Se il provider_id non è già associato ad altro account → link creato
      3. Al prossimo login con quel provider → token Buddyliko emesso

    Flusso unlink:
      1. POST /api/auth/unlink/{provider}
      2. Verificato che resti almeno un metodo di login (password o altro provider)
    """

    SUPPORTED_PROVIDERS = {'google', 'facebook', 'github', 'microsoft'}

    def __init__(self, conn, RealDictCursor):
        self.conn = conn
        self.RealDictCursor = RealDictCursor
        self._init_table()

    def _init_table(self):
        cur = self.conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS user_auth_providers (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(255) NOT NULL,
                provider VARCHAR(50) NOT NULL,
                provider_id VARCHAR(255) NOT NULL,
                provider_email VARCHAR(255),
                provider_name VARCHAR(255),
                linked_at TIMESTAMPTZ DEFAULT NOW(),
                last_used_at TIMESTAMPTZ,
                UNIQUE(provider, provider_id),
                UNIQUE(user_id, provider)
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_uap_user ON user_auth_providers(user_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_uap_provider ON user_auth_providers(provider, provider_id)")

    def get_linked_providers(self, user_id: str) -> list[dict]:
        """Lista provider collegati a questo account."""
        cur = self.conn.cursor(cursor_factory=self.RealDictCursor)
        cur.execute("""
            SELECT provider, provider_email, provider_name, linked_at, last_used_at
            FROM user_auth_providers WHERE user_id = %s ORDER BY linked_at
        """, (str(user_id),))
        return [dict(r) for r in cur.fetchall()]

    def find_user_by_provider(self, provider: str, provider_id: str) -> str | None:
        """Trova user_id dato provider+provider_id. Ritorna None se non trovato."""
        cur = self.conn.cursor()
        cur.execute("""
            SELECT user_id FROM user_auth_providers
            WHERE provider = %s AND provider_id = %s
        """, (provider, str(provider_id)))
        row = cur.fetchone()
        return str(row[0]) if row else None

    def link_provider(self, user_id: str, provider: str,
                      provider_id: str, provider_email: str = None,
                      provider_name: str = None) -> tuple[bool, str]:
        """
        Collega un provider OAuth all'account utente.
        Ritorna (success, message).
        """
        if provider not in self.SUPPORTED_PROVIDERS:
            return False, f"Provider non supportato: {provider}"

        # Controlla che questo provider_id non sia già collegato ad un altro account
        existing_user = self.find_user_by_provider(provider, provider_id)
        if existing_user and existing_user != str(user_id):
            return False, f"Questo account {provider} è già collegato a un altro utente"

        # Controlla che l'utente non abbia già questo provider
        cur = self.conn.cursor()
        cur.execute("""
            SELECT id FROM user_auth_providers
            WHERE user_id = %s AND provider = %s
        """, (str(user_id), provider))
        if cur.fetchone():
            # Aggiorna provider_id (potrebbe essere cambiato)
            cur.execute("""
                UPDATE user_auth_providers
                SET provider_id = %s, provider_email = %s, provider_name = %s,
                    last_used_at = NOW()
                WHERE user_id = %s AND provider = %s
            """, (str(provider_id), provider_email, provider_name, str(user_id), provider))
            return True, f"Account {provider} aggiornato"

        cur.execute("""
            INSERT INTO user_auth_providers
                (user_id, provider, provider_id, provider_email, provider_name)
            VALUES (%s, %s, %s, %s, %s)
        """, (str(user_id), provider, str(provider_id), provider_email, provider_name))
        return True, f"Account {provider} collegato con successo"

    def unlink_provider(self, user_id: str, provider: str,
                        has_password: bool) -> tuple[bool, str]:
        """
        Scollega un provider OAuth dall'account.
        Blocca se sarebbe l'unico metodo di login rimasto.
        """
        cur = self.conn.cursor()
        # Conta i provider attualmente collegati
        cur.execute("""
            SELECT COUNT(*) FROM user_auth_providers WHERE user_id = %s
        """, (str(user_id),))
        count = cur.fetchone()[0]

        if not has_password and count <= 1:
            return False, ("Impossibile scollegare: è l'unico metodo di login. "
                           "Imposta prima una password per il tuo account.")

        cur.execute("""
            DELETE FROM user_auth_providers WHERE user_id = %s AND provider = %s
        """, (str(user_id), provider))
        if cur.rowcount == 0:
            return False, f"Account {provider} non trovato per questo utente"
        return True, f"Account {provider} scollegato"

    def touch_last_used(self, user_id: str, provider: str):
        """Aggiorna last_used_at al login."""
        cur = self.conn.cursor()
        cur.execute("""
            UPDATE user_auth_providers SET last_used_at = NOW()
            WHERE user_id = %s AND provider = %s
        """, (str(user_id), provider))

    def migrate_legacy_provider(self, user_id: str, provider: str,
                                provider_id: str, provider_email: str = None):
        """
        Migra i provider dall'utente con il vecchio schema
        (auth_provider/auth_provider_id su users) alla nuova tabella.
        Chiamato automaticamente al primo login se auth_provider != 'local'.
        """
        if not provider or provider == 'local':
            return
        existing = self.find_user_by_provider(provider, str(provider_id))
        if not existing:
            self.link_provider(user_id, provider, provider_id, provider_email)

def create_auth_dependency(auth_manager: AuthManager):
    """Create FastAPI dependency for authentication"""
    
    async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
        """Verify token and return current user"""
        token = credentials.credentials
        
        valid, payload = auth_manager.verify_token(token)
        if not valid:
            raise HTTPException(status_code=401, detail="Invalid or expired token")
        
        user = auth_manager.storage.get_user(payload['user_id'])
        if not user:
            raise HTTPException(status_code=401, detail="User not found")
        
        # Remove sensitive data
        user_data = {k: v for k, v in user.items() if k != 'password_hash'}
        return user_data
    
    return get_current_user


# ===========================================================================
# USAGE EXAMPLE
# ===========================================================================

if __name__ == '__main__':
    from storage_layer import StorageFactory
    
    # Initialize storage
    storage = StorageFactory.get_storage()
    
    # Initialize auth
    auth = AuthManager(storage, secret_key="your-secret-key-change-in-production")
    
    # Register user
    success, msg, user = auth.register_user("test@example.com", "password123", "Test User")
    if success:
        print(f"✅ Registered: {user}")
    
    # Login
    success, msg, user_with_token = auth.login("test@example.com", "password123")
    if success:
        print(f"✅ Logged in, token: {user_with_token['token'][:20]}...")
    
    # Verify token
    valid, payload = auth.verify_token(user_with_token['token'])
    print(f"✅ Token valid: {valid}, user_id: {payload.get('user_id')}")
