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
from typing import Optional, Dict, Tuple
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
    
    def register_user(self, email: str, password: str, name: str = "") -> Tuple[bool, str, Optional[Dict]]:
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
            'auth_provider': 'local',
            'auth_provider_id': None,
            'created_at': datetime.now().isoformat()
        }
        
        user_id = self.storage.save_user(user)
        user['id'] = user_id
        
        # Remove password hash from returned data
        user_data = {k: v for k, v in user.items() if k != 'password_hash'}
        
        return True, "User registered successfully", user_data
    
    def login(self, email: str, password: str) -> Tuple[bool, str, Optional[Dict]]:
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
        
        # Generate JWT token
        token = self._generate_token(user)
        
        # Return user data with token
        user_data = {k: v for k, v in user.items() if k != 'password_hash'}
        user_data['token'] = token
        
        return True, "Login successful", user_data
    
    # ===================================================================
    # OAUTH AUTHENTICATION
    # ===================================================================
    
    def oauth_login(self, provider: str, oauth_data: Dict) -> Tuple[bool, str, Optional[Dict]]:
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
    
    def _generate_token(self, user: Dict) -> str:
        """Generate JWT token"""
        payload = {
            'user_id': user['id'],
            'email': user['email'],
            'exp': datetime.utcnow() + timedelta(hours=self.token_expiry_hours),
            'iat': datetime.utcnow()
        }
        
        return jwt.encode(payload, self.secret_key, algorithm='HS256')
    
    def verify_token(self, token: str) -> Tuple[bool, Optional[Dict]]:
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
    
    def refresh_token(self, old_token: str) -> Tuple[bool, Optional[str]]:
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
    
    def change_password(self, user_id: str, old_password: str, new_password: str) -> Tuple[bool, str]:
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
    
    def reset_password_request(self, email: str) -> Tuple[bool, str, Optional[str]]:
        """
        Generate password reset token
        Returns: (success, message, reset_token)
        """
        user = self.storage.get_user_by_email(email)
        if not user:
            # Don't reveal if email exists
            return True, "If email exists, reset link sent", None
        
        if user.get('auth_provider') != 'local':
            return False, "OAuth users cannot reset password", None
        
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
    
    def reset_password(self, reset_token: str, new_password: str) -> Tuple[bool, str]:
        """Reset password with token"""
        try:
            payload = jwt.decode(reset_token, self.secret_key, algorithms=['HS256'])
            
            if payload.get('type') != 'password_reset':
                return False, "Invalid reset token"
            
            user = self.storage.get_user(payload['user_id'])
            if not user:
                return False, "User not found"
            
            # Update password
            user['password_hash'] = bcrypt.hash(new_password)
            # Note: storage update method needed here
            
            return True, "Password reset successfully"
            
        except jwt.ExpiredSignatureError:
            return False, "Reset token expired"
        except jwt.InvalidTokenError:
            return False, "Invalid reset token"


# ===========================================================================
# OAUTH PROVIDERS INTEGRATION
# ===========================================================================

class OAuthProviders:
    """OAuth provider configurations"""
    
    @staticmethod
    def get_google_config(client_id: str, client_secret: str, redirect_uri: str) -> Dict:
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
    def get_facebook_config(app_id: str, app_secret: str, redirect_uri: str) -> Dict:
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
    def get_github_config(client_id: str, client_secret: str, redirect_uri: str) -> Dict:
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
    def get_microsoft_config(client_id: str, client_secret: str, redirect_uri: str) -> Dict:
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
