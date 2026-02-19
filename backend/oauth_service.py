"""
OAuth Service
Google, Facebook, GitHub authentication and account linking
"""

import httpx
import secrets
from typing import Dict, Any, Optional, Tuple
from urllib.parse import urlencode
from fastapi import HTTPException, status
from sqlalchemy.orm import Session
import os
from dotenv import load_dotenv

from backend.models.database import User, AuthProvider, AuthProviderType, UserRole, UserStatus
from backend.services.auth_service import (
    create_user_from_oauth,
    find_user_by_oauth_provider,
    link_oauth_to_user,
    create_access_token
)

load_dotenv()

# ============================================
# OAUTH CONFIGURATION
# ============================================

# Google OAuth
GOOGLE_CLIENT_ID = os.getenv('GOOGLE_CLIENT_ID')
GOOGLE_CLIENT_SECRET = os.getenv('GOOGLE_CLIENT_SECRET')
GOOGLE_REDIRECT_URI = os.getenv('GOOGLE_REDIRECT_URI', 'http://localhost:8080/api/auth/google/callback')

# Facebook OAuth
FACEBOOK_APP_ID = os.getenv('FACEBOOK_APP_ID')
FACEBOOK_APP_SECRET = os.getenv('FACEBOOK_APP_SECRET')
FACEBOOK_REDIRECT_URI = os.getenv('FACEBOOK_REDIRECT_URI', 'http://localhost:8080/api/auth/facebook/callback')

# GitHub OAuth
GITHUB_CLIENT_ID = os.getenv('GITHUB_CLIENT_ID')
GITHUB_CLIENT_SECRET = os.getenv('GITHUB_CLIENT_SECRET')
GITHUB_REDIRECT_URI = os.getenv('GITHUB_REDIRECT_URI', 'http://localhost:8080/api/auth/github/callback')

# ============================================
# OAUTH STATE MANAGEMENT (in-memory for now)
# TODO: Move to Redis for production
# ============================================

oauth_states = {}  # {state: {user_id, action, expires_at}}

def create_oauth_state(user_id: Optional[int] = None, action: str = "login") -> str:
    """
    Create OAuth state token for CSRF protection
    
    Args:
        user_id: If linking, the current user ID
        action: "login" or "link"
    """
    state = secrets.token_urlsafe(32)
    oauth_states[state] = {
        "user_id": user_id,
        "action": action,
        "created_at": __import__('datetime').datetime.utcnow()
    }
    return state

def verify_oauth_state(state: str) -> Optional[Dict[str, Any]]:
    """Verify and consume OAuth state token"""
    state_data = oauth_states.pop(state, None)
    
    if not state_data:
        return None
    
    # Check expiry (5 minutes)
    from datetime import datetime, timedelta
    if datetime.utcnow() - state_data['created_at'] > timedelta(minutes=5):
        return None
    
    return state_data

# ============================================
# GOOGLE OAUTH
# ============================================

def get_google_auth_url(user_id: Optional[int] = None, action: str = "login") -> str:
    """
    Get Google OAuth authorization URL
    
    Args:
        user_id: If linking, current user ID
        action: "login" (new/existing user) or "link" (add to existing account)
    """
    
    if not GOOGLE_CLIENT_ID:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Google OAuth not configured"
        )
    
    state = create_oauth_state(user_id, action)
    
    params = {
        "client_id": GOOGLE_CLIENT_ID,
        "redirect_uri": GOOGLE_REDIRECT_URI,
        "response_type": "code",
        "scope": "openid email profile",
        "state": state,
        "access_type": "offline",
        "prompt": "consent"
    }
    
    return f"https://accounts.google.com/o/oauth2/v2/auth?{urlencode(params)}"

async def handle_google_callback(
    db: Session,
    code: str,
    state: str,
    current_user: Optional[User] = None
) -> Tuple[User, str]:
    """
    Handle Google OAuth callback
    
    Returns: (User, access_token)
    """
    
    # Verify state
    state_data = verify_oauth_state(state)
    if not state_data:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid or expired state token"
        )
    
    action = state_data['action']
    
    # Exchange code for token
    async with httpx.AsyncClient() as client:
        token_response = await client.post(
            "https://oauth2.googleapis.com/token",
            data={
                "code": code,
                "client_id": GOOGLE_CLIENT_ID,
                "client_secret": GOOGLE_CLIENT_SECRET,
                "redirect_uri": GOOGLE_REDIRECT_URI,
                "grant_type": "authorization_code"
            }
        )
        
        if token_response.status_code != 200:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Failed to get access token from Google"
            )
        
        token_data = token_response.json()
        access_token = token_data.get('access_token')
        
        # Get user info from Google
        user_response = await client.get(
            "https://www.googleapis.com/oauth2/v2/userinfo",
            headers={"Authorization": f"Bearer {access_token}"}
        )
        
        if user_response.status_code != 200:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Failed to get user info from Google"
            )
        
        google_user = user_response.json()
    
    # Extract user data
    oauth_data = {
        'id': google_user['id'],
        'email': google_user.get('email'),
        'name': google_user.get('name', 'User'),
        'avatar_url': google_user.get('picture')
    }
    
    # Handle based on action
    if action == "link":
        # Link to existing account
        if not current_user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Must be logged in to link account"
            )
        
        link_oauth_to_user(db, current_user, AuthProviderType.GOOGLE, oauth_data)
        
        # Return existing user with new token
        token = create_access_token(
            data={"sub": current_user.id, "email": current_user.email, "role": current_user.role.value}
        )
        return current_user, token
    
    else:  # action == "login"
        # Check if user exists with this Google ID
        user = find_user_by_oauth_provider(db, AuthProviderType.GOOGLE, oauth_data['id'])
        
        if user:
            # Existing user - login
            # Update last login
            from datetime import datetime
            user.last_login_at = datetime.utcnow()
            
            # Update provider last_used_at
            google_provider = db.query(AuthProvider).filter(
                AuthProvider.user_id == user.id,
                AuthProvider.provider == AuthProviderType.GOOGLE
            ).first()
            if google_provider:
                google_provider.last_used_at = datetime.utcnow()
            
            db.commit()
            
            # Check user status
            if user.status == UserStatus.BLOCKED:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Account blocked"
                )
            
            if user.status == UserStatus.SUSPENDED:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=f"Account suspended: {user.suspension_reason or 'Contact admin'}"
                )
            
            if user.status == UserStatus.PENDING:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Account pending approval"
                )
            
            token = create_access_token(
                data={"sub": user.id, "email": user.email, "role": user.role.value}
            )
            return user, token
        
        else:
            # New user - create account
            user = create_user_from_oauth(db, AuthProviderType.GOOGLE, oauth_data)
            
            token = create_access_token(
                data={"sub": user.id, "email": user.email, "role": user.role.value}
            )
            return user, token

# ============================================
# FACEBOOK OAUTH
# ============================================

def get_facebook_auth_url(user_id: Optional[int] = None, action: str = "login") -> str:
    """Get Facebook OAuth authorization URL"""
    
    if not FACEBOOK_APP_ID:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Facebook OAuth not configured"
        )
    
    state = create_oauth_state(user_id, action)
    
    params = {
        "client_id": FACEBOOK_APP_ID,
        "redirect_uri": FACEBOOK_REDIRECT_URI,
        "state": state,
        "scope": "email,public_profile"
    }
    
    return f"https://www.facebook.com/v12.0/dialog/oauth?{urlencode(params)}"

async def handle_facebook_callback(
    db: Session,
    code: str,
    state: str,
    current_user: Optional[User] = None
) -> Tuple[User, str]:
    """Handle Facebook OAuth callback"""
    
    # Verify state
    state_data = verify_oauth_state(state)
    if not state_data:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid or expired state token"
        )
    
    action = state_data['action']
    
    # Exchange code for token
    async with httpx.AsyncClient() as client:
        token_response = await client.get(
            "https://graph.facebook.com/v12.0/oauth/access_token",
            params={
                "client_id": FACEBOOK_APP_ID,
                "client_secret": FACEBOOK_APP_SECRET,
                "redirect_uri": FACEBOOK_REDIRECT_URI,
                "code": code
            }
        )
        
        if token_response.status_code != 200:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Failed to get access token from Facebook"
            )
        
        token_data = token_response.json()
        access_token = token_data.get('access_token')
        
        # Get user info
        user_response = await client.get(
            "https://graph.facebook.com/me",
            params={
                "fields": "id,name,email,picture",
                "access_token": access_token
            }
        )
        
        if user_response.status_code != 200:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Failed to get user info from Facebook"
            )
        
        fb_user = user_response.json()
    
    # Extract user data
    oauth_data = {
        'id': fb_user['id'],
        'email': fb_user.get('email'),
        'name': fb_user.get('name', 'User'),
        'avatar_url': fb_user.get('picture', {}).get('data', {}).get('url')
    }
    
    # Handle based on action (same logic as Google)
    if action == "link":
        if not current_user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Must be logged in to link account"
            )
        
        link_oauth_to_user(db, current_user, AuthProviderType.FACEBOOK, oauth_data)
        
        token = create_access_token(
            data={"sub": current_user.id, "email": current_user.email, "role": current_user.role.value}
        )
        return current_user, token
    
    else:  # login
        user = find_user_by_oauth_provider(db, AuthProviderType.FACEBOOK, oauth_data['id'])
        
        if user:
            # Existing user - login
            from datetime import datetime
            user.last_login_at = datetime.utcnow()
            
            fb_provider = db.query(AuthProvider).filter(
                AuthProvider.user_id == user.id,
                AuthProvider.provider == AuthProviderType.FACEBOOK
            ).first()
            if fb_provider:
                fb_provider.last_used_at = datetime.utcnow()
            
            db.commit()
            
            # Check status
            if user.status != UserStatus.APPROVED:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=f"Account not active: {user.status.value}"
                )
            
            token = create_access_token(
                data={"sub": user.id, "email": user.email, "role": user.role.value}
            )
            return user, token
        
        else:
            # New user
            user = create_user_from_oauth(db, AuthProviderType.FACEBOOK, oauth_data)
            
            token = create_access_token(
                data={"sub": user.id, "email": user.email, "role": user.role.value}
            )
            return user, token

# ============================================
# GITHUB OAUTH
# ============================================

def get_github_auth_url(user_id: Optional[int] = None, action: str = "login") -> str:
    """Get GitHub OAuth authorization URL"""
    
    if not GITHUB_CLIENT_ID:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="GitHub OAuth not configured"
        )
    
    state = create_oauth_state(user_id, action)
    
    params = {
        "client_id": GITHUB_CLIENT_ID,
        "redirect_uri": GITHUB_REDIRECT_URI,
        "state": state,
        "scope": "read:user user:email"
    }
    
    return f"https://github.com/login/oauth/authorize?{urlencode(params)}"

async def handle_github_callback(
    db: Session,
    code: str,
    state: str,
    current_user: Optional[User] = None
) -> Tuple[User, str]:
    """Handle GitHub OAuth callback"""
    
    # Verify state
    state_data = verify_oauth_state(state)
    if not state_data:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid or expired state token"
        )
    
    action = state_data['action']
    
    # Exchange code for token
    async with httpx.AsyncClient() as client:
        token_response = await client.post(
            "https://github.com/login/oauth/access_token",
            data={
                "client_id": GITHUB_CLIENT_ID,
                "client_secret": GITHUB_CLIENT_SECRET,
                "code": code,
                "redirect_uri": GITHUB_REDIRECT_URI
            },
            headers={"Accept": "application/json"}
        )
        
        if token_response.status_code != 200:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Failed to get access token from GitHub"
            )
        
        token_data = token_response.json()
        access_token = token_data.get('access_token')
        
        # Get user info
        user_response = await client.get(
            "https://api.github.com/user",
            headers={
                "Authorization": f"Bearer {access_token}",
                "Accept": "application/json"
            }
        )
        
        if user_response.status_code != 200:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Failed to get user info from GitHub"
            )
        
        gh_user = user_response.json()
        
        # Get emails (GitHub doesn't always include email in /user)
        email = gh_user.get('email')
        if not email:
            emails_response = await client.get(
                "https://api.github.com/user/emails",
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Accept": "application/json"
                }
            )
            
            if emails_response.status_code == 200:
                emails = emails_response.json()
                # Get primary email
                primary = next((e for e in emails if e.get('primary')), None)
                if primary:
                    email = primary.get('email')
    
    # Extract user data
    oauth_data = {
        'id': str(gh_user['id']),
        'email': email,
        'name': gh_user.get('name') or gh_user.get('login', 'User'),
        'username': gh_user.get('login'),
        'avatar_url': gh_user.get('avatar_url')
    }
    
    # Handle based on action
    if action == "link":
        if not current_user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Must be logged in to link account"
            )
        
        link_oauth_to_user(db, current_user, AuthProviderType.GITHUB, oauth_data)
        
        token = create_access_token(
            data={"sub": current_user.id, "email": current_user.email, "role": current_user.role.value}
        )
        return current_user, token
    
    else:  # login
        user = find_user_by_oauth_provider(db, AuthProviderType.GITHUB, oauth_data['id'])
        
        if user:
            # Existing user
            from datetime import datetime
            user.last_login_at = datetime.utcnow()
            
            gh_provider = db.query(AuthProvider).filter(
                AuthProvider.user_id == user.id,
                AuthProvider.provider == AuthProviderType.GITHUB
            ).first()
            if gh_provider:
                gh_provider.last_used_at = datetime.utcnow()
            
            db.commit()
            
            # Check status
            if user.status != UserStatus.APPROVED:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=f"Account not active: {user.status.value}"
                )
            
            token = create_access_token(
                data={"sub": user.id, "email": user.email, "role": user.role.value}
            )
            return user, token
        
        else:
            # New user
            user = create_user_from_oauth(db, AuthProviderType.GITHUB, oauth_data)
            
            token = create_access_token(
                data={"sub": user.id, "email": user.email, "role": user.role.value}
            )
            return user, token
