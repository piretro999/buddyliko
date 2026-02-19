"""
Auth API Endpoints
Registration, Login, OAuth, Password management
"""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.dependencies import get_current_user, get_current_active_user
from backend.services import auth_service
from backend.models.schemas import (
    UserRegister, UserLogin, TokenResponse, UserResponse,
    PasswordReset, PasswordResetConfirm, PasswordChange,
    SuccessResponse, AuthProviderResponse
)
from backend.models.database import User

router = APIRouter(prefix="/api/auth", tags=["Authentication"])

# ============================================
# REGISTER & LOGIN
# ============================================

@router.post("/register", response_model=TokenResponse, status_code=status.HTTP_201_CREATED)
def register(
    user_data: UserRegister,
    db: Session = Depends(get_db)
):
    """
    Register new user with email/password
    
    - First user becomes MASTER automatically
    - Other users start as PENDING (need approval)
    - Returns access token
    """
    
    user = auth_service.register_user(db, user_data)
    
    # Create access token
    access_token = auth_service.create_access_token(
        data={"sub": user.id, "email": user.email, "role": user.role.value}
    )
    
    # Load auth providers for response
    db.refresh(user)
    
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "user": user
    }

@router.post("/login", response_model=TokenResponse)
def login(
    login_data: UserLogin,
    db: Session = Depends(get_db)
):
    """
    Login with email/password
    
    Returns access token if credentials valid
    """
    
    user = auth_service.authenticate_user(db, login_data)
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # Create access token
    access_token = auth_service.create_access_token(
        data={"sub": user.id, "email": user.email, "role": user.role.value}
    )
    
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "user": user
    }

@router.get("/me", response_model=UserResponse)
def get_current_user_info(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Get current authenticated user info
    """
    
    # Load auth providers
    db.refresh(user)
    
    return user

@router.post("/logout", response_model=SuccessResponse)
def logout(user: User = Depends(get_current_user)):
    """
    Logout (client should delete token)
    
    Note: JWT tokens can't be invalidated server-side
    Client must delete the token
    For true logout, implement token blacklist
    """
    
    return {
        "success": True,
        "message": "Logged out successfully. Please delete your access token."
    }

# ============================================
# PASSWORD MANAGEMENT
# ============================================

@router.post("/password/forgot", response_model=SuccessResponse)
def forgot_password(
    reset_data: PasswordReset,
    db: Session = Depends(get_db)
):
    """
    Request password reset
    
    Sends email with reset token (if email exists)
    Always returns success for security
    """
    
    token = auth_service.create_password_reset_token(db, reset_data.email)
    
    if token:
        # TODO: Send email with reset link
        # email_service.send_password_reset(reset_data.email, token)
        pass
    
    return {
        "success": True,
        "message": "If email exists, password reset link has been sent"
    }

@router.post("/password/reset", response_model=SuccessResponse)
def reset_password(
    reset_confirm: PasswordResetConfirm,
    db: Session = Depends(get_db)
):
    """
    Reset password using token from email
    """
    
    user = auth_service.reset_password_with_token(
        db,
        reset_confirm.token,
        reset_confirm.new_password
    )
    
    return {
        "success": True,
        "message": "Password reset successfully"
    }

@router.post("/password/change", response_model=SuccessResponse)
def change_password(
    password_data: PasswordChange,
    user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Change password (requires current password)
    """
    
    auth_service.change_password(
        db,
        user,
        password_data.current_password,
        password_data.new_password
    )
    
    return {
        "success": True,
        "message": "Password changed successfully"
    }

# ============================================
# AUTH PROVIDERS (Account Linking)
# ============================================

@router.get("/providers", response_model=list[AuthProviderResponse])
def get_linked_providers(
    user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Get all linked auth providers for current user
    """
    
    db.refresh(user)
    return user.auth_providers

@router.post("/providers/{provider}/link", response_model=SuccessResponse)
async def link_provider(
    provider: str,
    code: str,  # OAuth code from provider
    state: str,  # CSRF token
    user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Link OAuth provider to current user account
    
    User must be logged in to link a provider
    This prevents account hijacking
    """
    
    from backend.services import oauth_service
    from backend.models.database import AuthProviderType
    
    # Validate provider
    provider_lower = provider.lower()
    if provider_lower not in ['google', 'facebook', 'github']:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid provider: {provider}"
        )
    
    # Handle callback based on provider
    if provider_lower == 'google':
        updated_user, _ = await oauth_service.handle_google_callback(db, code, state, user)
    elif provider_lower == 'facebook':
        updated_user, _ = await oauth_service.handle_facebook_callback(db, code, state, user)
    elif provider_lower == 'github':
        updated_user, _ = await oauth_service.handle_github_callback(db, code, state, user)
    
    return {
        "success": True,
        "message": f"{provider} linked successfully"
    }

@router.delete("/providers/{provider}/unlink", response_model=SuccessResponse)
def unlink_provider(
    provider: str,
    user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Unlink OAuth provider from current user
    
    Cannot unlink last provider
    """
    
    from backend.models.database import AuthProviderType
    
    try:
        provider_enum = AuthProviderType[provider.upper()]
    except KeyError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid provider: {provider}"
        )
    
    auth_service.unlink_oauth_provider(db, user, provider_enum)
    
    return {
        "success": True,
        "message": f"{provider} unlinked successfully"
    }

@router.post("/providers/{provider}/set-primary", response_model=SuccessResponse)
def set_primary_provider(
    provider: str,
    user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Set a provider as primary (used for email notifications)
    """
    
    from backend.models.database import AuthProviderType
    
    try:
        provider_enum = AuthProviderType[provider.upper()]
    except KeyError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid provider: {provider}"
        )
    
    auth_service.set_primary_provider(db, user, provider_enum)
    
    return {
        "success": True,
        "message": f"{provider} set as primary"
    }

# ============================================
# OAUTH LOGIN FLOWS
# ============================================

from backend.services import oauth_service
from backend.dependencies import get_optional_user

@router.get("/google/login")
def google_login(user: Optional[User] = Depends(get_optional_user)):
    """
    Redirect to Google OAuth
    
    If user is logged in → linking mode
    If user is not logged in → login mode
    """
    
    action = "link" if user else "login"
    user_id = user.id if user else None
    
    auth_url = oauth_service.get_google_auth_url(user_id, action)
    
    return {"url": auth_url}

@router.get("/google/callback", response_model=TokenResponse)
async def google_callback(
    code: str,
    state: str,
    user: Optional[User] = Depends(get_optional_user),
    db: Session = Depends(get_db)
):
    """
    Handle Google OAuth callback
    
    Returns JWT token for new/existing user
    Or links to existing account if user is logged in
    """
    
    user, access_token = await oauth_service.handle_google_callback(db, code, state, user)
    
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "user": user
    }

@router.get("/facebook/login")
def facebook_login(user: Optional[User] = Depends(get_optional_user)):
    """Redirect to Facebook OAuth"""
    
    action = "link" if user else "login"
    user_id = user.id if user else None
    
    auth_url = oauth_service.get_facebook_auth_url(user_id, action)
    
    return {"url": auth_url}

@router.get("/facebook/callback", response_model=TokenResponse)
async def facebook_callback(
    code: str,
    state: str,
    user: Optional[User] = Depends(get_optional_user),
    db: Session = Depends(get_db)
):
    """Handle Facebook OAuth callback"""
    
    user, access_token = await oauth_service.handle_facebook_callback(db, code, state, user)
    
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "user": user
    }

@router.get("/github/login")
def github_login(user: Optional[User] = Depends(get_optional_user)):
    """Redirect to GitHub OAuth"""
    
    action = "link" if user else "login"
    user_id = user.id if user else None
    
    auth_url = oauth_service.get_github_auth_url(user_id, action)
    
    return {"url": auth_url}

@router.get("/github/callback", response_model=TokenResponse)
async def github_callback(
    code: str,
    state: str,
    user: Optional[User] = Depends(get_optional_user),
    db: Session = Depends(get_db)
):
    """Handle GitHub OAuth callback"""
    
    user, access_token = await oauth_service.handle_github_callback(db, code, state, user)
    
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "user": user
    }
