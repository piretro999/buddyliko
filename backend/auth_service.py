"""
Authentication Service
Handles user registration, login, OAuth, JWT tokens, account linking
"""

from passlib.context import CryptContext
from jose import JWTError, jwt
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from fastapi import HTTPException, status
from typing import Optional, Dict, Any
import secrets
import os
from dotenv import load_dotenv

from backend.models.database import User, AuthProvider, UserRole, UserStatus, AuthProviderType
from backend.models.schemas import UserRegister, UserLogin

load_dotenv()

# Configuration
SECRET_KEY = os.getenv('SECRET_KEY', secrets.token_urlsafe(32))
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv('ACCESS_TOKEN_EXPIRE_MINUTES', '10080'))  # 7 days

# Password hashing
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# ============================================
# PASSWORD UTILITIES
# ============================================

def hash_password(password: str) -> str:
    """Hash a password"""
    return pwd_context.hash(password)

def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a password against a hash"""
    return pwd_context.verify(plain_password, hashed_password)

def generate_random_password(length: int = 12) -> str:
    """Generate a random secure password"""
    import string
    import random
    
    chars = string.ascii_letters + string.digits + "!@#$%^&*"
    password = ''.join(random.choice(chars) for _ in range(length))
    
    # Ensure it has uppercase, digit, and special char
    if not any(c.isupper() for c in password):
        password = password[:5] + random.choice(string.ascii_uppercase) + password[6:]
    if not any(c.isdigit() for c in password):
        password = password[:7] + random.choice(string.digits) + password[8:]
    if not any(c in "!@#$%^&*" for c in password):
        password = password[:9] + random.choice("!@#$%^&*") + password[10:]
    
    return password

# ============================================
# JWT TOKEN UTILITIES
# ============================================

def create_access_token(data: Dict[str, Any], expires_delta: Optional[timedelta] = None) -> str:
    """Create JWT access token"""
    to_encode = data.copy()
    
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

def decode_access_token(token: str) -> Dict[str, Any]:
    """Decode and verify JWT token"""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )

# ============================================
# USER AUTHENTICATION
# ============================================

def register_user(db: Session, user_data: UserRegister) -> User:
    """
    Register new user with email/password
    First user becomes MASTER automatically
    """
    
    # Check if email already exists
    existing = db.query(User).filter(User.email == user_data.email).first()
    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email already registered"
        )
    
    # Check if this is the first user (becomes MASTER)
    user_count = db.query(User).count()
    is_first_user = (user_count == 0)
    
    # Create user
    user = User(
        email=user_data.email,
        password_hash=hash_password(user_data.password),
        name=user_data.name,
        role=UserRole.MASTER if is_first_user else UserRole.USER,
        status=UserStatus.APPROVED if is_first_user else UserStatus.PENDING
    )
    
    db.add(user)
    db.flush()  # Get user.id
    
    # Create EMAIL auth provider
    auth_provider = AuthProvider(
        user_id=user.id,
        provider=AuthProviderType.EMAIL,
        provider_user_id=user_data.email,
        provider_email=user_data.email,
        is_primary=True
    )
    
    db.add(auth_provider)
    db.commit()
    db.refresh(user)
    
    return user

def authenticate_user(db: Session, login_data: UserLogin) -> Optional[User]:
    """
    Authenticate user with email/password
    Returns User if valid, None if invalid
    """
    
    # Find user by email
    user = db.query(User).filter(User.email == login_data.email).first()
    if not user:
        return None
    
    # Check if user has password (not OAuth-only)
    if not user.password_hash:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="This account uses social login. Please login with Google/Facebook/GitHub."
        )
    
    # Verify password
    if not verify_password(login_data.password, user.password_hash):
        return None
    
    # Check user status
    if user.status == UserStatus.BLOCKED:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Your account has been blocked. Contact support."
        )
    
    if user.status == UserStatus.SUSPENDED:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Your account is suspended. Reason: {user.suspension_reason or 'Contact admin'}"
        )
    
    if user.status == UserStatus.PENDING:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Your account is pending approval. Please wait for admin confirmation."
        )
    
    # Update last login
    user.last_login_at = datetime.utcnow()
    
    # Update auth provider last_used_at
    email_provider = db.query(AuthProvider).filter(
        AuthProvider.user_id == user.id,
        AuthProvider.provider == AuthProviderType.EMAIL
    ).first()
    if email_provider:
        email_provider.last_used_at = datetime.utcnow()
    
    db.commit()
    
    return user

def get_current_user_from_token(db: Session, token: str) -> User:
    """
    Get current user from JWT token
    Used by FastAPI Depends
    """
    
    payload = decode_access_token(token)
    user_id: int = payload.get("sub")
    
    if user_id is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials"
        )
    
    user = db.query(User).filter(User.id == user_id).first()
    
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found"
        )
    
    # Check status
    if user.status == UserStatus.BLOCKED:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account blocked"
        )
    
    if user.status == UserStatus.SUSPENDED:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account suspended"
        )
    
    return user

# ============================================
# OAUTH HELPERS
# ============================================

def find_user_by_oauth_provider(
    db: Session,
    provider: AuthProviderType,
    provider_user_id: str
) -> Optional[User]:
    """
    Find user by OAuth provider ID
    """
    auth_provider = db.query(AuthProvider).filter(
        AuthProvider.provider == provider,
        AuthProvider.provider_user_id == provider_user_id
    ).first()
    
    if auth_provider:
        return auth_provider.user
    
    return None

def create_user_from_oauth(
    db: Session,
    provider: AuthProviderType,
    oauth_data: Dict[str, Any]
) -> User:
    """
    Create new user from OAuth data
    Used when user signs up with Google/Facebook/GitHub
    """
    
    # Check if email already exists
    email = oauth_data.get('email')
    if email:
        existing = db.query(User).filter(User.email == email).first()
        if existing:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Email {email} is already registered. Please login and link your {provider.value} account."
            )
    
    # Check if this is first user
    user_count = db.query(User).count()
    is_first_user = (user_count == 0)
    
    # Create user
    user = User(
        email=email or f"{provider.value.lower()}_{oauth_data['id']}@buddyliko.local",
        password_hash=None,  # No password for OAuth-only users
        name=oauth_data.get('name', 'User'),
        avatar_url=oauth_data.get('avatar_url'),
        role=UserRole.MASTER if is_first_user else UserRole.USER,
        status=UserStatus.APPROVED if is_first_user else UserStatus.PENDING
    )
    
    db.add(user)
    db.flush()
    
    # Create auth provider
    auth_provider = AuthProvider(
        user_id=user.id,
        provider=provider,
        provider_user_id=oauth_data['id'],
        provider_email=oauth_data.get('email'),
        provider_username=oauth_data.get('username'),
        is_primary=True
    )
    
    db.add(auth_provider)
    db.commit()
    db.refresh(user)
    
    return user

def link_oauth_to_user(
    db: Session,
    user: User,
    provider: AuthProviderType,
    oauth_data: Dict[str, Any],
    force: bool = False
) -> AuthProvider:
    """
    Link OAuth provider to existing user account
    Called when user is already logged in and wants to add Google/Facebook/GitHub
    
    Returns: AuthProvider
    Raises: HTTPException if provider already linked to another user
    """
    
    # Check if this provider is already linked to ANOTHER user
    existing_provider = db.query(AuthProvider).filter(
        AuthProvider.provider == provider,
        AuthProvider.provider_user_id == oauth_data['id']
    ).first()
    
    if existing_provider:
        if existing_provider.user_id == user.id:
            # Already linked to this user - just update
            existing_provider.provider_email = oauth_data.get('email')
            existing_provider.provider_username = oauth_data.get('username')
            existing_provider.linked_at = datetime.utcnow()
            db.commit()
            return existing_provider
        else:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"This {provider.value} account is already linked to another user."
            )
    
    # Check email mismatch warning
    oauth_email = oauth_data.get('email')
    if oauth_email and oauth_email != user.email and not force:
        # This is a WARNING, but we allow it
        # Frontend should show confirmation dialog
        pass
    
    # Create new auth provider link
    auth_provider = AuthProvider(
        user_id=user.id,
        provider=provider,
        provider_user_id=oauth_data['id'],
        provider_email=oauth_email,
        provider_username=oauth_data.get('username'),
        is_primary=False  # New links are not primary
    )
    
    db.add(auth_provider)
    db.commit()
    db.refresh(auth_provider)
    
    return auth_provider

def unlink_oauth_provider(
    db: Session,
    user: User,
    provider: AuthProviderType
) -> bool:
    """
    Unlink OAuth provider from user
    
    Returns: True if unlinked
    Raises: HTTPException if it's the last provider
    """
    
    # Get all user's providers
    user_providers = db.query(AuthProvider).filter(
        AuthProvider.user_id == user.id
    ).all()
    
    # Can't remove last provider
    if len(user_providers) <= 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot remove last authentication method. Add another method first."
        )
    
    # Find provider to remove
    target_provider = next(
        (p for p in user_providers if p.provider == provider),
        None
    )
    
    if not target_provider:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"{provider.value} not linked to this account"
        )
    
    # If removing primary, promote another
    if target_provider.is_primary:
        next_provider = next(p for p in user_providers if p.id != target_provider.id)
        next_provider.is_primary = True
        db.add(next_provider)
    
    # Remove
    db.delete(target_provider)
    db.commit()
    
    return True

def set_primary_provider(
    db: Session,
    user: User,
    provider: AuthProviderType
) -> AuthProvider:
    """
    Set a provider as primary for user
    """
    
    # Get all providers
    providers = db.query(AuthProvider).filter(
        AuthProvider.user_id == user.id
    ).all()
    
    # Find target
    target = next((p for p in providers if p.provider == provider), None)
    
    if not target:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"{provider.value} not linked to this account"
        )
    
    # Update all: remove primary from all, set target as primary
    for p in providers:
        p.is_primary = (p.id == target.id)
        db.add(p)
    
    db.commit()
    db.refresh(target)
    
    return target

# ============================================
# PASSWORD RESET
# ============================================

def create_password_reset_token(db: Session, email: str) -> str:
    """
    Create password reset token for user
    Returns token that should be sent via email
    """
    
    user = db.query(User).filter(User.email == email).first()
    
    if not user:
        # Don't reveal if email exists or not (security)
        # Return success anyway
        return None
    
    # Generate token
    token = secrets.token_urlsafe(32)
    
    # Set token and expiry (1 hour)
    user.password_reset_token = token
    user.password_reset_expires = datetime.utcnow() + timedelta(hours=1)
    
    db.commit()
    
    return token

def reset_password_with_token(db: Session, token: str, new_password: str) -> User:
    """
    Reset password using token
    """
    
    user = db.query(User).filter(User.password_reset_token == token).first()
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid or expired reset token"
        )
    
    # Check expiry
    if user.password_reset_expires < datetime.utcnow():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Reset token has expired. Request a new one."
        )
    
    # Update password
    user.password_hash = hash_password(new_password)
    user.password_reset_token = None
    user.password_reset_expires = None
    user.force_password_change = False
    
    db.commit()
    db.refresh(user)
    
    return user

def change_password(db: Session, user: User, current_password: str, new_password: str) -> User:
    """
    Change user password (requires current password)
    """
    
    # Verify current password
    if not user.password_hash:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="This account uses social login and has no password"
        )
    
    if not verify_password(current_password, user.password_hash):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Current password is incorrect"
        )
    
    # Update password
    user.password_hash = hash_password(new_password)
    user.force_password_change = False
    
    db.commit()
    db.refresh(user)
    
    return user
