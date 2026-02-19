"""
User Management Service
Admin operations: approve, suspend, promote, block users
"""

from sqlalchemy.orm import Session
from fastapi import HTTPException, status
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta

from backend.models.database import (
    User, UserRole, UserStatus, RoleChange, AuditLog,
    AuthProvider, AuthProviderType
)
from backend.services.auth_service import hash_password, generate_random_password

# ============================================
# USER APPROVAL
# ============================================

def approve_user(db: Session, user_id: int, admin: User) -> User:
    """Approve pending user"""
    
    user = db.query(User).filter(User.id == user_id).first()
    
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    
    if user.status != UserStatus.PENDING:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"User is not pending (current status: {user.status.value})"
        )
    
    user.status = UserStatus.APPROVED
    user.approved_by = admin.id
    user.approved_at = datetime.utcnow()
    
    log_action(db, admin.id, "APPROVE_USER", "USER", user.id,
               {"status": UserStatus.PENDING.value}, {"status": UserStatus.APPROVED.value})
    
    db.commit()
    db.refresh(user)
    return user

def reject_user(db: Session, user_id: int, admin: User, reason: str) -> User:
    """Reject/Delete pending user"""
    
    user = db.query(User).filter(User.id == user_id).first()
    
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    
    if user.status != UserStatus.PENDING:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                          detail=f"Can only reject pending users")
    
    log_action(db, admin.id, "REJECT_USER", "USER", user.id,
               {"email": user.email}, {"reason": reason, "deleted": True})
    
    db.delete(user)
    db.commit()
    return user

def suspend_user(db: Session, user_id: int, admin: User, reason: str) -> User:
    """Suspend user account"""
    
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    
    if user.role in [UserRole.ADMIN, UserRole.MASTER] and admin.role != UserRole.MASTER:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN,
                          detail="Only MASTER can suspend ADMIN/MASTER")
    
    old_status = user.status
    user.status = UserStatus.SUSPENDED
    user.suspended_by = admin.id
    user.suspended_at = datetime.utcnow()
    user.suspension_reason = reason
    
    log_action(db, admin.id, "SUSPEND_USER", "USER", user.id,
               {"status": old_status.value}, {"status": "SUSPENDED", "reason": reason})
    
    db.commit()
    db.refresh(user)
    return user

def unsuspend_user(db: Session, user_id: int, admin: User) -> User:
    """Reactivate suspended user"""
    
    user = db.query(User).filter(User.id == user_id).first()
    if not user or user.status != UserStatus.SUSPENDED:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="User not suspended")
    
    user.status = UserStatus.APPROVED
    user.suspended_by = None
    user.suspended_at = None
    user.suspension_reason = None
    
    log_action(db, admin.id, "UNSUSPEND_USER", "USER", user.id,
               {"status": "SUSPENDED"}, {"status": "APPROVED"})
    
    db.commit()
    db.refresh(user)
    return user

def block_user(db: Session, user_id: int, admin: User, reason: str) -> User:
    """Permanently block user"""
    
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    
    if user.role in [UserRole.ADMIN, UserRole.MASTER]:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN,
                          detail="Cannot block ADMIN/MASTER")
    
    old_status = user.status
    user.status = UserStatus.BLOCKED
    user.blocked_by = admin.id
    user.blocked_at = datetime.utcnow()
    
    log_action(db, admin.id, "BLOCK_USER", "USER", user.id,
               {"status": old_status.value}, {"status": "BLOCKED", "reason": reason})
    
    db.commit()
    db.refresh(user)
    return user

def promote_user(db: Session, user_id: int, to_role: UserRole, admin: User, reason: Optional[str] = None) -> User:
    """Promote user to new role (MASTER only)"""
    
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    
    if admin.role != UserRole.MASTER:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Only MASTER can promote")
    
    if to_role == UserRole.MASTER:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Cannot promote to MASTER")
    
    old_role = user.role
    role_change = RoleChange(user_id=user.id, from_role=old_role, to_role=to_role,
                            changed_by=admin.id, reason=reason)
    user.role = to_role
    
    db.add(role_change)
    log_action(db, admin.id, "PROMOTE_USER", "USER", user.id,
               {"role": old_role.value}, {"role": to_role.value})
    
    db.commit()
    db.refresh(user)
    return user

def force_password_reset(db: Session, user_id: int, admin: User) -> User:
    """Force user to change password on next login"""
    
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    
    user.force_password_change = True
    
    from backend.services.auth_service import create_password_reset_token
    token = create_password_reset_token(db, user.email)
    
    log_action(db, admin.id, "FORCE_PASSWORD_RESET", "USER", user.id)
    
    db.commit()
    db.refresh(user)
    return user

def set_temporary_password(db: Session, user_id: int, admin: User, send_email: bool = True) -> Dict[str, Any]:
    """Set temporary password for user"""
    
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    
    temp_password = generate_random_password(12)
    user.password_hash = hash_password(temp_password)
    user.force_password_change = True
    
    log_action(db, admin.id, "SET_TEMPORARY_PASSWORD", "USER", user.id)
    
    db.commit()
    
    return {
        "temporary_password": temp_password,
        "force_change_on_login": True,
        "user_email": user.email
    }

def get_pending_users(db: Session) -> List[User]:
    """Get all pending users"""
    return db.query(User).filter(User.status == UserStatus.PENDING).order_by(User.created_at.desc()).all()

def get_admin_stats(db: Session) -> Dict[str, Any]:
    """Get admin dashboard statistics"""
    
    from backend.models.database import UserPlan
    
    total = db.query(User).count()
    pending = db.query(User).filter(User.status == UserStatus.PENDING).count()
    approved = db.query(User).filter(User.status == UserStatus.APPROVED).count()
    
    thirty_days_ago = datetime.utcnow() - timedelta(days=30)
    active = db.query(User).filter(User.last_login_at >= thirty_days_ago).count()
    
    return {
        "total_users": total,
        "pending_users": pending,
        "approved_users": approved,
        "active_users_30d": active,
        "users_by_role": {
            "MASTER": db.query(User).filter(User.role == UserRole.MASTER).count(),
            "ADMIN": db.query(User).filter(User.role == UserRole.ADMIN).count(),
            "USER": db.query(User).filter(User.role == UserRole.USER).count()
        },
        "users_by_plan": {
            "FREE": db.query(User).filter(User.plan == UserPlan.FREE).count(),
            "PRO": db.query(User).filter(User.plan == UserPlan.PRO).count(),
            "ENTERPRISE": db.query(User).filter(User.plan == UserPlan.ENTERPRISE).count()
        }
    }

def log_action(db: Session, user_id: int, action: str, target_type: Optional[str] = None,
               target_id: Optional[int] = None, old_value: Optional[Dict] = None,
               new_value: Optional[Dict] = None):
    """Log action to audit log"""
    
    log = AuditLog(user_id=user_id, action=action, target_type=target_type,
                   target_id=target_id, old_value=old_value, new_value=new_value)
    db.add(log)
