"""Admin API Endpoints"""

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from typing import List, Optional

from backend.database import get_db
from backend.dependencies import require_admin, require_master
from backend.services import user_service
from backend.models.schemas import *
from backend.models.database import User, UserRole, UserStatus, AuditLog

router = APIRouter(prefix="/api/admin", tags=["Admin"])

@router.get("/stats", response_model=AdminDashboardStats)
def get_dashboard_stats(admin: User = Depends(require_admin), db: Session = Depends(get_db)):
    stats = user_service.get_admin_stats(db)
    stats['mrr'] = 0.00
    stats['arr'] = 0.00
    stats['total_mappings'] = 0
    stats['total_projects'] = 0
    stats['total_storage_gb'] = 0
    stats['ai_calls_today'] = 0
    stats['ai_cost_today'] = 0.00
    return stats

@router.get("/users/pending", response_model=List[UserResponse])
def get_pending_users(admin: User = Depends(require_admin), db: Session = Depends(get_db)):
    return user_service.get_pending_users(db)

@router.post("/users/{user_id}/approve", response_model=UserResponse)
def approve_user(user_id: int, admin: User = Depends(require_admin), db: Session = Depends(get_db)):
    return user_service.approve_user(db, user_id, admin)

@router.post("/users/{user_id}/reject", response_model=SuccessResponse)
def reject_user(user_id: int, request: RejectUserRequest, admin: User = Depends(require_admin), db: Session = Depends(get_db)):
    user_service.reject_user(db, user_id, admin, request.reason)
    return {"success": True, "message": "User rejected"}

@router.get("/users", response_model=List[UserResponse])
def list_users(status: Optional[UserStatus] = None, role: Optional[UserRole] = None,
               limit: int = Query(100, le=1000), admin: User = Depends(require_admin), db: Session = Depends(get_db)):
    query = db.query(User)
    if status:
        query = query.filter(User.status == status)
    if role:
        query = query.filter(User.role == role)
    return query.order_by(User.created_at.desc()).limit(limit).all()

@router.post("/users/{user_id}/suspend", response_model=UserResponse)
def suspend_user(user_id: int, request: SuspendUserRequest, admin: User = Depends(require_admin), db: Session = Depends(get_db)):
    return user_service.suspend_user(db, user_id, admin, request.reason)

@router.post("/users/{user_id}/unsuspend", response_model=UserResponse)
def unsuspend_user(user_id: int, admin: User = Depends(require_admin), db: Session = Depends(get_db)):
    return user_service.unsuspend_user(db, user_id, admin)

@router.post("/users/{user_id}/promote", response_model=UserResponse)
def promote_user(user_id: int, request: PromoteUserRequest, master: User = Depends(require_master), db: Session = Depends(get_db)):
    return user_service.promote_user(db, user_id, request.to_role, master, request.reason)

@router.get("/audit-log", response_model=List[AuditLogResponse])
def get_audit_log(user_id: Optional[int] = None, limit: int = Query(50, le=500),
                   admin: User = Depends(require_admin), db: Session = Depends(get_db)):
    query = db.query(AuditLog)
    if user_id:
        query = query.filter(AuditLog.user_id == user_id)
    return query.order_by(AuditLog.timestamp.desc()).limit(limit).all()
