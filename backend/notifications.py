"""Notifications API"""

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from typing import List

from backend.database import get_db
from backend.dependencies import get_current_active_user
from backend.services import notification_service
from backend.models.schemas import *
from backend.models.database import User

router = APIRouter(prefix="/api/notifications", tags=["Notifications"])

@router.get("/", response_model=List[NotificationResponse])
def get_notifications(user: User = Depends(get_current_active_user), db: Session = Depends(get_db)):
    return notification_service.get_user_notifications(db, user.id)

@router.get("/unread-count")
def get_unread_count(user: User = Depends(get_current_active_user), db: Session = Depends(get_db)):
    return {"count": notification_service.get_unread_count(db, user.id)}

@router.post("/mark-read", response_model=SuccessResponse)
def mark_read(request: MarkNotificationReadRequest, user: User = Depends(get_current_active_user), db: Session = Depends(get_db)):
    notification_service.mark_as_read(db, request.notification_ids, user.id)
    return {"success": True}

@router.post("/mark-all-read", response_model=SuccessResponse)
def mark_all_read(user: User = Depends(get_current_active_user), db: Session = Depends(get_db)):
    notification_service.mark_all_as_read(db, user.id)
    return {"success": True}
