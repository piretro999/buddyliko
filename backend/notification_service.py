"""Notification Service"""

from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime, timedelta

from backend.models.database import Notification, NotificationType, User

def create_notification(
    db: Session,
    user_id: int,
    type: NotificationType,
    title: str,
    message: str,
    action_url: Optional[str] = None,
    action_label: Optional[str] = None
) -> Notification:
    """Create in-app notification"""
    
    notification = Notification(
        user_id=user_id,
        type=type,
        title=title,
        message=message,
        action_url=action_url,
        action_label=action_label
    )
    
    db.add(notification)
    db.commit()
    db.refresh(notification)
    
    return notification

def get_user_notifications(db: Session, user_id: int, limit: int = 50) -> List[Notification]:
    """Get user notifications"""
    
    return db.query(Notification).filter(
        Notification.user_id == user_id
    ).order_by(Notification.created_at.desc()).limit(limit).all()

def get_unread_count(db: Session, user_id: int) -> int:
    """Get unread notification count"""
    
    return db.query(Notification).filter(
        Notification.user_id == user_id,
        Notification.read == False
    ).count()

def mark_as_read(db: Session, notification_ids: List[int], user_id: int):
    """Mark notifications as read"""
    
    db.query(Notification).filter(
        Notification.id.in_(notification_ids),
        Notification.user_id == user_id
    ).update({"read": True, "read_at": datetime.utcnow()}, synchronize_session=False)
    
    db.commit()

def mark_all_as_read(db: Session, user_id: int):
    """Mark all notifications as read"""
    
    db.query(Notification).filter(
        Notification.user_id == user_id,
        Notification.read == False
    ).update({"read": True, "read_at": datetime.utcnow()}, synchronize_session=False)
    
    db.commit()

def cleanup_old_notifications(db: Session, days: int = 30):
    """Delete notifications older than X days"""
    
    cutoff = datetime.utcnow() - timedelta(days=days)
    
    db.query(Notification).filter(
        Notification.created_at < cutoff
    ).delete()
    
    db.commit()
