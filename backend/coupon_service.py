"""Coupon & Credits Service"""

from sqlalchemy.orm import Session
from fastapi import HTTPException
from datetime import datetime, date
from decimal import Decimal

from backend.models.database import Coupon, CouponUsage, UserCredit, CreditTransaction, User, CreditType, CreditTransactionType

def validate_coupon(db: Session, code: str, user: User, plan: str) -> dict:
    """Validate coupon code"""
    
    coupon = db.query(Coupon).filter(Coupon.code == code.upper(), Coupon.active == True).first()
    
    if not coupon:
        return {"valid": False, "message": "Invalid coupon code"}
    
    # Check dates
    today = date.today()
    if today < coupon.valid_from or today > coupon.valid_to:
        return {"valid": False, "message": "Coupon expired"}
    
    # Check usage limit
    if coupon.max_uses and coupon.times_used >= coupon.max_uses:
        return {"valid": False, "message": "Coupon usage limit reached"}
    
    # Check one per user
    if coupon.one_per_user:
        used = db.query(CouponUsage).filter(
            CouponUsage.coupon_id == coupon.id,
            CouponUsage.user_id == user.id
        ).first()
        if used:
            return {"valid": False, "message": "Coupon already used"}
    
    # Check applicable plans
    if coupon.applicable_plans and plan not in coupon.applicable_plans:
        return {"valid": False, "message": f"Coupon not valid for {plan} plan"}
    
    return {"valid": True, "coupon": coupon}

def apply_coupon(db: Session, coupon: Coupon, user: User, amount: Decimal) -> Decimal:
    """Apply coupon and record usage"""
    
    from backend.models.database import CouponType
    
    if coupon.type == CouponType.PERCENTAGE:
        discount = amount * (coupon.value / 100)
    else:
        discount = min(coupon.value, amount)
    
    # Record usage
    usage = CouponUsage(coupon_id=coupon.id, user_id=user.id, discount_amount=discount)
    coupon.times_used += 1
    
    db.add(usage)
    db.commit()
    
    return discount

def add_credits(db: Session, user_id: int, amount: Decimal, type: CreditType, reason: str, granted_by: int = None):
    """Add credits to user account"""
    
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Create credit record
    credit = UserCredit(user_id=user_id, amount=amount, type=type, reason=reason, granted_by=granted_by)
    db.add(credit)
    
    # Update balance
    user.credits_balance += amount
    
    # Transaction log
    transaction = CreditTransaction(
        user_id=user_id,
        amount=amount,
        balance_after=user.credits_balance,
        type=CreditTransactionType.ADD,
        description=reason
    )
    db.add(transaction)
    
    db.commit()
    return credit

def use_credits(db: Session, user: User, amount: Decimal, description: str = None) -> bool:
    """Use credits from user balance"""
    
    if user.credits_balance < amount:
        return False
    
    user.credits_balance -= amount
    
    transaction = CreditTransaction(
        user_id=user.id,
        amount=-amount,
        balance_after=user.credits_balance,
        type=CreditTransactionType.USE,
        description=description
    )
    db.add(transaction)
    db.commit()
    
    return True
