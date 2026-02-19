"""Billing API Endpoints"""

from fastapi import APIRouter, Depends, Request
from sqlalchemy.orm import Session
import stripe
import os

from backend.database import get_db
from backend.dependencies import get_current_active_user
from backend.services import billing_service
from backend.models.schemas import *
from backend.models.database import User

router = APIRouter(prefix="/api/billing", tags=["Billing"])

@router.post("/checkout", response_model=CheckoutSessionResponse)
def create_checkout(data: CreateCheckoutSessionRequest, user: User = Depends(get_current_active_user), db: Session = Depends(get_db)):
    result = billing_service.create_checkout_session(db, user, data.plan, data.interval)
    return result

@router.post("/webhook")
async def stripe_webhook(request: Request, db: Session = Depends(get_db)):
    """Stripe webhook handler"""
    
    payload = await request.body()
    sig_header = request.headers.get('stripe-signature')
    
    try:
        event = stripe.Webhook.construct_event(
            payload, sig_header, os.getenv('STRIPE_WEBHOOK_SECRET')
        )
        
        billing_service.handle_webhook(db, event['type'], event['data'])
        
        return {"success": True}
    except Exception as e:
        return {"error": str(e)}, 400

@router.post("/cancel", response_model=SuccessResponse)
def cancel_subscription(user: User = Depends(get_current_active_user), db: Session = Depends(get_db)):
    billing_service.cancel_subscription(db, user)
    return {"success": True, "message": "Subscription cancelled"}
