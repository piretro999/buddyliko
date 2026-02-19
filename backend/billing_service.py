"""Billing Service - Stripe Integration"""

import stripe
import os
from sqlalchemy.orm import Session
from fastapi import HTTPException
from datetime import datetime, timedelta
from typing import Dict, Any

from backend.models.database import User, Invoice, Payment, InvoiceStatus, PaymentStatus, UserPlan

stripe.api_key = os.getenv('STRIPE_SECRET_KEY')

def create_checkout_session(db: Session, user: User, plan: UserPlan, interval: str) -> Dict[str, str]:
    """Create Stripe checkout session"""
    
    if not stripe.api_key:
        raise HTTPException(status_code=500, detail="Stripe not configured")
    
    # Get or create customer
    if not user.stripe_customer_id:
        customer = stripe.Customer.create(email=user.email, name=user.name)
        user.stripe_customer_id = customer.id
        db.commit()
    
    # Price IDs from .env
    price_id = os.getenv(f'STRIPE_PRICE_{plan.value}_{interval.upper()}')
    
    if not price_id:
        raise HTTPException(status_code=400, detail=f"Plan {plan.value} {interval} not configured")
    
    # Create session
    session = stripe.checkout.Session.create(
        customer=user.stripe_customer_id,
        payment_method_types=['card'],
        line_items=[{'price': price_id, 'quantity': 1}],
        mode='subscription',
        success_url=os.getenv('FRONTEND_URL', 'http://localhost:8000') + '/success?session_id={CHECKOUT_SESSION_ID}',
        cancel_url=os.getenv('FRONTEND_URL', 'http://localhost:8000') + '/cancel'
    )
    
    return {'session_id': session.id, 'url': session.url}

def handle_webhook(db: Session, event_type: str, data: Dict[str, Any]):
    """Handle Stripe webhook events"""
    
    if event_type == 'checkout.session.completed':
        session = data['object']
        customer_id = session['customer']
        subscription_id = session['subscription']
        
        # Find user
        user = db.query(User).filter(User.stripe_customer_id == customer_id).first()
        if user:
            user.stripe_subscription_id = subscription_id
            user.status = 'APPROVED'
            user.plan_started_at = datetime.utcnow()
            user.plan_expires_at = datetime.utcnow() + timedelta(days=30)
            db.commit()
    
    elif event_type == 'customer.subscription.deleted':
        subscription = data['object']
        customer_id = subscription['customer']
        
        user = db.query(User).filter(User.stripe_customer_id == customer_id).first()
        if user:
            user.plan = UserPlan.FREE
            user.stripe_subscription_id = None
            db.commit()
    
    elif event_type == 'invoice.payment_succeeded':
        invoice_obj = data['object']
        customer_id = invoice_obj['customer']
        
        user = db.query(User).filter(User.stripe_customer_id == customer_id).first()
        if user:
            # Create invoice record
            invoice = Invoice(
                invoice_number=f"INV-{datetime.utcnow().strftime('%Y%m%d')}-{user.id}",
                user_id=user.id,
                amount=invoice_obj['amount_paid'] / 100,
                status=InvoiceStatus.PAID,
                stripe_invoice_id=invoice_obj['id'],
                paid_at=datetime.utcnow()
            )
            db.add(invoice)
            db.commit()

def cancel_subscription(db: Session, user: User):
    """Cancel user subscription"""
    
    if not user.stripe_subscription_id:
        raise HTTPException(status_code=400, detail="No active subscription")
    
    stripe.Subscription.delete(user.stripe_subscription_id)
    
    user.plan = UserPlan.FREE
    user.stripe_subscription_id = None
    db.commit()
