"""
Buddyliko Database Models
SQLAlchemy ORM models
"""

from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, Date, 
    Numeric, Text, Enum, ForeignKey, TIMESTAMP, BigInteger
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
import enum

Base = declarative_base()

# ============================================
# ENUMS
# ============================================

class UserRole(str, enum.Enum):
    MASTER = "MASTER"
    ADMIN = "ADMIN"
    USER = "USER"

class UserStatus(str, enum.Enum):
    PENDING = "PENDING"
    APPROVED = "APPROVED"
    SUSPENDED = "SUSPENDED"
    BLOCKED = "BLOCKED"

class UserPlan(str, enum.Enum):
    FREE = "FREE"
    PRO = "PRO"
    ENTERPRISE = "ENTERPRISE"

class AuthProviderType(str, enum.Enum):
    EMAIL = "EMAIL"
    GOOGLE = "GOOGLE"
    FACEBOOK = "FACEBOOK"
    GITHUB = "GITHUB"

class OrgMemberRole(str, enum.Enum):
    OWNER = "OWNER"
    ADMIN = "ADMIN"
    MEMBER = "MEMBER"

class NotificationType(str, enum.Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    SUCCESS = "SUCCESS"
    ERROR = "ERROR"

class CouponType(str, enum.Enum):
    PERCENTAGE = "PERCENTAGE"
    FIXED = "FIXED"

class CreditType(str, enum.Enum):
    ADMIN_GRANT = "ADMIN_GRANT"
    REFUND = "REFUND"
    PROMO = "PROMO"
    REFERRAL = "REFERRAL"

class CreditTransactionType(str, enum.Enum):
    ADD = "ADD"
    USE = "USE"
    EXPIRE = "EXPIRE"

class InvoiceStatus(str, enum.Enum):
    DRAFT = "DRAFT"
    PAID = "PAID"
    FAILED = "FAILED"
    REFUNDED = "REFUNDED"

class PaymentStatus(str, enum.Enum):
    PENDING = "PENDING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"

# ============================================
# USERS & AUTH
# ============================================

class User(Base):
    __tablename__ = 'users'
    
    id = Column(Integer, primary_key=True)
    email = Column(String(255), unique=True, nullable=False)
    password_hash = Column(String(255), nullable=True)
    name = Column(String(255))
    avatar_url = Column(String(500))
    
    # Role & Status
    role = Column(Enum(UserRole), default=UserRole.USER, nullable=False)
    status = Column(Enum(UserStatus), default=UserStatus.PENDING, nullable=False)
    
    # Plan & Billing
    plan = Column(Enum(UserPlan), default=UserPlan.FREE, nullable=False)
    plan_started_at = Column(DateTime)
    plan_expires_at = Column(DateTime)
    stripe_customer_id = Column(String(255))
    stripe_subscription_id = Column(String(255))
    
    # Credits
    credits_balance = Column(Numeric(10, 2), default=0.00)
    
    # Password Management
    password_reset_token = Column(String(255))
    password_reset_expires = Column(DateTime)
    force_password_change = Column(Boolean, default=False)
    
    # Approval Tracking
    approved_by = Column(Integer, ForeignKey('users.id'))
    approved_at = Column(DateTime)
    suspended_by = Column(Integer, ForeignKey('users.id'))
    suspended_at = Column(DateTime)
    suspension_reason = Column(Text)
    blocked_by = Column(Integer, ForeignKey('users.id'))
    blocked_at = Column(DateTime)
    
    # Organization
    primary_org_id = Column(Integer, ForeignKey('organizations.id'))
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    last_login_at = Column(DateTime)
    
    # Relationships
    auth_providers = relationship("AuthProvider", back_populates="user", cascade="all, delete-orphan")
    notifications = relationship("Notification", back_populates="user", cascade="all, delete-orphan")
    credits = relationship("UserCredit", back_populates="user", cascade="all, delete-orphan")
    credit_transactions = relationship("CreditTransaction", back_populates="user", cascade="all, delete-orphan")
    primary_organization = relationship("Organization", foreign_keys=[primary_org_id])
    

class AuthProvider(Base):
    __tablename__ = 'auth_providers'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id', ondelete='CASCADE'), nullable=False)
    provider = Column(Enum(AuthProviderType), nullable=False)
    provider_user_id = Column(String(255), nullable=False)
    provider_email = Column(String(255))
    provider_username = Column(String(255))
    is_primary = Column(Boolean, default=False)
    linked_at = Column(DateTime, default=datetime.utcnow)
    last_used_at = Column(DateTime)
    
    # Relationships
    user = relationship("User", back_populates="auth_providers")

# ============================================
# ORGANIZATIONS
# ============================================

class Organization(Base):
    __tablename__ = 'organizations'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    slug = Column(String(100), unique=True, nullable=False)
    owner_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    
    # Plan & Billing
    plan = Column(Enum(UserPlan), default=UserPlan.FREE, nullable=False)
    plan_expires_at = Column(DateTime)
    stripe_customer_id = Column(String(255))
    stripe_subscription_id = Column(String(255))
    
    # Settings
    settings = Column(JSONB)
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    owner = relationship("User", foreign_keys=[owner_id])
    members = relationship("OrganizationMember", back_populates="organization", cascade="all, delete-orphan")
    invites = relationship("OrganizationInvite", back_populates="organization", cascade="all, delete-orphan")

class OrganizationMember(Base):
    __tablename__ = 'organization_members'
    
    id = Column(Integer, primary_key=True)
    organization_id = Column(Integer, ForeignKey('organizations.id', ondelete='CASCADE'), nullable=False)
    user_id = Column(Integer, ForeignKey('users.id', ondelete='CASCADE'), nullable=False)
    role = Column(Enum(OrgMemberRole), default=OrgMemberRole.MEMBER, nullable=False)
    invited_by = Column(Integer, ForeignKey('users.id'))
    joined_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    organization = relationship("Organization", back_populates="members")
    user = relationship("User", foreign_keys=[user_id])
    inviter = relationship("User", foreign_keys=[invited_by])

class OrganizationInvite(Base):
    __tablename__ = 'organization_invites'
    
    id = Column(Integer, primary_key=True)
    organization_id = Column(Integer, ForeignKey('organizations.id', ondelete='CASCADE'), nullable=False)
    email = Column(String(255), nullable=False)
    role = Column(Enum(OrgMemberRole), nullable=False)
    invited_by = Column(Integer, ForeignKey('users.id'), nullable=False)
    token = Column(String(255), unique=True, nullable=False)
    expires_at = Column(DateTime, nullable=False)
    accepted = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    organization = relationship("Organization", back_populates="invites")
    inviter = relationship("User")

# ============================================
# USAGE TRACKING
# ============================================

class UserUsage(Base):
    __tablename__ = 'user_usage'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id', ondelete='CASCADE'))
    organization_id = Column(Integer, ForeignKey('organizations.id', ondelete='CASCADE'))
    
    # Period
    period_start = Column(Date, nullable=False)
    period_end = Column(Date, nullable=False)
    
    # Counters
    mappings_count = Column(Integer, default=0)
    projects_count = Column(Integer, default=0)
    storage_bytes = Column(BigInteger, default=0)
    ai_calls_count = Column(Integer, default=0)
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class DailyStats(Base):
    __tablename__ = 'daily_stats'
    
    id = Column(Integer, primary_key=True)
    date = Column(Date, unique=True, nullable=False)
    
    # User Metrics
    total_users = Column(Integer, default=0)
    active_users = Column(Integer, default=0)
    new_signups = Column(Integer, default=0)
    free_users = Column(Integer, default=0)
    pro_users = Column(Integer, default=0)
    enterprise_users = Column(Integer, default=0)
    
    # Revenue
    mrr = Column(Numeric(10, 2), default=0.00)
    
    # Usage
    total_mappings = Column(Integer, default=0)
    total_projects = Column(Integer, default=0)
    total_storage_gb = Column(Numeric(10, 2), default=0.00)
    ai_calls = Column(Integer, default=0)
    ai_cost = Column(Numeric(10, 2), default=0.00)
    
    created_at = Column(DateTime, default=datetime.utcnow)

# ============================================
# BILLING
# ============================================

class PricingConfig(Base):
    __tablename__ = 'pricing_config'
    
    id = Column(Integer, primary_key=True)
    plan_name = Column(Enum(UserPlan), nullable=False)
    
    # Pricing
    price_monthly = Column(Numeric(10, 2), nullable=False)
    price_yearly = Column(Numeric(10, 2), nullable=False)
    
    # Limits
    max_mappings = Column(Integer, nullable=False)
    max_projects = Column(Integer, nullable=False)
    max_storage_mb = Column(Integer, nullable=False)
    ai_calls_per_month = Column(Integer, nullable=False)
    
    # Features
    features = Column(JSONB)
    
    # Activation
    active = Column(Boolean, default=True)
    effective_from = Column(Date, nullable=False)
    
    created_at = Column(DateTime, default=datetime.utcnow)

class Invoice(Base):
    __tablename__ = 'invoices'
    
    id = Column(Integer, primary_key=True)
    invoice_number = Column(String(50), unique=True, nullable=False)
    user_id = Column(Integer, ForeignKey('users.id'))
    organization_id = Column(Integer, ForeignKey('organizations.id'))
    
    # Amount
    amount = Column(Numeric(10, 2), nullable=False)
    currency = Column(String(3), default='EUR')
    
    # Status
    status = Column(Enum(InvoiceStatus), default=InvoiceStatus.DRAFT)
    
    # Stripe
    stripe_invoice_id = Column(String(255))
    
    # PDF
    pdf_url = Column(String(500))
    
    # Timestamps
    paid_at = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)

class Payment(Base):
    __tablename__ = 'payments'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    invoice_id = Column(Integer, ForeignKey('invoices.id'))
    
    # Amount
    amount = Column(Numeric(10, 2), nullable=False)
    currency = Column(String(3), default='EUR')
    
    # Method
    payment_method = Column(String(50))
    
    # Stripe
    stripe_payment_id = Column(String(255))
    
    # Status
    status = Column(Enum(PaymentStatus), default=PaymentStatus.PENDING)
    
    created_at = Column(DateTime, default=datetime.utcnow)

# ============================================
# COUPONS & CREDITS
# ============================================

class Coupon(Base):
    __tablename__ = 'coupons'
    
    id = Column(Integer, primary_key=True)
    code = Column(String(50), unique=True, nullable=False)
    
    # Discount
    type = Column(Enum(CouponType), nullable=False)
    value = Column(Numeric(10, 2), nullable=False)
    
    # Applicable Plans
    applicable_plans = Column(JSONB)
    
    # Validity
    valid_from = Column(Date, nullable=False)
    valid_to = Column(Date, nullable=False)
    
    # Usage Limits
    max_uses = Column(Integer)
    times_used = Column(Integer, default=0)
    one_per_user = Column(Boolean, default=True)
    
    # Meta
    created_by = Column(Integer, ForeignKey('users.id'))
    active = Column(Boolean, default=True)
    
    created_at = Column(DateTime, default=datetime.utcnow)

class CouponUsage(Base):
    __tablename__ = 'coupon_usage'
    
    id = Column(Integer, primary_key=True)
    coupon_id = Column(Integer, ForeignKey('coupons.id'), nullable=False)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    discount_amount = Column(Numeric(10, 2), nullable=False)
    used_at = Column(DateTime, default=datetime.utcnow)

class UserCredit(Base):
    __tablename__ = 'user_credits'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id', ondelete='CASCADE'), nullable=False)
    
    # Amount
    amount = Column(Numeric(10, 2), nullable=False)
    
    # Type & Reason
    type = Column(Enum(CreditType), nullable=False)
    reason = Column(Text)
    
    # Granted By
    granted_by = Column(Integer, ForeignKey('users.id'))
    
    # Expiration
    expires_at = Column(DateTime)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    user = relationship("User", foreign_keys=[user_id], back_populates="credits")

class CreditTransaction(Base):
    __tablename__ = 'credit_transactions'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id', ondelete='CASCADE'), nullable=False)
    
    # Amount
    amount = Column(Numeric(10, 2), nullable=False)
    balance_after = Column(Numeric(10, 2), nullable=False)
    
    # Type
    type = Column(Enum(CreditTransactionType), nullable=False)
    
    # Reference
    reference_type = Column(String(50))
    reference_id = Column(Integer)
    description = Column(Text)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    user = relationship("User", foreign_keys=[user_id], back_populates="credit_transactions")

# ============================================
# NOTIFICATIONS
# ============================================

class Notification(Base):
    __tablename__ = 'notifications'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id', ondelete='CASCADE'), nullable=False)
    
    # Type & Content
    type = Column(Enum(NotificationType), default=NotificationType.INFO)
    title = Column(String(255), nullable=False)
    message = Column(Text, nullable=False)
    
    # Action
    action_url = Column(String(500))
    action_label = Column(String(100))
    
    # Read Status
    read = Column(Boolean, default=False)
    read_at = Column(DateTime)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    user = relationship("User", back_populates="notifications")

# ============================================
# AUDIT LOG
# ============================================

class AuditLog(Base):
    __tablename__ = 'audit_log'
    
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, default=datetime.utcnow)
    
    # Actor
    user_id = Column(Integer, ForeignKey('users.id'))
    
    # Action
    action = Column(String(100), nullable=False)
    
    # Target
    target_type = Column(String(50))
    target_id = Column(Integer)
    
    # Changes
    old_value = Column(JSONB)
    new_value = Column(JSONB)
    
    # Context
    ip_address = Column(String(45))
    user_agent = Column(Text)
    
    # Result
    success = Column(Boolean, default=True)
    error_message = Column(Text)

class RoleChange(Base):
    __tablename__ = 'role_changes'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    from_role = Column(Enum(UserRole), nullable=False)
    to_role = Column(Enum(UserRole), nullable=False)
    changed_by = Column(Integer, ForeignKey('users.id'), nullable=False)
    reason = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)
