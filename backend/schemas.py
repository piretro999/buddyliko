"""
Buddyliko Pydantic Schemas
Request/Response validation models
"""

from pydantic import BaseModel, EmailStr, validator, Field
from typing import Optional, List, Dict, Any
from datetime import datetime, date
from decimal import Decimal
from enum import Enum

# ============================================
# ENUMS (matching database)
# ============================================

class UserRole(str, Enum):
    MASTER = "MASTER"
    ADMIN = "ADMIN"
    USER = "USER"

class UserStatus(str, Enum):
    PENDING = "PENDING"
    APPROVED = "APPROVED"
    SUSPENDED = "SUSPENDED"
    BLOCKED = "BLOCKED"

class UserPlan(str, Enum):
    FREE = "FREE"
    PRO = "PRO"
    ENTERPRISE = "ENTERPRISE"

class AuthProviderType(str, Enum):
    EMAIL = "EMAIL"
    GOOGLE = "GOOGLE"
    FACEBOOK = "FACEBOOK"
    GITHUB = "GITHUB"

class OrgMemberRole(str, Enum):
    OWNER = "OWNER"
    ADMIN = "ADMIN"
    MEMBER = "MEMBER"

class NotificationType(str, Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    SUCCESS = "SUCCESS"
    ERROR = "ERROR"

class CouponType(str, Enum):
    PERCENTAGE = "PERCENTAGE"
    FIXED = "FIXED"

# ============================================
# AUTH SCHEMAS
# ============================================

class UserRegister(BaseModel):
    email: EmailStr
    password: str = Field(..., min_length=8)
    name: str = Field(..., min_length=2)
    
    @validator('password')
    def validate_password(cls, v):
        if not any(c.isupper() for c in v):
            raise ValueError('Password must contain at least one uppercase letter')
        if not any(c.isdigit() for c in v):
            raise ValueError('Password must contain at least one digit')
        return v

class UserLogin(BaseModel):
    email: EmailStr
    password: str

class OAuthCallback(BaseModel):
    code: str
    state: Optional[str] = None

class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: 'UserResponse'

class PasswordReset(BaseModel):
    email: EmailStr

class PasswordResetConfirm(BaseModel):
    token: str
    new_password: str = Field(..., min_length=8)

class PasswordChange(BaseModel):
    current_password: str
    new_password: str = Field(..., min_length=8)

# ============================================
# USER SCHEMAS
# ============================================

class AuthProviderResponse(BaseModel):
    id: int
    provider: AuthProviderType
    provider_email: Optional[str]
    provider_username: Optional[str]
    is_primary: bool
    linked_at: datetime
    last_used_at: Optional[datetime]
    
    class Config:
        orm_mode = True

class UserResponse(BaseModel):
    id: int
    email: str
    name: str
    avatar_url: Optional[str]
    role: UserRole
    status: UserStatus
    plan: UserPlan
    plan_expires_at: Optional[datetime]
    credits_balance: Decimal
    created_at: datetime
    last_login_at: Optional[datetime]
    
    # Optional detailed fields
    auth_providers: Optional[List[AuthProviderResponse]] = None
    
    class Config:
        orm_mode = True

class UserUpdate(BaseModel):
    name: Optional[str] = None
    avatar_url: Optional[str] = None

class UserAdminUpdate(BaseModel):
    role: Optional[UserRole] = None
    status: Optional[UserStatus] = None
    plan: Optional[UserPlan] = None
    plan_expires_at: Optional[datetime] = None
    suspension_reason: Optional[str] = None

# ============================================
# AUTH PROVIDER LINKING
# ============================================

class LinkProviderRequest(BaseModel):
    code: str  # OAuth code from provider

class UnlinkProviderRequest(BaseModel):
    provider: AuthProviderType

class SetPrimaryProviderRequest(BaseModel):
    provider: AuthProviderType

# ============================================
# ORGANIZATION SCHEMAS
# ============================================

class OrganizationCreate(BaseModel):
    name: str = Field(..., min_length=2, max_length=255)
    slug: str = Field(..., min_length=2, max_length=100, regex=r'^[a-z0-9-]+$')
    plan: UserPlan = UserPlan.FREE

class OrganizationUpdate(BaseModel):
    name: Optional[str] = None
    settings: Optional[Dict[str, Any]] = None

class OrganizationResponse(BaseModel):
    id: int
    name: str
    slug: str
    owner_id: int
    plan: UserPlan
    plan_expires_at: Optional[datetime]
    created_at: datetime
    
    class Config:
        orm_mode = True

class OrganizationMemberResponse(BaseModel):
    id: int
    user_id: int
    role: OrgMemberRole
    joined_at: datetime
    user: UserResponse
    
    class Config:
        orm_mode = True

class InviteMemberRequest(BaseModel):
    email: EmailStr
    role: OrgMemberRole = OrgMemberRole.MEMBER

class UpdateMemberRoleRequest(BaseModel):
    role: OrgMemberRole

class AcceptInviteRequest(BaseModel):
    token: str

# ============================================
# USAGE SCHEMAS
# ============================================

class UsageResponse(BaseModel):
    mappings_count: int
    projects_count: int
    storage_bytes: int
    ai_calls_count: int
    period_start: date
    period_end: date
    
    # Limits (from plan)
    max_mappings: int
    max_projects: int
    max_storage_mb: int
    ai_calls_per_month: int
    
    # Percentages
    mappings_percent: float
    projects_percent: float
    storage_percent: float
    ai_calls_percent: float

class DailyStatsResponse(BaseModel):
    date: date
    total_users: int
    active_users: int
    new_signups: int
    free_users: int
    pro_users: int
    enterprise_users: int
    mrr: Decimal
    total_mappings: int
    total_projects: int
    total_storage_gb: Decimal
    ai_calls: int
    ai_cost: Decimal
    
    class Config:
        orm_mode = True

# ============================================
# BILLING SCHEMAS
# ============================================

class PricingConfigResponse(BaseModel):
    plan_name: UserPlan
    price_monthly: Decimal
    price_yearly: Decimal
    max_mappings: int
    max_projects: int
    max_storage_mb: int
    ai_calls_per_month: int
    features: Dict[str, Any]
    
    class Config:
        orm_mode = True

class CreateCheckoutSessionRequest(BaseModel):
    plan: UserPlan
    interval: str = Field(..., regex='^(monthly|yearly)$')
    coupon_code: Optional[str] = None

class CheckoutSessionResponse(BaseModel):
    session_id: str
    url: str

class InvoiceResponse(BaseModel):
    id: int
    invoice_number: str
    amount: Decimal
    currency: str
    status: str
    pdf_url: Optional[str]
    paid_at: Optional[datetime]
    created_at: datetime
    
    class Config:
        orm_mode = True

class StripeWebhookEvent(BaseModel):
    type: str
    data: Dict[str, Any]

# ============================================
# COUPON SCHEMAS
# ============================================

class CouponCreate(BaseModel):
    code: str = Field(..., min_length=3, max_length=50)
    type: CouponType
    value: Decimal = Field(..., gt=0)
    applicable_plans: List[UserPlan]
    valid_from: date
    valid_to: date
    max_uses: Optional[int] = None
    one_per_user: bool = True

class CouponResponse(BaseModel):
    id: int
    code: str
    type: CouponType
    value: Decimal
    applicable_plans: List[str]
    valid_from: date
    valid_to: date
    max_uses: Optional[int]
    times_used: int
    active: bool
    created_at: datetime
    
    class Config:
        orm_mode = True

class ValidateCouponRequest(BaseModel):
    code: str
    plan: UserPlan

class ValidateCouponResponse(BaseModel):
    valid: bool
    discount_amount: Optional[Decimal] = None
    message: Optional[str] = None

# ============================================
# CREDIT SCHEMAS
# ============================================

class AddCreditRequest(BaseModel):
    user_id: int
    amount: Decimal = Field(..., gt=0)
    reason: str
    expires_at: Optional[datetime] = None

class CreditTransactionResponse(BaseModel):
    id: int
    amount: Decimal
    balance_after: Decimal
    type: str
    description: Optional[str]
    created_at: datetime
    
    class Config:
        orm_mode = True

# ============================================
# NOTIFICATION SCHEMAS
# ============================================

class NotificationCreate(BaseModel):
    user_id: int
    type: NotificationType
    title: str
    message: str
    action_url: Optional[str] = None
    action_label: Optional[str] = None

class NotificationResponse(BaseModel):
    id: int
    type: NotificationType
    title: str
    message: str
    action_url: Optional[str]
    action_label: Optional[str]
    read: bool
    created_at: datetime
    
    class Config:
        orm_mode = True

class MarkNotificationReadRequest(BaseModel):
    notification_ids: List[int]

# ============================================
# AUDIT LOG SCHEMAS
# ============================================

class AuditLogResponse(BaseModel):
    id: int
    timestamp: datetime
    user_id: Optional[int]
    action: str
    target_type: Optional[str]
    target_id: Optional[int]
    old_value: Optional[Dict[str, Any]]
    new_value: Optional[Dict[str, Any]]
    ip_address: Optional[str]
    success: bool
    error_message: Optional[str]
    
    class Config:
        orm_mode = True

class AuditLogFilter(BaseModel):
    user_id: Optional[int] = None
    action: Optional[str] = None
    target_type: Optional[str] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    limit: int = Field(default=50, le=500)
    offset: int = 0

# ============================================
# ADMIN SCHEMAS
# ============================================

class AdminDashboardStats(BaseModel):
    # Users
    total_users: int
    pending_users: int
    active_users: int
    users_by_plan: Dict[str, int]
    
    # Revenue
    mrr: Decimal
    arr: Decimal
    
    # Usage
    total_mappings: int
    total_projects: int
    total_storage_gb: Decimal
    ai_calls_today: int
    ai_cost_today: Decimal

class ApproveUserRequest(BaseModel):
    user_id: int

class RejectUserRequest(BaseModel):
    user_id: int
    reason: str

class SuspendUserRequest(BaseModel):
    user_id: int
    reason: str

class ForcePasswordResetRequest(BaseModel):
    user_id: int

class SetTemporaryPasswordRequest(BaseModel):
    user_id: int
    send_email: bool = True

class SetTemporaryPasswordResponse(BaseModel):
    temporary_password: str
    force_change_on_login: bool = True

class PromoteUserRequest(BaseModel):
    user_id: int
    to_role: UserRole
    reason: Optional[str] = None

class ChangePlanRequest(BaseModel):
    user_id: int
    plan: UserPlan
    expires_at: Optional[datetime] = None

class ApplyDiscountRequest(BaseModel):
    user_id: int
    discount_percent: int = Field(..., ge=0, le=100)
    months: int = Field(..., ge=1, le=12)

class ResetUsageRequest(BaseModel):
    user_id: int

# ============================================
# PAGINATION
# ============================================

class PaginatedResponse(BaseModel):
    items: List[Any]
    total: int
    page: int
    page_size: int
    total_pages: int

# ============================================
# GENERIC RESPONSES
# ============================================

class SuccessResponse(BaseModel):
    success: bool = True
    message: Optional[str] = None
    data: Optional[Dict[str, Any]] = None

class ErrorResponse(BaseModel):
    success: bool = False
    error: str
    details: Optional[Dict[str, Any]] = None

# Update forward references
UserResponse.update_forward_refs()
TokenResponse.update_forward_refs()
