#!/usr/bin/env python3
"""
Buddyliko Setup Script
Initialize database, create first admin user
"""

import os
import sys
from pathlib import Path

# Add backend to path
sys.path.insert(0, str(Path(__file__).parent))

from backend.database import init_db, get_db_context
from backend.models.database import User, AuthProvider, UserRole, UserStatus, AuthProviderType, PricingConfig, UserPlan
from backend.services.auth_service import hash_password
from datetime import datetime, date

def setup_database():
    """Create all database tables"""
    print("🔨 Creating database tables...")
    init_db()
    print("✅ Database tables created")

def seed_pricing():
    """Insert default pricing config"""
    print("💰 Seeding pricing configuration...")
    
    with get_db_context() as db:
        # Check if already seeded
        existing = db.query(PricingConfig).count()
        if existing > 0:
            print("⚠️  Pricing already configured, skipping")
            return
        
        # FREE plan
        free_plan = PricingConfig(
            plan_name=UserPlan.FREE,
            price_monthly=0,
            price_yearly=0,
            max_mappings=5,
            max_projects=1,
            max_storage_mb=100,
            ai_calls_per_month=0,
            features={
                "support": False,
                "api_access": False,
                "export_formats": ["CSV", "JSON"]
            },
            effective_from=date(2025, 1, 1)
        )
        
        # PRO plan
        pro_plan = PricingConfig(
            plan_name=UserPlan.PRO,
            price_monthly=29,
            price_yearly=290,  # ~17% discount
            max_mappings=500,
            max_projects=50,
            max_storage_mb=10240,  # 10GB
            ai_calls_per_month=100,
            features={
                "support": "email",
                "api_access": True,
                "export_formats": ["CSV", "JSON", "XML", "Excel"]
            },
            effective_from=date(2025, 1, 1)
        )
        
        # ENTERPRISE plan
        enterprise_plan = PricingConfig(
            plan_name=UserPlan.ENTERPRISE,
            price_monthly=0,  # Custom pricing
            price_yearly=0,
            max_mappings=-1,  # Unlimited
            max_projects=-1,
            max_storage_mb=-1,
            ai_calls_per_month=-1,
            features={
                "support": "priority",
                "api_access": True,
                "export_formats": ["CSV", "JSON", "XML", "Excel", "Custom"],
                "white_label": True,
                "sla": True,
                "dedicated_support": True
            },
            effective_from=date(2025, 1, 1)
        )
        
        db.add(free_plan)
        db.add(pro_plan)
        db.add(enterprise_plan)
        db.commit()
    
    print("✅ Pricing configuration seeded")

def create_master_user(email: str, password: str, name: str):
    """Create first MASTER user"""
    print(f"👑 Creating MASTER user: {email}")
    
    with get_db_context() as db:
        # Check if any users exist
        existing_users = db.query(User).count()
        if existing_users > 0:
            print("⚠️  Users already exist. First user is already MASTER.")
            return
        
        # Create MASTER user
        master = User(
            email=email,
            password_hash=hash_password(password),
            name=name,
            role=UserRole.MASTER,
            status=UserStatus.APPROVED,
            created_at=datetime.utcnow(),
            last_login_at=None
        )
        
        db.add(master)
        db.flush()
        
        # Create EMAIL auth provider
        auth_provider = AuthProvider(
            user_id=master.id,
            provider=AuthProviderType.EMAIL,
            provider_user_id=email,
            provider_email=email,
            is_primary=True
        )
        
        db.add(auth_provider)
        db.commit()
    
    print(f"✅ MASTER user created: {email}")
    print(f"   Password: {password}")
    print("   ⚠️  CHANGE THIS PASSWORD IMMEDIATELY!")

def main():
    """Main setup flow"""
    print("=" * 60)
    print("🚀 BUDDYLIKO SETUP")
    print("=" * 60)
    print()
    
    # Check if .env exists
    if not os.path.exists('.env'):
        print("❌ .env file not found!")
        print("   1. Copy .env.example to .env")
        print("   2. Edit .env with your configuration")
        print("   3. Run setup again")
        sys.exit(1)
    
    # Load environment
    from dotenv import load_dotenv
    load_dotenv()
    
    # Check DATABASE_URL
    db_url = os.getenv('DATABASE_URL')
    if not db_url:
        print("❌ DATABASE_URL not set in .env")
        sys.exit(1)
    
    print(f"📊 Database: {db_url.split('@')[1] if '@' in db_url else db_url}")
    print()
    
    # Setup steps
    try:
        setup_database()
        seed_pricing()
        
        # Prompt for master user
        print()
        print("=" * 60)
        print("Create MASTER user:")
        print("=" * 60)
        
        email = input("Email: ").strip()
        if not email:
            print("❌ Email required")
            sys.exit(1)
        
        password = input("Password: ").strip()
        if not password or len(password) < 8:
            print("❌ Password must be at least 8 characters")
            sys.exit(1)
        
        name = input("Name: ").strip() or "Admin"
        
        create_master_user(email, password, name)
        
        print()
        print("=" * 60)
        print("✅ SETUP COMPLETE!")
        print("=" * 60)
        print()
        print("Next steps:")
        print("1. Start the server:")
        print("   uvicorn backend.api.main:app --reload --host 0.0.0.0 --port 8080")
        print()
        print("2. Login with your MASTER account:")
        print(f"   Email: {email}")
        print(f"   Password: {password}")
        print()
        print("3. Change your password immediately!")
        print()
        
    except Exception as e:
        print(f"❌ Setup failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
