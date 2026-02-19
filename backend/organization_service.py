"""Organization Service"""

from sqlalchemy.orm import Session
from fastapi import HTTPException, status
from typing import List
from datetime import datetime, timedelta
import secrets

from backend.models.database import Organization, OrganizationMember, OrganizationInvite, User, OrgMemberRole

def create_organization(db: Session, name: str, slug: str, owner: User) -> Organization:
    """Create new organization"""
    
    # Check slug unique
    existing = db.query(Organization).filter(Organization.slug == slug).first()
    if existing:
        raise HTTPException(status_code=400, detail="Slug already taken")
    
    org = Organization(name=name, slug=slug, owner_id=owner.id, plan=owner.plan)
    db.add(org)
    db.flush()
    
    # Add owner as member
    member = OrganizationMember(organization_id=org.id, user_id=owner.id, role=OrgMemberRole.OWNER)
    db.add(member)
    
    db.commit()
    db.refresh(org)
    return org

def invite_member(db: Session, org_id: int, email: str, role: OrgMemberRole, inviter: User) -> OrganizationInvite:
    """Invite user to organization"""
    
    org = db.query(Organization).filter(Organization.id == org_id).first()
    if not org:
        raise HTTPException(status_code=404, detail="Organization not found")
    
    # Check inviter permission
    member = db.query(OrganizationMember).filter(
        OrganizationMember.organization_id == org_id,
        OrganizationMember.user_id == inviter.id,
        OrganizationMember.role.in_([OrgMemberRole.OWNER, OrgMemberRole.ADMIN])
    ).first()
    
    if not member:
        raise HTTPException(status_code=403, detail="Only owner/admin can invite")
    
    # Generate token
    token = secrets.token_urlsafe(32)
    expires = datetime.utcnow() + timedelta(days=7)
    
    invite = OrganizationInvite(
        organization_id=org_id,
        email=email,
        role=role,
        invited_by=inviter.id,
        token=token,
        expires_at=expires
    )
    
    db.add(invite)
    db.commit()
    db.refresh(invite)
    
    return invite

def accept_invite(db: Session, token: str, user: User) -> OrganizationMember:
    """Accept organization invite"""
    
    invite = db.query(OrganizationInvite).filter(OrganizationInvite.token == token).first()
    if not invite:
        raise HTTPException(status_code=404, detail="Invalid invite")
    
    if invite.accepted:
        raise HTTPException(status_code=400, detail="Invite already used")
    
    if invite.expires_at < datetime.utcnow():
        raise HTTPException(status_code=400, detail="Invite expired")
    
    # Check if already member
    existing = db.query(OrganizationMember).filter(
        OrganizationMember.organization_id == invite.organization_id,
        OrganizationMember.user_id == user.id
    ).first()
    
    if existing:
        raise HTTPException(status_code=400, detail="Already a member")
    
    # Add member
    member = OrganizationMember(
        organization_id=invite.organization_id,
        user_id=user.id,
        role=invite.role,
        invited_by=invite.invited_by
    )
    
    invite.accepted = True
    
    db.add(member)
    db.commit()
    db.refresh(member)
    
    return member

def remove_member(db: Session, org_id: int, user_id: int, admin: User):
    """Remove member from organization"""
    
    # Check permission
    admin_member = db.query(OrganizationMember).filter(
        OrganizationMember.organization_id == org_id,
        OrganizationMember.user_id == admin.id,
        OrganizationMember.role.in_([OrgMemberRole.OWNER, OrgMemberRole.ADMIN])
    ).first()
    
    if not admin_member:
        raise HTTPException(status_code=403, detail="No permission")
    
    # Can't remove owner
    member = db.query(OrganizationMember).filter(
        OrganizationMember.organization_id == org_id,
        OrganizationMember.user_id == user_id
    ).first()
    
    if not member:
        raise HTTPException(status_code=404, detail="Member not found")
    
    if member.role == OrgMemberRole.OWNER:
        raise HTTPException(status_code=400, detail="Cannot remove owner")
    
    db.delete(member)
    db.commit()
