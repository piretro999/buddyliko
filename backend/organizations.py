"""Organization API Endpoints"""

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from typing import List

from backend.database import get_db
from backend.dependencies import get_current_active_user
from backend.services import organization_service
from backend.models.schemas import *
from backend.models.database import User

router = APIRouter(prefix="/api/organizations", tags=["Organizations"])

@router.post("/", response_model=OrganizationResponse)
def create_organization(data: OrganizationCreate, user: User = Depends(get_current_active_user), db: Session = Depends(get_db)):
    return organization_service.create_organization(db, data.name, data.slug, user)

@router.post("/{org_id}/invite", response_model=SuccessResponse)
def invite_member(org_id: int, data: InviteMemberRequest, user: User = Depends(get_current_active_user), db: Session = Depends(get_db)):
    invite = organization_service.invite_member(db, org_id, data.email, data.role, user)
    return {"success": True, "message": f"Invite sent to {data.email}"}

@router.post("/accept-invite", response_model=SuccessResponse)
def accept_invite(data: AcceptInviteRequest, user: User = Depends(get_current_active_user), db: Session = Depends(get_db)):
    organization_service.accept_invite(db, data.token, user)
    return {"success": True, "message": "Joined organization"}

@router.delete("/{org_id}/members/{user_id}", response_model=SuccessResponse)
def remove_member(org_id: int, user_id: int, user: User = Depends(get_current_active_user), db: Session = Depends(get_db)):
    organization_service.remove_member(db, org_id, user_id, user)
    return {"success": True, "message": "Member removed"}
