"""Public (unauthenticated) lead capture — mounted at /api/v1/public."""
import logging
from typing import Literal

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field
from sqlalchemy.exc import SQLAlchemyError

from central_api.ratelimit import limiter
from central_api.repositories.leads_repo import LeadRecord
from cnes_infra import config

logger = logging.getLogger(__name__)

router = APIRouter(tags=["public"])

_EMAIL = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"


class LeadCreate(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    email: str = Field(min_length=3, max_length=254, pattern=_EMAIL)
    interest: Literal["early_access", "pilot", "contact"]
    organization: str = Field(default="", max_length=200)
    municipality: str = Field(default="", max_length=120)
    role: str = Field(default="", max_length=120)
    message: str = Field(default="", max_length=2000)
    source_path: str = Field(default="", max_length=200)
    source_cta: str = Field(default="", max_length=50)
    privacy_notice_version: str = Field(min_length=1, max_length=10)
    newsletter_opt_in: bool = False


class LeadReceived(BaseModel):
    status: Literal["received"] = "received"


def _leads_limit() -> str:
    return config.LEADS_RATE_LIMIT


@router.post("/leads", response_model=LeadReceived, status_code=202)
@limiter.limit(_leads_limit)
def create_lead(body: LeadCreate, request: Request) -> LeadReceived:
    repo = request.app.state.leads_repo
    fields = {k: v.strip() if isinstance(v, str) else v for k, v in body.model_dump().items()}
    record = LeadRecord(**fields)
    try:
        lead_id = repo.create(record)
    except SQLAlchemyError as e:
        logger.error("lead_persist_failed error=%s", type(e).__name__)
        raise HTTPException(status_code=503, detail="leads_unavailable") from e
    # Only server-side values are logged: request fields would allow forged log
    # entries via newlines/control characters. The row itself holds the details.
    logger.info("lead_received id=%s", lead_id)
    return LeadReceived()
