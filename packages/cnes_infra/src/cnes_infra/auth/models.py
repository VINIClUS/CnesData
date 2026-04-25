"""Pydantic models for OAuth Device Flow + cert provisioning.

Shapes match RFC 8628 (Device Authorization Grant) plus internal
provisioning extensions.
"""
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


class DeviceAuthorizationResponse(BaseModel):
    device_code: str
    user_code: str
    verification_uri: str
    verification_uri_complete: str
    expires_in: int = Field(gt=0, le=3600)
    interval: int = Field(ge=1, le=60, default=5)


class TokenResponse(BaseModel):
    access_token: str
    token_type: Literal["Bearer"] = "Bearer"  # noqa: S105
    expires_in: int
    refresh_token: str


class TokenError(BaseModel):
    error: Literal[
        "authorization_pending",
        "slow_down",
        "access_denied",
        "expired_token",
        "invalid_grant",
        "invalid_client",
    ]
    error_description: str | None = None


class CertProvisionRequest(BaseModel):
    csr_pem: str
    machine_fingerprint: str = Field(min_length=8, max_length=128)


class CertProvisionResponse(BaseModel):
    cert_pem: str
    ca_chain_pem: str
    refresh_token: str
    expires_at: datetime


class CertRotateRequest(BaseModel):
    csr_pem: str
