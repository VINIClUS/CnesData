"""Authenticated dispatch-start claims shared by control plane and executor."""

from __future__ import annotations

import json
from hashlib import sha256

from pydantic import BaseModel, ConfigDict, field_validator


def _dispatch_id(value: str) -> str:
    if len(value) != 16 or any(char not in "0123456789abcdef" for char in value):
        raise ValueError("invalid_dispatch_id")
    try:
        int(value, 16)
    except ValueError as error:
        raise ValueError("invalid_dispatch_id") from error
    return value


def _sha256(value: str) -> str:
    if len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
        raise ValueError("invalid_sha256")
    try:
        int(value, 16)
    except ValueError as error:
        raise ValueError("invalid_sha256") from error
    return value


class _ClaimModel(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True, extra="forbid")


class DispatchStartClaim(_ClaimModel):
    """Proof that one dispatch won the start CAS."""

    dispatch_id: str
    generation: int
    permit_token: int
    fence_version: int
    request_sha256: str
    promotion_id: str | None = None

    _dispatch = field_validator("dispatch_id")(_dispatch_id)
    _request = field_validator("request_sha256")(_sha256)

    @field_validator("promotion_id")
    @classmethod
    def _promotion(cls, value: str | None) -> str | None:
        if value is not None and not value.strip():
            raise ValueError("blank_promotion_id")
        return value

    @field_validator("generation", "permit_token")
    @classmethod
    def _positive(cls, value: int) -> int:
        if isinstance(value, bool) or value < 1:
            raise ValueError("positive_claim_counter_required")
        return value

    @field_validator("fence_version")
    @classmethod
    def _non_negative_fence(cls, value: int) -> int:
        if isinstance(value, bool) or value < 0:
            raise ValueError("non_negative_fence_version_required")
        return value


def request_sha256(request: object) -> str:
    """Return the canonical digest bound to a start request."""
    if isinstance(request, BaseModel):
        value = request.model_dump(mode="json", exclude_none=True)
        value.pop("claim", None)
    else:
        value = request
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return sha256(encoded).hexdigest()


def claim_matches(
    claim: DispatchStartClaim,
    *,
    dispatch_id: str,
    generation: int,
    request: object,
) -> bool:
    """Check dispatch identity and canonical request binding."""
    return (
        claim.dispatch_id == dispatch_id
        and claim.generation == generation
        and claim.request_sha256 == request_sha256(request)
    )


__all__ = ["DispatchStartClaim", "claim_matches", "request_sha256"]
