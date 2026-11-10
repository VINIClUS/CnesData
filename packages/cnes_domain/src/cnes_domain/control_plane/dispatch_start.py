"""Authenticated dispatch-start claims shared by control plane and executor."""

from __future__ import annotations

import json
from dataclasses import dataclass
from hashlib import sha256
from typing import TYPE_CHECKING, cast

from pydantic import BaseModel, ConfigDict, field_validator

if TYPE_CHECKING:
    from datetime import datetime

    from cnes_domain.control_plane.commands import BindRunDispatch, FinishRunDispatch
    from cnes_domain.control_plane.entities import RunDispatch
    from cnes_domain.ports.control_plane import ControlPlanePort
    from cnes_domain.ports.environment_gate import DispatchDeferred, UnitAdmission
    from cnes_domain.ports.processing import ProcessorExecutorPort, StartRunExecution


@dataclass(frozen=True)
class StartAdmission:
    admission: UnitAdmission
    now: datetime
    lease_seconds: int

    def bind(
        self, dispatch: RunDispatch, execution_ref: str, claim: DispatchStartClaim | None
    ) -> BindRunDispatch:
        from cnes_domain.control_plane.commands import BindRunDispatch

        return BindRunDispatch(
            tenant_id=dispatch.tenant_id,
            run_id=dispatch.run_id,
            dispatch_id=dispatch.dispatch_id,
            execution_ref=execution_ref,
            now=self.now,
            lease_seconds=self.lease_seconds,
            claim=claim,
        )

    def cancel(self, dispatch: RunDispatch, claim: DispatchStartClaim | None) -> FinishRunDispatch:
        from cnes_domain.control_plane.commands import FinishRunDispatch
        from cnes_domain.control_plane.enums import DispatchOutcome

        return FinishRunDispatch(
            tenant_id=dispatch.tenant_id,
            run_id=dispatch.run_id,
            dispatch_id=dispatch.dispatch_id,
            outcome=DispatchOutcome.CANCELED,
            finished_at=self.now,
            claim=claim,
        )


def _dispatch_id(value: str) -> str:
    if len(value) != 16 or any(char not in "0123456789abcdef" for char in value):
        raise ValueError("invalid_dispatch_id")
    return value


def _sha256(value: str) -> str:
    if len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
        raise ValueError("invalid_sha256")
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


def claim_dispatch_start(
    control_plane: ControlPlanePort,
    dispatch: RunDispatch,
    request: StartRunExecution,
    attempt: StartAdmission,
) -> DispatchStartClaim | DispatchDeferred | None:
    """Reivindica o único envio permitido pela admissão nativa."""
    from cnes_domain.control_plane.commands import ClaimRunDispatchStart
    from cnes_domain.ports.environment_gate import DeferralReason, DispatchDeferred

    admission = attempt.admission
    if admission.token is None:
        return None
    claim = DispatchStartClaim(
        dispatch_id=dispatch.dispatch_id,
        generation=dispatch.generation,
        permit_token=admission.token,
        fence_version=admission.fence_version or 0,
        request_sha256=request_sha256(request),
        promotion_id=admission.promotion_id,
    )
    claimed = control_plane.claim_run_dispatch_start(
        ClaimRunDispatchStart(
            tenant_id=dispatch.tenant_id,
            run_id=dispatch.run_id,
            dispatch_id=dispatch.dispatch_id,
            claim=claim,
            now=attempt.now,
            lease_seconds=attempt.lease_seconds,
            environment=admission.environment or "prod",
        )
    )
    return claim if claimed is not None else DispatchDeferred(DeferralReason.PERMIT_HELD)


def execute_dispatch_start(
    executor: ProcessorExecutorPort,
    request: StartRunExecution,
    claim: DispatchStartClaim | None,
) -> str:
    """Envia o request original carregando o claim quando suportado pelo executor."""
    start_with_claim = getattr(executor, "start_with_claim", None)
    if claim is not None and callable(start_with_claim):
        return cast("str", start_with_claim(request, claim))
    return executor.start(request)
