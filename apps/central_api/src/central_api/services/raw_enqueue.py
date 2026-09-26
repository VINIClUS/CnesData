"""Criação idempotente dos jobs raw do Edge."""

from __future__ import annotations

from datetime import timedelta
from hashlib import sha256
from typing import TYPE_CHECKING, Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator

from cnes_domain.control_plane.commands import BeginIdempotency
from cnes_domain.control_plane.entities import Job, OutboxEvent
from cnes_domain.control_plane.enums import JobState

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import datetime

    from cnes_domain.ports.control_plane import ControlPlanePort

type RawSource = Literal["CNES_LOCAL", "SIHD", "BPA_MAG", "SIA_LOCAL"]

RAW_PAIRS: dict[RawSource, tuple[str, ...]] = {
    "CNES_LOCAL": ("CNES_VINCULO",),
    "SIHD": ("SIHD_INTERNACAO", "SIHD_PROC_AIH"),
    "BPA_MAG": ("BPA_C", "BPA_I"),
    "SIA_LOCAL": ("SIA_APA", "SIA_BPI", "SIA_BPIHST", "DIM_SIGTAP", "DIM_MUNICIPIO"),
}


class RawEnqueueRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tenant_id: str = Field(min_length=1)
    agent_id: str = Field(min_length=1)
    competencia: str = Field(pattern=r"^\d{4}-(0[1-9]|1[0-2])$")
    sources: tuple[RawSource, ...] | None = None
    cnes_snapshot_mode: Literal["FULL", "DELTA"] = "FULL"

    @field_validator("sources")
    @classmethod
    def unique_sources(cls, values: tuple[RawSource, ...] | None) -> tuple[RawSource, ...] | None:
        if values is not None and (not values or len(set(values)) != len(values)):
            raise ValueError("sources_invalid")
        return values


class RawEnqueueService:
    def __init__(self, control: ControlPlanePort, clock: Callable[[], datetime]) -> None:
        self._control = control
        self._clock = clock

    def enqueue(self, request: RawEnqueueRequest, key: str) -> list[str]:
        now = self._clock()
        canonical = request.model_copy(update={
            "sources": tuple(sorted(request.sources or RAW_PAIRS)),
        })
        digest = sha256(canonical.model_dump_json().encode()).hexdigest()
        outcome = self._control.begin_idempotency(BeginIdempotency(
            tenant_id=request.tenant_id, scope="raw-jobs.enqueue", key=key,
            request_hash=digest, resource_id=uuid4().hex,
            now=now, expires_at=now + timedelta(hours=24),
        ))
        return [
            self._ensure_job(
                request, outcome.record.resource_id, source, subtype,
                outcome.record.created_at,
            )
            for source in canonical.sources or ()
            for subtype in RAW_PAIRS[source]
        ]

    def _ensure_job(
        self, request: RawEnqueueRequest, batch: str,
        source: RawSource, subtype: str, now: datetime,
    ) -> str:
        job_id = sha256(f"{batch}\x1f{source}\x1f{subtype}".encode()).hexdigest()
        if self._control.get_job(request.tenant_id, job_id) is not None:
            return job_id
        job = Job(
            tenant_id=request.tenant_id, job_id=job_id, agent_id=request.agent_id,
            source_type=source, file_subtype=subtype, competencia=request.competencia,
            requested_snapshot_mode=(
                request.cnes_snapshot_mode if source == "CNES_LOCAL" else "FULL"
            ),
            state=JobState.PENDING, attempt=0, fencing_token=0,
            lease_owner=None, lease_until=None, result_manifest_id=None,
            result_manifest_key=None, error_code=None, created_at=now,
        )
        event = OutboxEvent(
            tenant_id=request.tenant_id,
            event_id=sha256(f"job.created\x1f{request.tenant_id}\x1f{job_id}".encode()).hexdigest(),
            event_type="job.created", aggregate_id=job_id,
            payload={"job_id": job_id, "source_type": source},
            created_at=now, delivered_at=None,
        )
        self._control.create_job(job, event)
        return job_id
