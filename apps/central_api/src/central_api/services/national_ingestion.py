"""Refresh raw do CNES nacional a partir dos arquivos PF oficiais."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from hashlib import sha256
from typing import TYPE_CHECKING

from central_api.services.raw_ingestion import RawAcceptance, RegisterRawManifest
from cnes_domain.control_plane.commands import ClaimJob
from cnes_domain.control_plane.entities import Agent, Job, OutboxEvent
from cnes_domain.control_plane.enums import AgentState, JobState
from cnes_domain.control_plane.errors import Conflict
from cnes_domain.control_plane.errors import ControlPlaneErrorCode as ErrorCode
from cnes_domain.control_plane.queries import RawManifestByIdQuery
from cnes_infra.ingestion import DatasusCnesRequest

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import datetime
    from typing import Protocol

    from central_api.services.raw_ingestion import RawIngestionService
    from cnes_domain.ports.control_plane import ControlPlanePort, TypedRawQueryPort
    from cnes_infra.ingestion import DatasusCnesRawAdapter

    class _ControlPlane(ControlPlanePort, TypedRawQueryPort, Protocol):
        pass

logger = logging.getLogger(__name__)

AGENT_ID = "system-datasus"
AGENT_VERSION = "1.0.0"
SOURCE_TYPE = "CNES_NACIONAL"
FILE_SUBTYPE = "CNES_VINCULO"
NATIONAL_LEASE_SECONDS = 1800
_TERMINAL_STATES = (
    JobState.SUCCEEDED,
    JobState.FAILED_FINAL,
    JobState.CANCELED,
)


@dataclass(frozen=True, slots=True)
class NationalRefreshRequest:
    tenant_id: str
    competencia: str
    snapshot_id: str
    idempotency_key: str

    def __post_init__(self) -> None:
        values = (self.tenant_id, self.competencia, self.snapshot_id, self.idempotency_key)
        if not all(value and value.strip() for value in values):
            raise ValueError("blank_value")


class NationalIngestionService:
    """Ingere vínculos PF nacionais pelo mesmo caminho raw do Edge."""

    def __init__(
        self,
        control_plane: _ControlPlane,
        raw_adapter: DatasusCnesRawAdapter,
        raw_ingestion: RawIngestionService,
        clock: Callable[[], datetime],
    ) -> None:
        self._control_plane = control_plane
        self._raw_adapter = raw_adapter
        self._raw_ingestion = raw_ingestion
        self._clock = clock

    def refresh(self, request: NationalRefreshRequest) -> RawAcceptance:
        """Args: request: Competência e chave de idempotência do refresh.
        Returns: Aceite canônico devolvido pelo registro raw.
        Raises: Conflict: Agente revogado, job terminal inválido ou não reivindicável.
        """
        self._ensure_agent(request.tenant_id)
        job = self._ensure_job(request)
        replay = self._terminal_replay(job)
        if replay is not None:
            return replay
        manifest = self._raw_adapter.extract(
            DatasusCnesRequest(
                tenant_id=request.tenant_id,
                competencia=request.competencia,
                file_subtype=FILE_SUBTYPE,
                snapshot_id=request.snapshot_id,
                agent_id=AGENT_ID,
                agent_version=AGENT_VERSION,
            )
        )
        logger.info(
            "national_refresh_extracted tenant_id=%s competencia=%s job_id=%s rows=%d",
            request.tenant_id,
            request.competencia,
            job.job_id,
            manifest.row_count,
        )
        return self._raw_ingestion.register(
            RegisterRawManifest(
                tenant_id=request.tenant_id,
                agent_id=AGENT_ID,
                job_id=job.job_id,
                owner=AGENT_ID,
                fencing_token=job.fencing_token,
                manifest=manifest,
                manifest_bytes=_canonical_bytes(manifest),
                now=self._clock(),
            )
        )

    def _ensure_agent(self, tenant_id: str) -> Agent:
        agent = self._control_plane.get_agent(tenant_id, AGENT_ID)
        if agent is not None:
            if agent.state is AgentState.REVOKED:
                raise Conflict(ErrorCode.AGENT_REVOKED)
            return agent
        created = Agent(
            tenant_id=tenant_id,
            agent_id=AGENT_ID,
            state=AgentState.ACTIVE,
            version=AGENT_VERSION,
            certificate_fingerprint=_agent_fingerprint(tenant_id),
            last_seen_at=None,
            created_at=self._clock(),
        )
        self._control_plane.put_agent(created)
        return created

    def _ensure_job(self, request: NationalRefreshRequest) -> Job:
        job_id = _job_id(request)
        job = self._control_plane.get_job(request.tenant_id, job_id)
        if job is None:
            job = self._create_job(request, job_id)
        if job.state in _TERMINAL_STATES:
            return job
        return self._claim(job)

    def _create_job(self, request: NationalRefreshRequest, job_id: str) -> Job:
        now = self._clock()
        job = Job(
            tenant_id=request.tenant_id,
            job_id=job_id,
            agent_id=AGENT_ID,
            source_type=SOURCE_TYPE,
            file_subtype=FILE_SUBTYPE,
            competencia=request.competencia,
            requested_snapshot_mode="FULL",
            state=JobState.PENDING,
            attempt=0,
            fencing_token=0,
            lease_owner=None,
            lease_until=None,
            result_manifest_id=None,
            result_manifest_key=None,
            error_code=None,
            created_at=now,
        )
        event = OutboxEvent(
            tenant_id=request.tenant_id,
            event_id=_event_id(request.tenant_id, job_id),
            event_type="job.created",
            aggregate_id=job_id,
            payload={"job_id": job_id, "source_type": SOURCE_TYPE},
            created_at=now,
            delivered_at=None,
        )
        try:
            return self._control_plane.create_job(job, event)
        except Conflict:
            existing = self._control_plane.get_job(request.tenant_id, job_id)
            if existing is None:
                raise
            return existing

    def _terminal_replay(self, job: Job) -> RawAcceptance | None:
        if job.state not in _TERMINAL_STATES:
            return None
        if job.state is not JobState.SUCCEEDED or job.result_manifest_id is None:
            raise Conflict(ErrorCode.JOB_TERMINAL_CONFLICT)
        record = self._control_plane.query_raw_manifest_by_id(
            RawManifestByIdQuery(job.tenant_id, job.result_manifest_id)
        )
        if record is None:
            raise Conflict(ErrorCode.JOB_TERMINAL_CONFLICT)
        logger.info(
            "national_refresh_replayed tenant_id=%s job_id=%s manifest_id=%s",
            job.tenant_id,
            job.job_id,
            record.manifest_id,
        )
        return RawAcceptance(
            accepted=True,
            manifest_id=record.manifest_id,
            manifest_sha256=record.manifest_sha256,
            full_resync_required=False,
            reason=None,
        )

    def _claim(self, job: Job) -> Job:
        claimed = self._control_plane.claim_job(
            ClaimJob(
                tenant_id=job.tenant_id,
                job_id=job.job_id,
                owner=AGENT_ID,
                now=self._clock(),
                lease_seconds=NATIONAL_LEASE_SECONDS,
            )
        )
        if claimed is None:
            raise Conflict(ErrorCode.JOB_STATE_CONFLICT)
        return claimed


def _canonical_bytes(manifest: object) -> bytes:
    return manifest.model_dump_json(exclude_none=False, by_alias=False).encode()


def _agent_fingerprint(tenant_id: str) -> str:
    return sha256(f"{AGENT_ID}\x1f{tenant_id}".encode()).hexdigest()


def _job_id(request: NationalRefreshRequest) -> str:
    parts = (
        request.tenant_id,
        SOURCE_TYPE,
        FILE_SUBTYPE,
        request.competencia,
        request.idempotency_key,
    )
    return sha256("\x1f".join(parts).encode()).hexdigest()


def _event_id(tenant_id: str, job_id: str) -> str:
    return sha256(f"job.created\x1f{tenant_id}\x1f{job_id}".encode()).hexdigest()
