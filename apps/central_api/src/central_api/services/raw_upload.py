"""Upload streaming e imutável de objetos raw."""

from __future__ import annotations

import re
from asyncio import to_thread
from dataclasses import dataclass
from hashlib import sha256
from tempfile import SpooledTemporaryFile
from typing import TYPE_CHECKING

from cnes_domain.control_plane.enums import JobState
from cnes_domain.control_plane.errors import Conflict

if TYPE_CHECKING:
    from collections.abc import AsyncIterable, Callable
    from datetime import datetime

    from cnes_domain.control_plane.entities import Job
    from cnes_domain.ports.control_plane import ControlPlanePort
    from cnes_domain.ports.object_store import ObjectStat, ObjectStorePort

RAW_UPLOAD_MAX_BYTES = 1024**3
RAW_UPLOAD_SPOOL_BYTES = 8 * 1024**2
_SAFE_SEGMENT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


class RawUploadError(RuntimeError):
    """Falha estável do upload raw."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class RawUploadNotFound(RawUploadError):
    """Job de upload inexistente."""


class RawUploadIdentityRejected(RawUploadError):
    """Identidade do job incompatível."""


class RawUploadLeaseRejected(RawUploadError):
    """Lease do job incompatível."""


class RawUploadFenceRejected(RawUploadError):
    """Fence do job incompatível."""


class RawUploadKeyRejected(RawUploadError):
    """Chave do objeto incompatível."""


class RawUploadTooLarge(RawUploadError):
    """Payload maior que o limite raw."""


class RawUploadEmpty(RawUploadError):
    """Payload raw vazio."""


class RawUploadConflict(RawUploadError):
    """Replay divergente de objeto imutável."""


@dataclass(frozen=True, slots=True)
class RawUploadRequest:
    """Comando imutável do upload raw autenticado."""

    tenant_id: str
    agent_id: str
    job_id: str
    fencing_token: int
    object_key: str


class RawUploadService:
    """Valida, spoola e publica um objeto raw imutável."""

    def __init__(
        self,
        control_plane: ControlPlanePort,
        object_store: ObjectStorePort,
        clock: Callable[[], datetime],
    ) -> None:
        self._control_plane = control_plane
        self._object_store = object_store
        self._clock = clock

    async def upload(
        self, request: RawUploadRequest, stream: AsyncIterable[bytes]
    ) -> ObjectStat:
        """Publica o stream após validar identidade, lease, fence e chave."""

        job = self._validate_request(request)
        existing = await to_thread(self._object_store.stat, request.object_key)
        self._validate_state(job, existing is not None)
        with SpooledTemporaryFile(max_size=RAW_UPLOAD_SPOOL_BYTES, mode="w+b") as spool:
            digest, size = await self._spool(stream, spool)
            if size == 0:
                raise RawUploadEmpty("payload_empty")
            existing = existing or await to_thread(self._object_store.stat, request.object_key)
            if existing is not None:
                self._validate_state(self._validate_request(request), True)
                return self._validate_replay(existing, digest, size)
            spool.seek(0)
            self._validate_state(self._validate_request(request), False)
            try:
                return await to_thread(self._object_store.put, request.object_key, spool, digest)
            except Conflict:
                return await self._resolve_publish_race(request, digest, size)

    def _validate_request(self, request: RawUploadRequest) -> Job:
        job = self._control_plane.get_job(request.tenant_id, request.job_id)
        if job is None:
            raise RawUploadNotFound("job_missing")
        if (job.tenant_id, job.agent_id) != (request.tenant_id, request.agent_id):
            raise RawUploadIdentityRejected("job_identity_mismatch")
        if job.fencing_token != request.fencing_token:
            raise RawUploadFenceRejected("job_fence_rejected")
        if not _valid_object_key(request.object_key, job):
            raise RawUploadKeyRejected("object_key_invalid")
        return job

    def _validate_state(self, job: Job, terminal_replay: bool) -> None:
        is_resync = job.state is JobState.FAILED_FINAL and bool(
            job.error_code and job.error_code.startswith("RAW_RESYNC_")
        )
        if terminal_replay and (job.state is JobState.SUCCEEDED or is_resync):
            return
        if job.state is not JobState.LEASED:
            raise RawUploadLeaseRejected("job_not_leased")
        if job.lease_owner != job.agent_id:
            raise RawUploadLeaseRejected("job_owner_lost")
        if job.lease_until is None or job.lease_until <= self._clock():
            raise RawUploadLeaseRejected("job_lease_expired")

    @staticmethod
    async def _spool(stream: AsyncIterable[bytes], spool) -> tuple[str, int]:
        digest = sha256()
        size = 0
        async for chunk in stream:
            size += len(chunk)
            if size > RAW_UPLOAD_MAX_BYTES:
                raise RawUploadTooLarge("payload_too_large")
            digest.update(chunk)
            await to_thread(spool.write, chunk)
        return digest.hexdigest(), size

    @staticmethod
    def _validate_replay(existing: ObjectStat, digest: str, size: int) -> ObjectStat:
        if (existing.size_bytes, existing.sha256) != (size, digest):
            raise RawUploadConflict("object_conflict")
        return existing

    async def _resolve_publish_race(
        self, request: RawUploadRequest, digest: str, size: int
    ) -> ObjectStat:
        existing = await to_thread(self._object_store.stat, request.object_key)
        if existing is None:
            raise RawUploadConflict("object_conflict")
        return self._validate_replay(existing, digest, size)


def _valid_object_key(key: str, job: Job) -> bool:
    parts = key.split("/")
    expected = ("raw", job.tenant_id, job.source_type, job.competencia)
    return (
        len(parts) == 6
        and tuple(parts[:4]) == expected
        and bool(_SAFE_SEGMENT.fullmatch(parts[4]))
        and parts[4] not in {".", ".."}
        and parts[5] == "data.parquet"
    )
