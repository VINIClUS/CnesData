"""Registro imutável de manifestos raw."""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from hashlib import sha256
from io import BytesIO
from itertools import pairwise
from typing import TYPE_CHECKING

from central_api.services.delta_policy import DeltaPolicy, ResyncReason, _DeltaContext
from cnes_contracts import RawManifest, SnapshotMode, manifest_sha256
from cnes_domain.control_plane.commands import CompleteJob, FailJob
from cnes_domain.control_plane.entities import Job, ManifestRef, OutboxEvent, RawManifestRecord
from cnes_domain.control_plane.enums import JobState
from cnes_domain.control_plane.errors import Conflict, FenceRejected, LeaseLost, NotFound
from cnes_domain.control_plane.errors import ControlPlaneErrorCode as ErrorCode
from cnes_domain.control_plane.queries import (
    AgentRawManifestChainQuery,
    LatestSucceededJobQuery,
    RawIdentity,
    RawManifestByIdQuery,
    RawResyncStateQuery,
)

if TYPE_CHECKING:
    from typing import Protocol

    from cnes_domain.ports.control_plane import ControlPlanePort, TypedRawQueryPort
    from cnes_domain.ports.object_store import ObjectStorePort

    class _ControlPlane(ControlPlanePort, TypedRawQueryPort, Protocol):
        pass

logger = logging.getLogger(__name__)
type AcceptedManifest = Callable[[RawManifestRecord], None]


def _noop(_: RawManifestRecord) -> None:
    return None


@dataclass(frozen=True, slots=True)
class RawAcceptance:
    accepted: bool
    manifest_id: str
    manifest_sha256: str
    full_resync_required: bool
    reason: ResyncReason | None


@dataclass(frozen=True, slots=True)
class _DeltaDecision:
    reason: ResyncReason | None
    expected_head_manifest_id: str | None
    marker_observed: bool


@dataclass(frozen=True, slots=True)
class RegisterRawManifest:
    tenant_id: str
    agent_id: str
    job_id: str
    owner: str
    fencing_token: int
    manifest: RawManifest
    manifest_bytes: bytes
    now: datetime

    def __post_init__(self) -> None:
        if not all((self.tenant_id, self.agent_id, self.job_id, self.owner)):
            raise ValueError("blank_value")
        if self.fencing_token < 0:
            raise ValueError("negative_counter")
        if self.now.tzinfo is None or self.now.utcoffset() != timedelta(0):
            raise ValueError("datetime_not_utc")


class RawIngestionService:
    def __init__(
        self,
        control_plane: _ControlPlane,
        object_store: ObjectStorePort,
        policy: DeltaPolicy,
        accepted_manifest: AcceptedManifest = _noop,
    ) -> None:
        self._control_plane = control_plane
        self._object_store = object_store
        self._policy = policy
        self._accepted_manifest = accepted_manifest

    def register(self, command: RegisterRawManifest) -> RawAcceptance:
        job = self._control_plane.get_job(command.tenant_id, command.job_id)
        if job is None:
            raise NotFound(ErrorCode.JOB_MISSING)
        digest = self._validate_identity_and_bytes(job, command)
        replay = self._terminal_replay(job, command, digest)
        if replay is not None:
            return replay
        self._validate_live_job(job, command)
        self._validate_data_object(command.manifest)
        record = _record(command.manifest, _manifest_key(command.manifest), digest)
        decision = self._delta_decision(command)
        if decision.reason is not None:
            return self._reject(command, digest, decision)
        return self._accept(command, record, decision.expected_head_manifest_id)

    @staticmethod
    def _validate_identity_and_bytes(job: Job, command: RegisterRawManifest) -> str:
        manifest = command.manifest
        expected = (
            job.tenant_id,
            job.agent_id,
            job.source_type,
            job.file_subtype,
            job.competencia,
            job.requested_snapshot_mode,
        )
        actual = (
            command.tenant_id,
            command.agent_id,
            manifest.source_type.value,
            manifest.file_subtype,
            manifest.competencia,
            manifest.snapshot_mode.value,
        )
        manifest_identity = (manifest.tenant_id, manifest.agent_id)
        if actual != expected or manifest_identity != (command.tenant_id, command.agent_id):
            raise Conflict(ErrorCode.MANIFEST_IDENTITY_CONFLICT)
        canonical = _canonical_bytes(manifest)
        if command.manifest_bytes != canonical:
            raise Conflict("manifest=noncanonical")
        return sha256(canonical).hexdigest()

    def _terminal_replay(
        self, job: Job, command: RegisterRawManifest, digest: str
    ) -> RawAcceptance | None:
        if job.state is JobState.SUCCEEDED:
            self._validate_accepted_replay(job, command, digest)
            return RawAcceptance(True, command.manifest.manifest_id, digest, False, None)
        if job.state is not JobState.FAILED_FINAL:
            return None
        prefix = "RAW_RESYNC_"
        if job.error_code is None or not job.error_code.startswith(prefix):
            return None
        try:
            reason = ResyncReason(job.error_code.removeprefix(prefix))
        except ValueError as error:
            raise Conflict("terminal_replay=conflict") from error
        if job.rejected_manifest_sha256 != digest:
            raise Conflict("terminal_replay=conflict")
        return RawAcceptance(False, command.manifest.manifest_id, digest, True, reason)

    def _validate_accepted_replay(
        self, job: Job, command: RegisterRawManifest, digest: str
    ) -> None:
        if job.result_manifest_id is None:
            raise Conflict("terminal_replay=conflict")
        query = RawManifestByIdQuery(job.tenant_id, job.result_manifest_id)
        record = self._control_plane.query_raw_manifest_by_id(query)
        if record is None or not _record_matches(command.manifest, record, digest):
            raise Conflict("terminal_replay=conflict")
        if job.result_manifest_key != record.manifest_key:
            raise Conflict("terminal_replay=conflict")
        self._validate_data_object(command.manifest, replay=True)
        self._validate_sidecar(record.manifest_key, command.manifest_bytes, digest)

    @staticmethod
    def _validate_live_job(job: Job, command: RegisterRawManifest) -> None:
        if job.state is not JobState.LEASED:
            raise LeaseLost(ErrorCode.JOB_NOT_LEASED)
        if job.lease_owner != command.owner:
            raise LeaseLost(ErrorCode.JOB_OWNER_LOST)
        if job.fencing_token != command.fencing_token:
            raise FenceRejected(ErrorCode.JOB_FENCE_REJECTED)
        if job.lease_until is None or job.lease_until <= command.now:
            raise LeaseLost(ErrorCode.JOB_LEASE_EXPIRED)

    def _validate_data_object(self, manifest: RawManifest, replay: bool = False) -> None:
        if manifest.object_key != _data_key(manifest):
            code = "terminal_replay=conflict" if replay else "object=divergent"
            raise Conflict(code)
        stat = self._object_store.stat(manifest.object_key)
        if stat is None:
            code = "terminal_replay=conflict" if replay else "object=missing"
            raise Conflict(code)
        expected = (manifest.object_key, manifest.size_bytes, manifest.object_sha256)
        if (stat.key, stat.size_bytes, stat.sha256) != expected:
            code = "terminal_replay=conflict" if replay else "object=divergent"
            raise Conflict(code)

    def _validate_sidecar(self, key: str, canonical: bytes, digest: str) -> None:
        stat = self._object_store.stat(key)
        if stat is None or (stat.key, stat.size_bytes, stat.sha256) != (
            key,
            len(canonical),
            digest,
        ):
            raise Conflict("terminal_replay=conflict")
        try:
            with self._object_store.open(key) as stream:
                if stream.read() != canonical:
                    raise Conflict("terminal_replay=conflict")
        except (FileNotFoundError, KeyError) as error:
            raise Conflict("terminal_replay=conflict") from error

    def _delta_decision(self, command: RegisterRawManifest) -> _DeltaDecision:
        if command.manifest.snapshot_mode is SnapshotMode.FULL:
            return _DeltaDecision(None, None, False)
        identity = _raw_identity(command.manifest)
        marker = self._control_plane.query_raw_resync_state(
            RawResyncStateQuery(identity, command.agent_id)
        )
        if marker is not None:
            reason = self._policy.evaluate(
                _DeltaContext(command.manifest, (), marker, command.now)
            )
            return _DeltaDecision(reason, None, True)
        latest = self._control_plane.query_latest_succeeded_job(
            LatestSucceededJobQuery(identity, command.agent_id)
        )
        if latest is None:
            reason = self._policy.evaluate(
                _DeltaContext(command.manifest, (), None, command.now)
            )
            return _DeltaDecision(reason, None, False)
        refs = self._control_plane.query_agent_raw_manifest_chain(
            AgentRawManifestChainQuery(identity, command.agent_id, limit=31)
        )
        latest_ref = None if not refs else refs[-1]
        if (
            latest_ref is None
            or latest_ref.manifest_id != latest.result_manifest_id
            or latest_ref.manifest_key != latest.result_manifest_key
        ):
            raise Conflict("raw_history=divergent")
        chain = self._load_chain(command, refs)
        reason = self._policy.evaluate(
            _DeltaContext(command.manifest, chain, None, command.now)
        )
        return _DeltaDecision(reason, latest_ref.manifest_id, False)

    def _load_chain(
        self, command: RegisterRawManifest, refs: tuple[ManifestRef, ...]
    ) -> tuple[RawManifest, ...]:
        manifests = []
        try:
            for reference in refs:
                query = RawManifestByIdQuery(command.tenant_id, reference.manifest_id)
                record = self._control_plane.query_raw_manifest_by_id(query)
                if record is None or record.manifest_key != reference.manifest_key:
                    raise ValueError("record_missing")
                with self._object_store.open(record.manifest_key) as stream:
                    payload = stream.read()
                historical = RawManifest.model_validate_json(payload)
                digest = manifest_sha256(historical)
                if payload != _canonical_bytes(historical):
                    raise ValueError("sidecar_noncanonical")
                if not _record_matches(historical, record, digest):
                    raise ValueError("record_divergent")
                manifests.append(historical)
        except Exception as error:
            raise Conflict("raw_history=divergent") from error
        chain = tuple(manifests)
        if not _valid_chain(chain, command.agent_id, command.manifest):
            raise Conflict("raw_history=divergent")
        return chain

    def _reject(
        self, command: RegisterRawManifest, digest: str, decision: _DeltaDecision
    ) -> RawAcceptance:
        reason = decision.reason
        if reason is None:
            raise ValueError("resync_reason_required")
        error_code = f"RAW_RESYNC_{reason.value}"
        failure = FailJob(
            tenant_id=command.tenant_id,
            job_id=command.job_id,
            owner=command.owner,
            fencing_token=command.fencing_token,
            error_code=error_code,
            retryable=False,
            rejected_manifest_sha256=digest,
            expected_head_manifest_id=decision.expected_head_manifest_id,
            expected_resync_marker=decision.marker_observed,
        )
        event = _event(command, digest, "raw.manifest.resync_required", reason=reason)
        self._control_plane.fail_job(failure, event)
        return RawAcceptance(False, command.manifest.manifest_id, digest, True, reason)

    def _accept(
        self, command: RegisterRawManifest, record: RawManifestRecord,
        expected_head_manifest_id: str | None,
    ) -> RawAcceptance:
        digest = record.manifest_sha256
        self._object_store.put(record.manifest_key, BytesIO(command.manifest_bytes), digest)
        completion = CompleteJob(
            tenant_id=command.tenant_id,
            job_id=command.job_id,
            owner=command.owner,
            fencing_token=command.fencing_token,
            manifest=record,
            expected_head_manifest_id=expected_head_manifest_id,
        )
        event = _event(
            command, digest, "raw.manifest.accepted", manifest_key=record.manifest_key
        )
        self._control_plane.complete_job(completion, event)
        try:
            self._accepted_manifest(record)
        except Exception as error:
            logger.error(
                "accepted_manifest_callback_failed tenant_id=%s job_id=%s manifest_id=%s "
                "exception_type=%s",
                command.tenant_id,
                command.job_id,
                command.manifest.manifest_id,
                type(error).__name__,
            )
        return RawAcceptance(True, command.manifest.manifest_id, digest, False, None)


def _canonical_bytes(manifest: RawManifest) -> bytes:
    return manifest.model_dump_json(exclude_none=False, by_alias=False).encode()


def _manifest_key(manifest: RawManifest) -> str:
    return _data_key(manifest).removesuffix("data.parquet") + "manifest.json"


def _data_key(manifest: RawManifest) -> str:
    return (
        f"raw/{manifest.tenant_id}/{manifest.source_type.value}/{manifest.competencia}/"
        f"{manifest.snapshot_id}/data.parquet"
    )


def _raw_identity(manifest: RawManifest) -> RawIdentity:
    return RawIdentity(
        manifest.tenant_id,
        manifest.source_type.value,
        manifest.file_subtype,
        manifest.competencia,
    )


def _record(manifest: RawManifest, key: str, digest: str) -> RawManifestRecord:
    return RawManifestRecord(
        tenant_id=manifest.tenant_id,
        manifest_id=manifest.manifest_id,
        manifest_key=key,
        agent_id=manifest.agent_id,
        source_type=manifest.source_type.value,
        file_subtype=manifest.file_subtype,
        competencia=manifest.competencia,
        snapshot_mode=manifest.snapshot_mode.value,
        snapshot_id=manifest.snapshot_id,
        base_snapshot_id=manifest.base_snapshot_id,
        sequence=manifest.sequence,
        previous_manifest_sha256=manifest.previous_manifest_sha256,
        manifest_sha256=digest,
        created_at=manifest.created_at,
    )


def _record_matches(manifest: RawManifest, record: RawManifestRecord, digest: str) -> bool:
    return record == _record(manifest, _manifest_key(manifest), digest)


def _valid_chain(
    chain: tuple[RawManifest, ...], agent_id: str, expected: RawManifest
) -> bool:
    if (
        not chain
        or chain[0].snapshot_mode is not SnapshotMode.FULL
        or _raw_identity(chain[0]) != _raw_identity(expected)
    ):
        return False
    for previous, current in pairwise(chain):
        if not _valid_link(previous, current, chain[0]):
            return False
    return chain[0].agent_id == agent_id


def _valid_link(
    previous: RawManifest,
    current: RawManifest,
    base: RawManifest,
) -> bool:
    return (
        current.snapshot_mode is SnapshotMode.DELTA
        and _raw_identity(current) == _raw_identity(base)
        and current.agent_id == base.agent_id
        and current.schema_version == base.schema_version
        and current.base_snapshot_id == base.snapshot_id
        and current.sequence == previous.sequence + 1
        and current.previous_manifest_sha256 == manifest_sha256(previous)
    )


def _event(
    command: RegisterRawManifest,
    digest: str,
    event_type: str,
    **extra: str | ResyncReason,
) -> OutboxEvent:
    manifest = command.manifest
    identity = "\x1f".join((event_type, command.tenant_id, command.job_id, digest))
    payload = {
        "job_id": command.job_id,
        "agent_id": command.agent_id,
        "manifest_id": manifest.manifest_id,
        "manifest_sha256": digest,
        "source_type": manifest.source_type.value,
        "file_subtype": manifest.file_subtype,
        "competencia": manifest.competencia,
        "snapshot_mode": manifest.snapshot_mode.value,
        **{key: value.value if isinstance(value, ResyncReason) else value
           for key, value in extra.items()},
    }
    return OutboxEvent(
        tenant_id=command.tenant_id,
        event_id=sha256(identity.encode()).hexdigest(),
        event_type=event_type,
        aggregate_id=command.job_id,
        payload=payload,
        created_at=command.now,
        delivered_at=None,
    )
