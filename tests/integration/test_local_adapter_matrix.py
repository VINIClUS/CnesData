"""Matriz de conformidade dos adapters locais Phase 2."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from io import BytesIO
from typing import TYPE_CHECKING, Any

import polars as pl
import pytest

from cnes_domain.control_plane.entities import Job, OutboxEvent
from cnes_domain.control_plane.enums import JobState
from cnes_domain.outbox_dispatcher import DispatchResult, dispatch_once

if TYPE_CHECKING:
    from pathlib import Path

NOW = datetime(2026, 9, 6, 12, tzinfo=UTC)
DELIVERED_AT = NOW + timedelta(seconds=1)


def _job() -> Job:
    return Job(
        tenant_id="354130",
        job_id="job-local",
        agent_id="agent-local",
        source_type="CNES",
        file_subtype="ST",
        competencia="2026-09",
        requested_snapshot_mode="FULL",
        state=JobState.PENDING,
        attempt=0,
        fencing_token=0,
        lease_owner=None,
        lease_until=None,
        result_manifest_id=None,
        result_manifest_key=None,
        error_code=None,
        created_at=NOW,
    )


def _event() -> OutboxEvent:
    return OutboxEvent(
        tenant_id="354130",
        event_id="event-local",
        event_type="job.created",
        aggregate_id="job-local",
        payload={"job_id": "job-local"},
        created_at=NOW,
        delivered_at=None,
    )


class _InterruptedControlPlane:
    def __init__(self, control_plane: Any) -> None:
        self._control_plane = control_plane

    def pending_outbox(self, limit: int) -> tuple[OutboxEvent, ...]:
        return self._control_plane.pending_outbox(limit)

    def mark_outbox_delivered(self, event_id: str, delivered_at: datetime) -> None:
        raise OSError("dispatch=interrupted")


@pytest.mark.local_profile
def test_exports_publicos_sao_unicos_e_dispatcher_permanece_no_dominio() -> None:
    import cnes_infra
    from cnes_infra import audit, control_plane, object_store
    from cnes_infra.audit import LocalAuditSink, S3ObjectLockAuditSink
    from cnes_infra.control_plane import DynamoDBControlPlane, SQLiteControlPlane
    from cnes_infra.object_store import FilesystemObjectStore, S3ObjectStore, S3Retention

    assert cnes_infra.__all__ == ("audit", "control_plane", "object_store")
    assert control_plane.__all__ == ("DynamoDBControlPlane", "SQLiteControlPlane")
    assert object_store.__all__ == ("FilesystemObjectStore", "S3ObjectStore", "S3Retention")
    assert audit.__all__ == ("LocalAuditSink", "S3ObjectLockAuditSink")
    assert all(
        value is not None
        for value in (
            SQLiteControlPlane,
            DynamoDBControlPlane,
            FilesystemObjectStore,
            S3ObjectStore,
            S3Retention,
            LocalAuditSink,
            S3ObjectLockAuditSink,
        )
    )
    aliases = {
        "SQLiteControlPlane",
        "DynamoDBControlPlane",
        "FilesystemObjectStore",
        "S3ObjectStore",
        "S3Retention",
        "LocalAuditSink",
        "S3ObjectLockAuditSink",
        "dispatch_once",
    }
    assert aliases.isdisjoint(vars(cnes_infra))


@pytest.mark.local_profile
def test_reabre_adapters_e_conclui_replay_sem_duplicar_efeitos(tmp_path: Path) -> None:
    from cnes_infra.audit import LocalAuditSink
    from cnes_infra.control_plane import SQLiteControlPlane
    from cnes_infra.object_store import FilesystemObjectStore

    database_path = tmp_path / "control-plane.sqlite3"
    object_root = tmp_path / "objects"
    audit_root = tmp_path / "audit-root"
    object_root.mkdir()
    body = b"phase-2-local"
    digest = sha256(body).hexdigest()
    control_plane = SQLiteControlPlane(database_path, lambda: NOW)
    control_plane.initialize()
    control_plane.create_job(_job(), _event())
    store = FilesystemObjectStore(object_root)
    store.put("staging/job-local.parquet", BytesIO(body), digest)
    published = store.promote(
        "staging/job-local.parquet", "raw/354130/job-local.parquet", digest
    )
    sink = LocalAuditSink(audit_root, parquet_batch_size=1)

    first = dispatch_once(_InterruptedControlPlane(control_plane), sink, DELIVERED_AT)

    reopened_control_plane = SQLiteControlPlane(database_path, lambda: DELIVERED_AT)
    reopened_control_plane.initialize()
    reopened_store = FilesystemObjectStore(object_root)
    reopened_sink = LocalAuditSink(audit_root, parquet_batch_size=1)
    second = dispatch_once(reopened_control_plane, reopened_sink, DELIVERED_AT)
    replayed = reopened_store.put("raw/354130/job-local.parquet", BytesIO(body), digest)

    log_path = audit_root / "audit" / "354130" / "2026" / "09" / "06" / "events.jsonl"
    parquet_paths = tuple((audit_root / "audit").glob("*/*/*/*/batch-*.parquet"))
    with reopened_store.open("raw/354130/job-local.parquet") as stored:
        stored_body = stored.read()

    assert first == DispatchResult(delivered=0, failed=1)
    assert second == DispatchResult(delivered=1, failed=0)
    assert reopened_control_plane.pending_outbox(10) == ()
    assert published == replayed
    assert stored_body == body
    assert len(log_path.read_text(encoding="utf-8").splitlines()) == 1
    assert json.loads(log_path.read_text(encoding="utf-8"))["event_id"] == "event-local"
    assert len(parquet_paths) == 1
    assert pl.read_parquet(parquet_paths[0]).get_column("event_id").to_list() == ["event-local"]
