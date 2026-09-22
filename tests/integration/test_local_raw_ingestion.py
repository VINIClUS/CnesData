"""Vertical raw local Phase 3 sobre SQLite e filesystem."""

from __future__ import annotations

import os
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from typing import TYPE_CHECKING, Any
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from central_api.app import create_app
from central_api.routes.raw_jobs import (
    get_control_plane,
    get_edge_identity,
    get_raw_ingestion_service,
    get_raw_upload_service,
)
from central_api.schemas.raw_api import EdgeIdentity
from cnes_contracts import RawManifest, SnapshotMode, SourceType, manifest_sha256
from cnes_domain.control_plane.entities import Agent, Job, OutboxEvent
from cnes_domain.control_plane.enums import AgentState, JobState
from cnes_domain.control_plane.queries import RawIdentity, RawManifestChainQuery

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

pytestmark = pytest.mark.local_profile

TENANT = "354130"
AGENT = "agent-local"
FINGERPRINT = "a" * 64
IDENTITY = EdgeIdentity(
    tenant_id=TENANT, agent_id=AGENT, certificate_fingerprint=FINGERPRINT
)
SCHEMA_VERSION = "cnes-profissional-v1"


@dataclass(frozen=True, slots=True)
class Stack:
    app: Any
    client: TestClient

    @property
    def control_plane(self) -> Any:
        return self.app.state.control_plane

    @property
    def object_store(self) -> Any:
        return self.app.state.object_store


@contextmanager
def local_stack(data_dir: Path) -> Iterator[Stack]:
    env = {"PROFILE": "local", "TENANT_ID": TENANT, "DATA_DIR": str(data_dir)}
    with patch.dict(os.environ, env):
        app = create_app()
        app.dependency_overrides[get_edge_identity] = lambda: IDENTITY
        with TestClient(app) as client:
            yield Stack(app, client)


def now() -> datetime:
    return datetime.now(UTC)


def seed_agent(stack: Stack) -> None:
    stack.control_plane.put_agent(
        Agent(
            tenant_id=TENANT,
            agent_id=AGENT,
            state=AgentState.ACTIVE,
            version="1.0.0",
            certificate_fingerprint=FINGERPRINT,
            last_seen_at=None,
            created_at=now(),
        )
    )


def seed_job(
    stack: Stack,
    job_id: str,
    mode: SnapshotMode,
    competencia: str = "2026-01",
) -> Job:
    moment = now()
    job = Job(
        tenant_id=TENANT,
        job_id=job_id,
        agent_id=AGENT,
        source_type=SourceType.CNES_LOCAL.value,
        file_subtype="CNES_VINCULO",
        competencia=competencia,
        requested_snapshot_mode=mode.value,
        state=JobState.PENDING,
        attempt=0,
        fencing_token=0,
        lease_owner=None,
        lease_until=None,
        result_manifest_id=None,
        result_manifest_key=None,
        error_code=None,
        created_at=moment,
    )
    event = OutboxEvent(
        tenant_id=TENANT,
        event_id=sha256(f"job.created\x1f{job_id}".encode()).hexdigest(),
        event_type="job.created",
        aggregate_id=job_id,
        payload={"job_id": job_id},
        created_at=moment,
        delivered_at=None,
    )
    return stack.control_plane.create_job(job, event)


def claim(stack: Stack) -> dict[str, Any]:
    response = stack.client.get("/api/v1/edge/jobs/next")
    assert response.status_code == 200, response.text
    return response.json()


def data_key(competencia: str, snapshot_id: str) -> str:
    return f"raw/{TENANT}/CNES_LOCAL/{competencia}/{snapshot_id}/data.parquet"


def manifest_key(competencia: str, snapshot_id: str) -> str:
    return data_key(competencia, snapshot_id).removesuffix("data.parquet") + "manifest.json"


def upload(stack: Stack, job: dict[str, Any], body: bytes, snapshot_id: str) -> Any:
    return stack.client.put(
        f"/api/v1/edge/jobs/{job['job_id']}/raw-object",
        content=body,
        headers={
            "X-Fencing-Token": str(job["fencing_token"]),
            "X-Object-Key": data_key(job["competencia"], snapshot_id),
            "Content-Type": "application/octet-stream",
        },
    )


def build_manifest(
    job: dict[str, Any],
    body: bytes,
    snapshot_id: str,
    previous: RawManifest | None = None,
) -> RawManifest:
    delta = previous is not None
    return RawManifest(
        manifest_version=1,
        manifest_id=snapshot_id,
        tenant_id=TENANT,
        source_type=SourceType.CNES_LOCAL,
        file_subtype="CNES_VINCULO",
        competencia=job["competencia"],
        agent_id=AGENT,
        agent_version="1.0.0",
        schema_version=SCHEMA_VERSION,
        snapshot_mode=SnapshotMode.DELTA if delta else SnapshotMode.FULL,
        snapshot_id=snapshot_id,
        base_snapshot_id=previous.snapshot_id if delta else None,
        sequence=previous.sequence + 1 if delta else 1,
        previous_manifest_sha256=manifest_sha256(previous) if delta else None,
        object_sha256=sha256(body).hexdigest(),
        row_count=1,
        size_bytes=len(body),
        object_key=data_key(job["competencia"], snapshot_id),
        created_at=now(),
    )


def register(stack: Stack, job: dict[str, Any], raw: RawManifest) -> Any:
    return stack.client.post(
        "/api/v1/edge/raw-manifests",
        json={
            "job_id": job["job_id"],
            "fencing_token": job["fencing_token"],
            "manifest": raw.model_dump(mode="json"),
        },
    )


def deliver_full(
    stack: Stack, job_id: str, snapshot_id: str, body: bytes, competencia: str = "2026-01"
) -> tuple[RawManifest, Any]:
    seed_job(stack, job_id, SnapshotMode.FULL, competencia)
    claimed = claim(stack)
    assert upload(stack, claimed, body, snapshot_id).status_code == 200
    raw = build_manifest(claimed, body, snapshot_id)
    return raw, register(stack, claimed, raw)


def chain(stack: Stack, competencia: str = "2026-01") -> tuple[Any, ...]:
    identity = RawIdentity(TENANT, SourceType.CNES_LOCAL.value, "CNES_VINCULO", competencia)
    return stack.control_plane.query_raw_manifest_chain(RawManifestChainQuery(identity))


def outbox_types(stack: Stack) -> list[str]:
    return [event.event_type for event in stack.control_plane.pending_outbox(100)]


def test_full_e_delta_chegam_ao_raw_sem_mudar_dataset_ativo(tmp_path: Path) -> None:
    with local_stack(tmp_path) as stack:
        seed_agent(stack)
        full, accepted = deliver_full(stack, "job-full", "base", b"full-payload")
        assert accepted.status_code == 200
        assert accepted.json()["accepted"] is True

        seed_job(stack, "job-delta", SnapshotMode.DELTA)
        claimed = claim(stack)
        assert upload(stack, claimed, b"delta-payload", "delta-1").status_code == 200
        delta = build_manifest(claimed, b"delta-payload", "delta-1", previous=full)
        response = register(stack, claimed, delta)

        assert response.status_code == 200
        assert response.json()["accepted"] is True
        for snapshot_id in ("base", "delta-1"):
            assert stack.object_store.stat(data_key("2026-01", snapshot_id)) is not None
            assert stack.object_store.stat(manifest_key("2026-01", snapshot_id)) is not None
        assert [ref.manifest_id for ref in chain(stack)] == ["base", "delta-1"]
        assert stack.control_plane.get_dataset_pointer(TENANT, "cnes") is None


def test_restart_entre_upload_e_replay_converge_sem_duplicar_eventos(tmp_path: Path) -> None:
    with local_stack(tmp_path) as stack:
        seed_agent(stack)
        seed_job(stack, "job-full", SnapshotMode.FULL)
        claimed = claim(stack)
        assert upload(stack, claimed, b"full-payload", "base").status_code == 200

    with local_stack(tmp_path) as restarted:
        raw = build_manifest(claimed, b"full-payload", "base")
        first = register(restarted, claimed, raw)
        second = register(restarted, claimed, raw)

        assert first.status_code == 200
        assert first.json() == second.json()
        assert [ref.manifest_id for ref in chain(restarted)] == ["base"]
        assert outbox_types(restarted).count("raw.manifest.accepted") == 1


def test_replay_divergente_do_objeto_imutavel_conflita_sem_sobrescrever(tmp_path: Path) -> None:
    with local_stack(tmp_path) as stack:
        seed_agent(stack)
        seed_job(stack, "job-full", SnapshotMode.FULL)
        claimed = claim(stack)
        assert upload(stack, claimed, b"full-payload", "base").status_code == 200

        response = upload(stack, claimed, b"outro-payload", "base")

        assert response.status_code == 409
        assert response.json() == {"detail": "object_conflict"}
        stored = stack.object_store.stat(data_key("2026-01", "base"))
        assert stored.sha256 == sha256(b"full-payload").hexdigest()


def test_resync_409_prepara_full_futuro_sem_corromper_fingerprints(tmp_path: Path) -> None:
    with local_stack(tmp_path) as stack:
        seed_agent(stack)
        full, _ = deliver_full(stack, "job-full", "base", b"full-payload")
        other, _ = deliver_full(
            stack, "job-other", "outro", b"outro-payload", competencia="2026-02"
        )

        seed_job(stack, "job-delta", SnapshotMode.DELTA)
        claimed = claim(stack)
        assert upload(stack, claimed, b"delta-payload", "delta-1").status_code == 200
        orphan = build_manifest(claimed, b"delta-payload", "delta-1", previous=full)
        orphan = orphan.model_copy(update={"base_snapshot_id": "desconhecido"})
        response = register(stack, claimed, orphan)

        assert response.status_code == 409
        body = response.json()
        assert body["full_resync_required"] is True
        assert body["reason"] == "BASE_UNKNOWN"
        rejected = stack.control_plane.get_job(TENANT, "job-delta")
        assert rejected.state is JobState.FAILED_FINAL
        assert rejected.error_code == "RAW_RESYNC_BASE_UNKNOWN"
        assert [ref.manifest_id for ref in chain(stack)] == ["base"]
        assert [ref.manifest_id for ref in chain(stack, "2026-02")] == [other.manifest_id]
        assert stack.control_plane.get_job(TENANT, "job-full").state is JobState.SUCCEEDED


def test_full_solicitado_reinicia_so_a_source_key(tmp_path: Path) -> None:
    with local_stack(tmp_path) as stack:
        seed_agent(stack)
        deliver_full(stack, "job-full", "base", b"full-payload")
        other, _ = deliver_full(
            stack, "job-other", "outro", b"outro-payload", competencia="2026-02"
        )

        _, accepted = deliver_full(stack, "job-resync", "base-2", b"resync-payload")

        assert accepted.status_code == 200
        assert accepted.json()["accepted"] is True
        assert [ref.manifest_id for ref in chain(stack)] == ["base-2"]
        assert [ref.manifest_id for ref in chain(stack, "2026-02")] == [other.manifest_id]


def test_profile_local_nao_constroi_postgres_minio_ou_gcp(tmp_path: Path) -> None:
    from cnes_infra.control_plane import SQLiteControlPlane
    from cnes_infra.object_store import FilesystemObjectStore

    def explode(*args: object, **kwargs: object) -> None:
        raise AssertionError("local_profile_built_legacy_backend")

    with patch("central_api.deps.create_engine", explode), patch(
        "central_api.deps.S3PresignedStorage", explode
    ), local_stack(tmp_path) as stack:
        seed_agent(stack)
        deliver_full(stack, "job-full", "base", b"full-payload")

        assert isinstance(stack.control_plane, SQLiteControlPlane)
        assert isinstance(stack.object_store, FilesystemObjectStore)
        assert not hasattr(stack.app.state, "dashboard_repo")


def test_rotas_recebem_as_instancias_de_app_state(tmp_path: Path) -> None:
    with local_stack(tmp_path) as stack:
        overrides = stack.app.dependency_overrides

        assert overrides[get_control_plane]() is stack.app.state.control_plane
        assert overrides[get_raw_upload_service]() is stack.app.state.raw_upload
        assert overrides[get_raw_ingestion_service]() is stack.app.state.raw_ingestion
        assert stack.app.state.raw_query is stack.app.state.control_plane
        assert stack.app.state.national_ingestion is not None
