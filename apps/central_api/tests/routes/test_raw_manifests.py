from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from io import BytesIO

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import ValidationError

from central_api.routes import raw_manifests
from central_api.routes.raw_jobs import (
    get_control_plane,
    get_edge_identity,
    get_raw_ingestion_service,
)
from central_api.routes.raw_manifests import router
from central_api.schemas.raw_api import EdgeIdentity, RawManifestSubmission
from central_api.services.delta_policy import DeltaPolicy, ResyncReason
from central_api.services.raw_ingestion import RawIngestionService
from cnes_contracts import RawManifest, SnapshotMode, SourceType, manifest_sha256
from cnes_domain.control_plane.entities import (
    Agent,
    Job,
    ManifestRef,
    RawManifestRecord,
    RawResyncState,
)
from cnes_domain.control_plane.enums import AgentState, JobState
from cnes_domain.ports.object_store import ObjectStat

NOW = datetime(2026, 7, 15, 12, tzinfo=UTC)
FINGERPRINT = sha256(b"certificate").hexdigest()
DATA = b"parquet"


def raw_manifest(**updates: object) -> RawManifest:
    values = {
        "manifest_version": 1,
        "manifest_id": "manifest-current",
        "tenant_id": "354130",
        "source_type": SourceType.CNES_LOCAL,
        "file_subtype": "CNES_VINCULO",
        "competencia": "2026-07",
        "agent_id": "agent-1",
        "agent_version": "1.0",
        "schema_version": "v1",
        "snapshot_mode": SnapshotMode.FULL,
        "snapshot_id": "current",
        "base_snapshot_id": None,
        "sequence": 1,
        "previous_manifest_sha256": None,
        "object_sha256": sha256(DATA).hexdigest(),
        "row_count": 1,
        "size_bytes": len(DATA),
        "object_key": "raw/354130/CNES_LOCAL/2026-07/current/data.parquet",
        "created_at": NOW,
    }
    return RawManifest(**(values | updates))


def job(mode: SnapshotMode = SnapshotMode.FULL) -> Job:
    return Job(
        tenant_id="354130",
        job_id="job-1",
        agent_id="agent-1",
        source_type="CNES_LOCAL",
        file_subtype="CNES_VINCULO",
        competencia="2026-07",
        requested_snapshot_mode=mode.value,
        state=JobState.LEASED,
        attempt=1,
        fencing_token=7,
        lease_owner="agent-1",
        lease_until=NOW + timedelta(minutes=5),
        result_manifest_id=None,
        result_manifest_key=None,
        error_code=None,
        created_at=NOW,
    )


def record(raw: RawManifest) -> RawManifestRecord:
    return RawManifestRecord(
        tenant_id=raw.tenant_id,
        manifest_id=raw.manifest_id,
        manifest_key=raw.object_key.removesuffix("data.parquet") + "manifest.json",
        agent_id=raw.agent_id,
        source_type=raw.source_type.value,
        file_subtype=raw.file_subtype,
        competencia=raw.competencia,
        snapshot_mode=raw.snapshot_mode.value,
        snapshot_id=raw.snapshot_id,
        base_snapshot_id=raw.base_snapshot_id,
        sequence=raw.sequence,
        previous_manifest_sha256=raw.previous_manifest_sha256,
        manifest_sha256=manifest_sha256(raw),
        created_at=raw.created_at,
    )


class ObjectStore:
    def __init__(self, current: RawManifest, history: tuple[RawManifest, ...] = ()) -> None:
        self.objects = {current.object_key: DATA}
        for item in history:
            self.objects[record(item).manifest_key] = canonical(item)

    def stat(self, key: str) -> ObjectStat | None:
        body = self.objects.get(key)
        if body is None:
            return None
        return ObjectStat(key, len(body), sha256(body).hexdigest())

    def put(self, key: str, body, expected_sha256: str) -> ObjectStat:
        value = body.read()
        current = self.objects.setdefault(key, value)
        if current != value:
            raise RuntimeError("object=immutable")
        return ObjectStat(key, len(value), expected_sha256)

    @contextmanager
    def open(self, key: str):
        yield BytesIO(self.objects[key])


class ControlPlane:
    def __init__(self, current: Job) -> None:
        self.job = current
        self.agent = Agent(
            tenant_id="354130",
            agent_id="agent-1",
            state=AgentState.ACTIVE,
            version="1.0",
            certificate_fingerprint=FINGERPRINT,
            last_seen_at=NOW,
            created_at=NOW,
        )
        self.marker = None
        self.latest = None
        self.chain: tuple[ManifestRef, ...] = ()
        self.records: dict[str, RawManifestRecord] = {}

    def get_agent(self, tenant_id: str, agent_id: str) -> Agent | None:
        return self.agent

    def get_job(self, tenant_id: str, job_id: str) -> Job | None:
        return self.job if (tenant_id, job_id) == (self.job.tenant_id, self.job.job_id) else None

    def query_raw_resync_state(self, _query):
        return self.marker

    def query_latest_succeeded_job(self, _query):
        return self.latest

    def query_agent_raw_manifest_chain(self, _query):
        return self.chain

    def query_raw_manifest_by_id(self, query):
        return self.records.get(query.manifest_id)

    def fail_job(self, command, _event):
        return self.job.model_copy(update={
            "state": JobState.FAILED_FINAL,
            "lease_owner": None,
            "lease_until": None,
            "error_code": command.error_code,
            "rejected_manifest_sha256": command.rejected_manifest_sha256,
        })

    def complete_job(self, command, _event):
        return self.job.model_copy(update={
            "state": JobState.SUCCEEDED,
            "lease_owner": None,
            "lease_until": None,
            "result_manifest_id": command.manifest.manifest_id,
            "result_manifest_key": command.manifest.manifest_key,
        })


def canonical(raw: RawManifest) -> bytes:
    return raw.model_dump_json(exclude_none=False, by_alias=False).encode()


def api_client(control: ControlPlane, store: ObjectStore) -> TestClient:
    app = FastAPI()
    app.include_router(router)
    identity = EdgeIdentity(
        tenant_id="354130", agent_id="agent-1", certificate_fingerprint=FINGERPRINT
    )
    service = RawIngestionService(control, store, DeltaPolicy())
    app.dependency_overrides[get_edge_identity] = lambda: identity
    app.dependency_overrides[get_control_plane] = lambda: control
    app.dependency_overrides[get_raw_ingestion_service] = lambda: service
    return TestClient(app)


def submission(raw: RawManifest) -> dict[str, object]:
    return {"job_id": "job-1", "fencing_token": 7, "manifest": raw.model_dump(mode="json")}


def test_schema_normaliza_objeto_json_preservando_contrato_estrito() -> None:
    raw = raw_manifest()

    parsed = RawManifestSubmission.model_validate(submission(raw))

    assert parsed.manifest == raw
    with pytest.raises(ValidationError):
        parsed.job_id = "other"


def test_manifesto_rejeita_tenant_do_corpo_divergente() -> None:
    raw = raw_manifest().model_copy(update={
        "tenant_id": "other",
        "object_key": "raw/other/CNES_LOCAL/2026-07/current/data.parquet",
    })
    control = ControlPlane(job())
    store = ObjectStore(raw)

    response = api_client(control, store).post("/api/v1/edge/raw-manifests", json=submission(raw))

    assert response.status_code == 409
    assert response.json() == {"detail": "manifest_identity_conflict"}


def test_manifesto_full_aceito_retorna_resposta_tipada(monkeypatch) -> None:
    monkeypatch.setattr(raw_manifests, "_utc_now", lambda: NOW)
    raw = raw_manifest()
    response = api_client(ControlPlane(job()), ObjectStore(raw)).post(
        "/api/v1/edge/raw-manifests", json=submission(raw)
    )

    assert response.status_code == 200
    assert response.json() == {
        "accepted": True,
        "manifest_id": raw.manifest_id,
        "manifest_sha256": manifest_sha256(raw),
        "full_resync_required": False,
        "reason": None,
    }


def test_manifesto_distingue_job_ausente(monkeypatch) -> None:
    monkeypatch.setattr(raw_manifests, "_utc_now", lambda: NOW)
    raw = raw_manifest()
    control = ControlPlane(job())
    control.job = job().model_copy(update={"job_id": "other"})

    response = api_client(control, ObjectStore(raw)).post(
        "/api/v1/edge/raw-manifests", json=submission(raw)
    )

    assert response.status_code == 404
    assert response.json() == {"detail": "job_missing"}


def delta_from(previous: RawManifest, sequence: int, **updates: object) -> RawManifest:
    values = {
        "manifest_id": f"manifest-{sequence}",
        "snapshot_mode": SnapshotMode.DELTA,
        "snapshot_id": f"delta-{sequence}",
        "base_snapshot_id": "base",
        "sequence": sequence,
        "previous_manifest_sha256": manifest_sha256(previous),
        "object_key": f"raw/354130/CNES_LOCAL/2026-07/delta-{sequence}/data.parquet",
    }
    return raw_manifest(**(values | updates))


def configure_resync(reason: ResyncReason):
    base = raw_manifest(
        manifest_id="manifest-base", snapshot_id="base",
        object_key="raw/354130/CNES_LOCAL/2026-07/base/data.parquet",
    )
    history = [base]
    current = delta_from(base, 2)
    control = ControlPlane(job(SnapshotMode.DELTA))
    if reason is ResyncReason.AGENT_RESYNC_REQUIRED:
        control.marker = RawResyncState(
            tenant_id="354130", agent_id="agent-1", source_type="CNES_LOCAL",
            file_subtype="CNES_VINCULO", competencia="2026-07", required_since=NOW,
        )
        return current, control, tuple(history)
    if reason is ResyncReason.BASE_UNKNOWN:
        return current, control, tuple(history)
    if reason is ResyncReason.SEQUENCE_GAP:
        current = delta_from(base, 3)
    elif reason is ResyncReason.HASH_CHAIN_MISMATCH:
        current = delta_from(base, 2, previous_manifest_sha256="b" * 64)
    elif reason is ResyncReason.SCHEMA_INCOMPATIBLE:
        current = delta_from(base, 2, schema_version="v2")
    elif reason is ResyncReason.BASE_TOO_OLD:
        base = base.model_copy(update={"created_at": NOW - timedelta(days=8)})
        history = [base]
        current = delta_from(base, 2)
    else:
        for sequence in range(2, 32):
            history.append(delta_from(history[-1], sequence))
        current = delta_from(history[-1], 32)
    refs = tuple(
        ManifestRef(
            manifest_id=item.manifest_id,
            manifest_key=record(item).manifest_key,
        )
        for item in history
    )
    control.records = {item.manifest_id: record(item) for item in history}
    control.chain = refs
    head = history[-1]
    control.latest = job().model_copy(update={
        "state": JobState.SUCCEEDED,
        "lease_owner": None,
        "lease_until": None,
        "result_manifest_id": head.manifest_id,
        "result_manifest_key": record(head).manifest_key,
    })
    return current, control, tuple(history)


@pytest.mark.parametrize("reason", list(ResyncReason))
def test_resync_retorna_409_com_resposta_tipada(monkeypatch, reason: ResyncReason) -> None:
    monkeypatch.setattr(raw_manifests, "_utc_now", lambda: NOW)
    raw, control, history = configure_resync(reason)
    store = ObjectStore(raw, history)

    response = api_client(control, store).post("/api/v1/edge/raw-manifests", json=submission(raw))

    digest = manifest_sha256(raw)
    assert response.status_code == 409
    assert response.json() == {
        "accepted": False,
        "manifest_id": raw.manifest_id,
        "manifest_sha256": digest,
        "full_resync_required": True,
        "reason": reason.value,
    }
