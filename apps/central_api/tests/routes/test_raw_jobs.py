from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from threading import Barrier, Lock

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from central_api.app import create_app
from central_api.routes import raw_jobs
from central_api.routes.raw_jobs import (
    get_control_plane,
    get_edge_identity,
    get_raw_upload_service,
    router,
)
from central_api.schemas.raw_api import EdgeIdentity
from central_api.services.raw_upload import RawUploadService
from cnes_domain.control_plane.entities import Agent, Job
from cnes_domain.control_plane.enums import AgentState, JobState
from cnes_domain.control_plane.errors import FenceRejected, LeaseLost, NotFound
from cnes_domain.ports.object_store import ObjectStat

NOW = datetime(2026, 7, 15, 12, tzinfo=UTC)
FINGERPRINT = sha256(b"certificate").hexdigest()
KEY = "raw/354130/CNES_LOCAL/2026-07/snapshot-1/data.parquet"


def agent(**updates: object) -> Agent:
    values = {
        "tenant_id": "354130",
        "agent_id": "agent-1",
        "state": AgentState.ACTIVE,
        "version": "1.0",
        "certificate_fingerprint": FINGERPRINT,
        "last_seen_at": NOW,
        "created_at": NOW,
    }
    return Agent(**(values | updates))


def job(**updates: object) -> Job:
    values = {
        "tenant_id": "354130",
        "job_id": "job-1",
        "agent_id": "agent-1",
        "source_type": "CNES_LOCAL",
        "file_subtype": "CNES_VINCULO",
        "competencia": "2026-07",
        "requested_snapshot_mode": "FULL",
        "state": JobState.PENDING,
        "attempt": 0,
        "fencing_token": 0,
        "lease_owner": None,
        "lease_until": None,
        "result_manifest_id": None,
        "result_manifest_key": None,
        "error_code": None,
        "created_at": NOW,
    }
    return Job(**(values | updates))


class ControlPlane:
    def __init__(self, current_agent: Agent | None = None, jobs: tuple[Job, ...] = ()) -> None:
        self.agent = current_agent
        self.jobs = jobs
        self.claimed: set[str] = set()
        self.lock = Lock()
        self.claim_barrier: Barrier | None = None
        self.calls: list[str] = []
        self.renew_error: Exception | None = None
        self.renew_result: Job | None = None

    def get_agent(self, tenant_id: str, agent_id: str) -> Agent | None:
        self.calls.append("get_agent")
        return self.agent

    def list_claimable_jobs(self, tenant_id: str, agent_id: str, limit: int):
        self.calls.append(f"list:{tenant_id}:{agent_id}:{limit}")
        return self.jobs[:limit]

    def claim_job(self, command):
        if self.claim_barrier is not None:
            self.claim_barrier.wait()
        with self.lock:
            if command.job_id in self.claimed:
                return None
            self.claimed.add(command.job_id)
        candidate = next(item for item in self.jobs if item.job_id == command.job_id)
        return candidate.model_copy(update={
            "state": JobState.LEASED,
            "attempt": candidate.attempt + 1,
            "fencing_token": candidate.fencing_token + 1,
            "lease_owner": command.owner,
            "lease_until": command.now + timedelta(seconds=command.lease_seconds),
        })

    def get_job(self, tenant_id: str, job_id: str) -> Job | None:
        self.calls.append(f"job:{tenant_id}:{job_id}")
        return next((item for item in self.jobs if item.job_id == job_id), None)

    def renew_job_lease(self, command):
        if self.renew_error:
            raise self.renew_error
        if self.renew_result is not None:
            return self.renew_result
        current = self.get_job(command.tenant_id, command.job_id)
        assert current is not None
        return current.model_copy(update={
            "lease_until": command.now + timedelta(seconds=command.lease_seconds),
        })


class ObjectStore:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.calls: list[str] = []

    def stat(self, key: str) -> ObjectStat | None:
        self.calls.append(f"stat:{key}")
        value = self.objects.get(key)
        if value is None:
            return None
        return ObjectStat(key, len(value), sha256(value).hexdigest())

    def put(self, key: str, body, expected_sha256: str) -> ObjectStat:
        self.calls.append(f"put:{key}")
        value = body.read()
        current = self.objects.setdefault(key, value)
        if current != value:
            raise RuntimeError("object=immutable")
        return ObjectStat(key, len(value), expected_sha256)


def client(control: ControlPlane, *, upload: RawUploadService | None = None) -> TestClient:
    app = FastAPI()
    app.include_router(router)
    identity = EdgeIdentity(
        tenant_id="354130", agent_id="agent-1", certificate_fingerprint=FINGERPRINT
    )
    app.dependency_overrides[get_edge_identity] = lambda: identity
    app.dependency_overrides[get_control_plane] = lambda: control
    upload_service = upload or RawUploadService(control, ObjectStore(), lambda: NOW)
    app.dependency_overrides[get_raw_upload_service] = lambda: upload_service
    return TestClient(app)


def test_mtls_ausente_rejeita_antes_de_consultar_job() -> None:
    control = ControlPlane(agent(), (job(),))
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_control_plane] = lambda: control

    response = TestClient(app).get(
        "/api/v1/edge/jobs/next",
        headers={"X-SSL-Client-DN": "agent-1", "X-SSL-Client-Fingerprint": FINGERPRINT},
    )

    assert response.status_code == 401
    assert response.json() == {"detail": "mtls_required"}
    assert control.calls == []


@pytest.mark.parametrize(
    "provider",
    [get_control_plane, get_raw_upload_service, raw_jobs.get_raw_ingestion_service],
)
def test_provider_nao_configurado_falha_fechado(provider) -> None:
    with pytest.raises(HTTPException) as captured:
        provider()

    assert captured.value.status_code == 503


@pytest.mark.parametrize(
    ("current", "status", "detail"),
    [
        (None, 403, "agent_missing"),
        (agent(state=AgentState.REVOKED), 403, "agent_revoked"),
        (agent(tenant_id="other"), 403, "agent_identity_mismatch"),
        (agent(certificate_fingerprint="b" * 64), 403, "certificate_fingerprint_mismatch"),
    ],
)
def test_identidade_invalida_rejeita_claim(current, status: int, detail: str) -> None:
    control = ControlPlane(current, (job(),))

    response = client(control).get("/api/v1/edge/jobs/next")

    assert response.status_code == status
    assert response.json() == {"detail": detail}
    assert not any(call.startswith("list:") for call in control.calls)


def test_claim_isola_tenant_e_agente_e_retorna_job_fortemente_reclamado(monkeypatch) -> None:
    monkeypatch.setattr(raw_jobs, "_utc_now", lambda: NOW)
    control = ControlPlane(agent(), (job(),))

    response = client(control).get("/api/v1/edge/jobs/next")

    assert response.status_code == 200
    assert response.json() == {
        "job_id": "job-1",
        "source_type": "CNES_LOCAL",
        "file_subtype": "CNES_VINCULO",
        "competencia": "2026-07",
        "requested_snapshot_mode": "FULL",
        "fencing_token": 1,
        "lease_until": "2026-07-15T12:05:00Z",
        "raw_upload_path": "/api/v1/edge/jobs/job-1/raw-object",
    }
    assert "list:354130:agent-1:10" in control.calls


def test_claim_tenta_ate_primeiro_cas_confirmado(monkeypatch) -> None:
    monkeypatch.setattr(raw_jobs, "_utc_now", lambda: NOW)
    first = job(job_id="job-1")
    second = job(job_id="job-2")
    control = ControlPlane(agent(), (first, second))
    control.claimed.add("job-1")

    response = client(control).get("/api/v1/edge/jobs/next")

    assert response.status_code == 200
    assert response.json()["job_id"] == "job-2"


def test_claim_ignora_candidato_de_outra_identidade(monkeypatch) -> None:
    monkeypatch.setattr(raw_jobs, "_utc_now", lambda: NOW)
    control = ControlPlane(agent(), (job(agent_id="other"),))

    response = client(control).get("/api/v1/edge/jobs/next")

    assert response.status_code == 204
    assert control.claimed == set()


def test_claim_sem_vencedor_retorna_204(monkeypatch) -> None:
    monkeypatch.setattr(raw_jobs, "_utc_now", lambda: NOW)
    control = ControlPlane(agent(), (job(),))
    control.claimed.add("job-1")

    response = client(control).get("/api/v1/edge/jobs/next")

    assert response.status_code == 204
    assert response.content == b""


def test_claim_concorrente_tem_um_unico_vencedor(monkeypatch) -> None:
    monkeypatch.setattr(raw_jobs, "_utc_now", lambda: NOW)
    control = ControlPlane(agent(), (job(),))
    control.claim_barrier = Barrier(2)
    api = client(control)

    with ThreadPoolExecutor(max_workers=2) as pool:
        responses = list(pool.map(lambda _: api.get("/api/v1/edge/jobs/next"), range(2)))

    assert sorted(response.status_code for response in responses) == [200, 204]


def test_heartbeat_distingue_job_ausente(monkeypatch) -> None:
    monkeypatch.setattr(raw_jobs, "_utc_now", lambda: NOW)
    response = client(ControlPlane(agent())).post(
        "/api/v1/edge/jobs/missing/heartbeat", json={"fencing_token": 7}
    )

    assert response.status_code == 404
    assert response.json() == {"detail": "job_missing"}


def test_heartbeat_rejeita_job_de_outro_agente(monkeypatch) -> None:
    monkeypatch.setattr(raw_jobs, "_utc_now", lambda: NOW)
    response = client(ControlPlane(agent(), (job(agent_id="other"),))).post(
        "/api/v1/edge/jobs/job-1/heartbeat", json={"fencing_token": 7}
    )

    assert response.status_code == 403
    assert response.json() == {"detail": "job_identity_mismatch"}


@pytest.mark.parametrize(
    ("error", "detail"),
    [
        (FenceRejected("fence_mismatch"), "job_fence_rejected"),
        (LeaseLost("lease_expired"), "job_lease_expired"),
        (LeaseLost("owner_mismatch"), "job_owner_lost"),
        (NotFound("job_missing"), "job_missing"),
    ],
)
def test_heartbeat_rejeita_fence_ou_lease_obsoleto(monkeypatch, error, detail: str) -> None:
    monkeypatch.setattr(raw_jobs, "_utc_now", lambda: NOW)
    leased = job(
        state=JobState.LEASED,
        fencing_token=7,
        lease_owner="agent-1",
        lease_until=NOW + timedelta(minutes=1),
    )
    control = ControlPlane(agent(), (leased,))
    control.renew_error = error

    response = client(control).post(
        "/api/v1/edge/jobs/job-1/heartbeat", json={"fencing_token": 7}
    )

    expected_status = 404 if isinstance(error, NotFound) else 409
    assert response.status_code == expected_status
    assert response.json() == {"detail": detail}


def test_heartbeat_renova_owner_fence_por_300_segundos(monkeypatch) -> None:
    monkeypatch.setattr(raw_jobs, "_utc_now", lambda: NOW)
    leased = job(
        state=JobState.LEASED,
        fencing_token=7,
        lease_owner="agent-1",
        lease_until=NOW + timedelta(minutes=1),
    )

    response = client(ControlPlane(agent(), (leased,))).post(
        "/api/v1/edge/jobs/job-1/heartbeat", json={"fencing_token": 7}
    )

    assert response.status_code == 200
    assert response.json() == {
        "job_id": "job-1",
        "fencing_token": 7,
        "lease_until": "2026-07-15T12:05:00Z",
    }


def test_heartbeat_rejeita_retorno_sem_lease(monkeypatch) -> None:
    monkeypatch.setattr(raw_jobs, "_utc_now", lambda: NOW)
    leased = job(
        state=JobState.LEASED,
        fencing_token=7,
        lease_owner="agent-1",
        lease_until=NOW + timedelta(minutes=1),
    )
    control = ControlPlane(agent(), (leased,))
    control.renew_result = job()

    response = client(control).post(
        "/api/v1/edge/jobs/job-1/heartbeat", json={"fencing_token": 7}
    )

    assert response.status_code == 409
    assert response.json() == {"detail": "job_not_leased"}


def test_upload_rejeita_media_type_incorreto() -> None:
    control = ControlPlane(agent(), (job(),))
    response = client(control).put(
        "/api/v1/edge/jobs/job-1/raw-object",
        content=b"payload",
        headers={"X-Fencing-Token": "7", "X-Object-Key": KEY, "Content-Type": "text/plain"},
    )

    assert response.status_code == 415
    assert response.json() == {"detail": "media_type_unsupported"}


def test_fingerprint_divergente_rejeita_antes_do_objeto() -> None:
    control = ControlPlane(agent(certificate_fingerprint="b" * 64), (job(),))
    store = ObjectStore()
    upload = RawUploadService(control, store, lambda: NOW)

    response = client(control, upload=upload).put(
        "/api/v1/edge/jobs/job-1/raw-object",
        content=b"payload",
        headers={
            "X-Fencing-Token": "7",
            "X-Object-Key": KEY,
            "Content-Type": "application/octet-stream",
        },
    )

    assert response.status_code == 403
    assert store.calls == []


@pytest.mark.parametrize(
    ("jobs", "key", "status", "detail"),
    [
        ((), KEY, 404, "job_missing"),
        ((job(agent_id="other"),), KEY, 409, "job_identity_mismatch"),
        (
            (
                job(
                    state=JobState.LEASED,
                    fencing_token=7,
                    lease_owner="agent-1",
                    lease_until=NOW + timedelta(minutes=1),
                ),
            ),
            "raw/other/CNES_LOCAL/2026-07/s/data.parquet",
            409,
            "object_key_invalid",
        ),
    ],
)
def test_upload_mapeia_falhas_do_servico(jobs, key: str, status: int, detail: str) -> None:
    control = ControlPlane(agent(), jobs)
    upload = RawUploadService(control, ObjectStore(), lambda: NOW)

    response = client(control, upload=upload).put(
        "/api/v1/edge/jobs/job-1/raw-object",
        content=b"payload",
        headers={
            "X-Fencing-Token": "7",
            "X-Object-Key": key,
            "Content-Type": "application/octet-stream",
        },
    )

    assert response.status_code == status
    assert response.json() == {"detail": detail}


@pytest.mark.parametrize(
    "case",
    [(2, b"abc", 413, "payload_too_large"), (1024**3, b"", 422, "payload_empty")],
)
def test_upload_mapeia_tamanho_invalido(monkeypatch, case) -> None:
    limit, body, status, detail = case
    monkeypatch.setattr("central_api.services.raw_upload.RAW_UPLOAD_MAX_BYTES", limit)
    leased = job(
        state=JobState.LEASED,
        fencing_token=7,
        lease_owner="agent-1",
        lease_until=NOW + timedelta(minutes=1),
    )
    control = ControlPlane(agent(), (leased,))
    upload = RawUploadService(control, ObjectStore(), lambda: NOW)

    response = client(control, upload=upload).put(
        "/api/v1/edge/jobs/job-1/raw-object",
        content=body,
        headers={
            "X-Fencing-Token": "7",
            "X-Object-Key": KEY,
            "Content-Type": "application/octet-stream",
        },
    )

    assert response.status_code == status
    assert response.json() == {"detail": detail}


@pytest.mark.parametrize("headers", [{}, {"X-Fencing-Token": "x"}, {"X-Fencing-Token": "7"}])
def test_upload_exige_headers_congelados(headers: dict[str, str]) -> None:
    control = ControlPlane(agent(), (job(),))
    response = client(control).put(
        "/api/v1/edge/jobs/job-1/raw-object",
        content=b"payload",
        headers=headers | {"Content-Type": "application/octet-stream"},
    )

    assert response.status_code == 422


@pytest.mark.parametrize(
    ("existing", "body", "status"),
    [(b"payload", b"payload", 200), (b"old", b"new", 409)],
)
def test_replay_pela_rota_preserva_imutabilidade(existing: bytes, body: bytes, status: int) -> None:
    leased = job(
        state=JobState.LEASED,
        fencing_token=7,
        lease_owner="agent-1",
        lease_until=NOW + timedelta(minutes=1),
    )
    control = ControlPlane(agent(), (leased,))
    store = ObjectStore()
    store.objects[KEY] = existing
    upload = RawUploadService(control, store, lambda: NOW)

    response = client(control, upload=upload).put(
        "/api/v1/edge/jobs/job-1/raw-object",
        content=body,
        headers={
            "X-Fencing-Token": "7",
            "X-Object-Key": KEY,
            "Content-Type": "application/octet-stream",
        },
    )

    assert response.status_code == status
    assert store.objects[KEY] == existing
    if status == 200:
        assert response.json()["object_sha256"] == sha256(body).hexdigest()
    else:
        assert response.json() == {"detail": "object_conflict"}


def test_create_app_continua_sem_rotas_edge() -> None:
    paths = {route.path for route in create_app().routes}

    assert all(not path.startswith("/api/v1/edge") for path in paths)
