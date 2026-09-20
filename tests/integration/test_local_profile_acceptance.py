"""Matriz de aceitação do app local composto: serving, ordem de autorização, restart, aws."""
from __future__ import annotations

import hashlib
from contextlib import contextmanager
from datetime import UTC, datetime
from hashlib import sha256
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING

import pytest
import yaml
from fastapi.testclient import TestClient

from central_api.composition import build_local_runtime
from central_api.ratelimit import limiter
from cnes_contracts.manifests.outputs import OutputManifest
from cnes_domain.control_plane.entities import ManifestRef, Membership, Run, RunDependency, RunUnit
from cnes_domain.control_plane.enums import RunStage, RunState, RunUnitState
from cnes_domain.profiles import ProfileNotImplemented, parse_profile
from cnes_infra.auth.local_credentials import LocalCredentialStore, build_user
from data_processor.composition import build_local_processor_runtime
from data_processor.orchestration.attempt_store import attempt_object_key, unit_attempt_prefix
from data_processor.orchestration.publisher import DatasetPublisher, PublishRequest

if TYPE_CHECKING:
    from cnes_domain.ports.object_store import ObjectStorePort

pytestmark = [pytest.mark.local_profile]

_TENANT = "354130"
_COMPETENCIA = "2026-01"
_RUN_ID = "run-1"
_NOW = datetime(2026, 1, 15, 12, tzinfo=UTC)
_PASSWORD = "correct-horse-battery"  # noqa: S105
_EMAIL = "gestor@epitacio.sp.gov.br"
_REPO_ROOT = Path(__file__).resolve().parents[2]


def _utc_now() -> datetime:
    return _NOW


@pytest.fixture(autouse=True)
def _reset_limiter() -> None:
    limiter.reset()


def _local_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PROFILE", "local")
    monkeypatch.setenv("TENANT_ID", _TENANT)
    monkeypatch.setenv("DATA_DIR", str(tmp_path))


def _seed_local_user(data_dir: Path) -> None:
    state_db = data_dir / "state" / "cnesdata.sqlite3"
    credentials = LocalCredentialStore(state_db)
    credentials.initialize()
    credentials.put_user(build_user("user-1", _EMAIL, _PASSWORD, _NOW))


def _seed_membership(control_plane, user_id: str = "user-1") -> None:
    control_plane.put_membership(Membership(
        tenant_id=_TENANT, user_id=user_id, role="gestor", created_at=_NOW, oidc_issuer=None,
    ))


def _publish_serving_run(control_plane, object_store: ObjectStorePort, run_id: str = _RUN_ID):
    run = Run(
        tenant_id=_TENANT, run_id=run_id, competencia=_COMPETENCIA, dataset_name="cnes",
        state=RunState.PUBLISHING,
        dependencies=(RunDependency(source_type="CNES", file_subtype="ST", required=True),),
        missing_sources=(), created_at=_NOW,
    )
    body = b'{"schema_version": "cnes-serving-v1"}'
    digest = hashlib.sha256(body).hexdigest()
    object_key = f"serving/{_TENANT}/{run_id}/overview.json"
    manifest = OutputManifest(
        manifest_version=1, manifest_id=f"serving-{run_id}", tenant_id=_TENANT,
        layer="serving", source_type=None, competencia=_COMPETENCIA,
        run_id=run_id, unit_id="unit-materialize", attempt=1,
        schema_version="cnes-serving-v1", object_key=object_key, object_sha256=digest,
        row_count=1, created_at=_NOW,
    )
    prefix = unit_attempt_prefix(
        SimpleNamespace(tenant_id=_TENANT, run_id=run_id, unit_id="unit-materialize", attempt=1)
    )
    source_key = attempt_object_key(prefix, object_key)
    object_store.put(source_key, BytesIO(body), digest)
    sidecar_key = attempt_object_key(prefix, f"manifests/{manifest.manifest_id}/manifest.json")
    payload = manifest.model_dump_json(exclude_none=False, by_alias=False).encode()
    object_store.put(sidecar_key, BytesIO(payload), sha256(payload).hexdigest())
    ref = ManifestRef(manifest_id=manifest.manifest_id, manifest_key=sidecar_key)
    unit = RunUnit(
        tenant_id=_TENANT, run_id=run_id, unit_id="unit-materialize", stage=RunStage.MATERIALIZE,
        source_type=None, file_subtype=None, partition="all",
        depends_on_unit_ids=("unit-upstream",), input_manifests=(),
        state=RunUnitState.SUCCEEDED, attempt=1, fencing_token=1, lease_owner=None,
        lease_until=None, dispatch_id=None, output_manifests=(ref,), error_code=None,
    )
    control_plane.put_run(run)
    publisher = DatasetPublisher(store=object_store, control_plane=control_plane)
    return publisher.publish(
        PublishRequest(run=run, units=(unit,), expected_version_id=None, now=_NOW)
    )


def _login(client: TestClient) -> None:
    response = client.post(
        "/api/v1/auth/local/login", json={"email": _EMAIL, "password": _PASSWORD}
    )
    assert response.status_code == 200, response.text


def test_health_local_nao_toca_postgres(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _local_env(tmp_path, monkeypatch)
    monkeypatch.setattr(
        "central_api.deps.create_engine",
        lambda *args, **kwargs: pytest.fail("local health must not create a SQL engine"),
    )
    from central_api.app import create_app

    with TestClient(create_app()) as client:
        response = client.get("/api/v1/system/health")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"
    assert response.json()["db_connected"] is True


class _SpyStore:
    """Encaminha para o store real, registrando a sequência (op, chave) das chamadas."""

    def __init__(self, inner: ObjectStorePort) -> None:
        self._inner = inner
        self.calls: list[tuple[str, str]] = []

    def stat(self, key: str):
        self.calls.append(("stat", key))
        return self._inner.stat(key)

    @contextmanager
    def open(self, key: str):
        self.calls.append(("open", key))
        with self._inner.open(key) as handle:
            yield handle

    def put(self, key: str, body, expected_sha256: str):
        return self._inner.put(key, body, expected_sha256)

    def promote(self, source_key: str, destination_key: str, expected_sha256: str):
        return self._inner.promote(source_key, destination_key, expected_sha256)

    def delete(self, key: str) -> None:
        self._inner.delete(key)


def test_serving_composto_usa_store_do_runtime(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _local_env(tmp_path, monkeypatch)
    from central_api.app import create_app
    from central_api.routes.serving import get_serving_object_store

    with TestClient(create_app()) as client:
        app = client.app
        _seed_local_user(tmp_path)
        _seed_membership(app.state.control_plane)
        _publish_serving_run(app.state.control_plane, app.state.object_store)
        _login(client)

        response = client.get("/api/v1/dashboard/serving/cnes/overview")

        assert response.status_code == 200
        assert response.json() == {"schema_version": "cnes-serving-v1"}
        assert app.dependency_overrides[get_serving_object_store]() is app.state.object_store


def test_run_manifest_aberto_antes_do_objeto_de_serving_concedido(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _local_env(tmp_path, monkeypatch)
    from central_api.app import create_app
    from central_api.routes.serving import get_serving_access, get_serving_object_store
    from central_api.services.serving_access import LocalServingAccess

    with TestClient(create_app()) as client:
        app = client.app
        _seed_local_user(tmp_path)
        _seed_membership(app.state.control_plane)
        result = _publish_serving_run(app.state.control_plane, app.state.object_store)
        _login(client)
        spy = _SpyStore(app.state.object_store)
        spied_access = LocalServingAccess(app.state.control_plane, spy)
        app.dependency_overrides[get_serving_access] = lambda: spied_access
        app.dependency_overrides[get_serving_object_store] = lambda: spy

        response = client.get("/api/v1/dashboard/serving/cnes/overview")

        assert response.status_code == 200
        serving_key = f"serving/{_TENANT}/{_RUN_ID}/overview.json"
        run_manifest_key = result.version.run_manifest_key
        opened = [key for op, key in spy.calls if op == "open"]
        assert run_manifest_key in opened
        assert serving_key in opened
        assert opened.index(run_manifest_key) < opened.index(serving_key)


def test_restart_preserva_sessao_pointer_e_objetos(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _local_env(tmp_path, monkeypatch)
    from central_api.app import create_app

    with TestClient(create_app()) as client:
        app = client.app
        _seed_local_user(tmp_path)
        _seed_membership(app.state.control_plane)
        _publish_serving_run(app.state.control_plane, app.state.object_store)

    with TestClient(create_app()) as reopened_client:
        reopened_app = reopened_client.app
        _login(reopened_client)
        pointer = reopened_app.state.control_plane.get_dataset_pointer(_TENANT, "cnes")
        assert pointer is not None
        assert pointer.version_id == _RUN_ID

        response = reopened_client.get("/api/v1/dashboard/serving/cnes/overview")
        assert response.status_code == 200
        assert response.json() == {"schema_version": "cnes-serving-v1"}


def test_profile_aws_nao_implementado_em_api_e_processor(tmp_path: Path) -> None:
    settings = parse_profile({
        "PROFILE": "aws", "TENANT_ID": _TENANT, "DATA_DIR": str(tmp_path),
        "AUTH_MODE": "oidc", "OIDC_ISSUER": "https://issuer.example",
    })

    with pytest.raises(ProfileNotImplemented, match="aws_runtime_plan_required"):
        build_local_runtime(settings, _utc_now)
    with pytest.raises(ProfileNotImplemented, match="aws_runtime_plan_required"):
        build_local_processor_runtime(settings, _utc_now)


def test_compose_profile_local_contem_apenas_api_processor_dashboard() -> None:
    compose = yaml.safe_load((_REPO_ROOT / "docker-compose.yml").read_text(encoding="utf-8"))

    local_services = {
        name for name, definition in compose["services"].items()
        if "local" in definition.get("profiles", ())
    }

    assert local_services == {"central-api-local", "data-processor-local", "web-dashboard-local"}
    infra_names = {"postgres", "minio", "keycloak", "dynamodb-local", "localstack"}
    assert local_services.isdisjoint(infra_names)


def test_compose_profile_local_usa_volume_nomeado_para_dados_gravaveis() -> None:
    compose = yaml.safe_load((_REPO_ROOT / "docker-compose.yml").read_text(encoding="utf-8"))

    assert compose["volumes"]["local_data"] is None
    for service in ("central-api-local", "data-processor-local"):
        assert compose["services"][service]["volumes"] == ["local_data:/data"]
