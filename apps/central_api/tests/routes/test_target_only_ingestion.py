"""Write-fence MIG-012: rotas de ingestão legada respondem 410 sem tocar Postgres nem S3."""
from __future__ import annotations

import asyncio
import inspect
from contextlib import ExitStack
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from central_api.agent_auth import AgentCertIdentity, agent_identity_if_required
from central_api.deps import get_engine, legacy_ingestion_retired
from cnes_infra import config
from cnes_infra.storage import extractions_repo
from cnes_infra.storage.s3_presigned import S3PresignedStorage

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

    from fastapi import FastAPI
    from httpx import Response

_RETIRED = {"detail": "legacy_ingestion_retired"}
_ADMIN_KEY = "chave-de-teste"
_IDENTITY = AgentCertIdentity(tenant_id="354130", agent_id="agent-1", machine_id="a1b2c3d4")
_STARTUP_PATCHES = (
    "central_api.deps.install_rls_listener",
    "central_api.deps.instrument_engine",
    "central_api.deps.install_query_counter",
)


@dataclass(frozen=True)
class _Route:
    path: str
    body: dict[str, Any]
    admin: bool = False


_JOBS_ROUTES = {
    "upload-url": _Route(
        "/api/v1/jobs/upload-url",
        {
            "job_id": str(uuid4()), "tenant_id": "354130", "source_type": "CNES_LOCAL",
            "tipo_extracao": "profissionais", "competencia": "2026-01-01",
            "intent": "cnes_profissionais", "machine_id": "a1b2c3d4",
        },
    ),
    "register": _Route(
        "/api/v1/jobs/register",
        {
            "job_id": str(uuid4()), "machine_id": "a1b2c3d4",
            "files": [{
                "minio_key": "354130/CNES_VINCULO/2026-01-01/job.parquet.gz",
                "fato_subtype": "CNES_VINCULO", "size_bytes": 1, "sha256": "a" * 64,
            }],
        },
    ),
    "fail": _Route(f"/api/v1/jobs/{uuid4()}/fail", {"error": "boom"}),
}
_ADMIN_ROUTES = {
    "enqueue": _Route(
        "/api/v1/extractions/enqueue",
        {"source_type": "BPA_MAG", "tenant_id": "354130", "competencia": "2026-02-01"},
        admin=True,
    ),
    "reap-leases": _Route("/api/v1/admin/reap-leases", {}, admin=True),
}
_ALL_ROUTES = _JOBS_ROUTES | _ADMIN_ROUTES
_BODY_ROUTES = _JOBS_ROUTES | {"enqueue": _ADMIN_ROUTES["enqueue"]}


def _tripwire(label: str) -> MagicMock:
    return MagicMock(name=label, side_effect=AssertionError(f"legacy_write label={label}"))


def _repo_functions() -> list[str]:
    return [
        name
        for name, function in inspect.getmembers(extractions_repo, inspect.isfunction)
        if not name.startswith("_") and function.__module__ == extractions_repo.__name__
    ]


def _storage_methods() -> list[str]:
    public = [name for name, _ in inspect.getmembers(S3PresignedStorage, inspect.isfunction)]
    return ["__init__", *(name for name in public if not name.startswith("_"))]


@dataclass
class _Spies:
    named: dict[str, MagicMock]
    create_engine: MagicMock
    engines_at_startup: int = 0

    def engine_dependency(self) -> Any:
        return self.named["deps.get_engine"]()

    def assert_untouched(self) -> None:
        fired = {label: spy.call_count for label, spy in self.named.items() if spy.call_count}
        assert fired == {}
        assert self.create_engine.call_count == self.engines_at_startup


@pytest.fixture
def spies() -> Iterator[_Spies]:
    repo = {name: _tripwire(f"extractions_repo.{name}") for name in _repo_functions()}
    storage = {name: _tripwire(f"S3PresignedStorage.{name}") for name in _storage_methods()}
    with ExitStack() as stack:
        for name, spy in repo.items():
            stack.enter_context(patch.object(extractions_repo, name, spy))
        for name, spy in storage.items():
            stack.enter_context(patch.object(S3PresignedStorage, name, spy))
        named = {f"extractions_repo.{n}": spy for n, spy in repo.items()}
        named |= {f"S3PresignedStorage.{n}": spy for n, spy in storage.items()}
        named["deps.get_engine"] = _tripwire("deps.get_engine")
        yield _Spies(named=named, create_engine=MagicMock(name="create_engine"))


@pytest.fixture(params=["legado", "local"])
def profile(
    request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> str:
    if request.param == "local":
        monkeypatch.setenv("PROFILE", "local")
        monkeypatch.setenv("TENANT_ID", "354130")
        monkeypatch.setenv("DATA_DIR", str(tmp_path))
    else:
        monkeypatch.delenv("PROFILE", raising=False)
    return request.param


@pytest.fixture
def client(
    profile: str, spies: _Spies, monkeypatch: pytest.MonkeyPatch,
) -> Iterator[TestClient]:
    from central_api.app import create_app

    monkeypatch.setattr(config, "ADMIN_TOKEN", _ADMIN_KEY)
    with ExitStack() as stack:
        for target in _STARTUP_PATCHES:
            stack.enter_context(patch(target))
        stack.enter_context(patch("central_api.deps.create_engine", spies.create_engine))
        app = create_app()
        app.dependency_overrides[agent_identity_if_required] = lambda: _IDENTITY
        app.dependency_overrides[get_engine] = spies.engine_dependency
        with TestClient(app) as test_client:
            spies.engines_at_startup = spies.create_engine.call_count
            yield test_client


def _post(client: TestClient, route: _Route, body: dict[str, Any] | None = None) -> Response:
    headers = {"X-Admin-Token": _ADMIN_KEY} if route.admin else {}
    return client.post(route.path, json=route.body if body is None else body, headers=headers)


@pytest.mark.parametrize("route", _ALL_ROUTES.values(), ids=_ALL_ROUTES.keys())
def test_rota_legada_responde_410_sem_tocar_postgres_nem_s3(
    client: TestClient, spies: _Spies, route: _Route,
) -> None:
    resp = _post(client, route)

    assert (resp.status_code, resp.json()) == (410, _RETIRED)
    spies.assert_untouched()


@pytest.mark.parametrize("payload", [{}, [], None], ids=["objeto_vazio", "lista", "sem_corpo"])
@pytest.mark.parametrize("route", _BODY_ROUTES.values(), ids=_BODY_ROUTES.keys())
def test_rota_com_corpo_responde_410_antes_de_validar_o_payload(
    client: TestClient, spies: _Spies, route: _Route, payload: object,
) -> None:
    headers = {"X-Admin-Token": _ADMIN_KEY} if route.admin else {}
    body: dict[str, Any] = {} if payload is None else {"json": payload}

    resp = client.post(route.path, headers=headers, **body)

    assert (resp.status_code, resp.json()) == (410, _RETIRED)
    spies.assert_untouched()


@pytest.mark.parametrize("route", _BODY_ROUTES.values(), ids=_BODY_ROUTES.keys())
def test_handler_responde_410_mesmo_sem_a_dependencia_do_fence(
    client: TestClient, spies: _Spies, route: _Route,
) -> None:
    cast("FastAPI", client.app).dependency_overrides[legacy_ingestion_retired] = lambda: None

    resp = _post(client, route)

    assert (resp.status_code, resp.json()) == (410, _RETIRED)
    spies.assert_untouched()


@pytest.mark.parametrize("route", _JOBS_ROUTES.values(), ids=_JOBS_ROUTES.keys())
def test_jobs_sem_certificado_continua_401_antes_do_fence(
    client: TestClient, spies: _Spies, route: _Route, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(config, "AGENT_MTLS_REQUIRED", True)
    cast("FastAPI", client.app).dependency_overrides.pop(agent_identity_if_required)

    resp = _post(client, route)

    assert resp.status_code == 401
    spies.assert_untouched()


@pytest.mark.parametrize("route", _ADMIN_ROUTES.values(), ids=_ADMIN_ROUTES.keys())
@pytest.mark.parametrize("token", ["", "token-errado"])
def test_admin_com_token_invalido_continua_401_antes_do_fence(
    client: TestClient, spies: _Spies, route: _Route, token: str,
) -> None:
    resp = client.post(route.path, json=route.body, headers={"X-Admin-Token": token})

    assert (resp.status_code, resp.json()) == (401, {"detail": "admin_token_required"})
    spies.assert_untouched()


async def test_startup_legado_nao_agenda_reaper_de_leases(
    spies: _Spies, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from central_api.app import create_app
    from central_api.deps import lifespan

    monkeypatch.delenv("PROFILE", raising=False)
    app = create_app()
    before = asyncio.all_tasks()
    with ExitStack() as stack:
        for target in _STARTUP_PATCHES:
            stack.enter_context(patch(target))
        stack.enter_context(patch("central_api.deps.create_engine", spies.create_engine))
        async with lifespan(app):
            scheduled = asyncio.all_tasks() - before

    assert scheduled == set()
    assert spies.named["extractions_repo.reap_expired"].call_count == 0
