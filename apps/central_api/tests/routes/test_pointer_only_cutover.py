"""Cutover pointer-only: serving lê só o pointer current; rotas SQL legadas aposentadas."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from io import BytesIO
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from central_api.middleware import AuthenticatedUser
from central_api.routes import overview, serving
from central_api.routes.serving import ServingPrincipal
from central_api.services.serving_access import LocalServingAccess
from cnes_contracts.manifests.outputs import OutputManifest, RunManifest
from cnes_domain.control_plane.commands import PublicationPermit, PublishDataset
from cnes_domain.control_plane.entities import DatasetVersion, Membership, OutboxEvent, Run
from cnes_domain.control_plane.enums import RunState
from cnes_domain.orchestration.source_catalog import build_source_catalog
from cnes_infra.control_plane.sqlite_adapter import SQLiteControlPlane
from cnes_infra.object_store import FilesystemObjectStore

if TYPE_CHECKING:
    from pathlib import Path

    from cnes_domain.ports.object_store import ObjectStorePort
    from cnes_domain.ports.serving import ServingGrant, ServingRequest

TENANT = "354130"
USER = "user-1"
COMPETENCIA = "2026-07"
DOCUMENT = "overview"
NOW = datetime(2026, 7, 2, tzinfo=UTC)
CATALOG_DATASETS = ("cnes", "sihd", "bpa", "sia")
UNKNOWN_DATASETS = ("demo", "gold", "CNES", "cnes-v2")
LEGACY_PATHS = (
    "/api/v1/dashboard/overview",
    "/api/v1/dashboard/faturamento/by-establishment",
)
RUN_DEPENDENCIES = build_source_catalog().for_pipeline("cnes").dependencies


class RepoSpy:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def __getattr__(self, name: str) -> Any:
        self.calls.append(name)
        raise AssertionError(f"dashboard_repo.{name}")


class RecordingAccess:
    def __init__(self, inner: LocalServingAccess) -> None:
        self._inner = inner
        self.requests: list[ServingRequest] = []

    def authorize(self, request: ServingRequest) -> ServingGrant:
        self.requests.append(request)
        return self._inner.authorize(request)


@dataclass(frozen=True, slots=True)
class Publication:
    dataset: str
    run_id: str
    expected: str | None
    omit: str | None = None


def serving_key(run_id: str) -> str:
    return f"serving/{TENANT}/{run_id}/{DOCUMENT}.json"


def manifest_key(run_id: str) -> str:
    return f"reconciliation/{TENANT}/{COMPETENCIA}/{run_id}/run-manifest.json"


def _run_manifest(publication: Publication, digest: str) -> RunManifest:
    output = OutputManifest(
        manifest_version=1, manifest_id=f"serving-{publication.run_id}", tenant_id=TENANT,
        layer="serving", source_type=None, competencia=COMPETENCIA, run_id=publication.run_id,
        unit_id="unit-materialize", attempt=1, schema_version="cnes-serving-v1",
        object_key=serving_key(publication.run_id), object_sha256=digest, row_count=1,
        created_at=NOW,
    )
    return RunManifest(
        manifest_version=1, tenant_id=TENANT, dataset_name=publication.dataset,
        run_id=publication.run_id, competencia=COMPETENCIA, outputs=(output,),
        missing_sources=(), published_at=NOW,
    )


def _stage_objects(store: ObjectStorePort, publication: Publication) -> None:
    body = json.dumps({"dataset": publication.dataset, "run_id": publication.run_id}).encode()
    digest = sha256(body).hexdigest()
    if publication.omit != "serving_object":
        store.put(serving_key(publication.run_id), BytesIO(body), digest)
    if publication.omit != "run_manifest":
        manifest = _run_manifest(publication, digest)
        payload = manifest.model_dump_json(exclude_none=False, by_alias=False).encode()
        store.put(manifest_key(publication.run_id), BytesIO(payload), sha256(payload).hexdigest())


def _publishing_run(publication: Publication) -> Run:
    return Run(
        tenant_id=TENANT, run_id=publication.run_id, competencia=COMPETENCIA,
        dataset_name=publication.dataset, state=RunState.PUBLISHING,
        dependencies=RUN_DEPENDENCIES, missing_sources=(), created_at=NOW,
    )


def _publish_command(publication: Publication) -> PublishDataset:
    run_id = publication.run_id
    version = DatasetVersion(
        tenant_id=TENANT, dataset_name=publication.dataset, version_id=run_id, run_id=run_id,
        run_manifest_key=manifest_key(run_id), created_at=NOW,
    )
    event = OutboxEvent(
        tenant_id=TENANT, event_id=f"published:{run_id}", event_type="reconciliation.published",
        aggregate_id=run_id, payload={"dataset_name": publication.dataset, "version_id": run_id},
        created_at=NOW, delivered_at=None,
    )
    return PublishDataset(
        version=version, pointer_name="current", expected_version_id=publication.expected,
        final_state=RunState.PUBLISHED, missing_sources=(),
        publication_permit=PublicationPermit(
            tenant_id=TENANT, run_id=run_id, policy_version=0, fencing_token=0,
        ),
        event=event,
    )


def build_app(access: RecordingAccess, store: ObjectStorePort, repo: RepoSpy) -> FastAPI:
    app = FastAPI()
    app.state.dashboard_repo = repo
    app.include_router(serving.router)
    app.include_router(overview.router, prefix="/api/v1/dashboard")
    principal = ServingPrincipal(tenant_id=TENANT, user_id=USER)
    app.dependency_overrides[serving.get_serving_principal] = lambda: principal
    app.dependency_overrides[serving.get_serving_access] = lambda: access
    app.dependency_overrides[serving.get_serving_object_store] = lambda: store

    @app.middleware("http")
    async def authenticated(request: Request, call_next):
        request.state.user = AuthenticatedUser(
            user_id=uuid4(), email="g@m", display_name=None, role="gestor", tenant_ids=[TENANT],
        )
        return await call_next(request)

    return app


class Cutover:
    def __init__(self, root: Path) -> None:
        self.control_plane = SQLiteControlPlane(root / "state.db", lambda: NOW)
        self.control_plane.initialize()
        self.control_plane.put_membership(
            Membership(tenant_id=TENANT, user_id=USER, role="viewer", created_at=NOW)
        )
        self.store = FilesystemObjectStore(root / "objects")
        self.repo = RepoSpy()
        self.access = RecordingAccess(LocalServingAccess(self.control_plane, self.store))
        self.client = TestClient(build_app(self.access, self.store, self.repo))

    def publish(self, publication: Publication) -> None:
        _stage_objects(self.store, publication)
        self.control_plane.put_run(_publishing_run(publication))
        self.control_plane.publish_dataset(_publish_command(publication))

    def seed_previous_and_current(self, dataset: str) -> tuple[str, str]:
        previous, current = f"{dataset}-previous", f"{dataset}-current"
        self.publish(Publication(dataset, previous, expected=None))
        self.publish(Publication(dataset, current, expected=previous))
        return previous, current

    def read_serving(self, dataset: str):
        return self.client.get(f"/api/v1/dashboard/serving/{dataset}/{DOCUMENT}")


@pytest.fixture
def cutover(tmp_path: Path) -> Cutover:
    return Cutover(tmp_path)


@pytest.mark.parametrize("dataset", CATALOG_DATASETS)
def test_serving_entrega_somente_o_run_do_pointer_current(cutover: Cutover, dataset: str) -> None:
    _, current = cutover.seed_previous_and_current(dataset)

    response = cutover.read_serving(dataset)

    assert response.status_code == 200
    assert response.json() == {"dataset": dataset, "run_id": current}
    assert response.headers["X-Dataset-Version"] == current
    assert cutover.repo.calls == []


@pytest.mark.parametrize("omit", ["serving_object", "run_manifest"])
@pytest.mark.parametrize("dataset", CATALOG_DATASETS)
def test_conteudo_ativo_ausente_retorna_503_sem_cair_na_versao_anterior(
    cutover: Cutover, dataset: str, omit: str
) -> None:
    previous = f"{dataset}-previous"
    cutover.publish(Publication(dataset, previous, expected=None))
    cutover.publish(Publication(dataset, f"{dataset}-current", expected=previous, omit=omit))

    response = cutover.read_serving(dataset)

    assert cutover.store.stat(serving_key(previous)) is not None
    assert response.status_code == 503
    assert response.json() == {"detail": "active_serving_unavailable"}
    assert "X-Dataset-Version" not in response.headers
    assert cutover.repo.calls == []


@pytest.mark.parametrize("path", LEGACY_PATHS)
def test_rotas_legadas_retornam_410_sem_tocar_o_repositorio_sql(
    cutover: Cutover, path: str
) -> None:
    response = cutover.client.get(path, headers={"X-Tenant-Id": TENANT})

    assert response.status_code == 410
    assert response.json() == {"detail": "legacy_route_retired"}
    assert cutover.repo.calls == []


@pytest.mark.parametrize("dataset", UNKNOWN_DATASETS)
def test_dataset_fora_do_catalogo_retorna_404_mesmo_com_pointer_publicado(
    cutover: Cutover, dataset: str
) -> None:
    cutover.publish(Publication(dataset, f"{dataset}-current", expected=None))

    response = cutover.read_serving(dataset)

    assert response.status_code == 404
    assert response.json() == {"detail": "dataset_unknown"}
    assert cutover.access.requests == []
    assert cutover.repo.calls == []
