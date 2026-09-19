from contextlib import contextmanager
from hashlib import sha256
from io import BytesIO

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from central_api.routes.serving import (
    ServingPrincipal,
    get_serving_access,
    get_serving_object_store,
    get_serving_principal,
    router,
)
from central_api.services.serving_access import ServingUnavailable
from cnes_domain.ports.object_store import ObjectStat
from cnes_domain.ports.serving import ServingGrant, ServingRequest

TENANT = "354130"
RUN_ID = "run-1"
DOCUMENT = "overview"
KEY = f"serving/{TENANT}/{RUN_ID}/{DOCUMENT}.json"


def principal(**updates: object) -> ServingPrincipal:
    values = {"tenant_id": TENANT, "user_id": "user-1"}
    return ServingPrincipal(**(values | updates))


def grant(**updates: object) -> ServingGrant:
    values = {
        "tenant_id": TENANT,
        "run_id": RUN_ID,
        "version_id": RUN_ID,
        "object_keys": (KEY,),
    }
    return ServingGrant(**(values | updates))


class Access:
    def __init__(
        self, grant: ServingGrant | None = None, error: ServingUnavailable | None = None
    ) -> None:
        self.grant = grant
        self.error = error
        self.requests: list[ServingRequest] = []

    def authorize(self, request: ServingRequest) -> ServingGrant:
        self.requests.append(request)
        if self.error is not None:
            raise self.error
        return self.grant


class ObjectStore:
    def __init__(self, objects: dict[str, bytes] | None = None) -> None:
        self.objects = dict(objects or {})
        self.stat_calls: list[str] = []
        self.opened: list[str] = []
        self._stat_overrides: dict[str, ObjectStat | None] = {}

    def override_stat(self, key: str, stat: ObjectStat | None) -> None:
        self._stat_overrides[key] = stat

    def stat(self, key: str) -> ObjectStat | None:
        self.stat_calls.append(key)
        if key in self._stat_overrides:
            return self._stat_overrides[key]
        body = self.objects.get(key)
        if body is None:
            return None
        return ObjectStat(key, len(body), sha256(body).hexdigest())

    @contextmanager
    def open(self, key: str):
        self.opened.append(key)
        body = self.objects.get(key)
        if body is None:
            raise FileNotFoundError(key)
        yield BytesIO(body)


def client(access: Access, store: ObjectStore, current: ServingPrincipal) -> TestClient:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_serving_principal] = lambda: current
    app.dependency_overrides[get_serving_access] = lambda: access
    app.dependency_overrides[get_serving_object_store] = lambda: store
    return TestClient(app)


@pytest.mark.parametrize(
    ("provider", "status"),
    [
        (get_serving_principal, 401),
        (get_serving_access, 503),
        (get_serving_object_store, 503),
    ],
)
def test_provider_nao_configurado_falha_fechado(provider, status: int) -> None:
    with pytest.raises(HTTPException) as captured:
        provider()

    assert captured.value.status_code == status


def test_documento_servido_com_sucesso_sem_redirect_nem_url_assinada() -> None:
    body = b'{"ok": true}'
    store = ObjectStore({KEY: body})
    access_double = Access(grant=grant())

    response = client(access_double, store, principal()).get(
        f"/api/v1/dashboard/serving/cnes/{DOCUMENT}"
    )

    assert response.status_code == 200
    assert response.content == body
    assert "Location" not in response.headers
    assert response.headers["ETag"] == f'"{sha256(body).hexdigest()}"'
    assert response.headers["X-Dataset-Version"] == RUN_ID
    assert response.headers["Cache-Control"] == "private, max-age=30"
    assert store.opened == [KEY]


def test_document_name_com_ponto_falha_antes_de_autorizar() -> None:
    access_double = Access(grant=grant())
    store = ObjectStore({KEY: b"{}"})

    response = client(access_double, store, principal()).get(
        "/api/v1/dashboard/serving/cnes/overview.json"
    )

    assert response.status_code == 422
    assert response.json() == {"detail": "document_name_invalid"}
    assert access_double.requests == []
    assert store.opened == []


def test_document_name_com_traversal_nao_abre_objeto() -> None:
    access_double = Access(grant=grant())
    store = ObjectStore({KEY: b"{}"})
    app_client = client(access_double, store, principal())

    escaped = app_client.get("/api/v1/dashboard/serving/cnes/..%2Fsecret")
    literal = app_client.get("/api/v1/dashboard/serving/cnes/..")

    assert escaped.status_code == 404
    assert literal.status_code == 404
    assert access_double.requests == []
    assert store.opened == []


def test_documento_fora_do_grant_retorna_404_sem_abrir() -> None:
    other_key = f"serving/{TENANT}/{RUN_ID}/other.json"
    access_double = Access(grant=grant(object_keys=(other_key,)))
    store = ObjectStore({KEY: b"{}", other_key: b"{}"})

    response = client(access_double, store, principal()).get(
        f"/api/v1/dashboard/serving/cnes/{DOCUMENT}"
    )

    assert response.status_code == 404
    assert response.json() == {"detail": "serving_document_not_found"}
    assert store.opened == []


def test_serving_unavailable_generico_retorna_503_sem_abrir() -> None:
    access_double = Access(error=ServingUnavailable("run_manifest_missing"))
    store = ObjectStore()

    response = client(access_double, store, principal()).get(
        f"/api/v1/dashboard/serving/cnes/{DOCUMENT}"
    )

    assert response.status_code == 503
    assert response.json() == {"detail": "active_serving_unavailable"}
    assert store.opened == []


def test_membership_denied_retorna_403() -> None:
    access_double = Access(error=ServingUnavailable("membership_denied"))
    store = ObjectStore()

    response = client(access_double, store, principal()).get(
        f"/api/v1/dashboard/serving/cnes/{DOCUMENT}"
    )

    assert response.status_code == 403
    assert response.json() == {"detail": "serving_forbidden"}


def test_objeto_sumido_apos_grant_retorna_503_sem_abrir() -> None:
    access_double = Access(grant=grant())
    store = ObjectStore()

    response = client(access_double, store, principal()).get(
        f"/api/v1/dashboard/serving/cnes/{DOCUMENT}"
    )

    assert response.status_code == 503
    assert response.json() == {"detail": "active_serving_unavailable"}
    assert store.opened == []
