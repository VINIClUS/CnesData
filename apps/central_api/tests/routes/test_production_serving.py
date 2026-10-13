"""Envelope 200 do serving aws, assinado pelo cliente S3 de produção e sem rede."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import parse_qs, urlsplit

import boto3
import pytest
from botocore.exceptions import EndpointConnectionError
from fastapi import FastAPI
from fastapi.testclient import TestClient

from central_api.routes.serving import (
    ServingEnvelope,
    ServingPrincipal,
    get_serving_delivery,
    get_serving_principal,
    router,
    signed_serving_delivery,
)
from central_api.services.serving_access import ServingUnavailable
from central_api.serving.aws_signed import (
    S3SignedServingAccess,
    ServingKeyForbidden,
    SignedServingRequest,
    SignedServingSettings,
)
from cnes_domain.ports.object_store import ObjectStat
from cnes_domain.ports.serving import ServingGrant, ServingRequest
from cnes_infra.aws.runtime import create_aws_clients
from cnes_infra.aws.settings import AwsRuntimeSettings
from packages.cnes_infra.tests.aws.production_values import PRODUCTION_VALUES

if TYPE_CHECKING:
    from botocore.client import BaseClient
    from httpx import Response
    from types_boto3_s3.client import S3Client

    from cnes_domain.ports.object_store import ObjectStorePort
    from cnes_domain.ports.serving import ServingAccessPort

NOW = datetime(2026, 10, 12, 12, tzinfo=UTC)
TENANT = "354130"
USER = "user-1"
RUN_ID = "run-01"
KEY = f"serving/{TENANT}/{RUN_ID}/overview.json"
ROUTE = "/api/v1/dashboard/serving/cnes/overview"
REGIONAL_HOST = "cnesdata-prod-data.s3.us-east-2.amazonaws.com"
NO_STORE = "private, no-store"
SETTINGS = AwsRuntimeSettings.from_mapping(PRODUCTION_VALUES)
ACCESS_REQUEST = ServingRequest(user_id=USER, tenant_id=TENANT, dataset_name="cnes")
LOGGERS = ("central_api", "cnes_domain", "cnes_infra", "uvicorn", "uvicorn.error")


class Policy:
    def __init__(self, grant: ServingGrant | None = None, error: Exception | None = None) -> None:
        self.grant = grant or ServingGrant(
            tenant_id=TENANT, run_id=RUN_ID, version_id=RUN_ID, object_keys=(KEY,),
        )
        self.error = error
        self.requests: list[ServingRequest] = []

    def authorize(self, request: ServingRequest) -> ServingGrant:
        self.requests.append(request)
        if self.error is not None:
            raise self.error
        return self.grant


class Store:
    def __init__(self, present: bool = True) -> None:
        self.present = present
        self.stats: list[str] = []

    def stat(self, key: str) -> ObjectStat | None:
        self.stats.append(key)
        return ObjectStat(key, 2, "0" * 64) if self.present else None


class RecordingSigner:
    def __init__(self, delegate: S3Client) -> None:
        self._delegate = delegate
        self.calls: list[dict[str, Any]] = []

    def generate_presigned_url(self, operation: str, **request: Any) -> str:
        self.calls.append({"operation": operation, **request})
        return self._delegate.generate_presigned_url(operation, **request)


@pytest.fixture
def signer(monkeypatch: pytest.MonkeyPatch) -> RecordingSigner:
    for name in ("AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3"):
        monkeypatch.delenv(name, raising=False)
    session = boto3.Session(
        aws_access_key_id="AKIAPRODSERVINGTEST", aws_secret_access_key="test",  # noqa: S106
    )
    return RecordingSigner(cast("S3Client", create_aws_clients(SETTINGS, session).s3))


def _access(
    signer: RecordingSigner, policy: Policy | None = None, store: Store | None = None,
    ttl: int = SETTINGS.serving_url_ttl_seconds,
) -> S3SignedServingAccess:
    return S3SignedServingAccess(
        cast("ServingAccessPort", policy or Policy()),
        cast("ObjectStorePort", store or Store()),
        cast("BaseClient", signer),
        SignedServingSettings(SETTINGS.data_bucket, ttl),
    )


def _get(access: S3SignedServingAccess) -> Response:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_serving_principal] = lambda: ServingPrincipal(TENANT, USER)
    app.dependency_overrides[get_serving_delivery] = lambda: signed_serving_delivery(
        access, lambda: NOW,
    )
    return TestClient(app).get(ROUTE, follow_redirects=False)


def _secrets(url: str) -> tuple[str, ...]:
    query = urlsplit(url).query
    return url, query, parse_qs(query)["X-Amz-Signature"][0], KEY


def _capture_logs(caplog: pytest.LogCaptureFixture) -> None:
    # botocore's own DEBUG lines print the canonical request; the API never enables them.
    caplog.set_level(logging.INFO)
    for name in LOGGERS:
        caplog.set_level(logging.DEBUG, logger=name)


def test_envelope_tem_so_url_version_e_expires(signer: RecordingSigner) -> None:
    response = _get(_access(signer))

    body = response.json()
    url = urlsplit(body["url"])
    assert response.status_code == 200
    assert set(body) == {"url", "version_id", "expires_in"}
    assert (body["version_id"], body["expires_in"]) == (RUN_ID, 300)
    assert (url.scheme, url.netloc, url.path) == ("https", REGIONAL_HOST, f"/{KEY}")
    assert parse_qs(url.query)["X-Amz-Expires"] == ["300"]
    assert "location" not in response.headers


def test_envelope_e_privado_sem_cache(signer: RecordingSigner) -> None:
    response = _get(_access(signer))

    query = parse_qs(urlsplit(response.json()["url"]).query)
    assert response.headers["cache-control"] == NO_STORE
    assert query["response-cache-control"] == [NO_STORE]
    assert query["response-content-type"] == ["application/json"]


@pytest.mark.parametrize(
    ("code", "present", "status", "detail"),
    [
        ("membership_denied", True, 403, "serving_forbidden"),
        ("entitlement_denied", True, 403, "serving_entitlement_denied"),
        ("entitlement_unavailable", True, 503, "billing_dependency_unavailable"),
        ("active_pointer_missing", True, 503, "active_serving_unavailable"),
        (None, False, 503, "active_serving_unavailable"),
    ],
    ids=["membership", "entitlement", "billing-indisponivel", "pointer", "objeto-ausente"],
)
def test_autoriza_e_consulta_o_objeto_antes_de_assinar(
    signer: RecordingSigner, code: str | None, present: bool, status: int, detail: str,
) -> None:
    policy = Policy(error=None if code is None else ServingUnavailable(code))
    store = Store(present=present)

    response = _get(_access(signer, policy, store))

    assert (response.status_code, response.json()) == (status, {"detail": detail})
    assert "location" not in response.headers
    assert policy.requests == [ACCESS_REQUEST]
    assert store.stats == ([] if code else [KEY])
    assert signer.calls == []


@pytest.mark.parametrize("prefix", ["raw", "normalized", "reconciliation", "tmp", "audit"])
def test_nega_prefixos_nao_serving(signer: RecordingSigner, prefix: str) -> None:
    hostile = ServingGrant.model_construct(
        tenant_id=TENANT, run_id=RUN_ID, version_id=RUN_ID,
        object_keys=(f"{prefix}/{TENANT}/{RUN_ID}/overview.json",),
    )
    store = Store()
    access = _access(signer, Policy(hostile), store)

    response = _get(access)
    for relative_name in (f"../../../{prefix}/{TENANT}/overview.json", f"../{prefix}.json"):
        with pytest.raises(ServingKeyForbidden, match=r"^serving_key_forbidden$"):
            access.grant(SignedServingRequest(ACCESS_REQUEST, relative_name), NOW)

    assert (response.status_code, response.json()) == (
        404, {"detail": "serving_document_not_found"},
    )
    assert (store.stats, signer.calls) == ([], [])


@pytest.mark.parametrize("ttl", [30, 900])
def test_envelope_declara_o_ttl_configurado_fora_da_producao(
    signer: RecordingSigner, ttl: int,
) -> None:
    body = _get(_access(signer, ttl=ttl)).json()

    assert body["expires_in"] == ttl
    assert parse_qs(urlsplit(body["url"]).query)["X-Amz-Expires"] == [str(ttl)]


def test_falha_de_transporte_s3_na_autorizacao_vira_503_sem_chave_no_log(
    signer: RecordingSigner, caplog: pytest.LogCaptureFixture,
) -> None:
    _capture_logs(caplog)
    endpoint = f"https://{REGIONAL_HOST}/{KEY}"
    store = Store()
    policy = Policy(error=EndpointConnectionError(endpoint_url=endpoint))

    response = _get(_access(signer, policy, store))

    assert (response.status_code, response.json()) == (
        503, {"detail": "active_serving_unavailable"},
    )
    assert ROUTE in caplog.text
    for secret in (endpoint, KEY, SETTINGS.data_bucket):
        assert secret not in caplog.text
    assert [
        (record.name, record.levelname, record.getMessage(), record.exc_info)
        for record in caplog.records
        if record.levelno >= logging.WARNING
    ] == [(
        "central_api.serving.aws_signed", "WARNING",
        "serving_authorization_lookup_failed error=EndpointConnectionError", None,
    )]
    assert policy.requests == [ACCESS_REQUEST]
    assert (store.stats, signer.calls) == ([], [])


def test_url_assinada_nunca_aparece_em_log(
    signer: RecordingSigner, caplog: pytest.LogCaptureFixture,
) -> None:
    _capture_logs(caplog)

    granted = _get(_access(signer))
    denied = _get(_access(signer, Policy(error=ServingUnavailable("entitlement_unavailable"))))
    missing = _get(_access(signer, store=Store(present=False)))

    assert ROUTE in caplog.text
    for secret in _secrets(granted.json()["url"]):
        assert secret not in caplog.text
        assert secret not in denied.text + missing.text


def test_envelope_e_grant_nao_vazam_url_nem_chave_no_repr(signer: RecordingSigner) -> None:
    grant = _access(signer).grant(SignedServingRequest(ACCESS_REQUEST, "overview.json"), NOW)
    envelope = ServingEnvelope(
        url=grant.url, version_id=grant.version_id, expires_in=grant.expires_in,
    )

    rendered = " ".join((repr(grant), str(grant), repr(envelope), str(envelope)))
    for secret in _secrets(grant.url):
        assert secret not in rendered
    assert envelope.model_dump() == {"url": grant.url, "version_id": RUN_ID, "expires_in": 300}
