"""Matriz CORS de produção (PROFILE=aws + AWS_PRODUCTION_POLICY=true) e seu discriminador."""
from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from cnes_infra import config
from cnes_infra.auth import TokenInvalid
from cnes_infra.aws import AwsRuntimeConfigurationError

if TYPE_CHECKING:
    from fastapi import FastAPI
    from httpx import Response

PRODUCTION_ORIGIN = "https://cnesdata.com.br"
FOREIGN_ORIGIN = "https://evil.example"
CONTRACT_HEADERS = "authorization, content-type, x-tenant-id"
HEALTH_PATH = "/api/v1/system/health"
SESSION_COOKIE = "sessao=abc"
DENIED_ORIGINS = [
    FOREIGN_ORIGIN,
    "http://cnesdata.com.br",
    "https://www.cnesdata.com.br",
    "https://api.cnesdata.com.br",
    "https://cnesdata.com.br.evil.example",
    "https://dev.cnesdata.example",
    "null",
]


class _RejectingVerifier:
    def verify(self, token: str) -> None:
        raise TokenInvalid("signature")


def _make_app() -> "FastAPI":
    with (
        patch("central_api.app.init_telemetry"),
        patch("central_api.deps.install_rls_listener"),
        patch("central_api.deps.instrument_engine"),
        patch("central_api.deps.install_query_counter"),
        patch("central_api.deps.create_engine"),
    ):
        from central_api.app import create_app
        return create_app()


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    monkeypatch.setenv("PROFILE", "aws")
    monkeypatch.setenv("AWS_PRODUCTION_POLICY", "true")
    monkeypatch.setattr(config, "CORS_ALLOWED_ORIGINS", "https://dev.cnesdata.example,*")
    app = _make_app()
    app.state.oidc_verifier = _RejectingVerifier()
    return TestClient(app)


def _preflight(
    client: TestClient, origin: str, method: str, headers: str | None,
) -> "Response":
    request_headers = {"Origin": origin, "Access-Control-Request-Method": method}
    if headers is not None:
        request_headers["Access-Control-Request-Headers"] = headers
    return client.options("/api/v1/activate/confirm", headers=request_headers)


def _listed(response: "Response", header: str) -> set[str]:
    return {item.strip().lower() for item in response.headers[header].split(",")}


@pytest.mark.parametrize("origin", DENIED_ORIGINS)
def test_preflight_permite_so_origem_cnesdata(client: TestClient, origin: str) -> None:
    allowed = _preflight(client, PRODUCTION_ORIGIN, "POST", CONTRACT_HEADERS)
    denied = _preflight(client, origin, "POST", CONTRACT_HEADERS)

    assert allowed.status_code == 200
    assert allowed.headers["access-control-allow-origin"] == PRODUCTION_ORIGIN
    assert denied.status_code == 400
    assert "access-control-allow-origin" not in denied.headers
    assert "access-control-allow-credentials" not in denied.headers


@pytest.mark.parametrize(("method", "headers"), [
    ("GET", "authorization, x-tenant-id"),
    ("POST", CONTRACT_HEADERS),
    ("POST", None),
])
def test_preflight_permite_get_e_post_com_headers_do_contrato(
    client: TestClient, method: str, headers: str | None,
) -> None:
    response = _preflight(client, PRODUCTION_ORIGIN, method, headers)

    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] == PRODUCTION_ORIGIN
    assert _listed(response, "access-control-allow-methods") == {"get", "post"}
    assert _listed(response, "access-control-allow-headers") == {
        "authorization", "content-type", "x-tenant-id",
        "accept", "accept-language", "content-language",
    }
    assert response.headers["access-control-max-age"] == "600"
    assert "access-control-allow-credentials" not in response.headers


@pytest.mark.parametrize(("method", "headers"), [
    ("PUT", "content-type"),
    ("PATCH", "content-type"),
    ("DELETE", None),
    ("POST", f"{CONTRACT_HEADERS}, x-admin-token"),
    ("POST", "content-type, cookie"),
    ("POST", "*"),
    ("GET", "authorization, x-requested-with"),
])
def test_preflight_nega_metodo_e_header_extra(
    client: TestClient, method: str, headers: str | None,
) -> None:
    response = _preflight(client, PRODUCTION_ORIGIN, method, headers)

    assert response.status_code == 400
    assert _listed(response, "access-control-allow-methods") == {"get", "post"}
    assert "access-control-allow-credentials" not in response.headers


def test_resposta_simples_ecoa_so_a_origem_cnesdata_sem_credentials(
    client: TestClient,
) -> None:
    allowed = client.get(
        HEALTH_PATH, headers={"Origin": PRODUCTION_ORIGIN, "Cookie": SESSION_COOKIE},
    )
    denied = client.get(
        HEALTH_PATH, headers={"Origin": FOREIGN_ORIGIN, "Cookie": SESSION_COOKIE},
    )

    assert allowed.status_code == 200
    assert allowed.headers["access-control-allow-origin"] == PRODUCTION_ORIGIN
    assert "access-control-allow-credentials" not in allowed.headers
    assert denied.status_code == 200
    assert "access-control-allow-origin" not in denied.headers
    assert "access-control-allow-credentials" not in denied.headers


@pytest.mark.parametrize(("method", "path", "status"), [
    ("POST", "/api/v1/activate/confirm", 401),
    ("POST", "/activate/confirm", 404),
    ("GET", "/api/v1/dashboard/agents/status", 401),
    ("GET", "/api/v1/rota-inexistente", 404),
])
def test_respostas_de_erro_seguem_a_politica_exata(
    client: TestClient, method: str, path: str, status: int,
) -> None:
    allowed = client.request(method, path, headers={"Origin": PRODUCTION_ORIGIN})
    denied = client.request(method, path, headers={"Origin": FOREIGN_ORIGIN})

    assert allowed.status_code == status
    assert allowed.headers["access-control-allow-origin"] == PRODUCTION_ORIGIN
    assert "access-control-allow-credentials" not in allowed.headers
    assert denied.status_code == status
    assert "access-control-allow-origin" not in denied.headers
    assert "access-control-allow-credentials" not in denied.headers


@pytest.mark.parametrize("profile", ["aws", " AWS ", "Aws"])
def test_cors_policy_de_producao_e_exata(profile: str) -> None:
    from central_api.app import CorsPolicy, cors_policy

    policy = cors_policy({"PROFILE": profile, "AWS_PRODUCTION_POLICY": "true"})

    assert policy == CorsPolicy(
        allow_origins=(PRODUCTION_ORIGIN,),
        allow_methods=("GET", "POST"),
        allow_headers=("Authorization", "Content-Type", "X-Tenant-Id"),
        allow_credentials=False,
        max_age=600,
    )


@pytest.mark.parametrize("values", [
    {},
    {"AWS_PRODUCTION_POLICY": "invalida"},
    {"PROFILE": "local", "AWS_PRODUCTION_POLICY": "true"},
    {"PROFILE": "aws"},
    {"PROFILE": "aws", "AWS_PRODUCTION_POLICY": "false"},
    {"PROFILE": "awss", "AWS_PRODUCTION_POLICY": "true"},
])
def test_fora_de_producao_aws_mantem_a_politica_atual(
    monkeypatch: pytest.MonkeyPatch, values: dict[str, str],
) -> None:
    from central_api.app import CorsPolicy, cors_policy

    monkeypatch.setattr(config, "CORS_ALLOWED_ORIGINS", "https://dev.cnesdata.example")

    assert cors_policy(values) == CorsPolicy(
        allow_origins=("https://dev.cnesdata.example",),
        allow_methods=("GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"),
        allow_headers=("Authorization", "Content-Type", "X-Tenant-Id"),
        allow_credentials=True,
        max_age=600,
    )


@pytest.mark.parametrize("raw", ["TRUE", "yes", ""])
def test_production_policy_invalida_falha_no_build_da_app(
    monkeypatch: pytest.MonkeyPatch, raw: str,
) -> None:
    monkeypatch.setenv("PROFILE", "aws")
    monkeypatch.setenv("AWS_PRODUCTION_POLICY", raw)

    with pytest.raises(AwsRuntimeConfigurationError, match="production_policy_invalid"):
        _make_app()
