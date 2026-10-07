"""Tests for app factory wiring after AuthMiddleware integration."""
from unittest.mock import patch

import pytest


def _make_app():
    with (
        patch("central_api.app.init_telemetry"),
        patch("central_api.deps.install_rls_listener"),
        patch("central_api.deps.instrument_engine"),
        patch("central_api.deps.install_query_counter"),
        patch("central_api.deps.create_engine"),
    ):
        from central_api.app import create_app
        return create_app()


def test_app_inclui_router_dashboard() -> None:
    app = _make_app()
    paths = {r.path for r in app.routes}
    assert "/api/v1/dashboard/auth/me" in paths
    assert "/api/v1/dashboard/tenants" in paths


def test_app_registra_auth_middleware() -> None:
    from central_api.middleware import AuthMiddleware
    app = _make_app()
    cls_names = {m.cls.__name__ for m in app.user_middleware}
    assert AuthMiddleware.__name__ in cls_names


def test_app_ordem_middleware_cors_depois_auth() -> None:
    """CORS answers preflight first; Auth wraps QueryCounter."""
    from fastapi.middleware.cors import CORSMiddleware

    from central_api.middleware import AuthMiddleware, QueryCounterMiddleware
    app = _make_app()
    classes = [m.cls for m in app.user_middleware]
    assert classes == [CORSMiddleware, AuthMiddleware, QueryCounterMiddleware]


def test_app_inclui_public_leads_router() -> None:
    app = _make_app()
    paths = {r.path for r in app.routes}
    assert "/api/v1/public/leads" in paths


def test_app_inclui_billing_e_webhook_routes() -> None:
    app = _make_app()
    paths = {route.path for route in app.routes}
    assert "/api/v1/billing/accounts" in paths
    assert "/api/v1/billing/accounts/{billing_account_id}/transfer" in paths
    assert "/api/v1/billing/checkout" in paths
    assert "/api/v1/billing/portal" in paths
    assert "/api/v1/billing/status" in paths
    assert "/api/v1/billing/webhooks/stripe" in paths


def test_app_inclui_access_requests_router() -> None:
    app = _make_app()
    paths = {r.path for r in app.routes}
    assert "/api/v1/dashboard/access-requests/mine" in paths
    assert "/api/v1/dashboard/access-requests" in paths
    assert "/api/v1/dashboard/access-requests/available-tenants" in paths


def test_oauth_error_handler_renderiza_body_rfc():
    """OAuthError raised inside route → 400 + {"error": "..."} body."""
    import os
    os.environ.setdefault("DB_URL", "postgresql+psycopg://u:p@localhost/x")
    os.environ.setdefault("MINIO_ENDPOINT", "x:9000")
    os.environ.setdefault("MINIO_ACCESS_KEY", "x")
    os.environ.setdefault("MINIO_SECRET_KEY", "x")
    os.environ.setdefault("MINIO_BUCKET", "x")

    from fastapi.testclient import TestClient

    from central_api.app import create_app
    from cnes_infra.auth.errors import OAuthError

    app = create_app()

    @app.get("/_test_oauth_error")
    def _raise_it():
        raise OAuthError("slow_down", description="aguarde",
                         extra={"interval": 10})

    with TestClient(app) as client:
        r = client.get("/_test_oauth_error")
    assert r.status_code == 400
    body = r.json()
    assert body["error"] == "slow_down"
    assert body["error_description"] == "aguarde"
    assert body["interval"] == 10


def test_oauth_error_handler_status_401_quando_invalid_token():
    import os
    os.environ.setdefault("DB_URL", "postgresql+psycopg://u:p@localhost/x")
    os.environ.setdefault("MINIO_ENDPOINT", "x:9000")
    os.environ.setdefault("MINIO_ACCESS_KEY", "x")
    os.environ.setdefault("MINIO_SECRET_KEY", "x")
    os.environ.setdefault("MINIO_BUCKET", "x")

    from fastapi.testclient import TestClient

    from central_api.app import create_app
    from cnes_infra.auth.errors import OAuthError

    app = create_app()

    @app.get("/_test_invalid_token")
    def _raise_it():
        raise OAuthError("invalid_token", status_code=401)

    with TestClient(app) as client:
        r = client.get("/_test_invalid_token")
    assert r.status_code == 401
    assert r.json() == {"error": "invalid_token"}


AWS_TENANT = "354130"
AWS_ISSUER = "https://id.example.test"
ACCEPTED_BEARER = "valid"
SIGNED_URL = "https://signed.example.test/serving/354130/run-01/overview.json?X-Amz-Signature=abc"


def _aws_env(monkeypatch) -> None:
    values = {
        "PROFILE": "aws",
        "AUTH_MODE": "oidc",
        "AWS_REGION": "us-east-1",
        "AWS_CONTROL_PLANE_TABLE": "cnesdata-test-control-plane",
        "AWS_DATA_BUCKET": "cnesdata-test-data",
        "AWS_AUDIT_BUCKET": "cnesdata-test-audit",
        "AWS_STATE_MACHINE_ARN": (
            "arn:aws:states:us-east-1:000000000000:stateMachine:cnesdata-test"
        ),
        "AWS_PROCESSOR_CONTAINER_NAME": "processor",
        "AWS_AUDIT_RETENTION_DAYS": "365",
        "OIDC_ISSUER": AWS_ISSUER,
        "OIDC_AUDIENCE": "cnesdata-dashboard",
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)


class _Verifier:
    def verify(self, token: str):
        from cnes_infra.auth import OidcPrincipal, TokenInvalid

        if token != ACCEPTED_BEARER:
            raise TokenInvalid("signature")
        return OidcPrincipal(
            issuer=AWS_ISSUER, subject="user-1", email=None, display_name=None,
        )


def _membership(tenant_id: str, user_id: str):
    from datetime import UTC, datetime

    from cnes_domain.control_plane.entities import Membership

    if tenant_id != AWS_TENANT:
        return None
    return Membership(
        tenant_id=tenant_id, user_id=user_id, role="viewer",
        created_at=datetime(2026, 9, 1, tzinfo=UTC), oidc_issuer=AWS_ISSUER,
    )


def _aws_runtime(signed):
    from unittest.mock import Mock

    from central_api.auth import MembershipAuthorizer
    from central_api.composition import AwsApiServices, RuntimeComponents

    control_plane = Mock(name="control_plane")
    control_plane.get_membership.side_effect = _membership
    return RuntimeComponents(
        control_plane=control_plane, object_store=Mock(), executor=Mock(), audit_sink=Mock(),
        raw_ingestion=Mock(), source_catalog=Mock(), run_planning=Mock(),
        services=AwsApiServices(
            membership_authorizer=MembershipAuthorizer(control_plane, Mock()),
            serving_access=signed,
        ),
    )


def _aws_client(monkeypatch, signed=None):
    from unittest.mock import Mock

    from fastapi import Request
    from fastapi.testclient import TestClient

    from cnes_domain.tenant import get_tenant_id

    _aws_env(monkeypatch)
    app = _make_app()

    @app.get("/api/v1/dashboard/__probe")
    def _dashboard_probe(request: Request) -> dict:
        return {"tenant": get_tenant_id(), "user": request.state.principal.subject}

    @app.get("/api/v1/edge/__probe")
    def _edge_probe() -> dict:
        return {"ok": True}

    @app.get("/provision/__probe")
    def _provision_probe() -> dict:
        return {"ok": True}

    runtime = _aws_runtime(signed or Mock())
    build = patch("central_api.deps.build_runtime", return_value=runtime)
    verifier = patch("central_api.deps.OidcVerifier", return_value=_Verifier())
    return build, verifier, TestClient(app)


def _bearer(
    credential: str = ACCEPTED_BEARER, tenant: str | None = AWS_TENANT,
) -> dict[str, str]:
    headers = {"Authorization": f"Bearer {credential}"}
    if tenant is not None:
        headers["X-Tenant-Id"] = tenant
    return headers


def test_aws_health_nao_exige_identidade(monkeypatch) -> None:
    build, verifier, client = _aws_client(monkeypatch)
    with build, verifier, client:
        response = client.get("/api/v1/system/health")

    assert response.status_code == 200


def test_aws_token_invalido_retorna_401_mesmo_em_modo_opcional(monkeypatch) -> None:
    build, verifier, client = _aws_client(monkeypatch)
    with build, verifier, client:
        client.app.state.auth_required = "optional"
        response = client.get("/api/v1/dashboard/__probe", headers=_bearer("forjado"))

    assert response.status_code == 401
    assert response.json() == {"detail": "token_invalid"}


def test_aws_rota_de_dashboard_sem_bearer_retorna_401(monkeypatch) -> None:
    build, verifier, client = _aws_client(monkeypatch)
    with build, verifier, client:
        response = client.get(
            "/api/v1/dashboard/__probe", headers={"X-Tenant-Id": AWS_TENANT},
        )

    assert response.status_code == 401
    assert response.json() == {"detail": "auth_required"}


def test_aws_rota_de_dashboard_sem_tenant_retorna_400(monkeypatch) -> None:
    build, verifier, client = _aws_client(monkeypatch)
    with build, verifier, client:
        response = client.get("/api/v1/dashboard/__probe", headers=_bearer(tenant=None))

    assert response.status_code == 400
    assert response.json() == {"detail": "tenant_header_required"}


def test_aws_membership_ausente_retorna_403(monkeypatch) -> None:
    build, verifier, client = _aws_client(monkeypatch)
    with build, verifier, client:
        response = client.get("/api/v1/dashboard/__probe", headers=_bearer(tenant="999999"))

    assert response.status_code == 403
    assert response.json() == {"detail": "tenant_not_allowed"}


def test_aws_tenant_autorizado_chega_ao_contexto_do_endpoint(monkeypatch) -> None:
    build, verifier, client = _aws_client(monkeypatch)
    with build, verifier, client:
        response = client.get("/api/v1/dashboard/__probe", headers=_bearer())

    assert response.status_code == 200
    assert response.json() == {"tenant": AWS_TENANT, "user": "user-1"}


@pytest.mark.parametrize("path", ["/api/v1/edge/__probe", "/provision/__probe"])
def test_aws_rotas_mtls_e_device_passam_sem_bearer(monkeypatch, path: str) -> None:
    build, verifier, client = _aws_client(monkeypatch)
    with build, verifier, client:
        response = client.get(path, headers={"X-Tenant-Id": "999999"})

    assert response.status_code == 200
    assert response.json() == {"ok": True}


def test_aws_bearer_valido_fora_do_dashboard_nao_exige_tenant(monkeypatch) -> None:
    build, verifier, client = _aws_client(monkeypatch)
    with build, verifier, client:
        response = client.get("/api/v1/edge/__probe", headers=_bearer(tenant=None))

    assert response.status_code == 200
    assert response.json() == {"ok": True}


def test_aws_serving_redireciona_com_principal_e_tenant_autorizados(monkeypatch) -> None:
    from datetime import UTC, datetime
    from unittest.mock import Mock

    from central_api.serving import S3SignedServingAccess, SignedServingGrant

    signed = Mock(spec=S3SignedServingAccess)
    signed.grant.return_value = SignedServingGrant(
        version_id="run-01", run_id="run-01",
        object_key="serving/354130/run-01/overview.json", url=SIGNED_URL,
        expires_at=datetime(2026, 9, 27, 12, 5, tzinfo=UTC),
    )
    build, verifier, client = _aws_client(monkeypatch, signed)
    with build, verifier, client:
        response = client.get(
            "/api/v1/dashboard/serving/cnes/overview", headers=_bearer(),
            follow_redirects=False,
        )

    assert response.status_code == 307
    assert response.headers["location"] == SIGNED_URL
    request = signed.grant.call_args.args[0]
    assert (request.access.user_id, request.access.tenant_id) == ("user-1", AWS_TENANT)
    assert request.relative_name == "overview.json"
