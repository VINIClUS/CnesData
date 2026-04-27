"""Tests for app factory wiring after AuthMiddleware integration."""
from unittest.mock import patch


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


def test_app_ordem_middleware_auth_runs_first() -> None:
    """Auth must wrap Tenant must wrap QueryCounter (runtime outer-to-inner)."""
    from central_api.middleware import (
        AuthMiddleware,
        QueryCounterMiddleware,
        TenantMiddleware,
    )
    app = _make_app()
    classes = [m.cls for m in app.user_middleware]
    assert classes[0] is AuthMiddleware
    assert classes[1] is TenantMiddleware
    assert classes[2] is QueryCounterMiddleware


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
