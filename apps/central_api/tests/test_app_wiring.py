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
