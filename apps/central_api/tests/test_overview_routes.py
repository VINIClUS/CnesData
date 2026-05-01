"""Tests for /api/v1/dashboard/overview + /faturamento/by-establishment."""
from unittest.mock import MagicMock
from uuid import uuid4

from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from central_api.middleware import AuthenticatedUser
from central_api.repositories.dashboard_repo import (
    FaturamentoChart,
    OverviewKpis,
)
from central_api.routes import overview


def _build(user, repo) -> TestClient:
    app = FastAPI()
    app.state.dashboard_repo = repo

    @app.middleware("http")
    async def inject(request: Request, call_next):
        if user is not None:
            request.state.user = user
        return await call_next(request)

    app.include_router(overview.router, prefix="/api/v1/dashboard")
    return TestClient(app)


def _user(tenants: list[str]) -> AuthenticatedUser:
    return AuthenticatedUser(
        user_id=uuid4(), email="g@m", display_name=None,
        role="gestor", tenant_ids=tenants,
    )


def test_overview_retorna_kpis_e_audita() -> None:
    user = _user(["354130"])
    repo = MagicMock()
    repo.overview_kpis.return_value = OverviewKpis(
        competencia_atual=202604,
        faturamento_atual_cents=120_000_000,
        faturamento_anterior_cents=115_000_000,
        aih_atual=312, aih_anterior=340,
        profissionais_ativos=421, profissionais_anterior=419,
        estabs_sem_producao=7, estabs_total=124,
        estabs_sem_producao_anterior=5,
    )
    c = _build(user, repo)
    r = c.get("/api/v1/dashboard/overview", headers={"X-Tenant-Id": "354130"})
    assert r.status_code == 200
    body = r.json()
    assert body["faturamento_atual_cents"] == 120_000_000
    assert body["estabs_sem_producao"] == 7
    actions = [c.kwargs["action"] for c in repo.log_action.call_args_list]
    assert "view_overview" in actions


def test_faturamento_chart_retorna_series() -> None:
    user = _user(["354130"])
    repo = MagicMock()
    repo.faturamento_by_establishment.return_value = FaturamentoChart(
        series=[{"competencia": "abr/2026", "UBS X": 1000, "outros": 200}],
        categories=["UBS X", "outros"],
    )
    c = _build(user, repo)
    r = c.get(
        "/api/v1/dashboard/faturamento/by-establishment?months=12",
        headers={"X-Tenant-Id": "354130"},
    )
    assert r.status_code == 200
    body = r.json()
    assert "series" in body
    assert "categories" in body
    actions = [c.kwargs["action"] for c in repo.log_action.call_args_list]
    assert "view_faturamento" in actions


def test_overview_responde_403_tenant_nao_pertence() -> None:
    user = _user(["354130"])
    c = _build(user, MagicMock())
    r = c.get("/api/v1/dashboard/overview", headers={"X-Tenant-Id": "999999"})
    assert r.status_code == 403


def test_overview_responde_401_sem_user() -> None:
    c = _build(None, MagicMock())
    r = c.get("/api/v1/dashboard/overview", headers={"X-Tenant-Id": "354130"})
    assert r.status_code == 401


def test_overview_emite_cache_control() -> None:
    user = _user(["354130"])
    repo = MagicMock()
    repo.overview_kpis.return_value = OverviewKpis(
        competencia_atual=202604,
        faturamento_atual_cents=0,
        faturamento_anterior_cents=0,
        aih_atual=0, aih_anterior=0,
        profissionais_ativos=0, profissionais_anterior=0,
        estabs_sem_producao=0, estabs_total=0,
        estabs_sem_producao_anterior=0,
    )
    c = _build(user, repo)
    r = c.get("/api/v1/dashboard/overview", headers={"X-Tenant-Id": "354130"})
    assert "max-age=30" in r.headers.get("Cache-Control", "")


def test_faturamento_chart_responde_400_se_months_invalido() -> None:
    user = _user(["354130"])
    repo = MagicMock()
    c = _build(user, repo)
    r = c.get(
        "/api/v1/dashboard/faturamento/by-establishment?months=0",
        headers={"X-Tenant-Id": "354130"},
    )
    assert r.status_code == 422
