"""Tests for DashboardRepo overview methods."""
import pytest
from sqlalchemy import text
from sqlalchemy.engine import Engine

from central_api.repositories.dashboard_repo import (
    DashboardRepo,
    _classify_status,
    _competencia_lag,
    _format_competencia,
    _previous_competencia,
)
from central_api.repositories.dashboard_repo_overview import (
    _build_faturamento_chart,
)
from cnes_infra.storage.dim_lookup import upsert_dim_municipio


@pytest.fixture
def repo(pg_engine: Engine) -> DashboardRepo:
    return DashboardRepo(pg_engine)


@pytest.fixture
def cleanup_overview(pg_engine: Engine):
    yield
    with pg_engine.begin() as conn:
        conn.execute(text("DELETE FROM gold.fato_producao_ambulatorial WHERE 1=1"))
        conn.execute(text("DELETE FROM gold.fato_internacao WHERE 1=1"))
        conn.execute(text("DELETE FROM gold.fato_vinculo_cnes WHERE 1=1"))
        conn.execute(text(
            "DELETE FROM gold.dim_estabelecimento WHERE sk_municipio IN "
            "(SELECT sk_municipio FROM gold.dim_municipio WHERE ibge6 IN "
            "('354130','999998','999997'))"
        ))


@pytest.mark.postgres
def test_overview_kpis_invariante_estabs_sem_producao(
    repo: DashboardRepo, pg_engine: Engine, cleanup_overview: None,
) -> None:
    with pg_engine.begin() as conn:
        upsert_dim_municipio(conn, {
            "ibge6": "354130", "ibge7": "3541308",
            "nome": "Presidente Epitácio", "uf": "SP",
        })
    kpis = repo.overview_kpis(tenant_id="354130", current_competencia=202604)
    assert kpis.estabs_sem_producao <= kpis.estabs_total
    assert kpis.competencia_atual == 202604


@pytest.mark.postgres
def test_overview_kpis_zero_quando_sem_dados(
    repo: DashboardRepo, pg_engine: Engine, cleanup_overview: None,
) -> None:
    with pg_engine.begin() as conn:
        upsert_dim_municipio(conn, {
            "ibge6": "999998", "ibge7": "9999988",
            "nome": "Vazio", "uf": "SP",
        })
    kpis = repo.overview_kpis(tenant_id="999998", current_competencia=202604)
    assert kpis.faturamento_atual_cents == 0
    assert kpis.aih_atual == 0
    assert kpis.profissionais_ativos == 0


@pytest.mark.postgres
def test_faturamento_by_establishment_retorna_n_meses(
    repo: DashboardRepo, pg_engine: Engine, cleanup_overview: None,
) -> None:
    with pg_engine.begin() as conn:
        upsert_dim_municipio(conn, {
            "ibge6": "999997", "ibge7": "9999977",
            "nome": "Test", "uf": "SP",
        })
    chart = repo.faturamento_by_establishment(
        tenant_id="999997", months=12, current_competencia=202604,
    )
    assert len(chart.series) == 12
    assert "competencia" in chart.series[0]
    assert chart.categories[-1] == "outros"


def test_competencia_lag_calcula() -> None:
    assert _competencia_lag(202604, 202604) == 0
    assert _competencia_lag(202604, 202603) == 1
    assert _competencia_lag(202604, 202601) == 3
    assert _competencia_lag(202604, None) is None


def test_classify_status_mapeia() -> None:
    assert _classify_status(None) == "no_data"
    assert _classify_status(0) == "ok"
    assert _classify_status(1) == "warning"
    assert _classify_status(3) == "error"


def test_previous_competencia_handles_year_boundary() -> None:
    assert _previous_competencia(202604) == 202603
    assert _previous_competencia(202601) == 202512
    assert _previous_competencia(202612) == 202611


def test_format_competencia_pt_br() -> None:
    assert _format_competencia(202604) == "abr/2026"
    assert _format_competencia(202612) == "dez/2026"
    assert _format_competencia(202601) == "jan/2026"


def test_build_faturamento_chart_top_e_outros() -> None:
    comps = [202603, 202604]
    top = [
        {"sk_estabelecimento": 1, "nome": "Hospital A"},
        {"sk_estabelecimento": 2, "nome": "Posto B"},
    ]
    rows = [
        {"competencia": 202603, "sk_estabelecimento": 1, "valor": 1000},
        {"competencia": 202603, "sk_estabelecimento": 2, "valor": 500},
        {"competencia": 202603, "sk_estabelecimento": 99, "valor": 50},
        {"competencia": 202604, "sk_estabelecimento": 1, "valor": 2000},
        {"competencia": 202604, "sk_estabelecimento": 99, "valor": 75},
    ]
    chart = _build_faturamento_chart(comps, top, rows)
    assert chart.categories == ["Hospital A", "Posto B", "outros"]
    assert chart.series[0]["competencia"] == "mar/2026"
    assert chart.series[0]["Hospital A"] == 1000
    assert chart.series[0]["Posto B"] == 500
    assert chart.series[0]["outros"] == 50
    assert chart.series[1]["Hospital A"] == 2000
    assert chart.series[1]["Posto B"] == 0
    assert chart.series[1]["outros"] == 75
