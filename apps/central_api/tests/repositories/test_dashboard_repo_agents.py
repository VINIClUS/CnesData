"""Tests for DashboardRepo.agent_status + recent_runs against landing.extractions."""
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text
from sqlalchemy.engine import Engine

from central_api.repositories.dashboard_repo import DashboardRepo


@pytest.fixture
def repo(pg_engine: Engine) -> DashboardRepo:
    return DashboardRepo(pg_engine)


def _seed(
    pg_engine: Engine, src: str, comp: str, registered: datetime,
    tenant: str = "354130", row_count: int = 100,
) -> None:
    files_json = (
        '[{"object_key": "k/x.parquet", "row_count": '
        + str(row_count) + ', "sha256": "' + ("a" * 64) + '"}]'
    )
    with pg_engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO landing.extractions
              (job_id, tenant_id, source_type, competencia, files,
               status, registered_at)
            VALUES (
                gen_random_uuid(), :t, :src, CAST(:comp AS DATE),
                CAST(:files AS JSONB), 'PENDING', :registered
            )
        """), {
            "t": tenant, "src": src, "comp": comp,
            "files": files_json, "registered": registered,
        })


@pytest.fixture
def cleanup_extractions(pg_engine: Engine):
    yield
    with pg_engine.begin() as conn:
        conn.execute(text(
            "DELETE FROM landing.extractions "
            "WHERE tenant_id IN ('354130','999998','999997')"
        ))


@pytest.mark.postgres
def test_agent_status_agrega_por_fonte(
    repo: DashboardRepo, pg_engine: Engine, cleanup_extractions: None,
) -> None:
    now = datetime.now(UTC)
    _seed(pg_engine, "CNES_LOCAL", "2026-04-01", now - timedelta(hours=6))
    _seed(pg_engine, "SIHD", "2026-04-01", now - timedelta(hours=5))
    _seed(pg_engine, "SIA_LOCAL", "2026-03-01", now - timedelta(days=32))
    rows = repo.agent_status(
        tenant_id="354130", current_competencia=202604,
    )
    by_src = {r.fonte_sistema: r for r in rows}
    assert by_src["CNES_LOCAL"].lag_months == 0
    assert by_src["CNES_LOCAL"].status == "ok"
    assert by_src["CNES_LOCAL"].row_count == 100
    assert by_src["SIA_LOCAL"].lag_months == 1
    assert by_src["SIA_LOCAL"].status == "warning"


@pytest.mark.postgres
def test_agent_status_marca_no_data_para_fontes_sem_extracao(
    repo: DashboardRepo, cleanup_extractions: None,
) -> None:
    rows = repo.agent_status(
        tenant_id="999998", current_competencia=202604,
    )
    assert all(r.status == "no_data" for r in rows)
    assert {r.fonte_sistema for r in rows} == {
        "CNES_LOCAL", "CNES_NACIONAL", "SIHD", "BPA_MAG", "SIA_LOCAL",
    }


@pytest.mark.postgres
def test_recent_runs_limita_e_ordena_por_ts_desc(
    repo: DashboardRepo, pg_engine: Engine, cleanup_extractions: None,
) -> None:
    now = datetime.now(UTC)
    months = ["2026-04-01", "2026-03-01", "2026-02-01", "2026-01-01"]
    for i in range(25):
        comp = months[i % 4]
        _seed(pg_engine, "CNES_LOCAL", comp,
              now - timedelta(minutes=i), tenant="999997")
    rows = repo.recent_runs(tenant_id="999997", limit=20)
    assert len(rows) == 20
    assert rows[0].extracao_ts > rows[-1].extracao_ts
    assert all(r.fonte_sistema == "CNES_LOCAL" for r in rows)


@pytest.mark.postgres
def test_recent_runs_retorna_vazio_sem_dados(
    repo: DashboardRepo, cleanup_extractions: None,
) -> None:
    rows = repo.recent_runs(tenant_id="999998", limit=20)
    assert rows == []


def test_competencia_lag_calcula_meses() -> None:
    from central_api.repositories.dashboard_repo import _competencia_lag
    assert _competencia_lag(202604, 202604) == 0
    assert _competencia_lag(202604, 202603) == 1
    assert _competencia_lag(202604, 202601) == 3
    assert _competencia_lag(202604, None) is None


def test_classify_status_mapeia_lag() -> None:
    from central_api.repositories.dashboard_repo import _classify_status
    assert _classify_status(None) == "no_data"
    assert _classify_status(0) == "ok"
    assert _classify_status(1) == "warning"
    assert _classify_status(3) == "error"
