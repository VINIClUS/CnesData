"""Stress: rampa de upserts para achar break-point."""
import pytest
from sqlalchemy import text

from cnes_infra.storage.repositories.estabelecimento_repo import EstabelecimentoRepository
from tests.perf.stress._harness import break_point, rampa_sync

pytestmark = pytest.mark.stress

_BASELINE_BREAK_POINT_RPS = 500


def test_upsert_break_point(pg_perf_engine) -> None:
    with pg_perf_engine.begin() as con:
        con.execute(text("TRUNCATE gold.dim_estabelecimento CASCADE"))

    contador = iter(range(10**6))

    def _um_upsert() -> None:
        i = next(contador)
        with pg_perf_engine.begin() as con:
            EstabelecimentoRepository(con).gravar([{
                "tenant_id": "355030",
                "cnes": f"{i:07d}",
                "fontes": {"STRESS": True},
            }])

    results = rampa_sync(
        _um_upsert, rps_values=[100, 250, 500, 1000, 2500], duracao_s=15.0,
    )
    for r in results:
        print(
            f"rps_alvo={r.rps_alvo} rps_obs={r.rps_observado:.1f} "
            f"p50={r.p50_ms:.1f}ms p99={r.p99_ms:.1f}ms err={r.error_rate:.3%}"
        )
    bp = break_point(results)
    assert bp >= _BASELINE_BREAK_POINT_RPS, (
        f"break-point regrediu: {bp} < {_BASELINE_BREAK_POINT_RPS}"
    )
