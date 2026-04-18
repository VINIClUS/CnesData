"""Spike: upsert com burst 10x; recovery p99 deve voltar a <= 1.1× pré."""
import pytest
from sqlalchemy import text

from cnes_infra.storage.repositories.estabelecimento_repo import (
    EstabelecimentoRepository,
)
from tests.perf.spike._harness import run_spike

pytestmark = pytest.mark.spike


def test_upsert_recupera_apos_burst(pg_perf_engine) -> None:
    with pg_perf_engine.begin() as con:
        con.execute(text("TRUNCATE gold.dim_estabelecimento CASCADE"))

    contador = iter(range(10**8))

    def _upsert() -> None:
        i = next(contador)
        with pg_perf_engine.begin() as con:
            EstabelecimentoRepository(con).gravar([{
                "tenant_id": "355030",
                "cnes": f"{i % 100_000:07d}",
                "fontes": {"SPIKE": True},
            }])

    report = run_spike(
        _upsert,
        baseline_rps=20, pre_s=60.0, burst_s=30.0, recovery_s=120.0,
    )
    print(
        f"pre_p99={report.pre.p99_ms:.1f}ms "
        f"burst_p99={report.burst.p99_ms:.1f}ms "
        f"recovery_p99={report.recovery.p99_ms:.1f}ms",
    )
    assert report.recovery.p99_ms <= report.pre.p99_ms * 1.1, (
        f"recovery_nao_estabilizou recovery_p99={report.recovery.p99_ms:.1f} "
        f"pre_p99={report.pre.p99_ms:.1f}"
    )
