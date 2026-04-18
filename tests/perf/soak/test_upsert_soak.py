"""Soak: 30 min de upserts constantes; detecta leak de mem/FDs."""
import pytest
from sqlalchemy import text

from cnes_infra.storage.repositories.estabelecimento_repo import (
    EstabelecimentoRepository,
)
from tests.perf.soak._harness import run_soak

pytestmark = pytest.mark.soak

_DURACAO_S = 1800.0
_RPS = 50
_SLOPE_LIMITE_MB_POR_MIN = 1.0
_FD_DELTA_LIMITE = 100


def test_upsert_soak_30min(pg_perf_engine) -> None:
    with pg_perf_engine.begin() as con:
        con.execute(text("TRUNCATE gold.dim_estabelecimento CASCADE"))

    contador = iter(range(10**8))

    def _um_upsert() -> None:
        i = next(contador)
        with pg_perf_engine.begin() as con:
            EstabelecimentoRepository(con).gravar([{
                "tenant_id": "355030",
                "cnes": f"{i % 10_000:07d}",
                "fontes": {"SOAK": True},
            }])

    report = run_soak(_um_upsert, duracao_s=_DURACAO_S, rps_alvo=_RPS)
    print(
        f"rss_inicio={report.rss_mb_inicio:.1f}MB "
        f"rss_fim={report.rss_mb_fim:.1f}MB "
        f"slope={report.rss_slope_mb_por_min:.2f}MB/min "
        f"fd_delta={report.fd_delta}"
    )
    assert report.rss_slope_mb_por_min < _SLOPE_LIMITE_MB_POR_MIN, (
        f"memory_leak slope={report.rss_slope_mb_por_min:.2f}MB/min"
    )
    assert report.fd_delta < _FD_DELTA_LIMITE, (
        f"fd_leak delta={report.fd_delta}"
    )
