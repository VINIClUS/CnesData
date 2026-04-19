"""Micro-bench: EstabelecimentoRepository.gravar chunks 1k/10k."""
import pytest
from sqlalchemy import text

from cnes_infra.storage.repositories.estabelecimento_repo import (
    EstabelecimentoRepository,
)

pytestmark = pytest.mark.perf_micro


def _rows(n: int) -> list[dict]:
    return [
        {
            "tenant_id": "355030",
            "cnes": f"{i:07d}",
            "fontes": {"PERF": True},
        }
        for i in range(n)
    ]


@pytest.mark.parametrize("n", [1_000, 10_000])
def test_gravar_scaling(benchmark, pg_perf_engine, n: int) -> None:
    with pg_perf_engine.begin() as con:
        con.execute(text("TRUNCATE gold.dim_estabelecimento CASCADE"))
    with pg_perf_engine.begin() as con:
        repo = EstabelecimentoRepository(con)
        benchmark.group = "gravar"
        benchmark(repo.gravar, _rows(n))
