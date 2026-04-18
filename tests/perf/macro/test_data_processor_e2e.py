"""Macro: pipeline data_processor com 100k rows — transform + upsert."""
import pytest
from sqlalchemy import text

from cnes_domain.processing.transformer import transformar
from cnes_infra.storage.repositories.estabelecimento_repo import (
    EstabelecimentoRepository,
)

pytestmark = pytest.mark.perf_macro


def _rows_para_repo(cnes_list: list[str]) -> list[dict]:
    return [
        {
            "tenant_id": "355030",
            "cnes": cnes,
            "fontes": {"E2E": True},
        }
        for cnes in cnes_list
    ]


def test_pipeline_transform_upsert_100k(benchmark, pg_perf_engine) -> None:
    from tests.perf.micro.test_transformer_bench import _df
    df_raw = _df(100_000)

    with pg_perf_engine.begin() as con:
        con.execute(text("TRUNCATE gold.dim_estabelecimento CASCADE"))

    def _pipeline() -> None:
        transformado = transformar(df_raw)
        cnes_list = transformado.get_column("CNES").to_list()
        with pg_perf_engine.begin() as con:
            repo = EstabelecimentoRepository(con)
            repo.gravar(_rows_para_repo(cnes_list))

    benchmark.group = "pipeline_100k"
    benchmark.pedantic(_pipeline, iterations=1, rounds=3)
