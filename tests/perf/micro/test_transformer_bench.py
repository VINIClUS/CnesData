"""Micro-bench: transformer.transformar para 1k, 10k, 100k rows."""
import polars as pl
import pytest

from cnes_domain.processing.transformer import transformar

pytestmark = pytest.mark.perf_micro


def _df(n: int) -> pl.DataFrame:
    return pl.DataFrame({
        "CPF": [f"{i:011d}" for i in range(n)],
        "CNS": ["702002887429583"] * n,
        "NOME_PROFISSIONAL": [f"PROF_{i}" for i in range(n)],
        "NOME_SOCIAL": [None] * n,
        "SEXO": ["F"] * n,
        "DATA_NASCIMENTO": ["1990-01-01"] * n,
        "CBO": ["515105"] * n,
        "TIPO_VINCULO": ["010101"] * n,
        "SUS": ["S"] * n,
        "CH_TOTAL": [40] * n,
        "CH_AMBULATORIAL": [40] * n,
        "CH_OUTRAS": [0] * n,
        "CH_HOSPITALAR": [0] * n,
        "CNES": ["0985333"] * n,
        "ESTABELECIMENTO": ["UBS"] * n,
        "TIPO_UNIDADE": ["02"] * n,
        "COD_MUNICIPIO": ["354130"] * n,
        "INE": [None] * n,
        "NOME_EQUIPE": [None] * n,
        "TIPO_EQUIPE": [None] * n,
    })


@pytest.mark.parametrize("n", [1_000, 10_000, 100_000])
def test_transformar_scaling(benchmark, n: int) -> None:
    df = _df(n)
    benchmark.group = "transformar"
    benchmark(transformar, df)
