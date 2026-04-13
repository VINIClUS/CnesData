"""Testes de integração do pipeline CnesData — requerem banco Firebird ativo."""

import importlib
import sys
from pathlib import Path

import polars as pl
import pytest

pytestmark = pytest.mark.integration



def test_conectar_retorna_conexao_ativa():
    from cnes_infra.ingestion.cnes_client import conectar

    con = conectar()
    try:
        assert con is not None
    finally:
        con.close()


def test_conexao_executa_query_simples():
    from cnes_infra.ingestion.cnes_client import conectar

    con = conectar()
    try:
        cur = con.cursor()
        cur.execute("SELECT 1 FROM RDB$DATABASE")
        row = cur.fetchone()
        assert row is not None
        assert row[0] == 1
    finally:
        con.close()


def test_dll_inexistente_levanta_file_not_found(monkeypatch):
    import dump_agent.config as config
    from cnes_infra.ingestion import cnes_client

    monkeypatch.setattr(config, "FIREBIRD_DLL", Path("/nao/existe/fbembed.dll"))
    importlib.reload(cnes_client)

    try:
        with pytest.raises(FileNotFoundError):
            cnes_client.conectar()
    finally:
        importlib.reload(cnes_client)


@pytest.fixture(scope="module")
def con_ativa():
    from cnes_infra.ingestion.cnes_client import conectar

    con = conectar()
    yield con
    con.close()


@pytest.fixture(scope="module")
def adapter_local(con_ativa):
    from cnes_infra.ingestion.cnes_local_adapter import CnesLocalAdapter

    return CnesLocalAdapter(con_ativa)


@pytest.fixture(scope="module")
def df_profissionais(adapter_local):
    return adapter_local.listar_profissionais()


@pytest.fixture(scope="module")
def df_estabelecimentos(adapter_local):
    return adapter_local.listar_estabelecimentos()


def test_adapter_local_retorna_profissionais(df_profissionais):
    from cnes_infra.ingestion.schemas import SCHEMA_PROFISSIONAL

    assert isinstance(df_profissionais, pl.DataFrame)
    assert len(df_profissionais) >= 100
    assert df_profissionais.columns == list(SCHEMA_PROFISSIONAL)


def test_adapter_local_retorna_estabelecimentos(df_estabelecimentos):
    from cnes_infra.ingestion.schemas import SCHEMA_ESTABELECIMENTO

    assert isinstance(df_estabelecimentos, pl.DataFrame)
    assert len(df_estabelecimentos) >= 5
    assert df_estabelecimentos.columns == list(SCHEMA_ESTABELECIMENTO)


def test_profissionais_tem_cpf_preenchido(df_profissionais):
    cpfs = df_profissionais.select(
        pl.col("CPF").str.strip_chars()
    ).drop_nulls()
    assert len(cpfs) == len(df_profissionais)
    assert cpfs.filter(pl.col("CPF") == "").is_empty()


def test_profissionais_tem_cnes_7_digitos(df_profissionais):
    cnes = df_profissionais.select(
        pl.col("CNES").str.strip_chars().str.len_chars().alias("len")
    ).drop_nulls()
    assert cnes.filter(pl.col("len") != 7).is_empty()


def test_todos_registros_do_municipio_correto(df_estabelecimentos):
    import dump_agent.config as config

    cod_mun_esperado = str(config.COD_MUN_IBGE)
    cod_mun = df_estabelecimentos.select(
        pl.col("COD_MUNICIPIO").str.strip_chars()
    ).drop_nulls()
    assert cod_mun.filter(
        pl.col("COD_MUNICIPIO") != cod_mun_esperado
    ).is_empty()


@pytest.fixture(scope="module")
def pipeline_offline(tmp_path_factory):
    saida = tmp_path_factory.mktemp("pipeline_out")
    argv_original = sys.argv[:]
    sys.argv = ["main.py", "--skip-nacional", "--skip-hr", "-o", str(saida)]
    try:
        import dump_agent.main as _main
        importlib.reload(_main)
        _main.main()
    finally:
        sys.argv = argv_original
    return saida


def test_pipeline_gera_csv_principal(pipeline_offline):
    nomes = [f.name for f in pipeline_offline.glob("*.csv")]
    assert any("Relatorio_Profissionais_CNES" in n for n in nomes)


def test_pipeline_gera_xlsx(pipeline_offline):
    xlsx = list(pipeline_offline.glob("*.xlsx"))
    assert xlsx


def test_csv_tem_pelo_menos_100_linhas(pipeline_offline):
    csv_principal = next(
        f for f in pipeline_offline.glob("*.csv")
        if "Relatorio_Profissionais_CNES" in f.name
    )
    df = pl.read_csv(csv_principal, separator=";")
    assert len(df) >= 100


def test_csv_sem_cpf_nulo(pipeline_offline):
    csv_principal = next(
        f for f in pipeline_offline.glob("*.csv")
        if "Relatorio_Profissionais_CNES" in f.name
    )
    df = pl.read_csv(csv_principal, separator=";")
    assert df["CPF"].null_count() == 0


def test_pipeline_retorna_zero(monkeypatch, tmp_path):
    saida = tmp_path / "out"
    saida.mkdir()
    monkeypatch.setattr(
        sys, "argv",
        ["main.py", "--skip-nacional", "--skip-hr", "-o", str(saida)],
    )
    import main as _main
    importlib.reload(_main)
    codigo = _main.main()
    assert codigo == 0


@pytest.mark.bigquery
def test_pipeline_completo_com_nacional(monkeypatch, tmp_path):
    saida = tmp_path / "out_nacional"
    saida.mkdir()
    monkeypatch.setattr(
        sys, "argv",
        ["main.py", "--skip-hr", "-o", str(saida), "-v"],
    )
    import main as _main
    importlib.reload(_main)
    codigo = _main.main()
    assert codigo == 0

    csvs = list(saida.glob("*.csv"))
    assert len(csvs) >= 1
