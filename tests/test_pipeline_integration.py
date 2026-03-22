"""Testes de integração do pipeline CnesData — requerem banco Firebird ativo."""

import importlib
import sys
from pathlib import Path

import pandas as pd
import pytest

pytestmark = pytest.mark.integration

_SRC = Path(__file__).parent.parent / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))


def test_conectar_retorna_conexao_ativa():
    from ingestion.cnes_client import conectar

    con = conectar()
    try:
        assert con is not None
    finally:
        con.close()


def test_conexao_executa_query_simples():
    from ingestion.cnes_client import conectar

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
    import config
    from ingestion import cnes_client

    monkeypatch.setattr(config, "FIREBIRD_DLL", Path("/nao/existe/fbembed.dll"))
    importlib.reload(cnes_client)

    try:
        with pytest.raises(FileNotFoundError):
            cnes_client.conectar()
    finally:
        importlib.reload(cnes_client)


@pytest.fixture(scope="module")
def con_ativa():
    from ingestion.cnes_client import conectar

    con = conectar()
    yield con
    con.close()


@pytest.fixture(scope="module")
def adapter_local(con_ativa):
    from ingestion.cnes_local_adapter import CnesLocalAdapter

    return CnesLocalAdapter(con_ativa)


@pytest.fixture(scope="module")
def df_profissionais(adapter_local):
    return adapter_local.listar_profissionais()


@pytest.fixture(scope="module")
def df_estabelecimentos(adapter_local):
    return adapter_local.listar_estabelecimentos()


def test_adapter_local_retorna_profissionais(df_profissionais):
    from ingestion.schemas import SCHEMA_PROFISSIONAL

    assert isinstance(df_profissionais, pd.DataFrame)
    assert len(df_profissionais) >= 100
    assert list(df_profissionais.columns) == list(SCHEMA_PROFISSIONAL)


def test_adapter_local_retorna_estabelecimentos(df_estabelecimentos):
    from ingestion.schemas import SCHEMA_ESTABELECIMENTO

    assert isinstance(df_estabelecimentos, pd.DataFrame)
    assert len(df_estabelecimentos) >= 5
    assert list(df_estabelecimentos.columns) == list(SCHEMA_ESTABELECIMENTO)


def test_profissionais_tem_cpf_preenchido(df_profissionais):
    cpfs = df_profissionais["CPF"].dropna().str.strip()
    assert cpfs.notna().all()
    assert (cpfs != "").all()


def test_profissionais_tem_cnes_7_digitos(df_profissionais):
    cnes_vals = df_profissionais["CNES"].dropna().str.strip()
    assert (cnes_vals.str.len() == 7).all(), "Todos os CNES devem ter exatamente 7 digitos"


def test_todos_registros_do_municipio_correto(df_estabelecimentos):
    import config

    cod_mun_esperado = str(config.COD_MUN_IBGE)
    cod_mun_real = df_estabelecimentos["COD_MUNICIPIO"].dropna().str.strip()
    assert (cod_mun_real == cod_mun_esperado).all(), (
        f"Todos estabelecimentos devem ter COD_MUNICIPIO={cod_mun_esperado}"
    )


@pytest.fixture(scope="module")
def pipeline_offline(tmp_path_factory):
    saida = tmp_path_factory.mktemp("pipeline_out")
    argv_original = sys.argv[:]
    sys.argv = ["main.py", "--skip-nacional", "--skip-hr", "-o", str(saida)]
    try:
        import main as _main
        importlib.reload(_main)
        _main.main()
    finally:
        sys.argv = argv_original
    return saida


def test_pipeline_gera_csv_principal(pipeline_offline):
    nomes = [f.name for f in pipeline_offline.glob("*.csv")]
    assert any("Relatorio_Profissionais_CNES" in n for n in nomes), (
        f"CSV principal nao encontrado em {pipeline_offline}. Arquivos: {nomes}"
    )


def test_pipeline_gera_xlsx(pipeline_offline):
    xlsx = list(pipeline_offline.glob("*.xlsx"))
    assert xlsx, f"Nenhum .xlsx gerado em {pipeline_offline}"


def test_csv_tem_pelo_menos_100_linhas(pipeline_offline):
    csv_principal = next(
        f for f in pipeline_offline.glob("*.csv")
        if "Relatorio_Profissionais_CNES" in f.name
    )
    df = pd.read_csv(csv_principal, sep=";", encoding="utf-8-sig")
    assert len(df) >= 100, f"CSV com apenas {len(df)} linhas — esperado >= 100"


def test_csv_sem_cpf_nulo(pipeline_offline):
    csv_principal = next(
        f for f in pipeline_offline.glob("*.csv")
        if "Relatorio_Profissionais_CNES" in f.name
    )
    df = pd.read_csv(csv_principal, sep=";", encoding="utf-8-sig")
    assert df["CPF"].notna().all(), "CSV final contem CPFs nulos"


def test_pipeline_retorna_zero(monkeypatch, tmp_path):
    saida = tmp_path / "out"
    saida.mkdir()
    monkeypatch.setattr(sys, "argv", ["main.py", "--skip-nacional", "--skip-hr", "-o", str(saida)])
    import main as _main
    importlib.reload(_main)
    codigo = _main.main()
    assert codigo == 0, f"main() retornou {codigo}, esperado 0"


@pytest.mark.bigquery
def test_pipeline_completo_com_nacional(monkeypatch, tmp_path):
    saida = tmp_path / "out_nacional"
    saida.mkdir()
    monkeypatch.setattr(sys, "argv", ["main.py", "--skip-hr", "-o", str(saida), "-v"])
    import main as _main
    importlib.reload(_main)
    codigo = _main.main()
    assert codigo == 0

    csvs = list(saida.glob("*.csv"))
    assert len(csvs) >= 1, "Pipeline nacional nao gerou nenhum CSV"

    nomes_cross_check = {
        "auditoria_rq006_estab_fantasma.csv",
        "auditoria_rq007_estab_ausente_local.csv",
        "auditoria_rq008_prof_fantasma_cns.csv",
        "auditoria_rq009_prof_ausente_local_cns.csv",
    }
    gerados = {f.name for f in csvs} & nomes_cross_check
    assert gerados, "Nenhum CSV de cross-check nacional gerado. Arquivos: {f.name for f in csvs}"
