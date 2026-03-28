"""Testes do HistoricoReader — DuckDB Gold + CSVs arquivados."""
import duckdb
import pytest

from storage.historico_reader import HistoricoReader


def _popular_duckdb(path):
    with duckdb.connect(str(path)) as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS gold")
        con.execute("""
            CREATE TABLE gold.evolucao_metricas_mensais (
                data_competencia VARCHAR PRIMARY KEY,
                total_vinculos INTEGER,
                total_ghost INTEGER,
                total_missing INTEGER,
                total_rq005 INTEGER
            )
        """)
        con.execute("""
            CREATE TABLE gold.auditoria_resultados (
                data_competencia VARCHAR,
                regra VARCHAR,
                total_anomalias INTEGER,
                PRIMARY KEY (data_competencia, regra)
            )
        """)
        con.execute("INSERT INTO gold.evolucao_metricas_mensais VALUES ('2024-11',350,2,3,5)")
        con.execute("INSERT INTO gold.evolucao_metricas_mensais VALUES ('2024-12',357,3,2,7)")
        con.execute("INSERT INTO gold.auditoria_resultados VALUES ('2024-11','RQ008',9)")
        con.execute("INSERT INTO gold.auditoria_resultados VALUES ('2024-11','RQ006',4)")
        con.execute("INSERT INTO gold.auditoria_resultados VALUES ('2024-12','RQ008',12)")
        con.execute("INSERT INTO gold.auditoria_resultados VALUES ('2024-12','RQ006',3)")
        con.execute("INSERT INTO gold.auditoria_resultados VALUES ('2024-12','RQ009',8)")


@pytest.fixture
def reader(tmp_path):
    db = tmp_path / "test.duckdb"
    _popular_duckdb(db)
    historico = tmp_path / "historico"
    comp_dir = historico / "2024-12"
    comp_dir.mkdir(parents=True)
    (comp_dir / "auditoria_rq008_prof_fantasma_cns.csv").write_text(
        "CNS,NOME_PROFISSIONAL,CNES\n7001234567890123,Ana Silva,2795001\n",
        encoding="utf-8",
    )
    return HistoricoReader(db, historico)


def test_listar_competencias_ordem_cronologica(reader):
    resultado = reader.listar_competencias()
    assert resultado == ["2024-11", "2024-12"]


def test_carregar_tendencias_sem_filtro_retorna_tudo(reader):
    df = reader.carregar_tendencias()
    assert set(df.columns) >= {"data_competencia", "regra", "total_anomalias"}
    assert len(df) == 5  # 2 linhas em nov + 3 em dez


def test_carregar_tendencias_filtra_por_regra(reader):
    df = reader.carregar_tendencias(regras=["RQ008"])
    assert list(df["regra"].unique()) == ["RQ008"]
    assert len(df) == 2


def test_carregar_tendencias_filtra_por_periodo(reader):
    df = reader.carregar_tendencias(competencia_inicio="2024-12", competencia_fim="2024-12")
    assert list(df["data_competencia"].unique()) == ["2024-12"]
    assert len(df) == 3


def test_carregar_kpis_retorna_dict_correto(reader):
    kpis = reader.carregar_kpis("2024-12")
    assert kpis["RQ008"] == 12
    assert kpis["RQ006"] == 3
    assert kpis["RQ009"] == 8


def test_carregar_delta_calcula_variacao_correta(reader):
    delta = reader.carregar_delta("2024-12")
    assert delta["RQ008"] == 3   # 12 - 9
    assert delta["RQ006"] == -1  # 3 - 4


def test_carregar_delta_retorna_zero_para_primeira_competencia(reader):
    delta = reader.carregar_delta("2024-11")
    assert delta["RQ008"] == 0
    assert delta["RQ006"] == 0


def test_carregar_registros_retorna_dataframe_via_csv(reader):
    df = reader.carregar_registros("RQ008", "2024-12")
    assert not df.empty
    assert "CNS" in df.columns
    assert df.iloc[0]["NOME_PROFISSIONAL"] == "Ana Silva"


def test_carregar_registros_retorna_vazio_quando_csv_ausente(reader):
    df = reader.carregar_registros("RQ008", "2024-11")
    assert df.empty


def test_listar_competencias_para_regra_filtra_por_arquivo(reader):
    disponiveis = reader.listar_competencias_para_regra("RQ008")
    assert "2024-12" in disponiveis
    assert "2024-11" not in disponiveis
