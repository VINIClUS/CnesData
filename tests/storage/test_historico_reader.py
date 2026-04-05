"""Testes do HistoricoReader — DuckDB Gold + CSVs arquivados."""
from datetime import datetime

import duckdb
import pytest

from storage.historico_reader import HistoricoReader


def _criar_schema_base(con) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS gold")
    con.execute("""
        CREATE TABLE IF NOT EXISTS gold.evolucao_metricas_mensais (
            data_competencia VARCHAR PRIMARY KEY,
            total_vinculos INTEGER,
            total_ghost INTEGER,
            total_missing INTEGER,
            total_rq005 INTEGER,
            gravado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS gold.auditoria_resultados (
            data_competencia VARCHAR,
            regra VARCHAR,
            total_anomalias INTEGER,
            PRIMARY KEY (data_competencia, regra)
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS gold.pipeline_runs (
            competencia VARCHAR PRIMARY KEY,
            local_disponivel BOOLEAN,
            nacional_disponivel BOOLEAN,
            hr_disponivel BOOLEAN,
            status VARCHAR,
            iniciado_em TIMESTAMP,
            concluido_em TIMESTAMP
        )
    """)


def _popular_duckdb(path):
    with duckdb.connect(str(path)) as con:
        _criar_schema_base(con)
        con.execute("INSERT INTO gold.evolucao_metricas_mensais VALUES ('2024-11',350,2,3,5,NULL)")
        con.execute("INSERT INTO gold.evolucao_metricas_mensais VALUES ('2024-12',357,3,2,7,NULL)")
        con.execute("INSERT INTO gold.auditoria_resultados VALUES ('2024-11','RQ008',9)")
        con.execute("INSERT INTO gold.auditoria_resultados VALUES ('2024-11','RQ006',4)")
        con.execute("INSERT INTO gold.auditoria_resultados VALUES ('2024-12','RQ008',12)")
        con.execute("INSERT INTO gold.auditoria_resultados VALUES ('2024-12','RQ006',3)")
        con.execute("INSERT INTO gold.auditoria_resultados VALUES ('2024-12','RQ009',8)")
        con.execute(
            "INSERT INTO gold.pipeline_runs VALUES (?,?,?,?,?,?,?)",
            ["2024-11", True, False, False, "parcial", None, None],
        )
        con.execute(
            "INSERT INTO gold.pipeline_runs VALUES (?,?,?,?,?,?,?)",
            ["2024-12", True, True, False, "completo", None, None],
        )


@pytest.fixture
def reader(tmp_path):
    db = tmp_path / "test.duckdb"
    _popular_duckdb(db)
    return HistoricoReader(db, tmp_path / "historico")


def test_listar_competencias_ordem_cronologica(reader):
    resultado = reader.listar_competencias()
    assert resultado == ["2024-11", "2024-12"]


def test_listar_competencias_inclui_nacional_only(tmp_path):
    db = tmp_path / "nacional_only.duckdb"
    with duckdb.connect(str(db)) as con:
        _criar_schema_base(con)
        con.execute(
            "INSERT INTO gold.pipeline_runs VALUES (?,?,?,?,?,?,?)",
            ["2025-01", False, True, False, "sem_dados_locais", None, None],
        )
    r = HistoricoReader(db, tmp_path / "historico")
    assert r.listar_competencias() == ["2025-01"]


def test_carregar_tendencias_sem_filtro_retorna_tudo(reader):
    df = reader.carregar_tendencias()
    assert set(df.columns) >= {"data_competencia", "regra", "total_anomalias"}
    assert len(df) == 2 + 3


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
    assert delta["RQ008"] == 12 - 9
    assert delta["RQ006"] == 3 - 4


def test_carregar_delta_retorna_zero_para_primeira_competencia(reader):
    delta = reader.carregar_delta("2024-11")
    assert delta["RQ008"] == 0
    assert delta["RQ006"] == 0


def test_carregar_total_vinculos_retorna_valor_correto(reader):
    assert reader.carregar_total_vinculos("2024-12") == 357


def test_carregar_total_vinculos_retorna_zero_quando_competencia_ausente(reader):
    assert reader.carregar_total_vinculos("2099-01") == 0


def _popular_duckdb_com_timestamps(path):
    with duckdb.connect(str(path)) as con:
        _criar_schema_base(con)
        con.execute(
            "INSERT INTO gold.evolucao_metricas_mensais VALUES (?,?,?,?,?,?)",
            ["2024-11", 350, 2, 3, 5, datetime(2024, 11, 10)],
        )
        con.execute(
            "INSERT INTO gold.evolucao_metricas_mensais VALUES (?,?,?,?,?,?)",
            ["2024-12", 357, 3, 2, 7, datetime(2024, 12, 10)],
        )
        con.execute(
            "INSERT INTO gold.evolucao_metricas_mensais VALUES (?,?,?,?,?,?)",
            ["2024-10", 300, 1, 1, 3, datetime(2024, 11, 20)],
        )
        _sql_run = "INSERT INTO gold.pipeline_runs VALUES (?,?,?,?,?,?,?)"
        con.execute(_sql_run, ["2024-11", True, False, False, "parcial",
                               datetime(2024, 11, 10), datetime(2024, 11, 10)])
        con.execute(_sql_run, ["2024-12", True, True, False, "completo",
                               datetime(2024, 12, 10), datetime(2024, 12, 10)])
        con.execute(_sql_run, ["2024-10", False, False, False, "sem_dados_locais",
                               datetime(2024, 11, 20), datetime(2024, 11, 20)])


@pytest.fixture
def reader_com_timestamps(tmp_path):
    db = tmp_path / "test_ts.duckdb"
    _popular_duckdb_com_timestamps(db)
    return HistoricoReader(db, tmp_path / "historico")


class TestListarCompetenciasValidas:

    def test_exclui_competencia_sem_local_disponivel(self, reader_com_timestamps):
        validas = reader_com_timestamps.listar_competencias_validas()
        assert "2024-10" not in validas

    def test_inclui_competencias_com_local_disponivel(self, reader_com_timestamps):
        validas = reader_com_timestamps.listar_competencias_validas()
        assert "2024-11" in validas
        assert "2024-12" in validas

    def test_retorna_em_ordem_cronologica(self, reader_com_timestamps):
        validas = reader_com_timestamps.listar_competencias_validas()
        assert validas == sorted(validas)

    def test_retorna_lista_vazia_quando_sem_dados(self, tmp_path):
        db = tmp_path / "empty.duckdb"
        with duckdb.connect(str(db)) as con:
            _criar_schema_base(con)
        r = HistoricoReader(db, tmp_path / "historico")
        assert r.listar_competencias_validas() == []


class TestContarCompetencias:

    def test_retorna_validas_e_total(self, reader_com_timestamps):
        validas, total = reader_com_timestamps.contar_competencias()
        assert total == 3
        assert validas == 2

    def test_zeros_quando_sem_dados(self, tmp_path):
        db = tmp_path / "empty2.duckdb"
        with duckdb.connect(str(db)) as con:
            _criar_schema_base(con)
        r = HistoricoReader(db, tmp_path / "historico")
        assert r.contar_competencias() == (0, 0)


@pytest.fixture
def reader_com_runs(tmp_path):
    db_path = tmp_path / "test.duckdb"
    db = duckdb.connect(str(db_path))
    db.execute("CREATE SCHEMA gold")
    db.execute("""CREATE TABLE gold.pipeline_runs (
        competencia VARCHAR PRIMARY KEY,
        local_disponivel BOOLEAN,
        nacional_disponivel BOOLEAN,
        hr_disponivel BOOLEAN,
        status VARCHAR,
        iniciado_em TIMESTAMP,
        concluido_em TIMESTAMP
    )""")
    db.execute("INSERT INTO gold.pipeline_runs VALUES ('2024-01', TRUE, FALSE, FALSE, 'parcial', NULL, NULL)")
    db.execute("INSERT INTO gold.pipeline_runs VALUES ('2024-02', FALSE, TRUE, FALSE, 'sem_dados_locais', NULL, NULL)")
    db.execute("INSERT INTO gold.pipeline_runs VALUES ('2024-03', TRUE, TRUE, FALSE, 'completo', NULL, NULL)")
    db.close()
    return HistoricoReader(db_path, tmp_path)


def test_listar_competencias_validas_usa_local_disponivel(reader_com_runs):
    """Competências válidas = aquelas com local_disponivel=TRUE no pipeline_runs."""
    resultado = reader_com_runs.listar_competencias_validas()
    assert resultado == ["2024-01", "2024-03"]


@pytest.fixture
def reader_com_auditoria(tmp_path):
    db_path = tmp_path / "test.duckdb"
    db = duckdb.connect(str(db_path))
    db.execute("CREATE SCHEMA gold")
    db.execute("""CREATE TABLE gold.auditoria_resultados (
        data_competencia VARCHAR,
        regra VARCHAR,
        total_anomalias INTEGER,
        gravado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (data_competencia, regra)
    )""")
    db.execute("INSERT INTO gold.auditoria_resultados (data_competencia, regra, total_anomalias) VALUES ('2024-01', 'RQ003B', 5)")
    db.execute("INSERT INTO gold.auditoria_resultados (data_competencia, regra, total_anomalias) VALUES ('2024-01', 'RQ006', NULL)")
    db.close()
    return HistoricoReader(db_path, tmp_path)


def test_carregar_kpis_retorna_none_para_regra_nula(reader_com_auditoria):
    """carregar_kpis() deve retornar None para total_anomalias NULL (regra não executada)."""
    kpis = reader_com_auditoria.carregar_kpis("2024-01")
    assert kpis["RQ003B"] == 5
    assert kpis["RQ006"] is None
