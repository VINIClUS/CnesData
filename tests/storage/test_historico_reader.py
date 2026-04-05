"""Testes do HistoricoReader — DuckDB Gold + CSVs arquivados."""
from datetime import datetime

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
                total_rq005 INTEGER,
                gravado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
        con.execute("INSERT INTO gold.evolucao_metricas_mensais VALUES ('2024-11',350,2,3,5,NULL)")
        con.execute("INSERT INTO gold.evolucao_metricas_mensais VALUES ('2024-12',357,3,2,7,NULL)")
        con.execute("INSERT INTO gold.auditoria_resultados VALUES ('2024-11','RQ008',9)")
        con.execute("INSERT INTO gold.auditoria_resultados VALUES ('2024-11','RQ006',4)")
        con.execute("INSERT INTO gold.auditoria_resultados VALUES ('2024-12','RQ008',12)")
        con.execute("INSERT INTO gold.auditoria_resultados VALUES ('2024-12','RQ006',3)")
        con.execute("INSERT INTO gold.auditoria_resultados VALUES ('2024-12','RQ009',8)")


@pytest.fixture
def reader(tmp_path):
    db = tmp_path / "test.duckdb"
    _popular_duckdb(db)
    return HistoricoReader(db, tmp_path / "historico")


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


def test_carregar_total_vinculos_retorna_valor_correto(reader):
    assert reader.carregar_total_vinculos("2024-12") == 357


def test_carregar_total_vinculos_retorna_zero_quando_competencia_ausente(reader):
    assert reader.carregar_total_vinculos("2099-01") == 0


def _popular_duckdb_com_timestamps(path):
    with duckdb.connect(str(path)) as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS gold")
        con.execute("""
            CREATE TABLE gold.evolucao_metricas_mensais (
                data_competencia VARCHAR PRIMARY KEY,
                total_vinculos   INTEGER,
                total_ghost      INTEGER,
                total_missing    INTEGER,
                total_rq005      INTEGER,
                gravado_em       TIMESTAMP
            )
        """)
        con.execute("""
            CREATE TABLE gold.auditoria_resultados (
                data_competencia VARCHAR,
                regra            VARCHAR,
                total_anomalias  INTEGER,
                PRIMARY KEY (data_competencia, regra)
            )
        """)
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


@pytest.fixture
def reader_com_timestamps(tmp_path):
    db = tmp_path / "test_ts.duckdb"
    _popular_duckdb_com_timestamps(db)
    return HistoricoReader(db, tmp_path / "historico")


class TestListarCompetenciasValidas:

    def test_exclui_competencia_capturada_fora_da_janela(self, reader_com_timestamps):
        validas = reader_com_timestamps.listar_competencias_validas()
        assert "2024-10" not in validas

    def test_inclui_competencias_capturadas_dentro_da_janela(self, reader_com_timestamps):
        validas = reader_com_timestamps.listar_competencias_validas()
        assert "2024-11" in validas
        assert "2024-12" in validas

    def test_retorna_em_ordem_cronologica(self, reader_com_timestamps):
        validas = reader_com_timestamps.listar_competencias_validas()
        assert validas == sorted(validas)

    def test_retorna_lista_vazia_quando_sem_dados(self, tmp_path):
        db = tmp_path / "empty.duckdb"
        with duckdb.connect(str(db)) as con:
            con.execute("CREATE SCHEMA IF NOT EXISTS gold")
            con.execute("""
                CREATE TABLE gold.evolucao_metricas_mensais (
                    data_competencia VARCHAR PRIMARY KEY,
                    total_vinculos INTEGER,
                    total_ghost INTEGER,
                    total_missing INTEGER,
                    total_rq005 INTEGER,
                    gravado_em TIMESTAMP
                )
            """)
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
            con.execute("CREATE SCHEMA IF NOT EXISTS gold")
            con.execute("""
                CREATE TABLE gold.evolucao_metricas_mensais (
                    data_competencia VARCHAR PRIMARY KEY,
                    total_vinculos INTEGER,
                    total_ghost INTEGER,
                    total_missing INTEGER,
                    total_rq005 INTEGER,
                    gravado_em TIMESTAMP
                )
            """)
        r = HistoricoReader(db, tmp_path / "historico")
        assert r.contar_competencias() == (0, 0)


def _popular_duckdb_com_glosas(path):
    with duckdb.connect(str(path)) as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS gold")
        con.execute("""
            CREATE TABLE gold.glosas_profissional (
                competencia             VARCHAR NOT NULL,
                regra                   VARCHAR NOT NULL,
                cpf                     VARCHAR,
                cns                     VARCHAR,
                nome_profissional       VARCHAR,
                sexo                    VARCHAR(1),
                cnes_estabelecimento    VARCHAR,
                motivo                  VARCHAR,
                criado_em_firebird      TIMESTAMP,
                criado_em_pipeline      TIMESTAMP NOT NULL,
                atualizado_em_pipeline  TIMESTAMP NOT NULL
            )
        """)


class TestCarregarGlosasHistoricas:

    def test_carregar_glosas_historicas_sem_filtro(self, tmp_path):
        db = tmp_path / "test_glosas.duckdb"
        _popular_duckdb_com_glosas(db)
        with duckdb.connect(str(db)) as con:
            con.execute("""
                INSERT INTO gold.glosas_profissional VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                "2026-01", "RQ008", "12345678901", "7001234567890123",
                "Ana Silva", "F", "2795001", "Motivo 1",
                datetime(2026, 1, 10), datetime(2026, 1, 15), datetime(2026, 1, 15)
            ])
            con.execute("""
                INSERT INTO gold.glosas_profissional VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                "2026-02", "RQ009", "98765432101", "7009876543210987",
                "Bruno Costa", "M", "2795002", "Motivo 2",
                datetime(2026, 2, 10), datetime(2026, 2, 15), datetime(2026, 2, 15)
            ])
        reader = HistoricoReader(db, tmp_path / "historico")
        df = reader.carregar_glosas_historicas()
        assert len(df) == 2
        assert set(df.columns) >= {
            "competencia", "regra", "cpf", "cns", "nome_profissional",
            "sexo", "cnes_estabelecimento", "motivo",
            "criado_em_firebird", "criado_em_pipeline", "atualizado_em_pipeline"
        }

    def test_carregar_glosas_historicas_com_filtro(self, tmp_path):
        db = tmp_path / "test_glosas_filtro.duckdb"
        _popular_duckdb_com_glosas(db)
        with duckdb.connect(str(db)) as con:
            con.execute("""
                INSERT INTO gold.glosas_profissional VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                "2026-01", "RQ008", "12345678901", "7001234567890123",
                "Ana Silva", "F", "2795001", "Motivo 1",
                datetime(2026, 1, 10), datetime(2026, 1, 15), datetime(2026, 1, 15)
            ])
            con.execute("""
                INSERT INTO gold.glosas_profissional VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                "2026-02", "RQ009", "98765432101", "7009876543210987",
                "Bruno Costa", "M", "2795002", "Motivo 2",
                datetime(2026, 2, 10), datetime(2026, 2, 15), datetime(2026, 2, 15)
            ])
        reader = HistoricoReader(db, tmp_path / "historico")
        df = reader.carregar_glosas_historicas(competencia_inicio="2026-02")
        assert len(df) == 1
        assert df.iloc[0]["competencia"] == "2026-02"
        assert df.iloc[0]["nome_profissional"] == "Bruno Costa"

    def test_carregar_glosas_historicas_tabela_ausente(self, tmp_path):
        db = tmp_path / "test_glosas_ausente.duckdb"
        with duckdb.connect(str(db)) as con:
            con.execute("CREATE SCHEMA IF NOT EXISTS gold")
        reader = HistoricoReader(db, tmp_path / "historico")
        df = reader.carregar_glosas_historicas()
        assert df.empty

    def test_filtrar_por_regra_retorna_apenas_regra_solicitada(self, tmp_path):
        db = tmp_path / "test_glosas_regra.duckdb"
        _popular_duckdb_com_glosas(db)
        agora = datetime(2026, 3, 1)
        with duckdb.connect(str(db)) as con:
            con.execute(
                "INSERT INTO gold.glosas_profissional VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                ["2026-03", "RQ008", "11111111111", "7001111111111111",
                 "Carlos", "M", "2795001", "Motivo A", agora, agora, agora],
            )
            con.execute(
                "INSERT INTO gold.glosas_profissional VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                ["2026-03", "RQ009", "22222222222", "7002222222222222",
                 "Dora", "F", "2795002", "Motivo B", agora, agora, agora],
            )
        reader = HistoricoReader(db, tmp_path / "historico")
        df = reader.carregar_glosas_historicas(regra="RQ008")
        assert len(df) == 1
        assert df.iloc[0]["regra"] == "RQ008"

    def test_filtrar_por_regra_e_competencia(self, tmp_path):
        db = tmp_path / "test_glosas_regra_comp.duckdb"
        _popular_duckdb_com_glosas(db)
        agora = datetime(2026, 3, 1)
        with duckdb.connect(str(db)) as con:
            con.execute(
                "INSERT INTO gold.glosas_profissional VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                ["2026-02", "RQ008", "11111111111", "7001111111111111",
                 "Carlos", "M", "2795001", "Motivo A", agora, agora, agora],
            )
            con.execute(
                "INSERT INTO gold.glosas_profissional VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                ["2026-03", "RQ008", "22222222222", "7002222222222222",
                 "Dora", "F", "2795002", "Motivo B", agora, agora, agora],
            )
        reader = HistoricoReader(db, tmp_path / "historico")
        df = reader.carregar_glosas_historicas(competencia_inicio="2026-03", regra="RQ008")
        assert len(df) == 1
        assert df.iloc[0]["competencia"] == "2026-03"

    def test_regra_none_retorna_todas_as_regras(self, tmp_path):
        db = tmp_path / "test_glosas_todas.duckdb"
        _popular_duckdb_com_glosas(db)
        agora = datetime(2026, 3, 1)
        with duckdb.connect(str(db)) as con:
            for regra in ("RQ008", "RQ009", "GHOST"):
                con.execute(
                    "INSERT INTO gold.glosas_profissional VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    ["2026-03", regra, "11111111111", None,
                     "Profissional", "M", "2795001", "Motivo", agora, agora, agora],
                )
        reader = HistoricoReader(db, tmp_path / "historico")
        df = reader.carregar_glosas_historicas()
        assert len(df) == 3


def _popular_duckdb_com_metricas(path):
    with duckdb.connect(str(path)) as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS gold")
        con.execute("""
            CREATE TABLE gold.metricas_avancadas (
                competencia                      VARCHAR PRIMARY KEY,
                taxa_anomalia_geral              DOUBLE,
                p90_ch_total                     DOUBLE,
                proporcao_feminina_geral         DOUBLE,
                n_reincidentes                   INTEGER,
                taxa_resolucao                   DOUBLE,
                velocidade_regularizacao_media   DOUBLE,
                top_glosas_json                  VARCHAR,
                anomalias_por_cbo_json           VARCHAR,
                proporcao_feminina_por_cnes_json VARCHAR,
                ranking_cnes_json                VARCHAR,
                gravado_em                       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)


class TestCarregarMetricasAvancadas:

    def test_retorna_dict_quando_competencia_existe(self, tmp_path):
        db = tmp_path / "test_metricas.duckdb"
        _popular_duckdb_com_metricas(db)
        with duckdb.connect(str(db)) as con:
            con.execute(
                """INSERT INTO gold.metricas_avancadas
                   (competencia, taxa_anomalia_geral, p90_ch_total,
                    proporcao_feminina_geral, n_reincidentes, taxa_resolucao,
                    velocidade_regularizacao_media, top_glosas_json,
                    anomalias_por_cbo_json, proporcao_feminina_por_cnes_json,
                    ranking_cnes_json)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                ["2026-03", 0.15, 40.0, 0.62, 3, 0.80, 12.5,
                 '[{"regra": "RQ008", "n": 5}]',
                 '{"225125": 3, "225170": 2}',
                 '{"2795001": 0.7}',
                 '[{"cnes": "2795001", "n": 8}]'],
            )
        reader = HistoricoReader(db, tmp_path / "historico")
        result = reader.carregar_metricas_avancadas("2026-03")
        assert result is not None
        assert result["taxa_anomalia_geral"] == pytest.approx(0.15)
        assert result["n_reincidentes"] == 3
        assert result["top_glosas_json"] == '[{"regra": "RQ008", "n": 5}]'

    def test_retorna_none_quando_competencia_ausente(self, tmp_path):
        db = tmp_path / "test_metricas_vazio.duckdb"
        _popular_duckdb_com_metricas(db)
        reader = HistoricoReader(db, tmp_path / "historico")
        assert reader.carregar_metricas_avancadas("2026-03") is None

    def test_retorna_none_quando_tabela_nao_existe(self, tmp_path):
        db = tmp_path / "test_metricas_sem_tabela.duckdb"
        with duckdb.connect(str(db)) as con:
            con.execute("CREATE SCHEMA IF NOT EXISTS gold")
        reader = HistoricoReader(db, tmp_path / "historico")
        assert reader.carregar_metricas_avancadas("2026-03") is None


def _popular_tabelas_novas(path):
    with duckdb.connect(str(path)) as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS gold")
        con.execute("""
            CREATE TABLE IF NOT EXISTS gold.profissionais_processados (
                competencia VARCHAR, cpf VARCHAR, cnes VARCHAR,
                cns VARCHAR, nome_profissional VARCHAR, sexo VARCHAR,
                cbo VARCHAR, tipo_vinculo VARCHAR, sus VARCHAR,
                ch_total INTEGER, ch_ambulatorial INTEGER, ch_outras INTEGER,
                ch_hospitalar INTEGER, fonte VARCHAR, alerta_status_ch VARCHAR,
                descricao_cbo VARCHAR, gravado_em TIMESTAMP
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS gold.estabelecimentos (
                competencia VARCHAR, cnes VARCHAR, nome_fantasia VARCHAR,
                tipo_unidade VARCHAR, cnpj_mantenedora VARCHAR,
                natureza_juridica VARCHAR, cod_municipio VARCHAR,
                vinculo_sus VARCHAR, fonte VARCHAR, gravado_em TIMESTAMP
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS gold.pipeline_runs (
                competencia VARCHAR PRIMARY KEY, local_disponivel BOOLEAN,
                nacional_disponivel BOOLEAN, hr_disponivel BOOLEAN,
                status VARCHAR, iniciado_em TIMESTAMP, concluido_em TIMESTAMP
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS gold.glosas_profissional (
                competencia VARCHAR, regra VARCHAR, cpf VARCHAR,
                cns VARCHAR, nome_profissional VARCHAR, sexo VARCHAR,
                cnes_estabelecimento VARCHAR, motivo VARCHAR,
                criado_em_firebird TIMESTAMP, criado_em_pipeline TIMESTAMP,
                atualizado_em_pipeline TIMESTAMP
            )
        """)
        con.execute("""
            INSERT INTO gold.profissionais_processados VALUES
            ('2026-03','12345678901','2795001','123456789012345','Ana Silva','F',
             '515105','30','S',40,20,10,10,'LOCAL','OK','Agente','2026-03-01')
        """)
        con.execute("""
            INSERT INTO gold.estabelecimentos VALUES
            ('2026-03','2795001','UBS Centro','01','55293427000117','1023','354130','S','LOCAL','2026-03-01')
        """)
        con.execute("""
            INSERT INTO gold.pipeline_runs VALUES
            ('2026-03',TRUE,TRUE,FALSE,'completo','2026-03-01','2026-03-01')
        """)
        con.execute("""
            INSERT INTO gold.glosas_profissional VALUES
            ('2026-03','RQ008','12345678901','123456789012345','Ana','F','2795001',
             'motivo','2026-03-01','2026-03-01','2026-03-01')
        """)


class TestNovosMetodosHistoricoReader:
    def test_carregar_profissionais(self, tmp_path):
        _popular_tabelas_novas(tmp_path / "test.duckdb")
        reader = HistoricoReader(tmp_path / "test.duckdb", tmp_path / "historico")
        df = reader.carregar_profissionais("2026-03")
        assert len(df) == 1
        assert df["CPF"].iloc[0] == "12345678901"

    def test_carregar_profissionais_vazio(self, tmp_path):
        _popular_tabelas_novas(tmp_path / "test.duckdb")
        reader = HistoricoReader(tmp_path / "test.duckdb", tmp_path / "historico")
        df = reader.carregar_profissionais("2025-01")
        assert df.empty

    def test_carregar_estabelecimentos(self, tmp_path):
        _popular_tabelas_novas(tmp_path / "test.duckdb")
        reader = HistoricoReader(tmp_path / "test.duckdb", tmp_path / "historico")
        df = reader.carregar_estabelecimentos("2026-03")
        assert list(df["CNES"]) == ["2795001"]

    def test_carregar_pipeline_run(self, tmp_path):
        _popular_tabelas_novas(tmp_path / "test.duckdb")
        reader = HistoricoReader(tmp_path / "test.duckdb", tmp_path / "historico")
        resultado = reader.carregar_pipeline_run("2026-03")
        assert resultado is not None
        assert resultado["status"] == "completo"

    def test_carregar_pipeline_run_ausente(self, tmp_path):
        _popular_tabelas_novas(tmp_path / "test.duckdb")
        reader = HistoricoReader(tmp_path / "test.duckdb", tmp_path / "historico")
        assert reader.carregar_pipeline_run("2025-01") is None

    def test_carregar_glosas_periodo(self, tmp_path):
        _popular_tabelas_novas(tmp_path / "test.duckdb")
        reader = HistoricoReader(tmp_path / "test.duckdb", tmp_path / "historico")
        df = reader.carregar_glosas_periodo("RQ008", "2026-03")
        assert len(df) == 1
        assert df["cpf"].iloc[0] == "12345678901"

    def test_carregar_glosas_periodo_regra_ausente(self, tmp_path):
        _popular_tabelas_novas(tmp_path / "test.duckdb")
        reader = HistoricoReader(tmp_path / "test.duckdb", tmp_path / "historico")
        df = reader.carregar_glosas_periodo("RQ010", "2026-03")
        assert df.empty

    def test_carregar_estabelecimentos_vazio(self, tmp_path):
        _popular_tabelas_novas(tmp_path / "test.duckdb")
        reader = HistoricoReader(tmp_path / "test.duckdb", tmp_path / "historico")
        df = reader.carregar_estabelecimentos("2025-01")
        assert df.empty
