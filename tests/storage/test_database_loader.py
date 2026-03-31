"""Testes do DatabaseLoader — persistência DuckDB Gold layer."""

from pathlib import Path

import duckdb

from storage.database_loader import DatabaseLoader
from analysis.evolution_tracker import Snapshot


def _tabelas_existentes(caminho_db: Path) -> list[str]:
    con = duckdb.connect(str(caminho_db), read_only=True)
    df = con.execute("SHOW ALL TABLES").df()
    con.close()
    return df["name"].tolist()


class TestInicializarSchema:
    def test_cria_tabela_evolucao_metricas(self, tmp_path):
        # Arrange
        loader = DatabaseLoader(tmp_path / "test.duckdb")

        # Act
        loader.inicializar_schema()

        # Assert
        assert "evolucao_metricas_mensais" in _tabelas_existentes(
            tmp_path / "test.duckdb"
        )

    def test_cria_tabela_auditoria_resultados(self, tmp_path):
        # Arrange
        loader = DatabaseLoader(tmp_path / "test.duckdb")

        # Act
        loader.inicializar_schema()

        # Assert
        assert "auditoria_resultados" in _tabelas_existentes(tmp_path / "test.duckdb")

    def test_idempotente_chamada_multiplas_vezes(self, tmp_path):
        # Arrange
        loader = DatabaseLoader(tmp_path / "test.duckdb")

        # Act — não deve levantar exceção
        loader.inicializar_schema()
        loader.inicializar_schema()


def _snapshot(competencia: str = "2024-12", vinculos: int = 357) -> Snapshot:
    return Snapshot(
        data_competencia=competencia,
        total_vinculos=vinculos,
        total_ghost=5,
        total_missing=3,
        total_rq005=8,
    )


def _ler_metricas(caminho_db: Path):
    con = duckdb.connect(str(caminho_db), read_only=True)
    df = con.execute("SELECT * FROM gold.evolucao_metricas_mensais").df()
    con.close()
    return df


class TestGravarMetricas:
    def test_insere_snapshot_na_tabela_gold(self, tmp_path):
        # Arrange
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()

        # Act
        loader.gravar_metricas(_snapshot())

        # Assert
        df = _ler_metricas(tmp_path / "test.duckdb")
        assert len(df) == 1
        assert df["data_competencia"].iloc[0] == "2024-12"
        assert df["total_vinculos"].iloc[0] == 357

    def test_upsert_substitui_competencia_existente(self, tmp_path):
        # Arrange
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        loader.gravar_metricas(_snapshot(vinculos=100))

        # Act — mesma competência, vinculos diferentes
        loader.gravar_metricas(_snapshot(vinculos=357))

        # Assert — deve haver apenas 1 linha (UPSERT, não INSERT duplo)
        df = _ler_metricas(tmp_path / "test.duckdb")
        assert len(df) == 1
        assert df["total_vinculos"].iloc[0] == 357

    def test_multiplas_competencias_inseridas(self, tmp_path):
        # Arrange
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()

        # Act
        loader.gravar_metricas(_snapshot("2024-11"))
        loader.gravar_metricas(_snapshot("2024-12"))

        # Assert
        df = _ler_metricas(tmp_path / "test.duckdb")
        assert len(df) == 2
        assert set(df["data_competencia"]) == {"2024-11", "2024-12"}


def _ler_auditoria(caminho_db: Path):
    con = duckdb.connect(str(caminho_db), read_only=True)
    df = con.execute("SELECT * FROM gold.auditoria_resultados ORDER BY regra").df()
    con.close()
    return df


class TestGravarAuditoria:
    def test_insere_contagem_por_regra(self, tmp_path):
        # Arrange
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()

        # Act
        loader.gravar_auditoria("2024-12", "RQ006", 3)

        # Assert
        df = _ler_auditoria(tmp_path / "test.duckdb")
        assert len(df) == 1
        assert df["regra"].iloc[0] == "RQ006"
        assert df["total_anomalias"].iloc[0] == 3

    def test_upsert_atualiza_contagem_existente(self, tmp_path):
        # Arrange
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        loader.gravar_auditoria("2024-12", "RQ006", 3)

        # Act — reexecução com nova contagem
        loader.gravar_auditoria("2024-12", "RQ006", 5)

        # Assert — linha única com valor atualizado
        df = _ler_auditoria(tmp_path / "test.duckdb")
        assert len(df) == 1
        assert df["total_anomalias"].iloc[0] == 5

    def test_insere_multiplas_regras_mesma_competencia(self, tmp_path):
        # Arrange
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()

        # Act
        loader.gravar_auditoria("2024-12", "RQ006", 2)
        loader.gravar_auditoria("2024-12", "RQ008", 7)

        # Assert
        df = _ler_auditoria(tmp_path / "test.duckdb")
        assert len(df) == 2
        assert set(df["regra"]) == {"RQ006", "RQ008"}


class TestCarregarHistorico:
    def test_retorna_lista_vazia_quando_banco_sem_dados(self, tmp_path):
        # Arrange
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()

        # Act
        resultado = loader.carregar_historico()

        # Assert
        assert resultado == []

    def test_retorna_snapshots_em_ordem_cronologica(self, tmp_path):
        # Arrange
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        loader.gravar_metricas(_snapshot("2024-11", vinculos=300))
        loader.gravar_metricas(_snapshot("2024-12", vinculos=357))

        # Act
        resultado = loader.carregar_historico()

        # Assert
        assert len(resultado) == 2
        assert resultado[0].data_competencia == "2024-11"
        assert resultado[1].data_competencia == "2024-12"
        assert resultado[1].total_vinculos == 357

    def test_snapshot_retornado_tem_todos_os_campos(self, tmp_path):
        # Arrange
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        loader.gravar_metricas(_snapshot("2024-12"))

        # Act
        resultado = loader.carregar_historico()

        # Assert
        s = resultado[0]
        assert s.data_competencia == "2024-12"
        assert s.total_vinculos == 357
        assert s.total_ghost == 5
        assert s.total_missing == 3
        assert s.total_rq005 == 8


class TestNovasTabelas:
    def test_cria_glosas_profissional(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        assert "glosas_profissional" in _tabelas_existentes(tmp_path / "test.duckdb")

    def test_cria_cache_nacional(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        assert "cache_nacional" in _tabelas_existentes(tmp_path / "test.duckdb")

    def test_cria_metricas_avancadas(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        assert "metricas_avancadas" in _tabelas_existentes(tmp_path / "test.duckdb")


class TestGravarGlosas:
    def test_insere_glosas(self, tmp_path):
        import pandas as pd
        from datetime import datetime
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        df = pd.DataFrame([{
            "competencia": "2026-03", "regra": "GHOST",
            "cpf": "12345678901", "cns": None,
            "nome_profissional": "JOAO", "sexo": "M",
            "cnes_estabelecimento": "1234567", "motivo": "AUSENTE_NO_RH",
            "criado_em_firebird": None,
            "criado_em_pipeline": datetime(2026, 3, 31),
            "atualizado_em_pipeline": datetime(2026, 3, 31),
        }])
        loader.gravar_glosas("2026-03", "GHOST", df)
        with duckdb.connect(str(tmp_path / "test.duckdb"), read_only=True) as con:
            resultado = con.execute(
                "SELECT * FROM gold.glosas_profissional WHERE competencia='2026-03'"
            ).df()
        assert len(resultado) == 1
        assert resultado.iloc[0]["cpf"] == "12345678901"

    def test_delete_antes_de_inserir(self, tmp_path):
        import pandas as pd
        from datetime import datetime
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        def _row(cpf):
            return pd.DataFrame([{
                "competencia": "2026-03", "regra": "GHOST",
                "cpf": cpf, "cns": None, "nome_profissional": "X",
                "sexo": None, "cnes_estabelecimento": None, "motivo": None,
                "criado_em_firebird": None,
                "criado_em_pipeline": datetime(2026, 3, 31),
                "atualizado_em_pipeline": datetime(2026, 3, 31),
            }])
        loader.gravar_glosas("2026-03", "GHOST", _row("11111111111"))
        loader.gravar_glosas("2026-03", "GHOST", _row("22222222222"))
        with duckdb.connect(str(tmp_path / "test.duckdb"), read_only=True) as con:
            total = con.execute(
                "SELECT COUNT(*) FROM gold.glosas_profissional WHERE competencia='2026-03' AND regra='GHOST'"
            ).fetchone()[0]
        assert total == 1


class TestCacheNacional:
    def test_gravar_e_ler_cache(self, tmp_path):
        from datetime import datetime
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        loader.gravar_cache_nacional("2026-03", "abc123")
        resultado = loader.ler_cache_nacional("2026-03")
        assert resultado is not None
        fingerprint, gravado_em = resultado
        assert fingerprint == "abc123"
        assert isinstance(gravado_em, datetime)

    def test_ler_cache_retorna_none_se_ausente(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        assert loader.ler_cache_nacional("2026-03") is None

    def test_gravar_cache_sobrescreve(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        loader.gravar_cache_nacional("2026-03", "primeiro")
        loader.gravar_cache_nacional("2026-03", "segundo")
        fp, _ = loader.ler_cache_nacional("2026-03")
        assert fp == "segundo"


class TestMetricasAvancadas:
    def test_gravar_metricas(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        loader.gravar_metricas_avancadas("2026-03", {
            "taxa_anomalia_geral": 0.12,
            "p90_ch_total": 40.0,
            "proporcao_feminina_geral": 0.65,
            "n_reincidentes": 3,
            "taxa_resolucao": 0.5,
            "velocidade_regularizacao_media": 2.0,
            "top_glosas_json": "[]",
            "anomalias_por_cbo_json": "[]",
            "proporcao_feminina_por_cnes_json": "[]",
            "ranking_cnes_json": "[]",
        })
        with duckdb.connect(str(tmp_path / "test.duckdb"), read_only=True) as con:
            df = con.execute(
                "SELECT taxa_anomalia_geral FROM gold.metricas_avancadas WHERE competencia='2026-03'"
            ).df()
        assert abs(df.iloc[0]["taxa_anomalia_geral"] - 0.12) < 1e-6
