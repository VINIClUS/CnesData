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
