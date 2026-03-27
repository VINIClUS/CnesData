"""Testes do DatabaseLoader — persistência DuckDB Gold layer."""

from pathlib import Path

import duckdb

from storage.database_loader import DatabaseLoader


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
