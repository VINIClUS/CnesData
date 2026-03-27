"""DatabaseLoader — persistência DuckDB com schema Gold (Medallion POC)."""

import logging
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)

_DDL_SCHEMA_GOLD = "CREATE SCHEMA IF NOT EXISTS gold"

_DDL_EVOLUCAO = """
    CREATE TABLE IF NOT EXISTS gold.evolucao_metricas_mensais (
        data_competencia VARCHAR PRIMARY KEY,
        total_vinculos   INTEGER,
        total_ghost      INTEGER,
        total_missing    INTEGER,
        total_rq005      INTEGER,
        gravado_em       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
"""

_DDL_AUDITORIA = """
    CREATE TABLE IF NOT EXISTS gold.auditoria_resultados (
        data_competencia VARCHAR,
        regra            VARCHAR,
        total_anomalias  INTEGER,
        gravado_em       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (data_competencia, regra)
    )
"""


class DatabaseLoader:
    """Gerencia a conexão e persistência no banco DuckDB local."""

    def __init__(self, caminho_db: Path) -> None:
        self._caminho_db = caminho_db

    def inicializar_schema(self) -> None:
        """Cria schemas e tabelas Gold se ainda não existirem."""
        with duckdb.connect(str(self._caminho_db)) as con:
            con.execute(_DDL_SCHEMA_GOLD)
            con.execute(_DDL_EVOLUCAO)
            con.execute(_DDL_AUDITORIA)
        logger.info("schema_gold inicializado db=%s", self._caminho_db)
