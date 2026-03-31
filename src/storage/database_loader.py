"""DatabaseLoader — persistência DuckDB com schema Gold (Medallion POC)."""

import logging
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Generator

import duckdb
import pandas as pd

from analysis.evolution_tracker import Snapshot

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

_DDL_GLOSAS = """
    CREATE TABLE IF NOT EXISTS gold.glosas_profissional (
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
"""

_DDL_CACHE_NACIONAL = """
    CREATE TABLE IF NOT EXISTS gold.cache_nacional (
        competencia        VARCHAR PRIMARY KEY,
        fingerprint_local  VARCHAR NOT NULL,
        gravado_em         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
"""

_DDL_METRICAS_AVANCADAS = """
    CREATE TABLE IF NOT EXISTS gold.metricas_avancadas (
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
"""


class DatabaseLoader:
    """Gerencia a conexão e persistência no banco DuckDB local."""

    def __init__(self, caminho_db: Path) -> None:
        self._caminho_db = caminho_db

    @contextmanager
    def _conectar(
        self, read_only: bool = False
    ) -> Generator[duckdb.DuckDBPyConnection, None, None]:
        with duckdb.connect(str(self._caminho_db), read_only=read_only) as con:
            yield con

    def inicializar_schema(self) -> None:
        """Cria schemas e tabelas Gold se ainda não existirem."""
        with self._conectar() as con:
            con.execute(_DDL_SCHEMA_GOLD)
            con.execute(_DDL_EVOLUCAO)
            con.execute(_DDL_AUDITORIA)
            con.execute(_DDL_GLOSAS)
            con.execute(_DDL_CACHE_NACIONAL)
            con.execute(_DDL_METRICAS_AVANCADAS)
        logger.info("schema_gold inicializado db=%s", self._caminho_db)

    def gravar_metricas(self, snapshot: Snapshot) -> None:
        """UPSERT das métricas do snapshot em gold.evolucao_metricas_mensais.

        Args:
            snapshot: Snapshot da competência a persistir.
        """
        with self._conectar() as con:
            con.execute(
                """
                INSERT OR REPLACE INTO gold.evolucao_metricas_mensais
                    (data_competencia, total_vinculos, total_ghost, total_missing, total_rq005)
                VALUES (?, ?, ?, ?, ?)
                """,
                [
                    snapshot.data_competencia,
                    snapshot.total_vinculos,
                    snapshot.total_ghost,
                    snapshot.total_missing,
                    snapshot.total_rq005,
                ],
            )
        logger.info(
            "metricas gravadas competencia=%s vinculos=%d",
            snapshot.data_competencia,
            snapshot.total_vinculos,
        )

    def gravar_auditoria(self, data_competencia: str, regra: str, total: int) -> None:
        """UPSERT de contagem de anomalias por regra em gold.auditoria_resultados.

        Args:
            data_competencia: Competência no formato 'YYYY-MM'.
            regra: Código da regra de auditoria (ex: 'RQ006').
            total: Total de anomalias detectadas.
        """
        with self._conectar() as con:
            con.execute(
                """
                INSERT OR REPLACE INTO gold.auditoria_resultados
                    (data_competencia, regra, total_anomalias)
                VALUES (?, ?, ?)
                """,
                [data_competencia, regra, total],
            )
        logger.info(
            "auditoria gravada competencia=%s regra=%s total=%d",
            data_competencia,
            regra,
            total,
        )

    def gravar_glosas(
        self, competencia: str, regra: str, df: pd.DataFrame
    ) -> None:
        """DELETE + INSERT de glosas para (competencia, regra).

        Args:
            competencia: Competência no formato 'YYYY-MM'.
            regra: Código da regra (ex: 'GHOST').
            df: DataFrame com schema gold.glosas_profissional.
        """
        if df.empty:
            return
        with self._conectar() as con:
            con.execute(
                "DELETE FROM gold.glosas_profissional WHERE competencia=? AND regra=?",
                [competencia, regra],
            )
            con.register("_df_glosas", df)
            con.execute("INSERT INTO gold.glosas_profissional SELECT * FROM _df_glosas")
        logger.info("glosas gravadas competencia=%s regra=%s total=%d", competencia, regra, len(df))

    def gravar_cache_nacional(self, competencia: str, fingerprint: str) -> None:
        """UPSERT em gold.cache_nacional.

        Args:
            competencia: Competência no formato 'YYYY-MM'.
            fingerprint: SHA256 dos dados locais.
        """
        with self._conectar() as con:
            con.execute(
                "INSERT OR REPLACE INTO gold.cache_nacional (competencia, fingerprint_local, gravado_em) "
                "VALUES (?, ?, CURRENT_TIMESTAMP)",
                [competencia, fingerprint],
            )
        logger.info("cache_nacional gravado competencia=%s", competencia)

    def ler_cache_nacional(
        self, competencia: str
    ) -> tuple[str, datetime] | None:
        """Retorna (fingerprint, gravado_em) do cache nacional ou None se ausente.

        Args:
            competencia: Competência no formato 'YYYY-MM'.

        Returns:
            Tupla (fingerprint, gravado_em) ou None.
        """
        with self._conectar(read_only=True) as con:
            df = con.execute(
                "SELECT fingerprint_local, gravado_em FROM gold.cache_nacional WHERE competencia=?",
                [competencia],
            ).df()
        if df.empty:
            return None
        row = df.iloc[0]
        return str(row["fingerprint_local"]), row["gravado_em"].to_pydatetime()

    def gravar_metricas_avancadas(self, competencia: str, metricas: dict) -> None:
        """INSERT OR REPLACE em gold.metricas_avancadas.

        Args:
            competencia: Competência no formato 'YYYY-MM'.
            metricas: Dicionário com chaves correspondentes às colunas da tabela.
        """
        with self._conectar() as con:
            con.execute(
                """
                INSERT OR REPLACE INTO gold.metricas_avancadas (
                    competencia, taxa_anomalia_geral, p90_ch_total,
                    proporcao_feminina_geral, n_reincidentes, taxa_resolucao,
                    velocidade_regularizacao_media, top_glosas_json,
                    anomalias_por_cbo_json, proporcao_feminina_por_cnes_json,
                    ranking_cnes_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """,
                [
                    competencia,
                    metricas.get("taxa_anomalia_geral"),
                    metricas.get("p90_ch_total"),
                    metricas.get("proporcao_feminina_geral"),
                    metricas.get("n_reincidentes"),
                    metricas.get("taxa_resolucao"),
                    metricas.get("velocidade_regularizacao_media"),
                    metricas.get("top_glosas_json"),
                    metricas.get("anomalias_por_cbo_json"),
                    metricas.get("proporcao_feminina_por_cnes_json"),
                    metricas.get("ranking_cnes_json"),
                ],
            )
        logger.info("metricas_avancadas gravadas competencia=%s", competencia)

    def carregar_historico(self) -> list[Snapshot]:
        """Retorna todos os snapshots do Gold ordenados por competência.

        Returns:
            Lista de Snapshot em ordem cronológica crescente.
        """
        with self._conectar(read_only=True) as con:
            df = con.execute(
                """
                SELECT data_competencia, total_vinculos, total_ghost, total_missing, total_rq005
                FROM gold.evolucao_metricas_mensais
                ORDER BY data_competencia
                """
            ).df()

        return [
            Snapshot(
                data_competencia=row["data_competencia"],
                total_vinculos=int(row["total_vinculos"]),
                total_ghost=int(row["total_ghost"]),
                total_missing=int(row["total_missing"]),
                total_rq005=int(row["total_rq005"]),
            )
            for _, row in df.iterrows()
        ]
