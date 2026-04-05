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

_DDL_DELTA_SNAPSHOT = """
    CREATE TABLE IF NOT EXISTS gold.delta_local_snapshot (
        competencia    VARCHAR PRIMARY KEY,
        n_novos        INTEGER NOT NULL DEFAULT 0,
        n_removidos    INTEGER NOT NULL DEFAULT 0,
        n_alterados    INTEGER NOT NULL DEFAULT 0,
        novos_json     VARCHAR,
        removidos_json VARCHAR,
        alterados_json VARCHAR,
        gravado_em     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
"""

_DDL_PROFISSIONAIS_PROCESSADOS = """
    CREATE TABLE IF NOT EXISTS gold.profissionais_processados (
        competencia        VARCHAR NOT NULL,
        cpf                VARCHAR NOT NULL,
        cnes               VARCHAR NOT NULL,
        cns                VARCHAR,
        nome_profissional  VARCHAR,
        sexo               VARCHAR(1),
        cbo                VARCHAR,
        tipo_vinculo       VARCHAR,
        sus                VARCHAR(1),
        ch_total           INTEGER,
        ch_ambulatorial    INTEGER,
        ch_outras          INTEGER,
        ch_hospitalar      INTEGER,
        fonte              VARCHAR,
        alerta_status_ch   VARCHAR,
        descricao_cbo      VARCHAR,
        gravado_em         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (competencia, cpf, cnes)
    )
"""

_DDL_ESTABELECIMENTOS = """
    CREATE TABLE IF NOT EXISTS gold.estabelecimentos (
        competencia        VARCHAR NOT NULL,
        cnes               VARCHAR NOT NULL,
        nome_fantasia      VARCHAR,
        tipo_unidade       VARCHAR,
        cnpj_mantenedora   VARCHAR,
        natureza_juridica  VARCHAR,
        cod_municipio      VARCHAR,
        vinculo_sus        VARCHAR(1),
        fonte              VARCHAR,
        gravado_em         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (competencia, cnes)
    )
"""

_DDL_CBO_LOOKUP = """
    CREATE TABLE IF NOT EXISTS gold.cbo_lookup (
        competencia  VARCHAR NOT NULL,
        codigo_cbo   VARCHAR NOT NULL,
        descricao    VARCHAR,
        PRIMARY KEY (competencia, codigo_cbo)
    )
"""

_DDL_PIPELINE_RUNS = """
    CREATE TABLE IF NOT EXISTS gold.pipeline_runs (
        competencia          VARCHAR PRIMARY KEY,
        local_disponivel     BOOLEAN NOT NULL DEFAULT FALSE,
        nacional_disponivel  BOOLEAN NOT NULL DEFAULT FALSE,
        hr_disponivel        BOOLEAN NOT NULL DEFAULT FALSE,
        status               VARCHAR NOT NULL,
        iniciado_em          TIMESTAMP,
        concluido_em         TIMESTAMP
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
            con.execute(_DDL_DELTA_SNAPSHOT)
            con.execute(_DDL_PROFISSIONAIS_PROCESSADOS)
            con.execute(_DDL_ESTABELECIMENTOS)
            con.execute(_DDL_CBO_LOOKUP)
            con.execute(_DDL_PIPELINE_RUNS)
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

    def gravar_auditoria(self, data_competencia: str, regra: str, total: int | None) -> None:
        """UPSERT de contagem de anomalias por regra em gold.auditoria_resultados.

        Args:
            data_competencia: Competência no formato 'YYYY-MM'.
            regra: Código da regra de auditoria (ex: 'RQ006').
            total: Total de anomalias detectadas, ou None se a regra não foi executada.
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
            "auditoria gravada competencia=%s regra=%s total=%s",
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

    def gravar_delta_snapshot(self, competencia: str, delta: dict) -> None:
        """INSERT OR REPLACE do delta de snapshot em gold.delta_local_snapshot.

        Args:
            competencia: Competência no formato 'YYYY-MM'.
            delta: Dicionário com chaves n_novos, n_removidos, n_alterados,
                   novos_json, removidos_json, alterados_json.
        """
        with self._conectar() as con:
            con.execute(
                """
                INSERT OR REPLACE INTO gold.delta_local_snapshot
                    (competencia, n_novos, n_removidos, n_alterados,
                     novos_json, removidos_json, alterados_json)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    competencia,
                    delta.get("n_novos", 0),
                    delta.get("n_removidos", 0),
                    delta.get("n_alterados", 0),
                    delta.get("novos_json"),
                    delta.get("removidos_json"),
                    delta.get("alterados_json"),
                ],
            )
        logger.info("delta_snapshot gravado competencia=%s", competencia)

    def gravar_profissionais(self, competencia: str, df: pd.DataFrame) -> None:
        """DELETE + INSERT de profissionais processados para uma competência.

        Args:
            competencia: Competência no formato 'YYYY-MM'.
            df: DataFrame com colunas do SCHEMA_PROFISSIONAL + ALERTA_STATUS_CH + DESCRICAO_CBO.
        """
        df_insert = df.rename(columns=str.lower).copy()
        df_insert.insert(0, "competencia", competencia)
        with self._conectar() as con:
            con.execute(
                "DELETE FROM gold.profissionais_processados WHERE competencia = ?",
                [competencia],
            )
            con.register("_tmp_prof", df_insert)
            con.execute("""
                INSERT INTO gold.profissionais_processados
                    (competencia, cpf, cnes, cns, nome_profissional, sexo, cbo,
                     tipo_vinculo, sus, ch_total, ch_ambulatorial, ch_outras,
                     ch_hospitalar, fonte, alerta_status_ch, descricao_cbo, gravado_em)
                SELECT competencia, cpf, cnes, cns, nome_profissional, sexo, cbo,
                       tipo_vinculo, sus, ch_total, ch_ambulatorial, ch_outras,
                       ch_hospitalar, fonte, alerta_status_ch, descricao_cbo,
                       CURRENT_TIMESTAMP
                FROM _tmp_prof
            """)
        logger.info("profissionais gravados competencia=%s total=%d", competencia, len(df))

    def gravar_estabelecimentos(self, competencia: str, df: pd.DataFrame) -> None:
        """DELETE + INSERT de estabelecimentos para uma competência.

        Args:
            competencia: Competência no formato 'YYYY-MM'.
            df: DataFrame com colunas do SCHEMA_ESTABELECIMENTO.
        """
        df_insert = df.rename(columns=str.lower).copy()
        df_insert.insert(0, "competencia", competencia)
        with self._conectar() as con:
            con.execute(
                "DELETE FROM gold.estabelecimentos WHERE competencia = ?",
                [competencia],
            )
            con.register("_tmp_estab", df_insert)
            con.execute("""
                INSERT INTO gold.estabelecimentos
                    (competencia, cnes, nome_fantasia, tipo_unidade, cnpj_mantenedora,
                     natureza_juridica, cod_municipio, vinculo_sus, fonte, gravado_em)
                SELECT competencia, cnes, nome_fantasia, tipo_unidade, cnpj_mantenedora,
                       natureza_juridica, cod_municipio, vinculo_sus, fonte, CURRENT_TIMESTAMP
                FROM _tmp_estab
            """)
        logger.info("estabelecimentos gravados competencia=%s total=%d", competencia, len(df))

    def gravar_cbo_lookup(self, competencia: str, lookup: dict[str, str]) -> None:
        """DELETE + INSERT do dicionário CBO para uma competência.

        Args:
            competencia: Competência no formato 'YYYY-MM'.
            lookup: Dicionário codigo_cbo → descricao.
        """
        if not lookup:
            return
        df = pd.DataFrame(
            [{"competencia": competencia, "codigo_cbo": k, "descricao": v}
             for k, v in lookup.items()]
        )
        with self._conectar() as con:
            con.execute(
                "DELETE FROM gold.cbo_lookup WHERE competencia = ?",
                [competencia],
            )
            con.register("_tmp_cbo", df)
            con.execute("INSERT INTO gold.cbo_lookup SELECT * FROM _tmp_cbo")
        logger.info("cbo_lookup gravado competencia=%s total=%d", competencia, len(lookup))

    def gravar_pipeline_run(
        self,
        competencia: str,
        local_disponivel: bool,
        nacional_disponivel: bool,
        hr_disponivel: bool,
        status: str,
    ) -> None:
        """INSERT OR REPLACE do status de execução em gold.pipeline_runs.

        Args:
            competencia: Competência no formato 'YYYY-MM'.
            local_disponivel: True se dados locais estavam disponíveis.
            nacional_disponivel: True se dados nacionais foram ingeridos.
            hr_disponivel: True se cross-check HR foi executado.
            status: 'completo' | 'parcial' | 'sem_dados_locais' | 'sem_dados'.
        """
        with self._conectar() as con:
            con.execute(
                """
                INSERT OR REPLACE INTO gold.pipeline_runs
                    (competencia, local_disponivel, nacional_disponivel, hr_disponivel,
                     status, iniciado_em, concluido_em)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """,
                [competencia, local_disponivel, nacional_disponivel, hr_disponivel, status],
            )
        logger.info("pipeline_run gravado competencia=%s status=%s", competencia, status)

    def profissional_existe(self, competencia: str) -> bool:
        """Retorna True se existirem profissionais processados para a competência.

        Args:
            competencia: Competência no formato 'YYYY-MM'.

        Returns:
            True quando pelo menos uma linha existir em gold.profissionais_processados.
        """
        with self._conectar(read_only=True) as con:
            df = con.execute(
                "SELECT COUNT(*) AS n FROM gold.profissionais_processados WHERE competencia = ?",
                [competencia],
            ).df()
        return int(df["n"].iloc[0]) > 0

    def carregar_profissionais(self, competencia: str) -> pd.DataFrame:
        """Carrega profissionais processados de uma competência.

        Args:
            competencia: Competência no formato 'YYYY-MM'.

        Returns:
            DataFrame com colunas em maiúsculas (SCHEMA_PROFISSIONAL + ALERTA_STATUS_CH + DESCRICAO_CBO).
        """
        with self._conectar(read_only=True) as con:
            df = con.execute(
                """SELECT cns, cpf, nome_profissional, sexo, cbo, cnes, tipo_vinculo,
                          sus, ch_total, ch_ambulatorial, ch_outras, ch_hospitalar,
                          fonte, alerta_status_ch, descricao_cbo
                   FROM gold.profissionais_processados WHERE competencia = ?""",
                [competencia],
            ).df()
        return df.rename(columns=str.upper)

    def carregar_estabelecimentos(self, competencia: str) -> pd.DataFrame:
        """Carrega estabelecimentos de uma competência.

        Args:
            competencia: Competência no formato 'YYYY-MM'.

        Returns:
            DataFrame com colunas em maiúsculas (SCHEMA_ESTABELECIMENTO).
        """
        with self._conectar(read_only=True) as con:
            df = con.execute(
                """SELECT cnes, nome_fantasia, tipo_unidade, cnpj_mantenedora,
                          natureza_juridica, cod_municipio, vinculo_sus, fonte
                   FROM gold.estabelecimentos WHERE competencia = ?""",
                [competencia],
            ).df()
        return df.rename(columns=str.upper)

    def carregar_cbo_lookup(self, competencia: str) -> dict[str, str]:
        """Carrega dicionário CBO de uma competência.

        Args:
            competencia: Competência no formato 'YYYY-MM'.

        Returns:
            Dicionário codigo_cbo → descricao.
        """
        with self._conectar(read_only=True) as con:
            df = con.execute(
                "SELECT codigo_cbo, descricao FROM gold.cbo_lookup WHERE competencia = ?",
                [competencia],
            ).df()
        return dict(zip(df["codigo_cbo"], df["descricao"].fillna("")))

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
