"""HistoricoReader — acesso ao histórico analítico (DuckDB Gold + CSVs arquivados)."""
import logging
from pathlib import Path

import duckdb
import pandas as pd


logger = logging.getLogger(__name__)

REGRAS_AUDITORIA: tuple[str, ...] = (
    "RQ003B", "RQ005_ACS", "RQ005_ACE", "GHOST", "MISSING",
    "RQ006", "RQ007", "RQ008", "RQ009", "RQ010", "RQ011",
)


class HistoricoReader:
    """Lê tendências do DuckDB Gold e registros individuais de CSVs arquivados."""

    def __init__(self, duckdb_path: Path, historico_dir: Path) -> None:
        self._duckdb_path = duckdb_path
        self._historico_dir = historico_dir

    def _ler_df(
        self, sql: str, params: list | None = None
    ) -> pd.DataFrame:
        with duckdb.connect(str(self._duckdb_path), read_only=True) as con:
            return con.execute(sql, params or []).df()

    def carregar_tendencias(
        self,
        regras: list[str] | None = None,
        competencia_inicio: str | None = None,
        competencia_fim: str | None = None,
    ) -> pd.DataFrame:
        """Retorna DataFrame(data_competencia, regra, total_anomalias) de gold.auditoria_resultados.

        Args:
            regras: Lista de regras para filtrar. None retorna todas.
            competencia_inicio: Competência mínima no formato YYYY-MM.
            competencia_fim: Competência máxima no formato YYYY-MM.

        Returns:
            DataFrame ordenado por data_competencia, regra.
        """
        conditions: list[str] = []
        params: list = []
        if regras:
            placeholders = ", ".join("?" * len(regras))
            conditions.append(f"regra IN ({placeholders})")
            params.extend(regras)
        if competencia_inicio:
            conditions.append("data_competencia >= ?")
            params.append(competencia_inicio)
        if competencia_fim:
            conditions.append("data_competencia <= ?")
            params.append(competencia_fim)
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        sql = f"""
            SELECT data_competencia, regra, total_anomalias
            FROM gold.auditoria_resultados
            {where}
            ORDER BY data_competencia, regra
        """
        with duckdb.connect(str(self._duckdb_path), read_only=True) as con:
            return con.execute(sql, params).df()

    def carregar_kpis(self, competencia: str) -> dict[str, int | None]:
        """Retorna {regra: total} para uma competência específica.

        Args:
            competencia: Competência no formato YYYY-MM.

        Returns:
            Dicionário regra → total_anomalias (None se regra não executada).
        """
        with duckdb.connect(str(self._duckdb_path), read_only=True) as con:
            df = con.execute(
                "SELECT regra, total_anomalias FROM gold.auditoria_resultados "
                "WHERE data_competencia = ?",
                [competencia],
            ).df()
        return {
            row["regra"]: None if pd.isna(row["total_anomalias"]) else int(row["total_anomalias"])
            for _, row in df.iterrows()
        }

    def carregar_delta(self, competencia: str) -> dict[str, int]:
        """Retorna variação de cada regra vs competência anterior (0 se não houver anterior).

        Args:
            competencia: Competência no formato YYYY-MM.

        Returns:
            Dicionário regra → delta (positivo = aumento, negativo = redução).
        """
        competencias = self.listar_competencias()
        if competencia not in competencias:
            return {}
        idx = competencias.index(competencia)
        atual = self.carregar_kpis(competencia)
        if idx == 0:
            return {regra: 0 for regra in atual}
        anterior = self.carregar_kpis(competencias[idx - 1])
        return {regra: total - anterior.get(regra, 0) for regra, total in atual.items()}

    def listar_competencias(self) -> list[str]:
        """Lista competências com execução registrada em gold.pipeline_runs, ordem crescente.

        Returns:
            Lista de strings YYYY-MM em ordem cronológica.
        """
        with duckdb.connect(str(self._duckdb_path), read_only=True) as con:
            df = con.execute(
                "SELECT DISTINCT competencia FROM gold.pipeline_runs ORDER BY competencia"
            ).df()
        return df["competencia"].tolist()

    def carregar_total_vinculos(self, competencia: str) -> int:
        """Retorna total de vínculos processados para uma competência.

        Args:
            competencia: Competência no formato YYYY-MM.

        Returns:
            Total de vínculos ou 0 se a competência não existir.
        """
        with duckdb.connect(str(self._duckdb_path), read_only=True) as con:
            df = con.execute(
                "SELECT total_vinculos FROM gold.evolucao_metricas_mensais "
                "WHERE data_competencia = ?",
                [competencia],
            ).df()
        return int(df["total_vinculos"].iloc[0]) if not df.empty else 0

    def listar_competencias_validas(self) -> list[str]:
        """Competências com local_disponivel=TRUE em gold.pipeline_runs.

        Returns:
            Lista de competências YYYY-MM em ordem ascendente.
        """
        with duckdb.connect(str(self._duckdb_path), read_only=True) as con:
            df = con.execute(
                "SELECT competencia FROM gold.pipeline_runs "
                "WHERE local_disponivel = TRUE "
                "AND regexp_matches(competencia, '^\\d{4}-\\d{2}$') "
                "ORDER BY competencia"
            ).df()
        return df["competencia"].tolist()

    def contar_competencias(self) -> tuple[int, int]:
        """Retorna (válidas, total) de competências no DuckDB.

        Returns:
            Tupla (n_validas, n_total).
        """
        total = len(self.listar_competencias())
        validas = len(self.listar_competencias_validas())
        return validas, total

    def carregar_glosas_historicas(
        self,
        competencia_inicio: str | None = None,
        regra: str | None = None,
    ) -> pd.DataFrame:
        """Retorna todas as glosas de gold.glosas_profissional.

        Args:
            competencia_inicio: Filtra competencias >= este valor (YYYY-MM). None retorna todas.
            regra: Filtra por regra específica (ex.: 'RQ008'). None retorna todas.

        Returns:
            DataFrame com todas as colunas de gold.glosas_profissional.
        """
        conditions: list[str] = []
        params: list = []
        if competencia_inicio:
            conditions.append("competencia >= ?")
            params.append(competencia_inicio)
        if regra:
            conditions.append("regra = ?")
            params.append(regra)
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        sql = f"SELECT * FROM gold.glosas_profissional {where}"
        try:
            return self._ler_df(sql, params)
        except duckdb.CatalogException:
            return pd.DataFrame()

    def carregar_metricas_avancadas(self, competencia: str) -> dict | None:
        """Retorna as métricas avançadas de uma competência específica.

        Args:
            competencia: Competência no formato YYYY-MM.

        Returns:
            Dict com todas as colunas de gold.metricas_avancadas, ou None se ausente.
        """
        try:
            df = self._ler_df(
                "SELECT * FROM gold.metricas_avancadas WHERE competencia = ?",
                [competencia],
            )
        except duckdb.CatalogException:
            return None
        if df.empty:
            return None
        return df.iloc[0].to_dict()

    def carregar_delta_snapshot(self, competencia: str) -> dict | None:
        """Retorna o delta de snapshot para uma competência específica.

        Args:
            competencia: Competência no formato YYYY-MM.

        Returns:
            Dict com todas as colunas de gold.delta_local_snapshot, ou None se ausente.
        """
        try:
            df = self._ler_df(
                "SELECT * FROM gold.delta_local_snapshot WHERE competencia = ?",
                [competencia],
            )
        except duckdb.CatalogException:
            return None
        if df.empty:
            return None
        return df.iloc[0].to_dict()

    def carregar_profissionais(self, competencia: str) -> pd.DataFrame:
        """Carrega profissionais processados de uma competência.

        Args:
            competencia: Competência no formato YYYY-MM.

        Returns:
            DataFrame com colunas em maiúsculas; vazio se ausente.
        """
        try:
            df = self._ler_df(
                """SELECT cns, cpf, nome_profissional, sexo, cbo, cnes, tipo_vinculo,
                          sus, ch_total, ch_ambulatorial, ch_outras, ch_hospitalar,
                          fonte, alerta_status_ch, descricao_cbo
                   FROM gold.profissionais_processados WHERE competencia = ?""",
                [competencia],
            )
        except duckdb.CatalogException:
            logger.warning("tabela_ausente table=gold.profissionais_processados comp=%s", competencia)
            return pd.DataFrame()
        return df.rename(columns=str.upper)

    def carregar_estabelecimentos(self, competencia: str) -> pd.DataFrame:
        """Carrega estabelecimentos de uma competência.

        Args:
            competencia: Competência no formato YYYY-MM.

        Returns:
            DataFrame com colunas em maiúsculas; vazio se ausente.
        """
        try:
            df = self._ler_df(
                """SELECT cnes, nome_fantasia, tipo_unidade, cnpj_mantenedora,
                          natureza_juridica, cod_municipio, vinculo_sus, fonte
                   FROM gold.estabelecimentos WHERE competencia = ?""",
                [competencia],
            )
        except duckdb.CatalogException:
            logger.warning("tabela_ausente table=gold.estabelecimentos competencia=%s", competencia)
            return pd.DataFrame()
        return df.rename(columns=str.upper)

    def carregar_pipeline_run(self, competencia: str) -> dict | None:
        """Retorna o registro de execução de pipeline para uma competência.

        Args:
            competencia: Competência no formato YYYY-MM.

        Returns:
            Dict com todas as colunas de gold.pipeline_runs, ou None se ausente.
        """
        try:
            df = self._ler_df(
                "SELECT * FROM gold.pipeline_runs WHERE competencia = ?",
                [competencia],
            )
        except duckdb.CatalogException:
            logger.warning("tabela_ausente table=gold.pipeline_runs competencia=%s", competencia)
            return None
        if df.empty:
            return None
        return df.iloc[0].to_dict()

    def carregar_glosas_periodo(self, regra: str, competencia: str) -> pd.DataFrame:
        """Carrega glosas de uma regra e competência de gold.glosas_profissional.

        Args:
            regra: Código da regra (ex: 'RQ008').
            competencia: Competência no formato YYYY-MM.

        Returns:
            DataFrame com registros ou DataFrame vazio se ausente.
        """
        try:
            return self._ler_df(
                "SELECT * FROM gold.glosas_profissional WHERE competencia = ? AND regra = ?",
                [competencia, regra],
            )
        except duckdb.CatalogException:
            logger.warning("tabela_ausente table=gold.glosas_profissional competencia=%s", competencia)
            return pd.DataFrame()
