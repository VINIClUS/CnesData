"""HistoricoReader — acesso ao histórico analítico (DuckDB Gold + CSVs arquivados)."""
import logging
from pathlib import Path

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)

CSV_MAP: dict[str, str] = {
    "RQ003B":    "auditoria_rq003b_multiplas_unidades.csv",
    "RQ005_ACS": "auditoria_rq005_acs_tacs_incorretos.csv",
    "RQ005_ACE": "auditoria_rq005_ace_tace_incorretos.csv",
    "GHOST":     "auditoria_ghost_payroll.csv",
    "MISSING":   "auditoria_missing_registration.csv",
    "RQ006":     "auditoria_rq006_estab_fantasma.csv",
    "RQ007":     "auditoria_rq007_estab_ausente_local.csv",
    "RQ008":     "auditoria_rq008_prof_fantasma_cns.csv",
    "RQ009":     "auditoria_rq009_prof_ausente_local_cns.csv",
    "RQ010":     "auditoria_rq010_divergencia_cbo.csv",
    "RQ011":     "auditoria_rq011_divergencia_ch.csv",
}


class HistoricoReader:
    """Lê tendências do DuckDB Gold e registros individuais de CSVs arquivados."""

    def __init__(self, duckdb_path: Path, historico_dir: Path) -> None:
        self._duckdb_path = duckdb_path
        self._historico_dir = historico_dir

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

    def carregar_kpis(self, competencia: str) -> dict[str, int]:
        """Retorna {regra: total} para uma competência específica.

        Args:
            competencia: Competência no formato YYYY-MM.

        Returns:
            Dicionário regra → total_anomalias.
        """
        with duckdb.connect(str(self._duckdb_path), read_only=True) as con:
            df = con.execute(
                "SELECT regra, total_anomalias FROM gold.auditoria_resultados "
                "WHERE data_competencia = ?",
                [competencia],
            ).df()
        return dict(zip(df["regra"], df["total_anomalias"].astype(int)))

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

    def carregar_registros(self, regra: str, competencia: str) -> pd.DataFrame:
        """Lê CSV arquivado via DuckDB read_csv_auto. Retorna DataFrame vazio se ausente.

        Args:
            regra: Chave da regra (ex.: "RQ008").
            competencia: Competência no formato YYYY-MM.

        Returns:
            DataFrame com os registros ou DataFrame vazio se o arquivo não existir.
        """
        nome = CSV_MAP.get(regra)
        if not nome:
            return pd.DataFrame()
        path = self._historico_dir / competencia / nome
        if not path.exists():
            logger.warning("csv_ausente regra=%s competencia=%s", regra, competencia)
            return pd.DataFrame()
        with duckdb.connect(":memory:") as con:
            return con.execute("SELECT * FROM read_csv_auto(?)", [str(path)]).df()

    def listar_competencias(self) -> list[str]:
        """Lista competências disponíveis em gold.evolucao_metricas_mensais, ordem crescente.

        Returns:
            Lista de strings YYYY-MM em ordem cronológica.
        """
        with duckdb.connect(str(self._duckdb_path), read_only=True) as con:
            df = con.execute(
                "SELECT DISTINCT data_competencia "
                "FROM gold.evolucao_metricas_mensais ORDER BY data_competencia"
            ).df()
        return df["data_competencia"].tolist()

    def listar_competencias_para_regra(self, regra: str) -> list[str]:
        """Lista competências com CSV arquivado para a regra, ordem crescente.

        Args:
            regra: Chave da regra (ex.: "RQ008").

        Returns:
            Lista de strings YYYY-MM com arquivos disponíveis.
        """
        nome = CSV_MAP.get(regra)
        if not nome:
            return []
        return sorted(p.parent.name for p in self._historico_dir.glob(f"*/{nome}"))

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
