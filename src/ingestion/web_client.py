"""Cliente CNES via Base dos Dados (BigQuery) — WP-002."""

import logging

import basedosdados as bd
import google.auth.exceptions
import pandas as pd

logger = logging.getLogger(__name__)

_SQL_ESTABELECIMENTOS: str = """
    SELECT
        ano, mes, id_municipio,
        id_municipio_6,
        id_estabelecimento_cnes,
        cnpj_mantenedora,
        id_natureza_juridica,
        tipo_unidade,
        tipo_gestao,
        indicador_vinculo_sus
    FROM `basedosdados.br_ms_cnes.estabelecimento`
    WHERE id_municipio = '{id_municipio}'
      AND ano = {ano}
      AND mes = {mes}
"""

_SQL_PROFISSIONAIS: str = """
    SELECT
        ano, mes, id_municipio,
        id_estabelecimento_cnes,
        cartao_nacional_saude,
        nome,
        cbo_2002,
        tipo_vinculo,
        indicador_atende_sus,
        carga_horaria_ambulatorial,
        carga_horaria_outros,
        carga_horaria_hospitalar
    FROM `basedosdados.br_ms_cnes.profissional`
    WHERE id_municipio = '{id_municipio}'
      AND ano = {ano}
      AND mes = {mes}
"""

_SQL_PROFISSIONAIS_POR_CNES: str = """
    SELECT
        ano, mes, id_municipio,
        id_estabelecimento_cnes,
        cartao_nacional_saude,
        nome,
        cbo_2002,
        tipo_vinculo,
        indicador_atende_sus,
        carga_horaria_ambulatorial,
        carga_horaria_outros,
        carga_horaria_hospitalar
    FROM `basedosdados.br_ms_cnes.profissional`
    WHERE id_estabelecimento_cnes = '{id_estabelecimento_cnes}'
      AND ano = {ano}
      AND mes = {mes}
"""

_SQL_EQUIPES: str = """
    SELECT *
    FROM `basedosdados.br_ms_cnes.equipe`
    WHERE id_municipio = '{id_municipio}'
      AND ano = {ano}
      AND mes = {mes}
"""


class CnesWebError(Exception):
    """Erro genérico de acesso ao BigQuery."""


class CnesWebAuthError(CnesWebError):
    """Credenciais Google Cloud inválidas ou expiradas."""


class CnesWebQuotaError(CnesWebError):
    """Quota mensal de 1TB excedida."""


class CnesWebClient:
    """Cliente para dados CNES via Base dos Dados (BigQuery)."""

    def __init__(self, billing_project_id: str) -> None:
        self._billing_project_id = billing_project_id

    def fetch_estabelecimentos(
        self,
        id_municipio: str,
        ano: int,
        mes: int,
    ) -> pd.DataFrame:
        """Busca estabelecimentos de um município em uma competência.

        Args:
            id_municipio: Código IBGE completo 7 dígitos (ex: '3541307').
            ano: Ano da competência.
            mes: Mês da competência (1-12).

        Returns:
            DataFrame com estabelecimentos.

        Raises:
            CnesWebError: Falha no BigQuery.
        """
        sql = _SQL_ESTABELECIMENTOS.format(
            id_municipio=id_municipio, ano=ano, mes=mes
        )
        df = self._executar_query(sql)
        logger.info(
            "fetch_estabelecimentos id_municipio=%s ano=%d mes=%d rows=%d",
            id_municipio, ano, mes, len(df),
        )
        if df.empty:
            logger.warning(
                "fetch_estabelecimentos id_municipio=%s ano=%d mes=%d rows=0 "
                "(dados ainda não publicados?)",
                id_municipio, ano, mes,
            )
        return df.copy()

    def fetch_profissionais(
        self,
        id_municipio: str,
        ano: int,
        mes: int,
    ) -> pd.DataFrame:
        """Busca todos os profissionais do município em uma competência.

        Args:
            id_municipio: Código IBGE 7 dígitos.
            ano: Ano da competência.
            mes: Mês da competência.

        Returns:
            DataFrame com profissionais (inclui cartao_nacional_saude).
        """
        sql = _SQL_PROFISSIONAIS.format(
            id_municipio=id_municipio, ano=ano, mes=mes
        )
        df = self._executar_query(sql)
        logger.info(
            "fetch_profissionais id_municipio=%s ano=%d mes=%d rows=%d",
            id_municipio, ano, mes, len(df),
        )
        if df.empty:
            logger.warning(
                "fetch_profissionais id_municipio=%s ano=%d mes=%d rows=0 "
                "(dados ainda não publicados?)",
                id_municipio, ano, mes,
            )
        return df.copy()

    def fetch_profissionais_por_estabelecimento(
        self,
        id_estabelecimento_cnes: str,
        ano: int,
        mes: int,
    ) -> pd.DataFrame:
        """Busca profissionais de um estabelecimento específico.

        Args:
            id_estabelecimento_cnes: Código CNES (7 dígitos).
            ano: Ano.
            mes: Mês.

        Returns:
            DataFrame com profissionais do estabelecimento.
        """
        sql = _SQL_PROFISSIONAIS_POR_CNES.format(
            id_estabelecimento_cnes=id_estabelecimento_cnes, ano=ano, mes=mes
        )
        df = self._executar_query(sql)
        logger.info(
            "fetch_profissionais_por_estabelecimento cnes=%s ano=%d mes=%d rows=%d",
            id_estabelecimento_cnes, ano, mes, len(df),
        )
        return df.copy()

    def fetch_equipes(
        self,
        id_municipio: str,
        ano: int,
        mes: int,
    ) -> pd.DataFrame:
        """Busca equipes de saúde de um município.

        Args:
            id_municipio: Código IBGE 7 dígitos.
            ano: Ano.
            mes: Mês.

        Returns:
            DataFrame com equipes.
        """
        sql = _SQL_EQUIPES.format(
            id_municipio=id_municipio, ano=ano, mes=mes
        )
        df = self._executar_query(sql)
        logger.info(
            "fetch_equipes id_municipio=%s ano=%d mes=%d rows=%d",
            id_municipio, ano, mes, len(df),
        )
        return df.copy()

    def _executar_query(self, sql: str) -> pd.DataFrame:
        try:
            return bd.read_sql(sql, billing_project_id=self._billing_project_id)
        except google.auth.exceptions.DefaultCredentialsError as exc:
            raise CnesWebAuthError(f"auth_error={exc}") from exc
        except Exception as exc:
            raise CnesWebError(f"bigquery_error={exc}") from exc
