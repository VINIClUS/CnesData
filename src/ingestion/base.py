"""Protocolos da camada de ingestão — contratos para todos os backends."""

from typing import Protocol, runtime_checkable

import pandas as pd


@runtime_checkable
class EstabelecimentoRepository(Protocol):
    """Contrato para fontes de dados de estabelecimentos."""

    def listar_estabelecimentos(self, competencia: tuple[int, int] | None = None) -> pd.DataFrame:
        """Retorna estabelecimentos padronizados (schema SCHEMA_ESTABELECIMENTO).

        Args:
            competencia: (ano, mes) opcional. None = dados correntes.

        Returns:
            DataFrame com colunas conforme SCHEMA_ESTABELECIMENTO.
        """
        ...


@runtime_checkable
class ProfissionalRepository(Protocol):
    """Contrato para fontes de dados de profissionais/vínculos."""

    def listar_profissionais(self, competencia: tuple[int, int] | None = None) -> pd.DataFrame:
        """Retorna vínculos profissional×estabelecimento padronizados.

        Args:
            competencia: (ano, mes) opcional.

        Returns:
            DataFrame com colunas conforme SCHEMA_PROFISSIONAL.
        """
        ...


@runtime_checkable
class EquipeRepository(Protocol):
    """Contrato para fontes de dados de equipes de saúde."""

    def listar_equipes(self, competencia: tuple[int, int] | None = None) -> pd.DataFrame:
        """Retorna equipes padronizadas.

        Args:
            competencia: (ano, mes) opcional.

        Returns:
            DataFrame com colunas conforme SCHEMA_EQUIPE.
        """
        ...
