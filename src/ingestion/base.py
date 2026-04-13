"""Protocolos da camada de ingestao — contratos para todos os backends."""

from typing import Protocol, runtime_checkable

import polars as pl


@runtime_checkable
class EstabelecimentoRepository(Protocol):

    def listar_estabelecimentos(self, competencia: tuple[int, int] | None = None) -> pl.DataFrame: ...


@runtime_checkable
class ProfissionalRepository(Protocol):

    def listar_profissionais(self, competencia: tuple[int, int] | None = None) -> pl.DataFrame: ...


@runtime_checkable
class EquipeRepository(Protocol):

    def listar_equipes(self, competencia: tuple[int, int] | None = None) -> pl.DataFrame: ...
