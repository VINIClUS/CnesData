"""Protocolos da camada de ingestao — contratos para todos os backends."""

from collections.abc import Iterable
from typing import Protocol, runtime_checkable


@runtime_checkable
class EstabelecimentoRepository(Protocol):
    def listar_estabelecimentos(
        self, competencia: tuple[int, int] | None = None,
    ) -> Iterable[dict]: ...


@runtime_checkable
class ProfissionalRepository(Protocol):
    def listar_profissionais(
        self, competencia: tuple[int, int] | None = None,
    ) -> Iterable[dict]: ...


@runtime_checkable
class EquipeRepository(Protocol):
    def listar_equipes(
        self, competencia: tuple[int, int] | None = None,
    ) -> Iterable[dict]: ...
