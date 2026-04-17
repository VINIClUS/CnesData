"""Ports de persistência — contratos sem dependência de infra ou Polars."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Protocol, Self

if TYPE_CHECKING:
    from collections.abc import Iterable

_logger = logging.getLogger(__name__)


class ProfissionalStoragePort(Protocol):
    def gravar(self, rows: Iterable[dict]) -> int: ...  # pragma: no cover - Protocol stub


class EstabelecimentoStoragePort(Protocol):
    def gravar(self, rows: Iterable[dict]) -> int: ...  # pragma: no cover - Protocol stub


class VinculoStoragePort(Protocol):
    def snapshot_replace(
        self, competencia: str, fonte: str, rows: Iterable[dict],
    ) -> int: ...  # pragma: no cover - Protocol stub


class UnitOfWorkPort(Protocol):
    profissionais: ProfissionalStoragePort
    estabelecimentos: EstabelecimentoStoragePort
    vinculos: VinculoStoragePort
    def __enter__(self) -> Self: ...  # pragma: no cover - Protocol stub
    def __exit__(self, *exc) -> None: ...  # pragma: no cover - Protocol stub


class NullProfissionalStorage:
    def gravar(self, rows: Iterable[dict]) -> int:
        _logger.warning("DB_URL nao configurado; profissionais nao gravados")
        return 0


class NullEstabelecimentoStorage:
    def gravar(self, rows: Iterable[dict]) -> int:
        _logger.warning("DB_URL nao configurado; estabelecimentos nao gravados")
        return 0


class NullVinculoStorage:
    def snapshot_replace(
        self, competencia: str, fonte: str, rows: Iterable[dict],
    ) -> int:
        _logger.warning(
            "DB_URL nao configurado; vinculos nao gravados competencia=%s",
            competencia,
        )
        return 0


class NullUnitOfWork:
    def __init__(self) -> None:
        self.profissionais = NullProfissionalStorage()
        self.estabelecimentos = NullEstabelecimentoStorage()
        self.vinculos = NullVinculoStorage()

    def __enter__(self) -> NullUnitOfWork:
        return self

    def __exit__(self, *exc) -> None:
        pass
