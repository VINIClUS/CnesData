"""PostgresUnitOfWork — coordenador transacional."""

from __future__ import annotations

from typing import TYPE_CHECKING, Self

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

from cnes_infra.storage.repositories.estabelecimento_repo import (
    EstabelecimentoRepository,
)
from cnes_infra.storage.repositories.profissional_repo import (
    ProfissionalRepository,
)
from cnes_infra.storage.repositories.vinculo_repo import VinculoRepository


class PostgresUnitOfWork:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def __enter__(self) -> Self:
        self._con = self._engine.connect()
        self._tx = self._con.begin()
        self.profissionais = ProfissionalRepository(self._con)
        self.estabelecimentos = EstabelecimentoRepository(self._con)
        self.vinculos = VinculoRepository(self._con)
        return self

    def __exit__(self, exc_type, *_) -> None:
        if exc_type:
            self._tx.rollback()
        else:
            self._tx.commit()
        self._con.close()
