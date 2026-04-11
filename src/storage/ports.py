"""StoragePort — interface de persistência (domínio puro, sem dependências de infra)."""
import logging
from typing import Protocol

import pandas as pd

_logger = logging.getLogger(__name__)


class StoragePort(Protocol):
    def gravar_profissionais(self, competencia: str, df: pd.DataFrame) -> None: ...
    def gravar_estabelecimentos(self, competencia: str, df: pd.DataFrame) -> None: ...
    def registrar_pipeline_run(self, competencia: str, estado: dict) -> None: ...


class NullStoragePort:
    """Fallback de persistência: loga avisos em vez de gravar."""

    def gravar_profissionais(self, competencia: str, df: pd.DataFrame) -> None:
        _logger.warning(
            "DB_URL nao configurado; profissionais nao gravados competencia=%s", competencia
        )

    def gravar_estabelecimentos(self, competencia: str, df: pd.DataFrame) -> None:
        _logger.warning(
            "DB_URL nao configurado; estabelecimentos nao gravados competencia=%s", competencia
        )

    def registrar_pipeline_run(self, competencia: str, estado: dict) -> None:
        pass
