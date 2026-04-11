"""StoragePort — interface de persistência (domínio puro, sem dependências de infra)."""
from typing import Protocol

import pandas as pd


class StoragePort(Protocol):
    def gravar_profissionais(self, competencia: str, df: pd.DataFrame) -> None: ...
    def gravar_estabelecimentos(self, competencia: str, df: pd.DataFrame) -> None: ...
    def registrar_pipeline_run(self, competencia: str, estado: dict) -> None: ...
