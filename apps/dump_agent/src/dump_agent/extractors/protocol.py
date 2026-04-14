"""Protocol para extractors de dados."""

from pathlib import Path
from typing import Protocol, runtime_checkable

from cnes_domain.models.extraction import ExtractionParams
from dump_agent.io_guard import SpoolGuard


@runtime_checkable
class Extractor(Protocol):
    def extract(
        self,
        params: ExtractionParams,
        con: object,
        tmp_dir: Path,
        guard: SpoolGuard,
    ) -> Path: ...
