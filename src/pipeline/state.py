"""PipelineState — contentor imutável de dados inter-stage."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

import polars as pl

if TYPE_CHECKING:
    from ingestion.quarantine import QuarantineBuffer


@dataclass
class PipelineState:
    """Todos os dados trocados entre stages do pipeline."""

    competencia_ano: int
    competencia_mes: int
    output_path: Path
    target_source: Literal["LOCAL", "NACIONAL", "AMBOS"] = "LOCAL"

    con: Any = None
    cbo_lookup: dict[str, str] = field(default_factory=dict)

    df_prof_local: pl.DataFrame = field(default_factory=pl.DataFrame)
    df_estab_local: pl.DataFrame = field(default_factory=pl.DataFrame)
    df_prof_nacional: pl.DataFrame = field(default_factory=pl.DataFrame)
    df_estab_nacional: pl.DataFrame = field(default_factory=pl.DataFrame)
    df_processado: pl.DataFrame = field(default_factory=pl.DataFrame)

    quarantine_buffer: "QuarantineBuffer | None" = None

    @property
    def competencia_str(self) -> str:
        return f"{self.competencia_ano}-{self.competencia_mes:02d}"
