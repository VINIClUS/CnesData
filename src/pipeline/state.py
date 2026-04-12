"""PipelineState — contentor imutável de dados inter-stage."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

if TYPE_CHECKING:
    from ingestion.quarantine import QuarantineBuffer


@dataclass
class PipelineState:
    """Todos os dados trocados entre stages do pipeline."""

    competencia_ano: int
    competencia_mes: int
    output_path: Path
    executar_nacional: bool

    con: Any = None
    cbo_lookup: dict[str, str] = field(default_factory=dict)

    df_prof_local: pd.DataFrame = field(default_factory=pd.DataFrame)
    df_estab_local: pd.DataFrame = field(default_factory=pd.DataFrame)
    df_prof_nacional: pd.DataFrame = field(default_factory=pd.DataFrame)
    df_estab_nacional: pd.DataFrame = field(default_factory=pd.DataFrame)
    df_processado: pd.DataFrame = field(default_factory=pd.DataFrame)

    local_disponivel: bool = True
    nacional_disponivel: bool = False
    quarantine_buffer: "QuarantineBuffer | None" = None
    force_reingestao: bool = False

    @property
    def competencia_str(self) -> str:
        return f"{self.competencia_ano}-{self.competencia_mes:02d}"
