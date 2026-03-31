"""PipelineState — contentor imutável de dados inter-stage."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd


@dataclass
class PipelineState:
    """Todos os dados trocados entre stages do pipeline."""

    competencia_ano: int
    competencia_mes: int
    output_path: Path
    executar_nacional: bool
    executar_hr: bool

    con: Any = None
    cbo_lookup: dict[str, str] = field(default_factory=dict)

    df_prof_local: pd.DataFrame = field(default_factory=pd.DataFrame)
    df_estab_local: pd.DataFrame = field(default_factory=pd.DataFrame)
    df_prof_nacional: pd.DataFrame = field(default_factory=pd.DataFrame)
    df_estab_nacional: pd.DataFrame = field(default_factory=pd.DataFrame)
    df_processado: pd.DataFrame = field(default_factory=pd.DataFrame)
    df_multi_unidades: pd.DataFrame = field(default_factory=pd.DataFrame)
    df_acs_incorretos: pd.DataFrame = field(default_factory=pd.DataFrame)
    df_ace_incorretos: pd.DataFrame = field(default_factory=pd.DataFrame)
    df_ghost: pd.DataFrame = field(default_factory=pd.DataFrame)
    df_missing: pd.DataFrame = field(default_factory=pd.DataFrame)
    df_estab_fantasma: pd.DataFrame = field(default_factory=pd.DataFrame)
    df_estab_ausente: pd.DataFrame = field(default_factory=pd.DataFrame)
    df_prof_fantasma: pd.DataFrame = field(default_factory=pd.DataFrame)
    df_prof_ausente: pd.DataFrame = field(default_factory=pd.DataFrame)
    df_cbo_diverg: pd.DataFrame = field(default_factory=pd.DataFrame)
    df_ch_diverg: pd.DataFrame = field(default_factory=pd.DataFrame)

    nacional_validado: bool = False
    fingerprint_local: str = ""
    metricas_avancadas: dict = field(default_factory=dict)

    @property
    def competencia_str(self) -> str:
        return f"{self.competencia_ano}-{self.competencia_mes:02d}"
