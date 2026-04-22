"""JSON Schema exporter for all contract models."""
from __future__ import annotations

import json
from typing import TYPE_CHECKING

from cnes_contracts.dims import (
    CBO,
    CID10,
    Competencia,
    Estabelecimento,
    Municipio,
    ProcedimentoSUS,
    Profissional,
)
from cnes_contracts.fatos import (
    Internacao,
    ProcedimentoAIH,
    ProducaoAmbulatorial,
    VinculoCNES,
)
from cnes_contracts.jobs import JobTransitionEvent
from cnes_contracts.landing import Extraction, ExtractionRegisterPayload

if TYPE_CHECKING:
    from pathlib import Path


MODELS: list[type] = [
    Profissional,
    Estabelecimento,
    ProcedimentoSUS,
    CBO,
    CID10,
    Municipio,
    Competencia,
    VinculoCNES,
    ProducaoAmbulatorial,
    Internacao,
    ProcedimentoAIH,
    Extraction,
    ExtractionRegisterPayload,
    JobTransitionEvent,
]


def export_all(target_dir: Path) -> list[Path]:
    """Export JSON Schema for all models.

    Args:
        target_dir: directory where JSON files are written

    Returns:
        list of Path objects written
    """
    target_dir.mkdir(parents=True, exist_ok=True)
    written = []
    for model_cls in MODELS:
        schema = model_cls.model_json_schema()
        path = target_dir / f"{model_cls.__name__.lower()}.json"
        path.write_text(
            json.dumps(schema, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        written.append(path)
    return written
