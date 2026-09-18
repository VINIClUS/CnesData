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
from cnes_contracts.landing import (
    Extraction,
    ExtractionRegisterPayload,
    FileManifest,
    UploadUrlRequest,
    UploadUrlResponse,
)
from cnes_contracts.manifests.outputs import OutputManifest, RunManifest, ServingDocument
from cnes_contracts.manifests.processing import (
    MaterializeRequest,
    MaterializeResult,
    NormalizeRequest,
    NormalizeResult,
    ReconcileRequest,
    ReconcileResult,
)
from cnes_contracts.manifests.raw import RawManifest

if TYPE_CHECKING:
    from pathlib import Path


MODELS: list[tuple[type, str]] = [
    (Profissional, "profissional.json"),
    (Estabelecimento, "estabelecimento.json"),
    (ProcedimentoSUS, "procedimentosus.json"),
    (CBO, "cbo.json"),
    (CID10, "cid10.json"),
    (Municipio, "municipio.json"),
    (Competencia, "competencia.json"),
    (VinculoCNES, "vinculocnes.json"),
    (ProducaoAmbulatorial, "producaoambulatorial.json"),
    (Internacao, "internacao.json"),
    (ProcedimentoAIH, "procedimentoaih.json"),
    (Extraction, "extraction.json"),
    (ExtractionRegisterPayload, "extractionregisterpayload.json"),
    (FileManifest, "file_manifest.json"),
    (JobTransitionEvent, "jobtransitionevent.json"),
    (UploadUrlRequest, "uploadurlrequest.json"),
    (UploadUrlResponse, "uploadurlresponse.json"),
    (RawManifest, "raw_manifest.json"),
    (OutputManifest, "output_manifest.json"),
    (RunManifest, "run_manifest.json"),
    (ServingDocument, "serving_document.json"),
    (NormalizeRequest, "normalize_request.json"),
    (NormalizeResult, "normalize_result.json"),
    (ReconcileRequest, "reconcile_request.json"),
    (ReconcileResult, "reconcile_result.json"),
    (MaterializeRequest, "materialize_request.json"),
    (MaterializeResult, "materialize_result.json"),
]


def export_all(target_dir: Path | str) -> list[Path]:
    """Export JSON Schema for all models.

    Args:
        target_dir: directory where JSON files are written

    Returns:
        list of Path objects written
    """
    from pathlib import Path as _Path

    target = _Path(target_dir) if not isinstance(target_dir, _Path) else target_dir
    target.mkdir(parents=True, exist_ok=True)
    written = []
    for model_cls, filename in MODELS:
        schema = model_cls.model_json_schema()
        path = target / filename
        path.write_text(
            json.dumps(schema, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        written.append(path)
    return written
