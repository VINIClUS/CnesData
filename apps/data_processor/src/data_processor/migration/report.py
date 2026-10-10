"""Serializacao canonica dos relatorios por dataset e montagem do agregado MIG-010."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from hashlib import sha256
from typing import TYPE_CHECKING

from data_processor.migration.equivalence import ComparisonStatus

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence

    from cnes_contracts.manifests.outputs import OutputManifest
    from data_processor.migration.equivalence import (
        DatasetSpec,
        DocumentSpec,
        EquivalenceContract,
        MetricComparison,
    )


@dataclass(frozen=True, slots=True)
class SourceEquivalenceReport:
    tenant_id: str
    dataset: str
    source_types: tuple[str, ...]
    competencia: str
    legacy_sha256: str
    candidate_version_id: str
    comparisons: tuple[MetricComparison, ...]

    @property
    def accepted(self) -> bool:
        return bool(self.comparisons) and all(
            item.status is not ComparisonStatus.MISMATCH for item in self.comparisons
        )


@dataclass(frozen=True, slots=True)
class OutputEvidence:
    asserted: bool
    layer: str
    object_key: str
    object_sha256: str
    row_count: int


@dataclass(frozen=True, slots=True)
class Request:
    sources: tuple[str, ...]
    first: str
    last: str


@dataclass(frozen=True, slots=True)
class Stamp:
    tenant_id: str
    contract_sha256: str
    git_commit: str


@dataclass(frozen=True, slots=True)
class Covered:
    dataset: str
    competencia: str
    accepted: bool
    report: str
    report_sha256: str
    outputs: tuple[OutputEvidence, ...]


@dataclass(frozen=True, slots=True)
class Failure:
    dataset: str
    competencia: str
    error: str


type Outcome = Covered | Failure


def _dump(payload: Mapping[str, object]) -> bytes:
    text = json.dumps(payload, sort_keys=True, indent=2, ensure_ascii=False, allow_nan=False)
    return f"{text}\n".encode()


def report_bytes(report: SourceEquivalenceReport, evidence: Mapping[str, object]) -> bytes:
    """Serializa o relatorio por dataset em JSON de chaves ordenadas.

    Args: report: comparacoes do dataset. evidence: proveniencia, saidas e hashes do candidato.
    Returns: bytes canonicos (UTF-8, chaves ordenadas, newline final).
    """
    counts = [item.status.value for item in report.comparisons]
    payload: dict[str, object] = {
        **evidence,
        "tenant_id": report.tenant_id,
        "dataset": report.dataset,
        "source_types": list(report.source_types),
        "competencia": report.competencia,
        "legacy_sha256": report.legacy_sha256,
        "candidate_version_id": report.candidate_version_id,
        "accepted": report.accepted,
        "summary": {status.value: counts.count(status.value) for status in ComparisonStatus},
        "comparisons": [asdict(item) for item in report.comparisons],
    }
    return _dump(payload)


def aggregate_bytes(payload: Mapping[str, object]) -> bytes:
    """Serializa o agregado em JSON de chaves ordenadas.

    Args: payload: hashes dos relatorios, cobertura, falhas, contrato e commit.
    Returns: bytes canonicos (UTF-8, chaves ordenadas, newline final).
    """
    return _dump(payload)


def sha256_hex(data: bytes) -> str:
    """Args: data: bytes a resumir. Returns: SHA-256 hexadecimal minusculo."""
    return sha256(data).hexdigest()


def output_evidence(
    outputs: Iterable[OutputManifest], documents: Iterable[DocumentSpec]
) -> tuple[OutputEvidence, ...]:
    """Marca cada saida publicada como afirmada (tem oraculo no contrato) ou nao.

    Args: outputs: saidas do RunManifest. documents: documentos comparados do dataset.
    Returns: uma evidencia por saida, na ordem do manifest.
    """
    declared = {(item.candidate.layer, item.candidate.leaf) for item in documents}
    return tuple(
        OutputEvidence(
            (output.layer, output.object_key.rpartition("/")[2]) in declared,
            output.layer, output.object_key, output.object_sha256, output.row_count,
        )
        for output in outputs
    )


def _month_index(competencia: str) -> int:
    year, month = competencia.split("-")
    return int(year) * 12 + int(month) - 1


def window_months(first: str, last: str) -> tuple[str, ...]:
    """Lista os meses da janela pedida.

    Args: first/last: competencias `YYYY-MM` inicial e final, inclusivas.
    Returns: todos os meses da janela em ordem; vazio se `first` for posterior a `last`.
    """
    span = range(_month_index(first), _month_index(last) + 1)
    return tuple(f"{index // 12:04d}-{index % 12 + 1:02d}" for index in span)


def aggregate_accepted(outcomes: Sequence[Outcome]) -> bool:
    """Args: outcomes: resultado de cada job. Returns: True se ha job e todos foram aceitos."""
    return bool(outcomes) and all(isinstance(item, Covered) and item.accepted for item in outcomes)


def _dataset_summary(
    spec: DatasetSpec, months: Sequence[str], jobs: Sequence[Covered]
) -> dict[str, object]:
    unasserted = [output for job in jobs for output in job.outputs if not output.asserted]
    total = sum(len(job.outputs) for job in jobs)
    return {
        "covered": [month for month in months if month in spec.competencias],
        "outputs": {
            "asserted": total - len(unasserted),
            "unasserted": len(unasserted),
            "unasserted_layers": sorted({output.layer for output in unasserted}),
        },
        "provenance": spec.provenance.model_dump(),
        "uncovered": [month for month in months if month not in spec.competencias],
    }


def build_aggregate(
    stamp: Stamp, contract: EquivalenceContract, request: Request, outcomes: Sequence[Outcome]
) -> dict[str, object]:
    """Monta o agregado: pedido, cobertura da janela, proveniencia, relatorios e falhas.

    Args: stamp: tenant, hash do contrato e commit. contract: contrato aplicado. request:
        fontes e janela pedidas. outcomes: relatorio gravado ou falha de cada job.
    Returns: payload serializavel com `accepted` e, por dataset, meses cobertos e descobertos.
    """
    months = window_months(request.first, request.last)
    covered = [item for item in outcomes if isinstance(item, Covered)]
    return {
        "accepted": aggregate_accepted(outcomes),
        "contract_sha256": stamp.contract_sha256,
        "contract_version": contract.contract_version,
        "covered": [
            {
                "accepted": item.accepted, "competencia": item.competencia,
                "dataset": item.dataset, "report": item.report, "report_sha256": item.report_sha256,
            }
            for item in covered
        ],
        "datasets": {
            name: _dataset_summary(
                contract.datasets[name], months, [job for job in covered if job.dataset == name]
            )
            for name in request.sources
        },
        "failures": [asdict(item) for item in outcomes if isinstance(item, Failure)],
        "git_commit": stamp.git_commit,
        "requested": {"from": request.first, "sources": list(request.sources), "to": request.last},
        "tenant_id": stamp.tenant_id,
    }
