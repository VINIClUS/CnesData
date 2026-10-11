"""Serializacao canonica dos relatorios por dataset e do agregado."""
from __future__ import annotations

import json
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

import pytest

from cnes_contracts.manifests.outputs import OutputManifest
from data_processor.migration.equivalence import (
    ComparisonStatus,
    DocumentSpec,
    MetricComparison,
    parse_contract,
)
from data_processor.migration.flatten import EMPTY_LIST
from data_processor.migration.report import (
    Covered,
    Failure,
    OutputEvidence,
    Request,
    SourceEquivalenceReport,
    Stamp,
    aggregate_accepted,
    aggregate_bytes,
    build_aggregate,
    output_evidence,
    report_bytes,
    sha256_hex,
    window_months,
)

_ROOT = Path(__file__).resolve().parents[4]
_CONTRACT = _ROOT / "docs" / "fixtures" / "migration" / "equivalence-contract-v1.json"
_NOW = datetime(2026, 10, 10, 12, tzinfo=UTC)
_MATCH, _EXPLAINED, _MISMATCH = (
    ComparisonStatus.MATCH, ComparisonStatus.EXPLAINED, ComparisonStatus.MISMATCH,
)


def _report(*comparisons: MetricComparison) -> SourceEquivalenceReport:
    return SourceEquivalenceReport(
        tenant_id="354130", dataset="sihd", source_types=("SIHD",), competencia="2026-01",
        legacy_sha256="a" * 64, candidate_version_id="run-1", comparisons=comparisons,
    )


def test_relatorio_vazio_ou_com_mismatch_nao_e_aceito() -> None:
    match = MetricComparison("d::x", 1, 1, _MATCH, None)
    explained = MetricComparison("d::y", "a", "b", _EXPLAINED, "R-1")
    mismatch = MetricComparison("d::z", 1, 2, _MISMATCH, None)

    assert _report().accepted is False
    assert _report(match, mismatch).accepted is False
    assert _report(match, explained).accepted is True


def test_serializa_relatorio_e_agregado_com_sha256_estavel() -> None:
    comparisons = (
        MetricComparison("d::x", 1, 1, _MATCH, None),
        MetricComparison("d::y", "a", "b", _EXPLAINED, "R-1"),
        MetricComparison("d::z", None, "c", _MISMATCH, None, "legacy"),
    )
    evidence: dict[str, object] = {"run_manifest_sha256": "b" * 64, "outputs": [{"asserted": True}]}

    first = report_bytes(_report(*comparisons), evidence)
    second = report_bytes(_report(*comparisons), dict(reversed(list(evidence.items()))))
    payload = json.loads(first)

    assert first == second
    assert list(payload) == sorted(payload)
    assert payload["accepted"] is False
    assert payload["summary"] == {"EXPLAINED": 1, "MATCH": 1, "MISMATCH": 1}
    assert payload["comparisons"][2]["absent"] == "legacy"
    assert sha256_hex(first) == sha256(first).hexdigest()
    shuffled = {"reports": {"b": "2", "a": "1"}, "commit": "abc"}
    ordered = {"commit": "abc", "reports": {"a": "1", "b": "2"}}
    assert aggregate_bytes(shuffled) == aggregate_bytes(ordered)


def test_serializa_container_vazio_como_objeto_tipado() -> None:
    item = MetricComparison("d::x", EMPTY_LIST, None, _MISMATCH, None, "candidate")

    payload = json.loads(report_bytes(_report(item), {}))

    assert payload["comparisons"][0]["legacy_value"] == {"empty": "list"}
    assert payload["comparisons"][0]["candidate_value"] is None


def _output(layer: Any, name: str) -> OutputManifest:
    prefix = {"reconciliation": "reconciliation/354130/2026-01/r1", "serving": "serving/354130/r1"}
    return OutputManifest(
        manifest_version=1, manifest_id=f"{layer}-{name}", tenant_id="354130", layer=layer,
        source_type=None, competencia="2026-01", run_id="r1", unit_id="u1", attempt=1,
        schema_version="v1", object_key=f"{prefix[layer]}/{name}", object_sha256="a" * 64,
        row_count=3, created_at=_NOW,
    )


def test_marca_como_afirmada_so_a_saida_declarada_no_contrato() -> None:
    document = DocumentSpec.model_validate({
        "doc_id": "sihd-serving-overview", "oracle": {"file": "x.json"},
        "candidate": {"layer": "serving", "leaf": "overview.json"},
    })
    outputs = [
        _output("reconciliation", "overview.json"), _output("serving", "overview.json"),
        _output("serving", "extra.json"),
    ]

    evidence = output_evidence(outputs, [document])

    flags = [(item.layer, item.object_key.rpartition("/")[2], item.asserted) for item in evidence]
    assert flags == [
        ("reconciliation", "overview.json", False), ("serving", "overview.json", True),
        ("serving", "extra.json", False),
    ]
    assert {item.row_count for item in evidence} == {3}


@pytest.mark.parametrize(("first", "last", "expected"), [
    ("2026-05", "2026-05", ["2026-05"]),
    ("2026-11", "2027-02", ["2026-11", "2026-12", "2027-01", "2027-02"]),
    ("2026-05", "2026-04", []),
])
def test_lista_os_meses_da_janela_pedida(first: str, last: str, expected: list[str]) -> None:
    assert window_months(first, last) == tuple(expected)
    assert len(window_months("2026-01", "2026-12")) == 12


def _covered(dataset: str, competencia: str, *flags: bool, accepted: bool = True) -> Covered:
    outputs = tuple(
        OutputEvidence(flag, "serving" if flag else "reconciliation", f"k{i}", "h" * 64, 1)
        for i, flag in enumerate(flags)
    )
    name = f"{dataset}/{competencia}.json"
    return Covered(dataset, competencia, accepted, name, "e" * 64, outputs)


def test_aceita_o_agregado_somente_com_job_e_sem_falha_ou_mismatch() -> None:
    ok = _covered("bpa", "2026-08")
    failure = Failure("sihd", "2026-01", "wave_timeout run_id=x")

    assert aggregate_accepted([]) is False
    assert aggregate_accepted([ok]) is True
    assert aggregate_accepted([ok, _covered("sia", "2026-01", accepted=False)]) is False
    assert aggregate_accepted([ok, failure]) is False


def test_agregado_registra_pedido_cobertura_proveniencia_e_saidas_por_dataset() -> None:
    contract = parse_contract(_CONTRACT.read_bytes())
    outcomes = [
        _covered("bpa", "2026-08", True, True, False), Failure("sihd", "2026-01", "erro key=v"),
    ]

    payload: dict[str, Any] = build_aggregate(
        Stamp("354130", "c" * 64, "d" * 40), contract,
        Request(("bpa", "sihd"), "2026-01", "2026-12"), outcomes,
    )

    assert payload["accepted"] is False
    assert payload["requested"] == {"from": "2026-01", "sources": ["bpa", "sihd"], "to": "2026-12"}
    assert payload["failures"] == [
        {"competencia": "2026-01", "dataset": "sihd", "error": "erro key=v"},
    ]
    assert payload["covered"] == [{
        "accepted": True, "competencia": "2026-08", "dataset": "bpa",
        "report": "bpa/2026-08.json", "report_sha256": "e" * 64,
    }]
    bpa, sihd = payload["datasets"]["bpa"], payload["datasets"]["sihd"]
    assert bpa["covered"] == ["2026-08"]
    assert bpa["uncovered"] == [f"2026-{n:02d}" for n in (1, 2, 3, 4, 5, 6, 7, 9, 10, 11, 12)]
    assert bpa["provenance"]["kind"] == "reproduction"
    assert bpa["provenance"]["data_nature"] == "synthetic"
    assert bpa["outputs"] == {
        "asserted": 2, "unasserted": 1, "unasserted_layers": ["reconciliation"],
    }
    assert sihd["covered"] == ["2026-01"]
    assert sihd["outputs"] == {"asserted": 0, "unasserted": 0, "unasserted_layers": []}
    assert (payload["contract_sha256"], payload["git_commit"]) == ("c" * 64, "d" * 40)
