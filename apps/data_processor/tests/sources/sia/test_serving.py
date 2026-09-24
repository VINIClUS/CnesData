"""TDD de materialize_sia: documentos exatos e deny-list recursiva de PII."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

from data_processor.sources.sia.contract import SiaContractError
from data_processor.sources.sia.serving import materialize_sia

if TYPE_CHECKING:
    from cnes_contracts.manifests.processing import ReconcileResult

    from .conftest import SiaHarness

_DENY_LIST = (
    "PA_CNSPCN", "PRD_CPFPCT", "PRD_CNSPCN", "BPI_CNSMED", "BPI_CPFPCT", "BPI_CNSPAC",
    "APA_CPFRES", "APA_CPFDIR", "APA_CPFPCT", "APA_CNSPCT", "APA_CNSRES", "APA_CNSDIR",
    "APA_CNSEXE", "cns_profissional", "cns_paciente", "cpf", "cns",
)


def _materialize(sia: SiaHarness, **overrides: object):
    result: ReconcileResult = sia.reconcile_all()
    return materialize_sia(sia.materialize_request(result, **overrides), sia.store)


def _walk(value: object):
    if isinstance(value, dict):
        for key, item in value.items():
            yield key
            yield from _walk(item)
    elif isinstance(value, list):
        for item in value:
            yield from _walk(item)
    elif isinstance(value, str):
        yield value


def test_materializa_overview_e_by_establishment_iguais_ao_expected(sia: SiaHarness) -> None:
    result = _materialize(sia)

    expected = sia.load_fixture("expected_serving.json")
    assert [document.document_name for document in result.documents] == [
        "by-establishment", "overview",
    ]
    for manifest, document in zip(result.manifests, result.documents, strict=True):
        assert manifest.object_key == (
            f"serving/354130/run-sia-1/{document.document_name}.json"
        )
        assert manifest.schema_version == f"sia-{document.document_name}-v1"
        assert json.loads(sia.store.objects[manifest.object_key]) == expected[
            document.document_name
        ]


def test_grava_somente_os_dois_documentos_de_serving(sia: SiaHarness) -> None:
    request = sia.materialize_request(sia.reconcile_all())

    materialize_sia(request, sia.store)

    serving = {key for key in sia.store.objects if key.startswith("serving/")}
    assert serving == set(request.target_keys)


def test_documentos_declaram_dataset_sia(sia: SiaHarness) -> None:
    result = _materialize(sia)

    assert {document.payload["dataset"] for document in result.documents} == {"sia"}


def test_serving_nao_contem_campo_da_deny_list(sia: SiaHarness) -> None:
    result = _materialize(sia)

    denied = {name.lower() for name in _DENY_LIST}
    for manifest in result.manifests:
        document = json.loads(sia.store.objects[manifest.object_key])
        tokens = {token.lower() for token in _walk(document)}
        assert tokens.isdisjoint(denied)
        assert not any("cns" in token or "cpf" in token for token in tokens)


def test_serving_nao_vaza_cns_das_fixtures_raw(sia: SiaHarness) -> None:
    result = _materialize(sia)

    identifiers = {
        value.strip()
        for rows in sia.load_fixture("raw_rows.json").values()
        for row in rows
        for column, value in row.items()
        if ("cns" in column or "cpf" in column) and value and value.strip()
    }
    assert identifiers
    for manifest in result.manifests:
        body = sia.store.objects[manifest.object_key].decode("utf-8")
        assert not any(identifier in body for identifier in identifiers)


def test_missing_sources_chegam_ao_overview(sia: SiaHarness) -> None:
    result = _materialize(sia, missing_sources=("SIA_BPIHST",))

    overview = next(item for item in result.documents if item.document_name == "overview")
    assert overview.payload["missing_sources"] == ["SIA_BPIHST"]


def test_bytes_de_serving_sao_idempotentes(sia: SiaHarness) -> None:
    request = sia.materialize_request(sia.reconcile_all())

    assert materialize_sia(request, sia.store) == materialize_sia(request, sia.store)


def test_rejeita_targets_fora_do_layout(sia: SiaHarness) -> None:
    request = sia.materialize_request(
        sia.reconcile_all(), target_keys=("serving/354130/run-sia-1/overview.json",)
    )

    with pytest.raises(SiaContractError, match="sia_serving_targets_invalid"):
        materialize_sia(request, sia.store)
