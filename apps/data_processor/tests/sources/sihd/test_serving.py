"""TDD de materialize_sihd: overview agregado igual a fixture e sem PII."""

from __future__ import annotations

import json

import pytest

from apps.data_processor.tests.sources.sihd import (
    COMPETENCIA,
    NOW,
    RUN_ID,
    TENANT,
    FakeObjectStore,
    load_json,
    normalize_all,
    raw_rows,
    reconcile_request,
)
from cnes_contracts.manifests.processing import MaterializeRequest
from data_processor.sources.sihd.contract import PII_DENY_LIST
from data_processor.sources.sihd.reconcile import reconcile_sihd
from data_processor.sources.sihd.serving import _assert_no_pii, materialize_sihd

_TARGET = f"serving/{TENANT}/{RUN_ID}/overview.json"


def _request(store: FakeObjectStore, target_key: str = _TARGET) -> MaterializeRequest:
    reconciled = reconcile_sihd(reconcile_request(normalize_all(store)), store)
    return MaterializeRequest(
        tenant_id=TENANT,
        competencia=COMPETENCIA,
        run_id=RUN_ID,
        unit_id="unit-materialize",
        attempt=1,
        reconciliation_manifest=reconciled.reconciliation_manifest,
        divergence_manifest=reconciled.divergence_manifest,
        missing_sources=(),
        target_keys=(target_key,),
        generated_at=NOW,
    )


def _walk(value: object) -> list[str]:
    if isinstance(value, dict):
        return [item for key, child in value.items() for item in (key, *_walk(child))]
    if isinstance(value, list):
        return [item for child in value for item in _walk(child)]
    return [value] if isinstance(value, str) else []


def test_serving_igual_a_fixture_congelada() -> None:
    store = FakeObjectStore()
    result = materialize_sihd(_request(store), store)
    rendered = json.loads(store.objects[_TARGET])
    assert rendered == load_json("expected_serving.json")
    payload = result.documents[0].payload
    assert {key: rendered[key] for key in payload} == payload


def test_retorna_documento_overview_do_dataset_sihd() -> None:
    store = FakeObjectStore()
    result = materialize_sihd(_request(store), store)
    (document,) = result.documents
    (manifest,) = result.manifests
    assert document.document_name == "overview"
    assert document.schema_version == "sihd-serving-v1"
    assert document.payload["dataset"] == "sihd"
    assert manifest.object_key == _TARGET
    assert manifest.object_sha256 == store.stat(_TARGET).sha256


def test_serving_nao_contem_pii_nem_identificador_de_linha() -> None:
    store = FakeObjectStore()
    materialize_sihd(_request(store), store)
    strings = _walk(json.loads(store.objects[_TARGET]))
    denied = {name.lower() for name in PII_DENY_LIST}
    assert not [item for item in strings if item.lower() in denied]
    raw_values = {
        str(value)
        for subtype in ("SIHD_INTERNACAO", "SIHD_PROC_AIH")
        for row in raw_rows(subtype)
        for name, value in row.items()
        if name in PII_DENY_LIST or name.endswith("NUM_AIH")
    }
    assert not [item for item in strings if item in raw_values]


def test_guarda_de_pii_rejeita_chave_proibida_aninhada() -> None:
    with pytest.raises(ValueError, match="pii_field_in_serving"):
        _assert_no_pii({"por_cnes": [{"cnes": "1", "Paciente_Nome": "x"}]})


def test_materializa_bytes_identicos_para_mesma_entrada() -> None:
    first = FakeObjectStore()
    second = FakeObjectStore()
    materialize_sihd(_request(first), first)
    materialize_sihd(_request(second), second)
    assert first.objects[_TARGET] == second.objects[_TARGET]


def test_rejeita_target_key_fora_do_layout() -> None:
    store = FakeObjectStore()
    request = _request(store, f"serving/{TENANT}/{RUN_ID}/outro.json")
    with pytest.raises(ValueError, match="unexpected_target_key"):
        materialize_sihd(request, store)
