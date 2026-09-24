"""Testes de materialize_bpa: documentos serving exatos, limitados e sem PII."""

from __future__ import annotations

import hashlib
import json
import re
from contextlib import nullcontext
from dataclasses import dataclass, field
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl
import pytest

from cnes_contracts.manifests.processing import (
    MaterializeRequest,
    NormalizeRequest,
    ReconcileRequest,
)
from cnes_contracts.manifests.raw import RawManifest, SourceType
from cnes_domain.ports.object_store import ObjectStat
from data_processor.sources.bpa import serving
from data_processor.sources.bpa.contract import BPA_LAYOUT
from data_processor.sources.bpa.normalize import normalize_bpa
from data_processor.sources.bpa.reconcile import reconcile_bpa
from data_processor.sources.bpa.serving import materialize_bpa

if TYPE_CHECKING:
    from contextlib import AbstractContextManager as ContextManager
    from typing import BinaryIO

    from cnes_contracts.manifests.processing import MaterializeResult, ReconcileResult

_FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "bpa"
_TENANT = "354130"
_COMPETENCIA = "2026-08"
_RUN_ID = "fixture-bpa-run-v1"
_NOW = datetime(2026, 9, 2, 14, 0, tzinfo=UTC)
_SERVING = f"serving/{_TENANT}/{_RUN_ID}"
_DENY_KEYS = frozenset({
    "prd_cnsmed", "prd_cnspac", "prd_cpf_pcnte", "prd_nmpac", "prd_dtnasc",
    "nu_cns_prof", "nu_cns_pac", "nu_cpf_pac", "cns", "cpf", "cns_profissional",
    "cns_paciente", "cpf_paciente", "nome_paciente", "data_nascimento",
})
_DENY_VALUES = re.compile(r"999000000000\d{3}|90000000\d{3}|PACIENTE TESTE")


@dataclass
class _FakeObjectStore:
    objects: dict[str, bytes] = field(default_factory=dict)

    def put(self, key: str, body: BinaryIO, expected_sha256: str) -> ObjectStat:
        payload = body.read()
        self.objects[key] = payload
        return ObjectStat(key=key, size_bytes=len(payload), sha256=expected_sha256)

    def open(self, key: str) -> ContextManager[BinaryIO]:
        return nullcontext(BytesIO(self.objects[key]))

    def stat(self, key: str) -> ObjectStat | None:
        payload = self.objects.get(key)
        if payload is None:
            return None
        return ObjectStat(key=key, size_bytes=len(payload),
                          sha256=hashlib.sha256(payload).hexdigest())

    def delete(self, key: str) -> None:
        self.objects.pop(key, None)

    def promote(self, source_key: str, destination_key: str, expected_sha256: str) -> ObjectStat:
        raise NotImplementedError


def _load(name: str) -> object:
    return json.loads((_FIXTURES / name).read_text(encoding="utf-8"))


def _put_raw(store: _FakeObjectStore, file_subtype: str) -> RawManifest:
    template = _load(f"raw_manifest_{file_subtype.lower()}.json")
    rows = _load("raw_rows.json")[file_subtype]
    output = BytesIO()
    pl.DataFrame(rows, schema_overrides={"prd_qt_p": pl.Float64}).write_parquet(output)
    payload = output.getvalue()
    digest = hashlib.sha256(payload).hexdigest()
    store.put(template["object_key"], BytesIO(payload), digest)
    merged = {**template, "object_sha256": digest, "size_bytes": len(payload)}
    return RawManifest.model_validate_json(json.dumps(merged))


def _normalize_all(store: _FakeObjectStore) -> tuple:
    prefix = f"normalized/{_TENANT}/BPA_MAG/{_COMPETENCIA}/{_RUN_ID}"
    manifests = []
    for layout in BPA_LAYOUT.normalized:
        request = NormalizeRequest(
            tenant_id=_TENANT, run_id=_RUN_ID, unit_id=f"unit-{layout.file_subtype.lower()}",
            attempt=1, source_type=SourceType.BPA_MAG,
            raw_manifests=(_put_raw(store, layout.file_subtype),),
            target_keys=tuple(f"{prefix}/{name}" for name in layout.normalized_filenames),
            normalized_at=_NOW,
        )
        manifests.extend(normalize_bpa(request, store).manifests)
    return tuple(manifests)


def _reconcile(store: _FakeObjectStore) -> ReconcileResult:
    prefix = f"reconciliation/{_TENANT}/{_COMPETENCIA}/{_RUN_ID}"
    request = ReconcileRequest(
        tenant_id=_TENANT, competencia=_COMPETENCIA, run_id=_RUN_ID, unit_id="unit-reconcile",
        attempt=1, normalized_manifests=_normalize_all(store),
        reconciliation_key=f"{prefix}/{BPA_LAYOUT.reconciliation_filename}",
        divergence_key=f"{prefix}/{BPA_LAYOUT.divergence_filename}", reconciled_at=_NOW,
    )
    return reconcile_bpa(request, store)


def _materialize_request(
    reconciled: ReconcileResult, target_keys: tuple[str, ...] | None = None
) -> MaterializeRequest:
    if target_keys is None:
        target_keys = tuple(f"{_SERVING}/{name}.json" for name in BPA_LAYOUT.serving_documents)
    return MaterializeRequest(
        tenant_id=_TENANT, competencia=_COMPETENCIA, run_id=_RUN_ID, unit_id="unit-materialize",
        attempt=1, reconciliation_manifest=reconciled.reconciliation_manifest,
        divergence_manifest=reconciled.divergence_manifest, missing_sources=(),
        target_keys=target_keys, generated_at=_NOW,
    )


def _materialize() -> tuple[_FakeObjectStore, MaterializeResult]:
    store = _FakeObjectStore()
    result = materialize_bpa(_materialize_request(_reconcile(store)), store)
    return store, result


def _walk_keys(value: object) -> list[str]:
    if isinstance(value, dict):
        return [*value, *[key for item in value.values() for key in _walk_keys(item)]]
    if isinstance(value, list):
        return [key for item in value for key in _walk_keys(item)]
    return []


def _walk_strings(value: object) -> list[str]:
    if isinstance(value, dict):
        return [text for item in value.values() for text in _walk_strings(item)]
    if isinstance(value, list):
        return [text for item in value for text in _walk_strings(item)]
    return [value] if isinstance(value, str) else []


def test_materializa_exatamente_overview_e_by_establishment() -> None:
    store, result = _materialize()
    expected = _load("expected_serving.json")

    assert [item.document_name for item in result.documents] == ["by-establishment", "overview"]
    assert [item.object_key for item in result.manifests] == [
        f"{_SERVING}/by-establishment.json", f"{_SERVING}/overview.json",
    ]
    assert sorted(key for key in store.objects if key.startswith("serving/")) == [
        f"{_SERVING}/by-establishment.json", f"{_SERVING}/overview.json",
    ]
    for name in ("by-establishment", "overview"):
        assert json.loads(store.objects[f"{_SERVING}/{name}.json"]) == expected[name]


def test_manifests_serving_verificam_sha256_e_schema() -> None:
    store, result = _materialize()

    for manifest, document in zip(result.manifests, result.documents, strict=True):
        payload = store.objects[manifest.object_key]
        assert manifest.object_sha256 == hashlib.sha256(payload).hexdigest()
        assert manifest.schema_version == "bpa-serving-v1"
        assert document.schema_version == "bpa-serving-v1"
        assert json.loads(payload)["dataset"] == "bpa"


def test_serving_nao_contem_cns_nem_cpf_em_nenhum_nivel() -> None:
    store, _ = _materialize()

    for key in (f"{_SERVING}/by-establishment.json", f"{_SERVING}/overview.json"):
        document = json.loads(store.objects[key])
        assert not {name.lower() for name in _walk_keys(document)} & _DENY_KEYS
        assert not [text for text in _walk_strings(document) if _DENY_VALUES.search(text)]


def test_lista_de_estabelecimentos_e_limitada(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(serving, "_MAX_ESTABELECIMENTOS", 1)
    store, _ = _materialize()

    document = json.loads(store.objects[f"{_SERVING}/by-establishment.json"])

    assert document["limite"] == 1
    assert document["total_estabelecimentos"] == 2
    assert document["truncado"] is True
    assert [item["cnes"] for item in document["estabelecimentos"]] == ["2269481"]


def test_bytes_de_serving_sao_idempotentes() -> None:
    first, _ = _materialize()
    second, _ = _materialize()

    assert first.objects == second.objects


def test_rejeita_target_keys_fora_do_layout() -> None:
    store = _FakeObjectStore()
    reconciled = _reconcile(store)
    request = _materialize_request(reconciled, (f"{_SERVING}/overview.json",))

    with pytest.raises(ValueError, match="bpa_serving_target_keys_invalidos"):
        materialize_bpa(request, store)
