"""Builders explícitos do seed: mesmos bytes dos codecs pydantic, sem pydantic no import."""
from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest

from cnes_contracts.manifests.raw import RawManifest, SnapshotMode, SourceType
from cnes_domain.control_plane.entities import RawManifestRecord, Run
from cnes_domain.control_plane.enums import RunState
from cnes_domain.orchestration.planner import PlanRequest, RawManifestRef, plan_run
from cnes_domain.orchestration.source_catalog import build_source_catalog
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.control_plane.dynamodb_codec import encode_model
from cnes_infra.control_plane.dynamodb_keys import raw_manifest_lookup_key
from cnes_infra.control_plane.dynamodb_run_codec import run_item
from scripts.promotion.mode_canary_contract import ContractError
from scripts.promotion.mode_canary_seed import (
    RawSource,
    SeedBinding,
    build_mode_seed,
    raw_manifest_body,
    seed_raw_item,
    seed_run_item,
    seed_unit_items,
)

_ROOT = Path(__file__).resolve().parents[3]
_FIXTURES = _ROOT / "tests/production/fixtures/mode_canary"
_VECTOR = json.loads(Path(__file__).with_name("mode_seed_vector.json").read_text("utf-8"))
_FILES = ("cnes-local-v1.parquet", "cnes-nacional-v1.parquet")
_OWNER = "4" * 64
_TENANT = "prod-canary-" + "0123456789abcdef" * 2
_BUCKET = "cnesdata-prod-data"


def _canonical(value: Any) -> bytes:
    text = json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return (text + "\n").encode()


def _fixtures() -> dict[str, bytes]:
    names = ("fixture-manifest.json", *_FILES)
    return {name: (_FIXTURES / name).read_bytes() for name in names}


def _binding(case: str) -> SeedBinding:
    entry = _VECTOR[case]
    moment = datetime.fromisoformat(entry["created_at"].replace("Z", "+00:00"))
    return SeedBinding(_TENANT, tuple(entry["run_ids"]), _OWNER, moment, _BUCKET)


def _sources() -> tuple[RawSource, ...]:
    files = json.loads((_FIXTURES / "fixture-manifest.json").read_text("utf-8"))["files"]
    return tuple(
        RawSource(f"{files[name]['manifest_id']}-{_OWNER[:16]}", files[name]["source_type"],
                  files[name]["schema_version"], files[name]["sha256"], files[name]["row_count"],
                  files[name]["size"])
        for name in sorted(files))


def _prefix(source: RawSource) -> str:
    return f"raw/{_TENANT}/{source.source_type}/2026-01/{source.snapshot}/"


def _oracle_manifest(binding: SeedBinding, source: RawSource) -> bytes:
    manifest = RawManifest(
        manifest_version=1, manifest_id=source.snapshot, tenant_id=binding.tenant_id,
        source_type=SourceType(source.source_type), file_subtype="CNES_VINCULO",
        competencia="2026-01", agent_id="mode-canary-seed", agent_version="1.0.0",
        schema_version=source.schema_version, snapshot_mode=SnapshotMode.FULL,
        snapshot_id=source.snapshot, base_snapshot_id=None, sequence=1,
        previous_manifest_sha256=None, object_sha256=source.object_sha256,
        row_count=source.row_count, size_bytes=source.size,
        object_key=_prefix(source) + "data.parquet", created_at=binding.created_at)
    return manifest.model_dump_json(exclude_none=False, by_alias=False).encode()


def _oracle_record(binding: SeedBinding, source: RawSource, digest: str) -> dict[str, Any]:
    record = RawManifestRecord(
        tenant_id=binding.tenant_id, manifest_id=source.snapshot,
        manifest_key=_prefix(source) + "manifest.json", agent_id="mode-canary-seed",
        source_type=source.source_type, file_subtype="CNES_VINCULO", competencia="2026-01",
        snapshot_mode="FULL", snapshot_id=source.snapshot, base_snapshot_id=None, sequence=1,
        previous_manifest_sha256=None, manifest_sha256=digest, created_at=binding.created_at)
    return encode_model(record, "RAWMANIFESTRECORD",
                        raw_manifest_lookup_key(binding.tenant_id, source.snapshot))


def _oracle_run(binding: SeedBinding, run_id: str) -> tuple[dict[str, Any], list[Any]]:
    fields: dict[str, Any] = {
        "tenant_id": binding.tenant_id, "run_id": run_id, "competencia": "2026-01",
        "dataset_name": "cnes", "dependencies": build_source_catalog().for_pipeline(
            "cnes").dependencies, "missing_sources": (), "created_at": binding.created_at}
    refs = tuple(RawManifestRef(source.snapshot, _prefix(source) + "manifest.json",
                                source.source_type, "CNES_VINCULO", "2026-01")
                 for source in _sources())
    plan = plan_run(PlanRequest(Run(**fields, state=RunState.PLANNED), refs, 1))
    adapter = DynamoDBControlPlane.__new__(DynamoDBControlPlane)
    return (run_item(Run(**fields, state=RunState.PROCESSING)),
            [adapter._unit_item(unit) for unit in plan.units])


@pytest.mark.parametrize("case", sorted(_VECTOR))
def test_seed_reproduz_os_bytes_do_vetor_dourado_da_base(case: str) -> None:
    seed = build_mode_seed(_binding(case), _fixtures())
    content = {"items": list(seed.items),
               "objects": [{"key": obj.key, "sha256": hashlib.sha256(obj.body).hexdigest()}
                           for obj in seed.objects],
               "manifest": seed.manifest.decode()}
    assert hashlib.sha256(_canonical(content)).hexdigest() == _VECTOR[case]["content_sha256"]
    assert seed.sha256 == _VECTOR[case]["seed_sha256"]


@pytest.mark.parametrize("case", sorted(_VECTOR))
def test_builders_explicitos_igualam_os_codecs_pydantic(case: str) -> None:
    binding = _binding(case)
    for source in _sources():
        body = raw_manifest_body(binding, source)
        assert body == _oracle_manifest(binding, source)
        digest = hashlib.sha256(body).hexdigest()
        item = seed_raw_item(binding, source, digest)
        assert _canonical(item) == _canonical(_oracle_record(binding, source, digest))
    for run_id in binding.run_ids:
        expected_run, expected_units = _oracle_run(binding, run_id)
        assert _canonical(seed_run_item(binding, run_id)) == _canonical(expected_run)
        units = seed_unit_items(binding, run_id, _sources())
        assert [_canonical(item) for item in units] == [_canonical(i) for i in expected_units]


def test_payload_dos_builders_preserva_a_ordem_de_declaracao_dos_modelos() -> None:
    binding = _binding("unit-microseconds")
    run = seed_run_item(binding, binding.run_ids[0])
    assert list(json.loads(run["payload"]["S"])) == list(Run.model_fields)
    assert run["payload"]["S"].endswith('"created_at":"2026-11-13T12:00:07.123456Z"}')
    assert run["gsi4sk"]["S"].startswith("2026-11-13T12:00:07.123456+00:00#")


def test_unidades_exigem_as_duas_fontes_do_pipeline_cnes() -> None:
    binding = _binding("unit")
    with pytest.raises(ContractError, match="field=plan"):
        seed_unit_items(binding, binding.run_ids[0], _sources()[:1])


@pytest.mark.parametrize(("field", "value"), [
    ("schema_version", ""), ("schema_version", 1), ("manifest_id", "a/b"), ("manifest_id", 7),
])
def test_fixture_recusa_snapshot_ou_schema_fora_da_forma(field: str, value: Any) -> None:
    fixtures = _fixtures()
    manifest = json.loads(fixtures["fixture-manifest.json"])
    manifest["files"][_FILES[0]][field] = value
    fixtures["fixture-manifest.json"] = json.dumps(manifest).encode()
    with pytest.raises(ContractError, match=f"field={_FILES[0]}"):
        build_mode_seed(_binding("unit"), fixtures)


def test_fixture_manifest_que_nao_e_json_e_recusado() -> None:
    with pytest.raises(ContractError, match="field=fixture_manifest"):
        build_mode_seed(_binding("unit"), _fixtures() | {"fixture-manifest.json": b"{"})
