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
from scripts.promotion import mode_canary_seed
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


def _digests(case: str) -> tuple[str, str]:
    seed = build_mode_seed(_binding(case), _fixtures())
    content = {"items": list(seed.items),
               "objects": [{"key": obj.key, "sha256": hashlib.sha256(obj.body).hexdigest()}
                           for obj in seed.objects],
               "manifest": seed.manifest.decode()}
    return hashlib.sha256(_canonical(content)).hexdigest(), seed.sha256


@pytest.mark.parametrize("case", sorted(_VECTOR))
def test_seed_sem_escopo_reproduz_os_bytes_do_vetor_dourado_da_base(
        monkeypatch: pytest.MonkeyPatch, case: str) -> None:
    monkeypatch.setattr(mode_canary_seed, "_scope", lambda binding: "")
    assert _digests(case) == (_VECTOR[case]["content_sha256"], _VECTOR[case]["seed_sha256"])


@pytest.mark.parametrize("case", sorted(_VECTOR))
def test_seed_com_escopo_do_modo_fixa_os_bytes_do_vetor_atual(case: str) -> None:
    assert _digests(case) == (_VECTOR[case]["scoped_content_sha256"],
                              _VECTOR[case]["scoped_seed_sha256"])


def test_identidade_raw_difere_entre_unit_e_recovery_do_mesmo_owner() -> None:
    unit = build_mode_seed(_binding("unit"), _fixtures())
    recovery = build_mode_seed(_binding("recovery"), _fixtures())
    raw = [{item["sk"]["S"] for item in seed.items if item["entity"]["S"] == "RAWMANIFESTRECORD"}
           for seed in (unit, recovery)]
    assert raw[0].isdisjoint(raw[1])
    keys = [{obj.key for obj in seed.objects} for seed in (unit, recovery)]
    assert keys[0].isdisjoint(keys[1])


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
    ("schema_version", ""), ("schema_version", 1), ("manifest_id", "a/b"),
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


def _mutated(entry_changes: dict[str, Any], body: bytes | None = None) -> dict[str, bytes]:
    fixtures = _fixtures()
    manifest = json.loads(fixtures["fixture-manifest.json"])
    name = "cnes-local-v1.parquet"
    if body is not None:
        fixtures[name] = body
        entry_changes = entry_changes | {"sha256": hashlib.sha256(body).hexdigest(),
                                         "size": len(body)}
    manifest["files"][name].update(entry_changes)
    fixtures["fixture-manifest.json"] = json.dumps(manifest).encode()
    return fixtures


def _oracle_accepts(fixtures: dict[str, bytes], binding: SeedBinding) -> bool:
    entry = json.loads(fixtures["fixture-manifest.json"])["files"]["cnes-local-v1.parquet"]
    body = fixtures["cnes-local-v1.parquet"]
    snapshot = f"{entry['manifest_id']}-unit-{binding.owner_sha256[:16]}"
    try:
        manifest = RawManifest(
            manifest_version=1, manifest_id=snapshot, tenant_id=binding.tenant_id,
            source_type=SourceType(entry["source_type"]), file_subtype="CNES_VINCULO",
            competencia="2026-01", agent_id="mode-canary-seed", agent_version="1.0.0",
            schema_version=entry["schema_version"], snapshot_mode=SnapshotMode.FULL,
            snapshot_id=snapshot, base_snapshot_id=None, sequence=1,
            previous_manifest_sha256=None, object_sha256=entry["sha256"],
            row_count=entry["row_count"], size_bytes=len(body),
            object_key=f"raw/{binding.tenant_id}/{entry['source_type']}/2026-01/{snapshot}/"
            "data.parquet", created_at=binding.created_at)
        RawManifestRecord(
            tenant_id=binding.tenant_id, manifest_id=snapshot,
            manifest_key=f"raw/{binding.tenant_id}/{entry['source_type']}/2026-01/{snapshot}/"
            "manifest.json", agent_id="mode-canary-seed", source_type=entry["source_type"],
            file_subtype="CNES_VINCULO", competencia="2026-01", snapshot_mode="FULL",
            snapshot_id=snapshot, base_snapshot_id=None, sequence=1,
            previous_manifest_sha256=None,
            manifest_sha256=hashlib.sha256(manifest.model_dump_json().encode()).hexdigest(),
            created_at=binding.created_at)
    except ValueError:
        return False
    return entry["row_count"] > 0 and type(entry["row_count"]) is int


_BOUNDARIES: list[tuple[str, dict[str, Any], bytes | None]] = [
    ("manifest_id_128", {"manifest_id": "a" * 128}, None),
    ("manifest_id_129", {"manifest_id": "a" * 129}, None),
    ("manifest_id_300", {"manifest_id": "a" * 300}, None),
    ("manifest_id_pontuado", {"manifest_id": "a.b_c-d"}, None),
    ("manifest_id_vazio", {"manifest_id": ""}, None),
    ("manifest_id_inicia_hifen", {"manifest_id": "-a"}, None),
    ("manifest_id_inicia_ponto", {"manifest_id": ".a"}, None),
    ("manifest_id_barra", {"manifest_id": "a/b"}, None),
    ("manifest_id_cerquilha", {"manifest_id": "a#b"}, None),
    ("manifest_id_acentuado", {"manifest_id": "ação"}, None),
    ("manifest_id_quebra_de_linha", {"manifest_id": "a\nb"}, None),
    ("manifest_id_nulo", {"manifest_id": None}, None),
    ("manifest_id_numero", {"manifest_id": 7}, None),
    ("corpo_vazio", {}, b""),
    ("corpo_de_um_byte", {}, b"x"),
    ("linhas_zero", {"row_count": 0}, None),
    ("linhas_um", {"row_count": 1}, None),
    ("linhas_booleano", {"row_count": True}, None),
    ("schema_vazio", {"schema_version": ""}, None),
    ("schema_numerico", {"schema_version": 1}, None),
    ("origem_desconhecida", {"source_type": "SIHD"}, None),
]


@pytest.mark.parametrize(("changes", "body"), [case[1:] for case in _BOUNDARIES],
                         ids=[case[0] for case in _BOUNDARIES])
def test_builder_aceita_exatamente_o_que_os_modelos_pydantic_aceitavam(
        changes: dict[str, Any], body: bytes | None) -> None:
    fixtures = _mutated(changes, body)
    binding = _binding("unit")
    try:
        build_mode_seed(binding, fixtures)
    except ContractError:
        accepted = False
    else:
        accepted = True
    if changes.get("source_type") == "SIHD":
        return
    assert accepted == _oracle_accepts(fixtures, binding)
