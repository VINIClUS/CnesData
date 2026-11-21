"""Seed sintético do canary de modo: raw, run e unidades por builders explícitos, sem pydantic."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Final, cast

from botocore.exceptions import ClientError

from cnes_domain.orchestration.mode_canary import is_canonical_mode_canary_tenant
from cnes_infra.control_plane.dynamodb_keys import (
    key_component,
    raw_manifest_lookup_key,
    run_entity_key,
    timestamp,
    unit_key,
)
from scripts.promotion._canonical import canonical_bytes, decode_json
from scripts.promotion.mode_canary_contract import ContractError

if TYPE_CHECKING:
    from collections.abc import Mapping

type Node = dict[str, Any]
SEED_SCHEMA: Final = "cnesdata.mode-canary-seed/v1"
FIXTURE_MANIFEST: Final = "fixture-manifest.json"
_FIXTURE_SCHEMA: Final = "cnesdata.mode-canary-fixtures/v1"
_FIXTURE_FIELDS: Final = frozenset({
    "schema", "competencia", "file_subtype", "files", "expectations", "synthetic_proof",
})
_FILE_FIELDS: Final = frozenset({
    "sha256", "size", "schema_version", "row_count", "source_type", "manifest_id", "origin",
})
_SOURCES: Final = frozenset({"CNES_LOCAL", "CNES_NACIONAL"})
_RUN_PREFIXES: Final = ("unit", "recovery-a", "recovery-b")
_DATASET: Final = "cnes"
_COMPETENCIA: Final = "2026-01"
_SUBTYPE: Final = "CNES_VINCULO"
_AGENT: Final = "mode-canary-seed"
_HEX64: Final = re.compile(r"[0-9a-f]{64}")
_MANIFEST_ID: Final = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
_MAX_RUNS: Final = 2
_SEPARATOR: Final = "\x1f"
# build_source_catalog().for_pipeline("cnes").dependencies, frozen here so the child skips pydantic.
_DEPENDENCIES: Final = (("CNES_LOCAL", True), ("CNES_NACIONAL", False))
ITEM_EXISTS: Final = frozenset({"ConditionalCheckFailedException"})
OBJECT_EXISTS: Final = frozenset({"PreconditionFailed", "ConditionalRequestConflict"})


@dataclass(frozen=True, slots=True)
class SeedBinding:
    """Identidade sintética autenticada do seed: tenant, runs do owner e instante declarado."""

    tenant_id: str
    run_ids: tuple[str, ...]
    owner_sha256: str
    created_at: datetime
    bucket: str


@dataclass(frozen=True, slots=True)
class RawSource:
    """Arquivo de fixture conferido, com o snapshot derivado do owner."""

    snapshot: str
    source_type: str
    schema_version: str
    object_sha256: str
    row_count: int
    size: int


@dataclass(frozen=True, slots=True)
class _Unit:
    unit_id: str
    stage: str
    source: RawSource | None
    depends_on: tuple[str, ...]
    partition: str


@dataclass(frozen=True, slots=True)
class SeedObject:
    key: str
    body: bytes


@dataclass(frozen=True, slots=True)
class ModeSeed:
    """Itens e objetos finitos do seed com o artifact `mode-seed` produzido antes das chamadas."""

    items: tuple[Node, ...]
    objects: tuple[SeedObject, ...]
    bucket: str
    manifest: bytes
    sha256: str


@dataclass(frozen=True, slots=True)
class SeedClients:
    dynamodb: Any
    s3: Any


def _require(condition: bool, field: str) -> None:
    if not condition:
        raise ContractError(f"reason=invalid field={field}")


def _check_binding(binding: SeedBinding) -> None:
    _require(is_canonical_mode_canary_tenant(binding.tenant_id), "tenant_id")
    owner = binding.owner_sha256
    _require(type(owner) is str and _HEX64.fullmatch(owner) is not None, "owner_sha256")
    allowed = {f"{prefix}-{owner[:16]}" for prefix in _RUN_PREFIXES}
    runs = binding.run_ids
    _require(type(runs) is tuple and 1 <= len(runs) <= _MAX_RUNS, "run_ids")
    _require(len(set(runs)) == len(runs) and set(runs) <= allowed, "run_ids")
    moment = binding.created_at
    _require(isinstance(moment, datetime) and moment.utcoffset() == timedelta(0), "created_at")
    bucket = binding.bucket
    _require(type(bucket) is str and bool(bucket) and "/" not in bucket, "bucket")


def _fixture_entry(name: str, entry: Any, body: bytes) -> Node:
    _require(type(entry) is dict and set(cast("Node", entry)) == _FILE_FIELDS, name)
    fields = cast("Node", entry)
    _require(fields["sha256"] == hashlib.sha256(body).hexdigest(), name)
    _require(fields["size"] == len(body) and fields["source_type"] in _SOURCES, name)
    _require(type(fields["row_count"]) is int and fields["row_count"] > 0, name)
    _require(type(fields["schema_version"]) is str and bool(fields["schema_version"]), name)
    _require(type(fields["manifest_id"]) is str
             and _MANIFEST_ID.fullmatch(fields["manifest_id"]) is not None, name)
    return fields


def _fixture_manifest(fixtures: Mapping[str, bytes]) -> dict[str, Node]:
    try:
        manifest = decode_json(fixtures[FIXTURE_MANIFEST])
    except (KeyError, ValueError) as error:
        raise ContractError("reason=invalid field=fixture_manifest") from error
    _require(type(manifest) is dict and set(manifest) == _FIXTURE_FIELDS, "fixture_manifest")
    fields = cast("Node", manifest)
    _require(fields["schema"] == _FIXTURE_SCHEMA, "schema")
    _require((fields["competencia"], fields["file_subtype"]) == (_COMPETENCIA, _SUBTYPE),
             "competencia")
    _require(set(fields["expectations"]) == {"gold_row_count"}, "expectations")
    _require(type(fields["files"]) is dict, "files")
    files = cast("dict[str, Any]", fields["files"])
    _require(set(fixtures) == {FIXTURE_MANIFEST, *files}, "fixtures")
    entries = {name: _fixture_entry(name, entry, fixtures[name]) for name, entry in files.items()}
    sources = sorted(entry["source_type"] for entry in entries.values())
    _require(sources == sorted(_SOURCES), "source_type")
    return entries


def _json(fields: Node) -> str:
    return json.dumps(fields, ensure_ascii=False, separators=(",", ":"))


def _instant(value: datetime) -> str:
    fraction = f".{value.microsecond:06d}" if value.microsecond else ""
    return f"{value:%Y-%m-%dT%H:%M:%S}{fraction}Z"


def _item(entity: str, key: tuple[str, str], payload: str,
          attributes: Mapping[str, str] | None = None) -> Node:
    item: Node = {"pk": {"S": key[0]}, "sk": {"S": key[1]}, "entity": {"S": entity},
                  "payload": {"S": payload}}
    for name, value in (attributes or {}).items():
        item[name] = {"S": value}
    return item


def _prefix(binding: SeedBinding, source: RawSource) -> str:
    return f"raw/{binding.tenant_id}/{source.source_type}/{_COMPETENCIA}/{source.snapshot}/"


def raw_manifest_body(binding: SeedBinding, source: RawSource) -> bytes:
    """Returns: `manifest.json` FULL na ordem de `RawManifest`, igual ao `model_dump_json`."""
    return _json({
        "manifest_version": 1, "manifest_id": source.snapshot, "tenant_id": binding.tenant_id,
        "source_type": source.source_type, "file_subtype": _SUBTYPE, "competencia": _COMPETENCIA,
        "agent_id": _AGENT, "agent_version": "1.0.0", "schema_version": source.schema_version,
        "snapshot_mode": "FULL", "snapshot_id": source.snapshot, "base_snapshot_id": None,
        "sequence": 1, "previous_manifest_sha256": None, "object_sha256": source.object_sha256,
        "row_count": source.row_count, "size_bytes": source.size,
        "object_key": _prefix(binding, source) + "data.parquet",
        "created_at": _instant(binding.created_at),
    }).encode()


def seed_raw_item(binding: SeedBinding, source: RawSource, manifest_sha256: str) -> Node:
    """Returns: Item RAWMANIFESTRECORD igual ao `encode_model` de `RawManifestRecord`."""
    payload = _json({
        "tenant_id": binding.tenant_id, "manifest_id": source.snapshot,
        "manifest_key": _prefix(binding, source) + "manifest.json", "agent_id": _AGENT,
        "source_type": source.source_type, "file_subtype": _SUBTYPE, "competencia": _COMPETENCIA,
        "snapshot_mode": "FULL", "snapshot_id": source.snapshot, "base_snapshot_id": None,
        "sequence": 1, "previous_manifest_sha256": None, "manifest_sha256": manifest_sha256,
        "created_at": _instant(binding.created_at),
    })
    key = raw_manifest_lookup_key(binding.tenant_id, source.snapshot)
    return _item("RAWMANIFESTRECORD", key, payload)


def seed_run_item(binding: SeedBinding, run_id: str) -> Node:
    """Returns: Item RUN PROCESSING igual ao `run_item` de `Run`, com o índice de recuperação."""
    tenant, created = binding.tenant_id, binding.created_at
    payload = _json({
        "tenant_id": tenant, "run_id": run_id, "competencia": _COMPETENCIA,
        "dataset_name": _DATASET, "state": "PROCESSING",
        "dependencies": [{"source_type": source, "file_subtype": _SUBTYPE, "required": required}
                         for source, required in _DEPENDENCIES],
        "missing_sources": [], "created_at": _instant(created),
    })
    recovery = f"{timestamp(created)}#{key_component(tenant)}#{key_component(run_id)}"
    return _item("RUN", run_entity_key(tenant, run_id), payload,
                 {"gsi4pk": "RUN_RECOVERABLE", "gsi4sk": recovery})


def _unit_id(run_id: str, stage: str, source: RawSource | None, partition: str) -> str:
    identity = (source.source_type, _SUBTYPE) if source is not None else ("", "")
    values = (run_id, stage, *identity, partition)
    return hashlib.sha256(_SEPARATOR.join(values).encode()).hexdigest()[:32]


def _unit_item(binding: SeedBinding, run_id: str, unit: _Unit) -> Node:
    tenant, source = binding.tenant_id, unit.source
    inputs = [] if source is None else [{
        "manifest_id": source.snapshot,
        "manifest_key": _prefix(binding, source) + "manifest.json",
    }]
    payload = _json({
        "tenant_id": tenant, "run_id": run_id, "unit_id": unit.unit_id, "stage": unit.stage,
        "source_type": None if source is None else source.source_type,
        "file_subtype": None if source is None else _SUBTYPE, "partition": unit.partition,
        "depends_on_unit_ids": list(unit.depends_on), "input_manifests": inputs,
        "state": "PENDING", "attempt": 0, "fencing_token": 0, "lease_owner": None,
        "lease_until": None, "dispatch_id": None, "output_manifests": [], "error_code": None,
    })
    attributes = {"gsi5pk": f"RUN_ITEMS#{key_component(tenant)}#{key_component(run_id)}",
                  "gsi5sk": f"UNIT#{key_component(unit.unit_id)}"}
    return _item("RUNUNIT", unit_key(tenant, run_id, unit.unit_id), payload, attributes)


def seed_unit_items(binding: SeedBinding, run_id: str,
                    sources: tuple[RawSource, ...]) -> list[Node]:
    """Returns: Itens RUNUNIT PENDING na ordem de `plan_run`: normalize, reconcile, materialize.

    Raises: ContractError: Fontes diferentes das duas dependências do pipeline cnes.
    """
    ordered = sorted(sources, key=lambda source: source.source_type)
    _require([source.source_type for source in ordered] == [name for name, _ in _DEPENDENCIES],
             "plan")
    normalize = [_Unit(_unit_id(run_id, "NORMALIZE", source, _COMPETENCIA), "NORMALIZE", source,
                       (), _COMPETENCIA) for source in ordered]
    reconcile = _Unit(_unit_id(run_id, "RECONCILE", None, "all"), "RECONCILE", None,
                      tuple(unit.unit_id for unit in normalize), "all")
    materialize = _Unit(_unit_id(run_id, "MATERIALIZE", None, "all"), "MATERIALIZE", None,
                        (reconcile.unit_id,), "all")
    return [_unit_item(binding, run_id, unit) for unit in (*normalize, reconcile, materialize)]


def _raw_source(binding: SeedBinding, entry: Node, body: bytes) -> RawSource:
    return RawSource(f"{entry['manifest_id']}-{binding.owner_sha256[:16]}", entry["source_type"],
                     entry["schema_version"], entry["sha256"], entry["row_count"], len(body))


def _item_sha(item: Node) -> str:
    return hashlib.sha256(canonical_bytes(item)).hexdigest()


def _item_entry(item: Node) -> Node:
    return {"pk": item["pk"]["S"], "sk": item["sk"]["S"], "sha256": _item_sha(item)}


def _object_entry(bucket: str, key: str, body: bytes) -> Node:
    return {"bucket": bucket, "key": key, "sha256": hashlib.sha256(body).hexdigest(),
            "size": len(body)}


def _seed_manifest(binding: SeedBinding, fixture: bytes, items: list[Node],
                   objects: list[SeedObject]) -> bytes:
    return canonical_bytes({
        "schema": SEED_SCHEMA, "owner_sha256": binding.owner_sha256,
        "tenant_id": binding.tenant_id, "run_ids": list(binding.run_ids),
        "fixture_manifest_sha256": hashlib.sha256(fixture).hexdigest(),
        "items": sorted((_item_entry(item) for item in items),
                        key=lambda entry: (entry["pk"], entry["sk"])),
        "objects": sorted((_object_entry(binding.bucket, obj.key, obj.body) for obj in objects),
                          key=lambda entry: entry["key"]),
    })


def build_mode_seed(binding: SeedBinding, fixtures: Mapping[str, bytes]) -> ModeSeed:
    """Returns: Itens/objetos do seed e o artifact `mode-seed` canônico.

    Raises: ContractError: Tenant, runs, owner, fixtures ou manifest fora do contrato fechado.
    """
    _check_binding(binding)
    entries = _fixture_manifest(fixtures)
    items: list[Node] = []
    objects: list[SeedObject] = []
    sources: list[RawSource] = []
    for name in sorted(entries):
        source = _raw_source(binding, entries[name], fixtures[name])
        body = raw_manifest_body(binding, source)
        items.append(seed_raw_item(binding, source, hashlib.sha256(body).hexdigest()))
        prefix = _prefix(binding, source)
        objects.extend([SeedObject(prefix + "data.parquet", fixtures[name]),
                        SeedObject(prefix + "manifest.json", body)])
        sources.append(source)
    for run_id in binding.run_ids:
        items.extend([seed_run_item(binding, run_id),
                      *seed_unit_items(binding, run_id, tuple(sources))])
    manifest = _seed_manifest(binding, fixtures[FIXTURE_MANIFEST], items, objects)
    return ModeSeed(tuple(items), tuple(objects), binding.bucket, manifest,
                    hashlib.sha256(manifest).hexdigest())


def _code(error: ClientError) -> str:
    return str(error.response.get("Error", {}).get("Code", ""))


def _put_item(client: Any, table: str, item: Node) -> Node:
    try:
        client.put_item(TableName=table, Item=item,
                        ConditionExpression="attribute_not_exists(pk)")
    except ClientError as error:
        if _code(error) not in ITEM_EXISTS:
            raise
    key = {"pk": item["pk"], "sk": item["sk"]}
    stored = client.get_item(TableName=table, Key=key, ConsistentRead=True).get("Item")
    if stored != item:
        raise ContractError(f"reason=seed_conflict sk={item['sk']['S']}")
    return _item_entry(cast("Node", stored))


def _put_object(client: Any, bucket: str, obj: SeedObject) -> Node:
    try:
        client.put_object(Bucket=bucket, Key=obj.key, Body=obj.body, IfNoneMatch="*")
    except ClientError as error:
        if _code(error) not in OBJECT_EXISTS:
            raise
    body = client.get_object(Bucket=bucket, Key=obj.key)["Body"].read()
    if body != obj.body:
        raise ContractError(f"reason=seed_conflict key={obj.key}")
    return _object_entry(bucket, obj.key, body)


def execute_mode_seed(seed: ModeSeed, clients: SeedClients, table: str) -> Node:
    """Returns: Recibo dos itens/objetos relidos após Put insert-only.

    Raises: ContractError: Item ou objeto já gravado com bytes diferentes (`seed_conflict`).
    """
    items = [_put_item(clients.dynamodb, table, item) for item in seed.items]
    objects = [_put_object(clients.s3, seed.bucket, obj) for obj in seed.objects]
    return {
        "seed_sha256": seed.sha256,
        "items": sorted(items, key=lambda entry: (entry["pk"], entry["sk"])),
        "objects": sorted(objects, key=lambda entry: entry["key"]),
    }


__all__ = [
    "FIXTURE_MANIFEST",
    "ITEM_EXISTS",
    "OBJECT_EXISTS",
    "SEED_SCHEMA",
    "ModeSeed",
    "RawSource",
    "SeedBinding",
    "SeedClients",
    "SeedObject",
    "build_mode_seed",
    "execute_mode_seed",
    "raw_manifest_body",
    "seed_raw_item",
    "seed_run_item",
    "seed_unit_items",
]
