"""Seed sintético do canary de modo: raw, run e unidades pelos codecs nativos."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Final, cast

from botocore.exceptions import ClientError

from cnes_contracts.manifests.raw import RawManifest, SnapshotMode, SourceType
from cnes_domain.control_plane.entities import RawManifestRecord, Run
from cnes_domain.control_plane.enums import RunState
from cnes_domain.orchestration.mode_canary import is_canonical_mode_canary_tenant
from cnes_domain.orchestration.planner import PlanRequest, RawManifestRef, plan_run
from cnes_domain.orchestration.source_catalog import build_source_catalog
from cnes_infra.control_plane.dynamodb_codec import encode_model
from cnes_infra.control_plane.dynamodb_keys import (
    key_component,
    raw_manifest_lookup_key,
    unit_key,
)
from cnes_infra.control_plane.dynamodb_run_codec import run_item
from scripts.promotion._canonical import canonical_bytes, decode_json
from scripts.promotion.mode_canary_contract import ContractError

if TYPE_CHECKING:
    from collections.abc import Mapping

    from cnes_domain.control_plane.entities import RunUnit

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
_MAX_RUNS: Final = 2


@dataclass(frozen=True, slots=True)
class SeedBinding:
    """Identidade sintética autenticada do seed: tenant, runs do owner e instante declarado."""

    tenant_id: str
    run_ids: tuple[str, ...]
    owner_sha256: str
    created_at: datetime
    bucket: str


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


def _raw_inputs(binding: SeedBinding, entry: Node, body: bytes) -> tuple[Node, list[SeedObject],
                                                                          RawManifestRef]:
    snapshot = f"{entry['manifest_id']}-{binding.owner_sha256[:16]}"
    source = entry["source_type"]
    prefix = f"raw/{binding.tenant_id}/{source}/{_COMPETENCIA}/{snapshot}/"
    manifest = RawManifest(
        manifest_version=1, manifest_id=snapshot, tenant_id=binding.tenant_id,
        source_type=SourceType(source), file_subtype=_SUBTYPE, competencia=_COMPETENCIA,
        agent_id=_AGENT, agent_version="1.0.0", schema_version=entry["schema_version"],
        snapshot_mode=SnapshotMode.FULL, snapshot_id=snapshot, base_snapshot_id=None,
        sequence=1, previous_manifest_sha256=None, object_sha256=entry["sha256"],
        row_count=entry["row_count"], size_bytes=len(body), object_key=prefix + "data.parquet",
        created_at=binding.created_at,
    )
    manifest_body = manifest.model_dump_json(exclude_none=False, by_alias=False).encode()
    record = RawManifestRecord(
        tenant_id=binding.tenant_id, manifest_id=snapshot,
        manifest_key=prefix + "manifest.json", agent_id=_AGENT, source_type=source,
        file_subtype=_SUBTYPE, competencia=_COMPETENCIA, snapshot_mode="FULL",
        snapshot_id=snapshot, base_snapshot_id=None, sequence=1, previous_manifest_sha256=None,
        manifest_sha256=hashlib.sha256(manifest_body).hexdigest(), created_at=binding.created_at,
    )
    lookup = encode_model(record, "RAWMANIFESTRECORD",
                          raw_manifest_lookup_key(binding.tenant_id, snapshot))
    objects = [SeedObject(prefix + "data.parquet", body),
               SeedObject(prefix + "manifest.json", manifest_body)]
    ref = RawManifestRef(snapshot, prefix + "manifest.json", source, _SUBTYPE, _COMPETENCIA)
    return lookup, objects, ref


def _unit_item(unit: RunUnit) -> Node:
    attributes = {
        "gsi5pk": f"RUN_ITEMS#{key_component(unit.tenant_id)}#{key_component(unit.run_id)}",
        "gsi5sk": f"UNIT#{key_component(unit.unit_id)}",
    }
    return encode_model(unit, "RUNUNIT", unit_key(unit.tenant_id, unit.run_id, unit.unit_id),
                        attributes)


def _run_items(binding: SeedBinding, run_id: str, refs: tuple[RawManifestRef, ...]) -> list[Node]:
    definition = build_source_catalog().for_pipeline(_DATASET)
    fields: Node = {
        "tenant_id": binding.tenant_id, "run_id": run_id, "competencia": _COMPETENCIA,
        "dataset_name": _DATASET, "dependencies": definition.dependencies,
        "missing_sources": (), "created_at": binding.created_at,
    }
    plan = plan_run(PlanRequest(Run(**fields, state=RunState.PLANNED), refs, 1))
    _require(not plan.missing_required and bool(plan.units), "plan")
    processing = Run(**fields, state=RunState.PROCESSING)
    return [run_item(processing), *(_unit_item(unit) for unit in plan.units)]


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
    refs: list[RawManifestRef] = []
    for name in sorted(entries):
        lookup, raw_objects, ref = _raw_inputs(binding, entries[name], fixtures[name])
        items.append(lookup)
        objects.extend(raw_objects)
        refs.append(ref)
    for run_id in binding.run_ids:
        items.extend(_run_items(binding, run_id, tuple(refs)))
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
        if _code(error) != "ConditionalCheckFailedException":
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
        if _code(error) not in {"PreconditionFailed", "ConditionalRequestConflict"}:
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
    "SEED_SCHEMA",
    "ModeSeed",
    "SeedBinding",
    "SeedClients",
    "SeedObject",
    "build_mode_seed",
    "execute_mode_seed",
]
