"""Fixtures do canary de modo: bytes pequenos, sintéticos e descritos pelo manifesto."""
from __future__ import annotations

import hashlib
import json
import re
from copy import deepcopy
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path
from typing import Any

import polars as pl
import pytest
from botocore.exceptions import ClientError

from cnes_contracts.manifests.raw import RawManifest
from cnes_domain.control_plane.entities import Run
from cnes_domain.control_plane.enums import RunState
from cnes_domain.orchestration.planner import PlanRequest, RawManifestRef, plan_run
from cnes_domain.orchestration.source_catalog import build_source_catalog
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.control_plane.dynamodb_keys import global_partition, tenant_partition
from cnes_infra.control_plane.dynamodb_run_codec import run_item
from scripts.promotion._canonical import canonical_bytes
from scripts.promotion.mode_canary_contract import ContractError
from scripts.promotion.mode_canary_seed import (
    SeedBinding,
    SeedClients,
    build_mode_seed,
    execute_mode_seed,
)

_ROOT = Path(__file__).resolve().parents[3]
_FIXTURES = _ROOT / "tests/production/fixtures/mode_canary"
_DOCS = _ROOT / "docs/fixtures/data-plane"
_FILES = ("cnes-local-v1.parquet", "cnes-nacional-v1.parquet")
_SYNTHETIC = {
    "CPF": re.compile(r"9000000000[0-9]"),
    "CNS": re.compile(r"999000000000[0-9]{3}"),
    "NOME_PROFISSIONAL": re.compile(r"PROFISSIONAL TESTE [0-9]{3}"),
    "CNES": re.compile(r"1234567"),
}


def _manifest() -> dict[str, Any]:
    return json.loads((_FIXTURES / "fixture-manifest.json").read_text(encoding="utf-8"))


def test_manifesto_tem_campos_fechados_e_sem_tenant_municipal() -> None:
    manifest = _manifest()

    assert set(manifest) == {"schema", "competencia", "file_subtype", "files", "expectations",
                             "synthetic_proof"}
    assert manifest["schema"] == "cnesdata.mode-canary-fixtures/v1"
    assert (manifest["competencia"], manifest["file_subtype"]) == ("2026-01", "CNES_VINCULO")
    assert sorted(manifest["files"]) == sorted(_FILES)
    assert "354130" not in json.dumps(manifest)
    assert manifest["expectations"] == {"gold_row_count": 7}


@pytest.mark.parametrize("name", _FILES)
def test_fixture_confere_sha_tamanho_linhas_e_origem_nos_docs(name: str) -> None:
    body = (_FIXTURES / name).read_bytes()
    entry = _manifest()["files"][name]

    assert set(entry) == {"sha256", "size", "schema_version", "row_count", "source_type",
                          "manifest_id", "origin"}
    assert entry["sha256"] == hashlib.sha256(body).hexdigest()
    assert entry["size"] == len(body)
    assert entry["row_count"] == pl.read_parquet(_FIXTURES / name).height
    assert entry["schema_version"] == "cnes-profissional-v1"
    assert entry["source_type"] in {"CNES_LOCAL", "CNES_NACIONAL"}
    assert body == (_DOCS / name).read_bytes()
    assert entry["origin"] == f"docs/fixtures/data-plane/{name}"


@pytest.mark.parametrize("name", _FILES)
def test_fixture_so_contem_identidades_sinteticas(name: str) -> None:
    frame = pl.read_parquet(_FIXTURES / name)

    for column, pattern in _SYNTHETIC.items():
        values = [value for value in frame[column].to_list() if value is not None]
        assert values
        assert all(pattern.fullmatch(value) for value in values), column
    cells = [str(value) for row in frame.rows() for value in row]
    assert not any("354130" in cell for cell in cells)
    proof = _manifest()["synthetic_proof"]
    assert proof == {column: pattern.pattern for column, pattern in _SYNTHETIC.items()}


_OWNER = "4" * 64
_TENANT = "prod-canary-" + "0123456789abcdef" * 2
_RUN = f"unit-{_OWNER[:16]}"
_RECOVERY = (f"recovery-a-{_OWNER[:16]}", f"recovery-b-{_OWNER[:16]}")
_AT = datetime(2026, 11, 13, 12, 0, tzinfo=UTC)
_BUCKET = "cnesdata-prod-data"
_TABLE = "cnesdata-prod-control-plane"
_ENTITIES = {"RAWMANIFESTRECORD", "RUN", "RUNUNIT"}


def _fixtures() -> dict[str, bytes]:
    names = ("fixture-manifest.json", *_FILES)
    return {name: (_FIXTURES / name).read_bytes() for name in names}


def _binding(**changes: Any) -> SeedBinding:
    fields: dict[str, Any] = {
        "tenant_id": _TENANT, "run_ids": (_RUN,), "owner_sha256": _OWNER, "created_at": _AT,
        "bucket": _BUCKET,
    }
    return SeedBinding(**(fields | changes))


def _payload(item: dict[str, Any]) -> dict[str, Any]:
    return json.loads(item["payload"]["S"])


class _Dynamo:
    def __init__(self) -> None:
        self.items: dict[tuple[str, str], dict[str, Any]] = {}
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def put_item(self, **request: Any) -> dict[str, Any]:
        self.calls.append(("put_item", request))
        key = (request["Item"]["pk"]["S"], request["Item"]["sk"]["S"])
        if key in self.items:
            raise ClientError({"Error": {"Code": "ConditionalCheckFailedException"}}, "PutItem")
        self.items[key] = deepcopy(request["Item"])
        return {}

    def get_item(self, **request: Any) -> dict[str, Any]:
        self.calls.append(("get_item", request))
        found = self.items.get((request["Key"]["pk"]["S"], request["Key"]["sk"]["S"]))
        return {} if found is None else {"Item": deepcopy(found)}


class _S3:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def put_object(self, **request: Any) -> dict[str, Any]:
        self.calls.append(("put_object", request))
        key = (request["Bucket"], request["Key"])
        if key in self.objects:
            raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")
        self.objects[key] = request["Body"]
        return {}

    def get_object(self, **request: Any) -> dict[str, Any]:
        self.calls.append(("get_object", request))
        return {"Body": BytesIO(self.objects[(request["Bucket"], request["Key"])])}


@pytest.mark.parametrize(
    "tenant", ["354130", _TENANT.upper(), "prod-canary-xyz", "prod-canary-" + "0" * 31],
)
def test_seed_rejeita_tenant_municipal_e_resultado_precalculado(tenant: str) -> None:
    with pytest.raises(ContractError):
        build_mode_seed(_binding(tenant_id=tenant), _fixtures())
    with pytest.raises(ContractError):
        build_mode_seed(_binding(), _fixtures() | {"output-manifest.json": b"{}"})
    manifest = _manifest() | {"outputs": ["serving/x.json"]}
    forged = _fixtures() | {"fixture-manifest.json": json.dumps(manifest).encode()}
    with pytest.raises(ContractError):
        build_mode_seed(_binding(), forged)
    seed = build_mode_seed(_binding(), _fixtures())
    assert {item["entity"]["S"] for item in seed.items} == _ENTITIES
    states = {item["entity"]["S"]: _payload(item).get("state") for item in seed.items}
    assert states["RUN"] == "PROCESSING"
    assert {_payload(i)["state"] for i in seed.items if i["entity"]["S"] == "RUNUNIT"} == {
        "PENDING"}


def _planned(run_id: str) -> tuple[Run, tuple[Any, ...]]:
    definition = build_source_catalog().for_pipeline("cnes")
    run = Run(
        tenant_id=_TENANT, run_id=run_id, competencia="2026-01", dataset_name="cnes",
        state=RunState.PLANNED, dependencies=definition.dependencies, missing_sources=(),
        created_at=_AT,
    )
    refs = []
    for name in _FILES:
        entry = _manifest()["files"][name]
        snapshot = f"{entry['manifest_id']}-{_OWNER[:16]}"
        key = f"raw/{_TENANT}/{entry['source_type']}/2026-01/{snapshot}/manifest.json"
        refs.append(RawManifestRef(snapshot, key, entry["source_type"], "CNES_VINCULO",
                                   "2026-01"))
    plan = plan_run(PlanRequest(run=run, manifests=tuple(refs), deployment_limit=1))
    return run, plan.units


def test_seed_usa_codecs_nativos_e_estado_inicial_declarado() -> None:
    seed = build_mode_seed(_binding(), _fixtures())
    run, units = _planned(_RUN)
    processing = run.model_copy(update={"state": RunState.PROCESSING})
    adapter = DynamoDBControlPlane.__new__(DynamoDBControlPlane)
    expected = [run_item(processing), *(adapter._unit_item(unit) for unit in units)]
    by_entity = [item for item in seed.items if item["entity"]["S"] != "RAWMANIFESTRECORD"]
    assert sorted(by_entity, key=_key) == sorted(expected, key=_key)
    bodies = {obj.key: obj.body for obj in seed.objects}
    for name in _FILES:
        entry = _manifest()["files"][name]
        snapshot = f"{entry['manifest_id']}-{_OWNER[:16]}"
        prefix = f"raw/{_TENANT}/{entry['source_type']}/2026-01/{snapshot}/"
        assert bodies[prefix + "data.parquet"] == (_FIXTURES / name).read_bytes()
        raw = RawManifest.model_validate_json(bodies[prefix + "manifest.json"])
        assert raw.model_dump_json(exclude_none=False, by_alias=False).encode() == bodies[
            prefix + "manifest.json"]
        assert (raw.tenant_id, raw.object_key, raw.object_sha256, raw.created_at) == (
            _TENANT, prefix + "data.parquet", entry["sha256"], _AT)


def _key(item: dict[str, Any]) -> tuple[str, str]:
    return item["pk"]["S"], item["sk"]["S"]


def test_seed_nao_escreve_outbox_global() -> None:
    seed = build_mode_seed(_binding(run_ids=_RECOVERY), _fixtures())
    base = tenant_partition(_TENANT)
    assert all(item["pk"]["S"] != global_partition() for item in seed.items)
    assert all(item["pk"]["S"].startswith(base) for item in seed.items)
    assert {_payload(i)["run_id"] for i in seed.items if i["entity"]["S"] == "RUN"} == set(
        _RECOVERY)
    assert all(obj.key.startswith(f"raw/{_TENANT}/") for obj in seed.objects)


@pytest.mark.parametrize(
    "runs", [("unit-" + "5" * 16,), (_RUN, _RUN), (*_RECOVERY, _RUN), ("recovery-c-x",)],
)
def test_seed_aceita_somente_runs_do_owner(runs: tuple[str, ...]) -> None:
    with pytest.raises(ContractError):
        build_mode_seed(_binding(run_ids=runs), _fixtures())


def test_manifesto_do_seed_lista_itens_e_objetos_antes_das_chamadas() -> None:
    seed = build_mode_seed(_binding(), _fixtures())
    document = json.loads(seed.manifest)

    assert seed.sha256 == hashlib.sha256(seed.manifest).hexdigest()
    assert seed.manifest == canonical_bytes(document)
    assert (document["owner_sha256"], document["tenant_id"]) == (_OWNER, _TENANT)
    fixture_sha = hashlib.sha256((_FIXTURES / "fixture-manifest.json").read_bytes())
    assert document["fixture_manifest_sha256"] == fixture_sha.hexdigest()
    assert document["items"] == sorted(
        ({"pk": i["pk"]["S"], "sk": i["sk"]["S"],
          "sha256": hashlib.sha256(canonical_bytes(i)).hexdigest()} for i in seed.items),
        key=lambda entry: (entry["pk"], entry["sk"]))
    assert document["objects"] == sorted(
        ({"bucket": _BUCKET, "key": o.key, "sha256": hashlib.sha256(o.body).hexdigest(),
          "size": len(o.body)} for o in seed.objects), key=lambda entry: entry["key"])


def test_replay_de_seed_exige_mesmos_bytes() -> None:
    dynamo, s3 = _Dynamo(), _S3()
    seed = build_mode_seed(_binding(), _fixtures())
    first = execute_mode_seed(seed, SeedClients(dynamo, s3), _TABLE)
    assert execute_mode_seed(seed, SeedClients(dynamo, s3), _TABLE) == first
    stored = deepcopy(dynamo.items)
    later = build_mode_seed(_binding(created_at=_AT.replace(hour=13)), _fixtures())

    with pytest.raises(ContractError, match="seed_conflict"):
        execute_mode_seed(later, SeedClients(dynamo, s3), _TABLE)
    assert dynamo.items == stored
    assert first["seed_sha256"] == seed.sha256
    assert first["items"] == json.loads(seed.manifest)["items"]
    assert first["objects"] == json.loads(seed.manifest)["objects"]


def test_execucao_so_usa_put_get_insert_only() -> None:
    dynamo, s3 = _Dynamo(), _S3()
    seed = build_mode_seed(_binding(), _fixtures())
    execute_mode_seed(seed, SeedClients(dynamo, s3), _TABLE)

    assert {name for name, _ in dynamo.calls} == {"put_item", "get_item"}
    assert {name for name, _ in s3.calls} == {"put_object", "get_object"}
    puts = [request for name, request in dynamo.calls if name == "put_item"]
    assert len(puts) == len(seed.items)
    assert all(r["ConditionExpression"] == "attribute_not_exists(pk)" for r in puts)
    assert all(r["TableName"] == _TABLE for r in puts)
    reads = [request for name, request in dynamo.calls if name == "get_item"]
    assert all(r["ConsistentRead"] is True for r in reads)
    objects = [request for name, request in s3.calls if name == "put_object"]
    assert all(r["IfNoneMatch"] == "*" and r["Bucket"] == _BUCKET for r in objects)
    assert len(objects) == len(seed.objects) == 2 * len(_FILES)
