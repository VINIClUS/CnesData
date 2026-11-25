"""Start nativo do unit: o caminho do produto gera o vetor e os construtores o reproduzem."""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import boto3
import pytest
from moto import mock_aws

from cnes_domain.control_plane.environment_gate import ClosePromotionFence
from cnes_infra.aws.mode_canary_dispatch import request_targets
from cnes_infra.aws.processor_settings import PRODUCTION_PROCESSOR_MAX_CONCURRENCY
from cnes_infra.aws.settings import _PRODUCTION_LEASE_SECONDS  # pyright: ignore[reportPrivateUsage]
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.control_plane.dynamodb_keys import run_partition, tenant_partition
from packages.cnes_infra.tests.aws.mode_canary_native import NativeCase, native_start
from packages.cnes_infra.tests.aws.test_mode_canary_unit_start import built_start
from packages.cnes_infra.tests.control_plane import test_dynamodb_adapter as dyn
from scripts.promotion.mode_canary_seed import SeedBinding, build_mode_seed

ROOT = Path(__file__).resolve().parents[3]
VECTOR: dict[str, Any] = json.loads(
    Path(__file__).with_name("mode_canary_frames_vector.json").read_text(encoding="utf-8"))
TABLE: str = VECTOR["table"]
FIXTURES = ROOT / "tests/production/fixtures/mode_canary"
FILES = {"fixture-manifest": "fixture-manifest.json", "fixture-cnes-local": "cnes-local-v1.parquet",
         "fixture-cnes-nacional": "cnes-nacional-v1.parquet"}
IAM = ROOT / "infra/opentofu/modules/runtime-iam/mode-canary.tf"
OWNER = "a" * 64
TENANT = "prod-canary-" + "c" * 32
RUN = f"unit-{OWNER[:16]}"
LOCATOR = {"tenant_id": TENANT, "run_ids": [RUN], "owner_sha256": OWNER,
           "record_sha256": "1" * 64, "phase": "unit", "context_kind": "promotion"}
CLOCKS = {
    "producao": (datetime(2026, 11, 13, 12, 5, tzinfo=UTC), True),
    "fim-do-mes": (datetime(2026, 12, 31, 23, 59, 30, tzinfo=UTC), False),
    "inicio-do-mes": (datetime(2027, 1, 1, tzinfo=UTC), False),
}
CASES: dict[str, dict[str, Any]] = {
    case["name"]: case for case in VECTOR["dispatch"]["cases"]}


class _Named:
    def __init__(self, client: Any) -> None:
        self._client = client

    def __getattr__(self, name: str) -> Any:
        method = getattr(self._client, name)
        return lambda **request: method(**(request | {"TableName": TABLE}))


def _seeded(client: Any, now: datetime) -> None:
    files = {FILES[name]: (FIXTURES / file).read_bytes() for name, file in FILES.items()}
    binding = SeedBinding(TENANT, (RUN,), OWNER, now - timedelta(minutes=4), "bucket")
    for item in build_mode_seed(binding, files).items:
        client.put_item(TableName=TABLE, Item=item)


def _native(name: str) -> dict[str, Any]:
    now, closed = CLOCKS[name]
    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        dyn._create_table(_Named(client))
        _seeded(client, now)
        if closed:
            plane = DynamoDBControlPlane(client, TABLE, lambda: now)
            assert plane.close_promotion_fence(
                ClosePromotionFence("prod", OWNER, now - timedelta(minutes=1)))
        case = NativeCase(TENANT, RUN, now, _PRODUCTION_LEASE_SECONDS, OWNER)
        native = native_start(client, TABLE, case)
    return {"name": name, "table": TABLE, "locator": LOCATOR, "now": now.isoformat(),
            "lease_seconds": _PRODUCTION_LEASE_SECONDS, "promotion_id": OWNER, **native}


def _flat(results: list[dict[str, Any]]) -> list[Any]:
    return [request for result in results for step in result["requests"].values()
            for request in step]


def product_dispatch() -> dict[str, Any]:
    """Returns: Seção `dispatch` do vetor: casos do caminho nativo e o gate derivado deles."""
    cases = [_native(name) for name in CLOCKS]
    targets = request_targets(_flat([case["result"] for case in cases]))
    return {"cases": cases, "gate": [list(target) for target in targets]}


def _mode_dispatch_grant() -> dict[str, set[str]]:
    text = IAM.read_text(encoding="utf-8")
    [names] = re.findall(r"mode_dispatch = concat\(\s*\[for name in \[([^\]]*)\]", text)
    partitions = [tenant_partition(TENANT), run_partition(TENANT, RUN)]
    grant: dict[str, set[str]] = {}
    for name in re.findall(r'"(\w+)"', names):
        [(actions, keys)] = re.findall(
            rf"\n\s*{name}\s*=\s*\{{\s*actions\s*=\s*\[([^\]]*)\],?\s*keys\s*=\s*"
            r"(\[[^\]]*\]|local\.mode_partition_keys)", text)
        scope = partitions if keys == "local.mode_partition_keys" else re.findall(
            r'"([^"]+)"', keys)
        for key in scope:
            grant.setdefault(key, set()).update(re.findall(r'"(\w+)"', actions))
    return grant


def test_caminho_nativo_do_produto_reproduz_o_vetor() -> None:
    assert product_dispatch() == VECTOR["dispatch"]


@pytest.mark.parametrize("name", list(CLOCKS))
def test_construtores_reproduzem_o_caso_do_vetor(name: str) -> None:
    assert built_start(CASES[name]) == CASES[name]["result"]


def test_gate_e_derivado_dos_requests_dos_construtores_sobre_os_casos() -> None:
    built = [built_start(case) for case in VECTOR["dispatch"]["cases"]]
    assert [list(target) for target in request_targets(_flat(built))] == VECTOR["dispatch"]["gate"]


def test_gate_derivado_cabe_no_grant_do_mode_dispatch_sem_mudanca_de_iam() -> None:
    grant = _mode_dispatch_grant()
    assert set(grant) >= {"ENV#prod#FENCE", "ENV#prod#SEMAPHORE", "ENV#prod#ATTEMPTS"}
    for action, table, pk, _ in VECTOR["dispatch"]["gate"]:
        assert table == TABLE
        assert action in grant.get(pk, set()), (action, pk)


def test_casos_cobrem_a_producao_e_a_virada_de_mes_do_attempts() -> None:
    months = {sk for _, _, pk, sk in VECTOR["dispatch"]["gate"] if pk == "ENV#prod#ATTEMPTS"}
    assert months == {"MONTH#2026-11", "MONTH#2026-12", "MONTH#2027-01"}
    production = CASES["producao"]["result"]
    assert production["max_concurrency"] == PRODUCTION_PROCESSOR_MAX_CONCURRENCY
    assert production["permit"]["fence_version"] >= 1
    leases = {case["lease_seconds"] for case in VECTOR["dispatch"]["cases"]}
    assert leases == {_PRODUCTION_LEASE_SECONDS}


def test_wave_do_seed_do_unit_e_a_dos_normalize_sem_dependencia() -> None:
    for case in VECTOR["dispatch"]["cases"]:
        ready = sorted(unit["unit_id"] for unit in case["units"]
                       if unit["stage"] == "NORMALIZE" and not unit["depends_on_unit_ids"])
        assert case["result"]["unit_ids"] == ready
        assert len(ready) == 2
