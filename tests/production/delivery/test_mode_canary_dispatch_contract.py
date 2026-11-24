"""Contrato do dispatch nativo do unit: o caminho do produto reproduz o vetor e o construtor."""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, cast

import boto3
from moto import mock_aws

from cnes_domain.control_plane.commands import (
    BindRunDispatch,
    ClaimRunDispatchStart,
    ReserveRunDispatch,
)
from cnes_domain.control_plane.dispatch_start import DispatchStartClaim, request_sha256
from cnes_domain.control_plane.environment_gate import AcquireUnitPermit, PermitHolder
from cnes_domain.orchestration.mode_canary import ModeCanaryBinding
from cnes_domain.ports.processing import StartRunExecution
from cnes_infra.aws.mode_canary_dispatch import (
    DispatchStep,
    run_dispatch_item,
    start_claim,
    start_execution,
    start_request_sha256,
    unit_dispatch,
)
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.control_plane.dynamodb_keys import dispatch_key, item_key
from cnes_infra.executor.step_functions import StepFunctionsExecutor
from packages.cnes_infra.tests.control_plane import test_dynamodb_adapter as dyn
from scripts.promotion.mode_canary_seed import SeedBinding, build_mode_seed

VECTOR: dict[str, Any] = json.loads(
    Path(__file__).with_name("mode_canary_frames_vector.json").read_text(encoding="utf-8"))
FIXTURES = Path(__file__).resolve().parents[3] / "tests/production/fixtures/mode_canary"
FILES = {"fixture-manifest": "fixture-manifest.json", "fixture-cnes-local": "cnes-local-v1.parquet",
         "fixture-cnes-nacional": "cnes-nacional-v1.parquet"}
OWNER = "a" * 64
TENANT = "prod-canary-" + "c" * 32
RUN = f"unit-{OWNER[:16]}"
MACHINE = "arn:aws:states:us-east-2:111122223333:stateMachine:cnesdata-prod"
NOW = datetime.fromisoformat("2026-11-13T12:05:00+00:00")
LEASE = 90
CONCURRENCY = 2


class _Sfn:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def start_execution(self, **request: Any) -> dict[str, Any]:
        self.calls.append(request)
        execution = MACHINE.replace(":stateMachine:", ":execution:")
        return {"executionArn": f"{execution}:{request['name']}"}


def _locator() -> ModeCanaryBinding:
    return ModeCanaryBinding(TENANT, (RUN,), OWNER, "1" * 64, "unit", "promotion")


def _seeded(client: Any) -> list[dict[str, Any]]:
    files = {FILES[name]: (FIXTURES / file).read_bytes() for name, file in FILES.items()}
    binding = SeedBinding(TENANT, (RUN,), OWNER, NOW - timedelta(minutes=4), "bucket")
    seed = build_mode_seed(binding, files)
    for item in seed.items:
        client.put_item(TableName=dyn._TABLE_NAME, Item=item)
    return [json.loads(item["payload"]["S"]) for item in seed.items
            if item["entity"]["S"] == "RUNUNIT"]


def _dispatch_item(client: Any) -> dict[str, Any]:
    key = item_key(*dispatch_key(TENANT, RUN))
    return client.get_item(TableName=dyn._TABLE_NAME, Key=key,
                           ConsistentRead=True)["Item"]


def _native(client: Any, units: list[dict[str, Any]]) -> dict[str, Any]:
    plane = DynamoDBControlPlane(client, dyn._TABLE_NAME, lambda: NOW)
    plan = unit_dispatch(_locator(), units, CONCURRENCY, NOW)
    reserved = plane.reserve_run_dispatch(ReserveRunDispatch(
        tenant_id=TENANT, run_id=RUN, wave_id=plan.wave_id, unit_ids=plan.unit_ids, now=NOW,
        lease_seconds=LEASE))
    items = {"RESERVED": _dispatch_item(client)}
    holder = PermitHolder(TENANT, RUN, reserved.dispatch_id)
    acquired = plane.acquire_unit_permit(AcquireUnitPermit("prod", holder, NOW, LEASE, OWNER))
    assert acquired.permit is not None
    token = acquired.permit.token
    fence = plane.observe_environment_gate("prod").fence.version
    request = StartRunExecution(tenant_id=TENANT, run_id=RUN, wave_id=reserved.wave_id,
                                dispatch_id=reserved.dispatch_id, unit_ids=reserved.unit_ids,
                                max_concurrency=CONCURRENCY)
    claim = DispatchStartClaim(dispatch_id=reserved.dispatch_id, generation=reserved.generation,
                               permit_token=token, fence_version=fence,
                               request_sha256=request_sha256(request), promotion_id=OWNER)
    plane.claim_run_dispatch_start(ClaimRunDispatchStart(
        tenant_id=TENANT, run_id=RUN, dispatch_id=reserved.dispatch_id, claim=claim, now=NOW,
        lease_seconds=LEASE))
    items["STARTING"] = _dispatch_item(client)
    sfn = _Sfn()
    execution = StepFunctionsExecutor(cast("Any", sfn), MACHINE).start_with_claim(request, claim)
    plane.bind_run_dispatch(BindRunDispatch(
        tenant_id=TENANT, run_id=RUN, dispatch_id=reserved.dispatch_id, execution_ref=execution,
        now=NOW, lease_seconds=LEASE, claim=claim))
    items["STARTED"] = _dispatch_item(client)
    [call] = sfn.calls
    return {"dispatch_id": reserved.dispatch_id, "wave_id": reserved.wave_id,
            "unit_ids": list(reserved.unit_ids), "start_execution": {
                "name": call["name"], "input": call["input"]},
            "start_request_sha256": request_sha256(request), "claim": claim.model_dump(),
            "permit": {"token": token, "fence_version": fence},
            "execution_ref": execution, "items": items}


def product_dispatch() -> dict[str, Any]:
    """Returns: Seção `dispatch` do vetor gerada pelo caminho nativo do produto sobre o seed."""
    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        dyn._create_table(client)
        units = _seeded(client)
        native = _native(client, units)
    return {"locator": {"tenant_id": TENANT, "run_ids": [RUN], "owner_sha256": OWNER,
                        "record_sha256": "1" * 64, "phase": "unit", "context_kind": "promotion"},
            "units": units, "max_concurrency": CONCURRENCY, "now": NOW.isoformat(),
            "lease_seconds": LEASE, "promotion_id": OWNER, "result": native}


def _builder(case: dict[str, Any]) -> dict[str, Any]:
    locator = case["locator"]
    binding = ModeCanaryBinding(locator["tenant_id"], tuple(locator["run_ids"]),
                                locator["owner_sha256"], locator["record_sha256"],
                                locator["phase"], locator["context_kind"])
    now = datetime.fromisoformat(case["now"])
    plan = unit_dispatch(binding, case["units"], case["max_concurrency"], now)
    native = case["result"]
    permit = native["permit"]
    claim = start_claim(plan, permit["token"], permit["fence_version"], case["promotion_id"])
    lease = now + timedelta(seconds=case["lease_seconds"])
    steps = {"RESERVED": DispatchStep("RESERVED", lease),
             "STARTING": DispatchStep("STARTING", lease, claim),
             "STARTED": DispatchStep("STARTED", lease, claim, native["execution_ref"])}
    return {"dispatch_id": plan.dispatch_id, "wave_id": plan.wave_id,
            "unit_ids": list(plan.unit_ids), "start_execution": start_execution(plan),
            "start_request_sha256": start_request_sha256(plan), "claim": claim,
            "permit": permit, "execution_ref": native["execution_ref"],
            "items": {state: run_dispatch_item(plan, step) for state, step in steps.items()}}


def test_construtor_reproduz_o_dispatch_nativo_do_vetor() -> None:
    case = VECTOR["dispatch"]
    assert _builder(case) == case["result"]


def test_caminho_nativo_do_produto_reproduz_o_vetor() -> None:
    assert product_dispatch() == VECTOR["dispatch"]


def test_wave_do_seed_do_unit_e_a_dos_normalize_sem_dependencia() -> None:
    case = VECTOR["dispatch"]
    ready = sorted(unit["unit_id"] for unit in case["units"]
                   if unit["stage"] == "NORMALIZE" and not unit["depends_on_unit_ids"])
    assert case["result"]["unit_ids"] == ready
    assert len(ready) == 2
