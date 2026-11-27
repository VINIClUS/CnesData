"""Start nativo do unit pelo caminho do coordinator, com cada request DynamoDB gravado por passo."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import timedelta
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

from cnes_domain.control_plane.commands import PutRunUnits, ReserveRunDispatch
from cnes_domain.control_plane.dispatch_start import (
    DispatchStartClaim,
    StartAdmission,
    claim_dispatch_start,
    execute_dispatch_start,
    request_sha256,
)
from cnes_domain.control_plane.enums import RunStage, RunState
from cnes_domain.orchestration.dispatch_admission import EnvironmentAdmission, dispatch_holder
from cnes_domain.orchestration.planner import (
    RunPlan,
    execution_request,
    logical_wave_id,
    ready_units,
)
from cnes_domain.ports.environment_gate import UnitAdmission
from cnes_infra.aws.processor_settings import PRODUCTION_PROCESSOR_MAX_CONCURRENCY
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.control_plane.dynamodb_keys import item_key, run_entity_key, unit_key
from cnes_infra.executor.step_functions import StepFunctionsExecutor
from packages.cnes_infra.tests.contracts.clock import _run, _unit

if TYPE_CHECKING:
    from datetime import datetime

    from cnes_domain.control_plane.entities import RunDispatch
    from cnes_domain.control_plane.environment_gate import PermitHolder

MACHINE = "arn:aws:states:us-east-2:111122223333:stateMachine:cnesdata-prod"
STEPS = ("reserve", "admit", "claim", "bind", "confirm")
_RECORDED = frozenset({"get_item", "put_item", "query", "transact_write_items", "update_item"})


class Recorder:
    """Cliente DynamoDB que grava passo, operação e kwargs de cada request."""

    def __init__(self, client: Any) -> None:
        self.client = client
        self.step = "plan"
        self.calls: list[tuple[str, str, dict[str, Any]]] = []

    def __getattr__(self, name: str) -> Any:
        if name not in _RECORDED:
            raise AssertionError(f"unrecorded_op={name}")
        attribute = getattr(self.client, name)

        def recorded(**kwargs: Any) -> Any:
            self.calls.append((self.step, name, json.loads(json.dumps(kwargs))))
            return attribute(**kwargs)

        return recorded

    def requests(self) -> dict[str, list[list[Any]]]:
        """Returns: Requests dos passos do start, na ordem em que o produto os enviou."""
        return {step: [[name, kwargs] for recorded, name, kwargs in self.calls
                       if recorded == step] for step in STEPS}


class Sfn:
    """Cliente Step Functions que devolve o ARN da execução nomeada pelo dispatch."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def start_execution(self, **request: Any) -> dict[str, Any]:
        self.calls.append(request)
        execution = MACHINE.replace(":stateMachine:", ":execution:")
        return {"executionArn": f"{execution}:{request['name']}"}


class _Canary:
    def __init__(self, promotion_id: str) -> None:
        self._promotion_id = promotion_id

    def binding_for(self, holder: PermitHolder, now: datetime) -> Any:
        return SimpleNamespace(promotion_id=self._promotion_id)


class _AbsentProbe:
    def probe(self, dispatch_id: str) -> Any:
        raise AssertionError(f"probe dispatch_id={dispatch_id}")


@dataclass(frozen=True, slots=True)
class NativeCase:
    """Run do unit, relógio único do estágio, lease e promoção do canário."""

    tenant_id: str
    run_id: str
    now: datetime
    lease_seconds: int
    promotion_id: str


def seed_unit_run(client: Any, table: str, case: NativeCase) -> None:
    """Grava o RUN em PROCESSING, n-a e n-b em NORMALIZE e rec em RECONCILE depois deles."""
    plane = DynamoDBControlPlane(client, table, lambda: case.now)
    plane.put_run(_run(case.run_id).model_copy(update={
        "tenant_id": case.tenant_id, "created_at": case.now - timedelta(minutes=4)}))
    normalize = tuple(_unit(unit_id).model_copy(update={
        "tenant_id": case.tenant_id, "run_id": case.run_id}) for unit_id in ("n-b", "n-a"))
    reconcile = normalize[0].model_copy(update={
        "unit_id": "rec", "stage": RunStage.RECONCILE, "source_type": None,
        "file_subtype": None, "input_manifests": (), "depends_on_unit_ids": ("n-a", "n-b")})
    plane.put_run_units(PutRunUnits(tenant_id=case.tenant_id, run_id=case.run_id,
                                    expected_run_state=RunState.PROCESSING,
                                    units=(*normalize, reconcile)))


def _plan(plane: DynamoDBControlPlane, case: NativeCase) -> RunPlan:
    run = plane.get_run(case.tenant_id, case.run_id)
    assert run is not None
    units = plane.list_run_units(case.tenant_id, case.run_id)
    return RunPlan(run, units, (), (), PRODUCTION_PROCESSOR_MAX_CONCURRENCY)


def _inputs(client: Any, table: str, case: NativeCase, unit_ids: tuple[str, ...]) -> Any:
    keys = [run_entity_key(case.tenant_id, case.run_id),
            *(unit_key(case.tenant_id, case.run_id, unit_id) for unit_id in unit_ids)]
    items = [client.get_item(TableName=table, Key=item_key(*key), ConsistentRead=True)["Item"]
             for key in keys]
    return items[0], items[1:]


def _reserve(plane: DynamoDBControlPlane, case: NativeCase, plan: RunPlan) -> RunDispatch:
    ready = ready_units(plan, case.now)
    return plane.reserve_run_dispatch(ReserveRunDispatch(
        tenant_id=case.tenant_id, run_id=case.run_id, wave_id=logical_wave_id(ready),
        unit_ids=tuple(unit.unit_id for unit in ready), now=case.now,
        lease_seconds=case.lease_seconds))


def native_start(client: Any, table: str, case: NativeCase) -> dict[str, Any]:
    """Returns: Entradas lidas, resultado e requests do start nativo do produto por passo."""
    recorder = Recorder(client)
    plane = DynamoDBControlPlane(cast("Any", recorder), table, lambda: case.now)
    plan = _plan(plane, case)
    run_item, unit_items = _inputs(client, table, case,
                                   tuple(unit.unit_id for unit in ready_units(plan, case.now)))
    recorder.step = "reserve"
    reserved = _reserve(plane, case, plan)
    request = execution_request(plan, reserved, min(len(reserved.unit_ids),
                                                    plan.deployment_limit))
    gate = EnvironmentAdmission(plane, _AbsentProbe(), "prod", case.lease_seconds, plane,
                                mode_canary=_Canary(case.promotion_id))
    recorder.step = "admit"
    admission = gate.admit(dispatch_holder(reserved, request), case.now)
    assert isinstance(admission, UnitAdmission)
    attempt = StartAdmission(admission, case.now, case.lease_seconds)
    recorder.step = "claim"
    claim = claim_dispatch_start(plane, reserved, request, attempt)
    assert isinstance(claim, DispatchStartClaim)
    sfn = Sfn()
    executor = StepFunctionsExecutor(cast("Any", sfn), MACHINE)
    execution = execute_dispatch_start(executor, request, claim)
    recorder.step = "bind"
    plane.bind_run_dispatch(attempt.bind(reserved, execution, claim))
    recorder.step = "confirm"
    gate.confirm(admission, execution, case.now)
    [call] = sfn.calls
    return {
        "units": [json.loads(unit.model_dump_json()) for unit in plan.units],
        "run_item": run_item, "unit_items": unit_items,
        "result": {
            "dispatch_id": reserved.dispatch_id, "wave_id": reserved.wave_id,
            "unit_ids": list(reserved.unit_ids), "max_concurrency": request.max_concurrency,
            "start_execution": {"name": call["name"], "input": call["input"]},
            "start_request_sha256": request_sha256(request), "claim": claim.model_dump(),
            "permit": {"token": admission.token, "fence_version": admission.fence_version},
            "execution_ref": execution, "requests": recorder.requests()},
    }
