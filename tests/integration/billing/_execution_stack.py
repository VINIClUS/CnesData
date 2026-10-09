"""Stack real de execucao faturada: control plane, callbacks de billing e coordinator."""

import json
from collections.abc import Callable, Iterator
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, cast

import boto3
from moto import mock_aws

from apps.data_processor.tests.orchestration.test_coordinator import (
    _FakeExecutor,
    _FakeObjectStore,
    _full_manifests,
    _processor,
)
from cnes_domain.billing.commands import AuthorizedRunCommand
from cnes_domain.billing.models import BillingEnforcementMode, RunAuthorization
from cnes_domain.control_plane.commands import ClaimRunUnit, PutRunUnits, TransitionRun
from cnes_domain.control_plane.entities import OutboxEvent, Run, RunDependency, RunDispatch
from cnes_domain.control_plane.enums import RunState
from cnes_domain.orchestration.planner import PlanRequest, RunPlan, plan_run
from cnes_domain.ports.processing import (
    ExecutionCallbacks,
    ExecutionPermit,
    ExecutionPolicyConfig,
    ExecutionStatus,
    StartRunExecution,
)
from cnes_domain.profiles import BillingMode
from cnes_infra.billing.dynamodb_quota_items import encode_run_billing_state
from cnes_infra.billing.settings import BillingSettings
from cnes_infra.billing.wiring import (
    BillingGateResources,
    build_entitlement_gate,
    build_execution_callbacks,
)
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.control_plane.dynamodb_keys import outbox_key
from cnes_infra.control_plane.sqlite_adapter import SQLiteControlPlane
from data_processor.orchestration.coordinator import CoordinatorDependencies, PipelineCoordinator
from data_processor.orchestration.publisher import DatasetPublisher
from data_processor.orchestration.unit_worker import UnitWorker, UnitWorkerDependencies
from packages.cnes_infra.tests.billing.billing_factories import NOW, TABLE_NAME, create_table
from packages.cnes_infra.tests.billing.quota_support import (
    ACCOUNT,
    TENANT,
    make_quota_snapshot,
    make_run_request,
    seed_snapshot,
)
from packages.cnes_infra.tests.contracts.clock import MutableClock

RUN_ID = "run-01"
COMPETENCIA = "2026-01"
DEPLOYMENT_LIMIT = 2
LEASE_SECONDS = 300
STRIPE_SETTINGS = BillingSettings(BillingMode.STRIPE, BillingEnforcementMode.ENFORCE, 0)
DISABLED_SETTINGS = BillingSettings(BillingMode.DISABLED, BillingEnforcementMode.OFF, 0)
DEPENDENCIES = (
    RunDependency(source_type="CNES_LOCAL", file_subtype="CNES_VINCULO", required=True),
    RunDependency(source_type="CNES_NACIONAL", file_subtype="CNES_VINCULO", required=False),
)


@dataclass(frozen=True, slots=True)
class Case:
    name: str
    dynamo: bool
    stripe: bool

    @property
    def settings(self) -> BillingSettings:
        return STRIPE_SETTINGS if self.stripe else DISABLED_SETTINGS


@dataclass
class PermitRecorder:
    returned: list[ExecutionPermit] = field(default_factory=list)
    seen: list[ExecutionPermit] = field(default_factory=list)

    def started(
        self, run: Run, request: StartRunExecution, execution_ref: str, permit: ExecutionPermit
    ) -> None:
        self.seen.append(permit)


class RecordingPolicy:
    def __init__(self, inner: Any, recorder: PermitRecorder) -> None:
        self._inner = inner
        self._recorder = recorder

    def __call__(self, run: Run, dispatch: RunDispatch, requested_limit: int) -> ExecutionPermit:
        permit = self._inner(run, dispatch, requested_limit)
        self._recorder.returned.append(permit)
        return permit


@dataclass
class HookedExecutor(_FakeExecutor):
    before_start: Callable[[], None] | None = None

    def start(self, request: StartRunExecution) -> str:
        hook, self.before_start = self.before_start, None
        if hook is not None:
            hook()
        return super().start(request)


@dataclass
class Stack:
    case: Case
    plane: Any
    clock: MutableClock
    client: Any
    executor: HookedExecutor
    recorder: PermitRecorder
    store: _FakeObjectStore
    coordinator: PipelineCoordinator


def _build_plane(case: Case, clock: MutableClock, tmp_path: Path) -> tuple[Any, Any]:
    if not case.dynamo:
        plane = SQLiteControlPlane(tmp_path / "cp.db", clock.now)
        plane.initialize()
        return plane, None
    client = boto3.client("dynamodb", region_name="us-east-1")
    create_table(client)
    seed_snapshot(client, make_quota_snapshot())
    plane = DynamoDBControlPlane(client, TABLE_NAME, clock.now, billing=case.settings)
    return plane, client


def _build_coordinator(
    case: Case, plane: Any, clock: MutableClock, client: Any,
) -> tuple[Any, ...]:
    recorder, executor, store = PermitRecorder(), HookedExecutor(), _FakeObjectStore()
    resources = (
        BillingGateResources(clock.now, DEPLOYMENT_LIMIT, client, TABLE_NAME)
        if case.dynamo
        else BillingGateResources(clock.now, DEPLOYMENT_LIMIT)
    )
    real = build_execution_callbacks(case.settings, plane, resources, recorder.started)
    callbacks = ExecutionCallbacks(RecordingPolicy(real.policy, recorder), real.started)
    execution = ExecutionPolicyConfig(DEPLOYMENT_LIMIT, LEASE_SECONDS, callbacks)
    publisher = DatasetPublisher(store=store, control_plane=plane)
    dependencies = CoordinatorDependencies(plane, executor, publisher, clock.now)
    return recorder, executor, store, PipelineCoordinator(dependencies, execution)


@contextmanager
def open_stack(case: Case, tmp_path: Path) -> Iterator[Stack]:
    clock = MutableClock(NOW)
    with ExitStack() as exits:
        if case.dynamo:
            exits.enter_context(mock_aws())
        plane, client = _build_plane(case, clock, tmp_path)
        recorder, executor, store, coordinator = _build_coordinator(case, plane, clock, client)
        yield Stack(case, plane, clock, client, executor, recorder, store, coordinator)


def _event(event_type: str) -> OutboxEvent:
    return OutboxEvent(
        tenant_id=TENANT, event_id=f"{event_type}:{RUN_ID}", event_type=event_type,
        aggregate_id=RUN_ID, payload={}, created_at=NOW, delivered_at=None,
    )


def _authorize_run(stack: Stack) -> None:
    request = make_run_request(dependencies=DEPENDENCIES, competencia=COMPETENCIA)
    if stack.case.stripe:
        resources = BillingGateResources(stack.clock.now, 4, stack.client, TABLE_NAME)
        build_entitlement_gate(STRIPE_SETTINGS, resources).authorize_create_run(request)
        return
    authorization = RunAuthorization(ACCOUNT, "plan_v1", 1, 4, None, NOW)
    stack.plane.create_unmetered_run(AuthorizedRunCommand(request, authorization))


def _plan(run: Run) -> RunPlan:
    return plan_run(PlanRequest(
        run=run, manifests=_full_manifests(), deployment_limit=DEPLOYMENT_LIMIT,
    ))


def create_processing_run(stack: Stack) -> Run:
    _authorize_run(stack)
    run = stack.plane.get_run(TENANT, RUN_ID)
    plan = _plan(run)
    stack.plane.put_run_units(PutRunUnits(
        tenant_id=TENANT, run_id=RUN_ID, expected_run_state=RunState.WAITING_INPUTS,
        units=plan.units,
    ))
    return stack.plane.transition_run(TransitionRun(
        tenant_id=TENANT, run_id=RUN_ID, expected_state=RunState.WAITING_INPUTS,
        new_state=RunState.PROCESSING, missing_sources=plan.missing_optional,
    ), _event("run.processing"))


def seed_run_without_companion(stack: Stack) -> Run:
    run = Run(
        tenant_id=TENANT, run_id=RUN_ID, competencia=COMPETENCIA, dataset_name="cnes_vinculos",
        state=RunState.PROCESSING, dependencies=DEPENDENCIES, missing_sources=(),
        created_at=NOW,
    )
    stack.plane.put_run(run)
    stack.plane.put_run_units(PutRunUnits(
        tenant_id=TENANT, run_id=RUN_ID, expected_run_state=RunState.PROCESSING,
        units=_plan(run).units,
    ))
    return run


def active_dispatch(stack: Stack) -> RunDispatch | None:
    return stack.plane.get_active_run_dispatch(TENANT, RUN_ID)


def billing_state(stack: Stack) -> Any:
    return stack.plane.get_run_billing_state(TENANT, RUN_ID)


def resume(stack: Stack) -> Any:
    return stack.coordinator.resume(TENANT, RUN_ID)


def claim_command(stack: Stack, dispatch: RunDispatch, unit_id: str) -> ClaimRunUnit:
    return ClaimRunUnit(
        tenant_id=TENANT, run_id=RUN_ID, unit_id=unit_id, dispatch_id=dispatch.dispatch_id,
        owner="worker-a", now=stack.clock.now(), lease_seconds=LEASE_SECONDS,
    )


def complete_wave(stack: Stack) -> RunDispatch:
    dispatch = cast("RunDispatch", active_dispatch(stack))
    dependencies = UnitWorkerDependencies(
        control_plane=stack.plane, store=stack.store, processor=_processor, clock=stack.clock.now,
    )
    for unit_id in dispatch.unit_ids:
        UnitWorker(dependencies).execute(claim_command(stack, dispatch, unit_id))
    stack.executor.set_status(cast("str", dispatch.execution_ref), ExecutionStatus.SUCCEEDED)
    return dispatch


def overwrite_companion(stack: Stack, **changes: Any) -> None:
    changed = encode_run_billing_state(replace(billing_state(stack), **changes))
    if stack.case.dynamo:
        stack.client.put_item(TableName=TABLE_NAME, Item=changed)
        return
    with stack.plane.write_transaction() as connection:
        connection.execute(
            "UPDATE run_billing_states SET data = ? WHERE tenant_id = ? AND run_id = ?",
            (json.dumps(changed), TENANT, RUN_ID),
        )


def has_outbox_event(stack: Stack, event_id: str) -> bool:
    if stack.client is None:
        return False
    _, sort_key = outbox_key(event_id)
    items = stack.client.scan(TableName=TABLE_NAME, ConsistentRead=True)["Items"]
    return any(item["sk"]["S"] == sort_key for item in items)
