"""Revogação imediata ponta a ponta: serviço real sobre os adapters DynamoDB."""

import json
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, cast
from uuid import uuid4

import pytest

from cnes_domain.billing.errors import BillingDisabledError
from cnes_domain.billing.execution import RunExecutionBindingCommand
from cnes_domain.billing.models import ReadConsistency, ReservationStatus, SubscriptionStatus
from cnes_domain.billing.revocation import (
    ImmediateRevocationCommand,
    ImmediateRevocationService,
    RevocationDependencies,
    RevocationPhase,
    RevocationSettings,
)
from cnes_domain.control_plane.commands import BindRunDispatch, ReserveRunDispatch
from cnes_domain.control_plane.entities import RunDispatch
from cnes_domain.control_plane.enums import (
    DispatchOutcome,
    DispatchState,
    RunStage,
    RunState,
    RunUnitState,
)
from cnes_domain.ports.processing import CancelRunExecution
from cnes_infra.billing.audit_outbox import DynamoBillingAudit
from cnes_infra.billing.disabled import DisabledEntitlementProjection
from cnes_infra.billing.dynamodb_projection import DynamoEntitlementProjection
from cnes_infra.control_plane.dynamodb_keys import dispatch_key
from packages.cnes_infra.tests.billing.billing_factories import NOW
from packages.cnes_infra.tests.billing.quota_support import ACCOUNT, TENANT
from packages.cnes_infra.tests.billing.revocation_support import (
    LEASE_SECONDS,
    RUN_ID,
    RevEnv,
    build_env,
    claim_unit,
    create_named_table,
    create_run,
    finish_wave,
    get_raw,
    make_unit,
    open_env,
    put_run_state,
    put_units,
    seed_units,
    start_wave,
    stored_reservation,
    stored_run,
    usage_counters,
)
from packages.cnes_infra.tests.billing.test_dynamodb_revocation_units import local_client

SECOND_RUN = "run-02"
COMMAND = ImmediateRevocationCommand(ACCOUNT, "admin-1", "fraud_confirmed", NOW)
LIMIT_PER_TRANSACTION = 100


class RecordingExecutor:
    def __init__(self, error: Exception | None = None, log: list[str] | None = None) -> None:
        self.requests: list[CancelRunExecution] = []
        self.error = error
        self.log = log

    def cancel(self, request: CancelRunExecution) -> None:
        self.requests.append(request)
        if self.log is not None:
            self.log.append("cancel")
        if self.error is not None:
            raise self.error

    def refs(self) -> set[tuple[str, str | None]]:
        return {(request.run_id, request.execution_ref) for request in self.requests}


class StoreSpy:
    def __init__(self, inner: Any) -> None:
        self.inner = inner
        self.calls: list[str] = []

    def __getattr__(self, name: str) -> Any:
        self.calls.append(name)
        return getattr(self.inner, name)


@dataclass(frozen=True, slots=True)
class Seeded:
    third: RunDispatch
    audited: dict[str, Any]
    simple: RunDispatch
    consumed: int


@pytest.fixture
def env() -> Any:
    with open_env() as opened:
        yield opened


@dataclass(frozen=True, slots=True)
class ServiceOptions:
    settings: RevocationSettings | None = None
    projection: Any = None
    store: Any = None
    audit: Any = None


def build_service(
    env: RevEnv, executor: Any, options: ServiceOptions | None = None
) -> ImmediateRevocationService:
    chosen = options or ServiceOptions()
    dependencies = RevocationDependencies(
        projection=chosen.projection
        or DynamoEntitlementProjection(env.spy, env.table, env.clock.now),
        store=chosen.store or env.store,
        executor=executor,
        audit=chosen.audit or DynamoBillingAudit(env.spy, env.table),
        clock=env.clock.now,
    )
    return ImmediateRevocationService(dependencies, chosen.settings or RevocationSettings())


def units_of(env: RevEnv, run_id: str) -> dict[str, Any]:
    return {unit.unit_id: unit for unit in env.plane.list_run_units(TENANT, run_id)}


def dispatch_of(env: RevEnv, run_id: str) -> RunDispatch:
    item = get_raw(env, dispatch_key(TENANT, run_id))
    assert item is not None
    return RunDispatch.model_validate_json(item["payload"]["S"])


def companion(env: RevEnv, run_id: str) -> Any:
    return env.store.get_run_billing_state(TENANT, run_id)


def start_run_wave(env: RevEnv, run_id: str, unit_ids: tuple[str, ...]) -> RunDispatch:
    dispatch = env.plane.reserve_run_dispatch(
        ReserveRunDispatch(
            tenant_id=TENANT, run_id=run_id, wave_id="9" * 16, unit_ids=unit_ids,
            now=env.clock.now(), lease_seconds=LEASE_SECONDS,
        )
    )
    reference = f"exec-{run_id}"
    env.plane.bind_run_dispatch(
        BindRunDispatch(
            tenant_id=TENANT, run_id=run_id, dispatch_id=dispatch.dispatch_id,
            execution_ref=reference, now=env.clock.now(), lease_seconds=LEASE_SECONDS,
        )
    )
    state = companion(env, run_id)
    env.plane.bind_run_execution(
        RunExecutionBindingCommand(
            tenant_id=TENANT, run_id=run_id, wave_id=dispatch.wave_id,
            dispatch_id=dispatch.dispatch_id, generation=dispatch.generation,
            execution_ref=reference, unit_ids=dispatch.unit_ids,
            expected_previous_dispatch_id=None, expected_previous_execution_ref=None,
            expected_entitlement_version=state.authorization.entitlement_version,
            expected_fencing_token=state.fencing_token, bound_at=env.clock.now(),
        )
    )
    return dispatch.model_copy(update={"execution_ref": reference})


def seed_simple_run(env: RevEnv, run_id: str) -> RunDispatch:
    create_run(env, run_id)
    units = (make_unit("unit-a", run_id=run_id), make_unit("unit-b", run_id=run_id))
    put_units(env, units, run_id)
    return start_run_wave(env, run_id, ("unit-a",))


def seed_three_wave_run(env: RevEnv) -> tuple[RunDispatch, dict[str, Any]]:
    create_run(env)
    put_units(env, (
        make_unit("unit-norm", RunStage.NORMALIZE),
        make_unit("unit-recon", RunStage.RECONCILE, depends_on_unit_ids=("unit-norm",)),
        make_unit("unit-mat1", RunStage.MATERIALIZE, depends_on_unit_ids=("unit-recon",)),
        make_unit("unit-mat2", RunStage.MATERIALIZE, depends_on_unit_ids=("unit-recon",)),
    ))
    first = start_wave(env, ("unit-norm",), None)
    finish_wave(env, first)
    second = start_wave(env, ("unit-recon",), first)
    finish_wave(env, second)
    third = start_wave(env, ("unit-mat1", "unit-mat2"), second)
    assert claim_unit(env, third, "unit-mat1") is not None
    audited = {key: units_of(env, RUN_ID)[key] for key in ("unit-norm", "unit-recon")}
    return third, audited


def seed_two_runs(env: RevEnv) -> Seeded:
    third, audited = seed_three_wave_run(env)
    simple = seed_simple_run(env, SECOND_RUN)
    return Seeded(third, audited, simple, usage_counters(env)["consumed_runs"])


def snapshot_of(env: RevEnv) -> Any:
    projection = DynamoEntitlementProjection(env.client, env.table, env.clock.now)
    return projection.get_snapshot(ACCOUNT, ReadConsistency.STRONG)


def outbox_events(env: RevEnv, event_type: str) -> list[Any]:
    return [e for e in env.plane.pending_outbox(500) if e.event_type == event_type]


def assert_run_canceled(env: RevEnv, run_id: str, dispatch: RunDispatch) -> None:
    assert stored_run(env, run_id).state is RunState.CANCELED
    stored = dispatch_of(env, run_id)
    assert stored.dispatch_id == dispatch.dispatch_id
    assert (stored.state, stored.terminal_outcome) == (
        DispatchState.TERMINAL, DispatchOutcome.CANCELED
    )
    state = companion(env, run_id)
    assert (state.cancel_requested, state.fencing_token) == (True, 1)
    assert (state.execution_status, state.execution_terminal_outcome) == (
        DispatchState.TERMINAL, DispatchOutcome.CANCELED
    )
    assert stored_reservation(env, run_id).status is ReservationStatus.RELEASED


def assert_units(env: RevEnv, seeded: Seeded) -> None:
    three = units_of(env, RUN_ID)
    for unit_id in ("unit-mat1", "unit-mat2"):
        assert three[unit_id].state is RunUnitState.CANCELED
    for unit_id, unit in seeded.audited.items():
        assert three[unit_id] == unit
        assert three[unit_id].state is RunUnitState.SUCCEEDED
    assert {u.state for u in units_of(env, SECOND_RUN).values()} == {RunUnitState.CANCELED}


def assert_outbox(env: RevEnv, version: int) -> None:
    revoked = outbox_events(env, "entitlement.revoked")
    assert [e.payload["reason_code"] for e in revoked] == ["fraud_confirmed"]
    assert revoked[0].payload["attributes"]["entitlement_version"] == version
    fences = outbox_events(env, "run.cancel_requested")
    canceled = outbox_events(env, "run.canceled")
    assert {e.aggregate_id for e in fences} == {RUN_ID, SECOND_RUN}
    assert {e.aggregate_id for e in canceled} == {RUN_ID, SECOND_RUN}
    for event in (*fences, *canceled):
        assert event.payload["reason_code"] == "revoked"
    for event in env.plane.pending_outbox(500):
        if event.tenant_id == TENANT:
            assert "fraud_confirmed" not in json.dumps(event.payload)


def revoke_two_runs_scenario(env: RevEnv) -> None:
    seeded = seed_two_runs(env)
    before = snapshot_of(env)
    executor = RecordingExecutor()

    result = build_service(env, executor).revoke(COMMAND)

    after = snapshot_of(env)
    assert after.subscription_status is SubscriptionStatus.ADMIN_REVOKED
    assert after.entitlement_version == before.entitlement_version + 1
    assert result.entitlement_version == after.entitlement_version
    assert set(result.fenced_run_ids) == {RUN_ID, SECOND_RUN}
    assert result.cancel_failures == ()
    assert executor.refs() == {(RUN_ID, "exec-3"), (SECOND_RUN, f"exec-{SECOND_RUN}")}
    assert len(executor.requests) == 2
    assert_run_canceled(env, RUN_ID, seeded.third)
    assert_run_canceled(env, SECOND_RUN, seeded.simple)
    assert_units(env, seeded)
    assert usage_counters(env)["consumed_runs"] == seeded.consumed
    progress = env.store.get_revocation_progress(ACCOUNT)
    assert progress is not None
    assert (progress.entitlement_version, progress.phase) == (
        after.entitlement_version, RevocationPhase.COMPLETE
    )
    assert_outbox(env, after.entitlement_version)


def test_servico_revoga_tres_waves_e_cancela_so_o_dispatch_mais_recente(env: RevEnv) -> None:
    revoke_two_runs_scenario(env)


@pytest.mark.dynamodb_local
def test_servico_revoga_tres_waves_no_dynamodb_local() -> None:
    client = local_client()
    table = f"revocation-service-{uuid4().hex[:12]}"
    create_named_table(client, table)
    try:
        revoke_two_runs_scenario(build_env(client, table))
    finally:
        client.delete_table(TableName=table)


def _is_fence_event(item: dict[str, Any]) -> bool:
    return json.loads(item["payload"]["S"])["event_type"] == "run.cancel_requested"


def classify(request: list[dict[str, Any]]) -> str:
    for action in request:
        item = action.get("Put", {}).get("Item")
        if item is None:
            continue
        if item["sk"]["S"] == "ENTITLEMENT":
            return "snapshot"
        if item["entity"]["S"] == "OUTBOXEVENT" and _is_fence_event(item):
            return "fence"
    return "other"


def test_ordem_snapshot_depois_fences_depois_cancelamento_do_executor(env: RevEnv) -> None:
    seed_two_runs(env)
    log: list[str] = []
    env.spy.before_transact = lambda: log.append(classify(env.spy.transactions[-1]))

    build_service(env, RecordingExecutor(log=log)).revoke(COMMAND)

    assert log.count("snapshot") == 1
    assert log.count("fence") == 2
    assert log.count("cancel") == 2
    snapshot_at = log.index("snapshot")
    fences = [index for index, label in enumerate(log) if label == "fence"]
    first_cancel = log.index("cancel")
    assert snapshot_at < min(fences)
    assert max(fences) < first_cancel


def test_falha_do_executor_nao_restaura_fence_no_dynamodb(env: RevEnv) -> None:
    seeded = seed_two_runs(env)
    executor = RecordingExecutor(error=RuntimeError("executor_down"))

    result = build_service(env, executor).revoke(COMMAND)

    assert set(result.cancel_failures) == {RUN_ID, SECOND_RUN}
    assert set(result.fenced_run_ids) == {RUN_ID, SECOND_RUN}
    for run_id in (RUN_ID, SECOND_RUN):
        state = companion(env, run_id)
        assert (state.cancel_requested, state.fencing_token) == (True, 1)
    assert stored_run(env, RUN_ID).state is RunState.CANCELED
    assert stored_run(env, SECOND_RUN).state is RunState.CANCELED
    assert_units(env, seeded)


def test_retry_da_revogacao_nao_incrementa_fence_nem_cancela_executor(env: RevEnv) -> None:
    seed_two_runs(env)
    executor = RecordingExecutor()
    service = build_service(env, executor)
    first = service.revoke(COMMAND)
    tokens = {run_id: companion(env, run_id).fencing_token for run_id in (RUN_ID, SECOND_RUN)}
    calls = len(executor.requests)
    transactions = len(env.spy.transactions)

    second = service.revoke(COMMAND)

    assert len(executor.requests) == calls
    assert tokens == {RUN_ID: 1, SECOND_RUN: 1}
    for run_id, token in tokens.items():
        assert companion(env, run_id).fencing_token == token
    new = env.spy.transactions[transactions:]
    assert not [tx for tx in new if classify(tx) in ("fence", "snapshot")]
    assert second.fenced_run_ids == ()
    assert second.cancel_failures == ()
    assert second.entitlement_version == first.entitlement_version


def nth_fence(position: int) -> Callable[[list[dict[str, Any]]], bool]:
    seen = [0]

    def predicate(request: list[dict[str, Any]]) -> bool:
        if classify(request) != "fence":
            return False
        seen[0] += 1
        return seen[0] == position

    return predicate


def first_cancellation_write(request: list[dict[str, Any]]) -> bool:
    return any(
        "CANCELED" in action.get("Put", {}).get("Item", {}).get("payload", {}).get("S", "")
        for action in request
    )


def inject_crash(env: RevEnv, predicate: Callable[[list[dict[str, Any]]], bool]) -> None:
    def hook() -> None:
        if predicate(env.spy.transactions[-1]):
            env.spy.before_transact = None
            raise RuntimeError("crash_injected")

    env.spy.before_transact = hook


@pytest.mark.parametrize(
    ("predicate", "phase"),
    [
        pytest.param(nth_fence(2), RevocationPhase.FENCING, id="durante_o_fence"),
        pytest.param(first_cancellation_write, RevocationPhase.FINALIZING, id="na_finalizacao"),
    ],
)
def test_retomada_apos_queda_pelo_cursor_de_revogacao(
    env: RevEnv, predicate: Callable[[list[dict[str, Any]]], bool], phase: RevocationPhase
) -> None:
    seed_simple_run(env, RUN_ID)
    seed_simple_run(env, SECOND_RUN)
    executor = RecordingExecutor()
    service = build_service(env, executor, ServiceOptions(RevocationSettings(run_page_size=1)))
    inject_crash(env, predicate)

    with pytest.raises(RuntimeError, match="crash_injected"):
        service.revoke(COMMAND)

    progress = env.store.get_revocation_progress(ACCOUNT)
    assert progress is not None
    assert progress.phase is phase
    assert progress.phase is not RevocationPhase.COMPLETE
    service.revoke(COMMAND)

    assert cast("Any", env.store.get_revocation_progress(ACCOUNT)).phase is RevocationPhase.COMPLETE
    for run_id in (RUN_ID, SECOND_RUN):
        assert stored_run(env, run_id).state is RunState.CANCELED
        assert companion(env, run_id).fencing_token == 1
    refs = [(request.run_id, request.execution_ref) for request in executor.requests]
    assert sorted(refs) == [(RUN_ID, f"exec-{RUN_ID}"), (SECOND_RUN, f"exec-{SECOND_RUN}")]


def test_published_degraded_nao_e_revogado(env: RevEnv) -> None:
    create_run(env)
    put_run_state(env, RunState.PUBLISHED_DEGRADED)
    seed_simple_run(env, SECOND_RUN)
    before = companion(env, RUN_ID)
    executor = RecordingExecutor()

    result = build_service(env, executor).revoke(COMMAND)

    assert result.fenced_run_ids == (SECOND_RUN,)
    assert stored_run(env, RUN_ID).state is RunState.PUBLISHED_DEGRADED
    after = companion(env, RUN_ID)
    assert (after.cancel_requested, after.fencing_token) == (False, before.fencing_token)
    assert after == before
    assert stored_run(env, SECOND_RUN).state is RunState.CANCELED
    assert {request.run_id for request in executor.requests} == {SECOND_RUN}


def test_modo_disabled_falha_fechado_sem_tocar_executor(env: RevEnv) -> None:
    seed_simple_run(env, RUN_ID)
    executor = RecordingExecutor()
    store = StoreSpy(env.store)
    projection = DisabledEntitlementProjection(env.clock.now)
    service = build_service(env, executor, ServiceOptions(projection=projection, store=store))
    transactions = len(env.spy.transactions)

    with pytest.raises(BillingDisabledError):
        service.revoke(COMMAND)

    assert executor.requests == []
    assert store.calls == []
    assert len(env.spy.transactions) == transactions
    assert stored_run(env, RUN_ID).state is RunState.PROCESSING


def test_run_com_150_unidades_converge_com_transacoes_de_ate_100_itens(env: RevEnv) -> None:
    create_run(env)
    seed_units(env, 150)
    env.spy.transactions.clear()

    options = ServiceOptions(RevocationSettings(unit_batch_size=50))

    result = build_service(env, RecordingExecutor(), options).revoke(COMMAND)

    assert result.fenced_run_ids == (RUN_ID,)
    assert stored_run(env).state is RunState.CANCELED
    units = units_of(env, RUN_ID)
    assert len(units) == 150
    assert {unit.state for unit in units.values()} == {RunUnitState.CANCELED}
    assert env.spy.transactions
    assert all(len(items) <= LIMIT_PER_TRANSACTION for items in env.spy.transactions)
