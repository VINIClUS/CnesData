"""Builders e cenário trifásico dos testes de revogação imediata sobre DynamoDB."""

import json
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any

import boto3
from moto import mock_aws

from cnes_domain.billing.execution import RunBillingState, RunExecutionBindingCommand
from cnes_domain.billing.models import QuotaReservation, ReservationStatus
from cnes_domain.billing.revocation import (
    CancelRunUnitsCommand,
    CancelRunUnitsResult,
    RevokeRunCommand,
)
from cnes_domain.control_plane.commands import (
    BindRunDispatch,
    ClaimRunUnit,
    CommitRunUnit,
    FinishRunDispatch,
    PutRunUnits,
    ReserveRunDispatch,
    TransitionRun,
)
from cnes_domain.control_plane.entities import (
    ManifestRef,
    OutboxEvent,
    Run,
    RunDispatch,
    RunUnit,
)
from cnes_domain.control_plane.enums import (
    DispatchOutcome,
    DispatchState,
    RunStage,
    RunState,
    RunUnitState,
)
from cnes_infra.billing.dynamodb_items import encode_snapshot
from cnes_infra.billing.dynamodb_quota import DynamoQuotaReservations
from cnes_infra.billing.dynamodb_quota_items import decode_reservation
from cnes_infra.billing.dynamodb_revocation import DynamoRevocationStore
from cnes_infra.billing.keys import reservation_key, run_lookup_key, usage_key
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.control_plane.dynamodb_codec import encode_model
from cnes_infra.control_plane.dynamodb_keys import (
    dispatch_key,
    item_key,
    key_component,
    run_entity_key,
    unit_key,
)
from cnes_infra.control_plane.dynamodb_run_codec import run_item
from packages.cnes_infra.tests.billing.billing_factories import NOW, TABLE_NAME, create_table
from packages.cnes_infra.tests.billing.quota_support import (
    ACCOUNT,
    TENANT,
    make_quota_snapshot,
    make_reserve_command,
)
from packages.cnes_infra.tests.billing.test_control_plane_extensions import (
    SpyClient,
    cancellation,
)
from packages.cnes_infra.tests.contracts.clock import MutableClock

RUN_ID = "run-01"
LEASE_SECONDS = 300
INDEXES = tuple(f"gsi{number}" for number in range(1, 7))


@dataclass(frozen=True, slots=True)
class RevEnv:
    client: Any
    spy: SpyClient
    clock: MutableClock
    table: str
    store: DynamoRevocationStore
    plane: DynamoDBControlPlane
    quota: DynamoQuotaReservations


def build_env(client: Any, table: str) -> RevEnv:
    client.put_item(TableName=table, Item=encode_snapshot(make_quota_snapshot()))
    clock = MutableClock(NOW)
    spy = SpyClient(client)
    return RevEnv(
        client=client,
        spy=spy,
        clock=clock,
        table=table,
        store=DynamoRevocationStore(spy, table, clock.now),
        plane=DynamoDBControlPlane(spy, table, clock.now),
        quota=DynamoQuotaReservations(spy, table, clock.now),
    )


@contextmanager
def open_env() -> Iterator[RevEnv]:
    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        create_table(client)
        yield build_env(client, TABLE_NAME)


def create_named_table(client: Any, table: str) -> None:
    names = ("pk", "sk", *(f"{index}{part}" for index in INDEXES for part in ("pk", "sk")))
    throughput = {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}
    indexes = [
        {
            "IndexName": index,
            "KeySchema": [
                {"AttributeName": f"{index}pk", "KeyType": "HASH"},
                {"AttributeName": f"{index}sk", "KeyType": "RANGE"},
            ],
            "Projection": {"ProjectionType": "ALL"},
            "ProvisionedThroughput": throughput,
        }
        for index in INDEXES
    ]
    client.create_table(
        TableName=table,
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[{"AttributeName": name, "AttributeType": "S"} for name in names],
        GlobalSecondaryIndexes=indexes,
        ProvisionedThroughput=throughput,
    )


def event_of(event_type: str, run_id: str = RUN_ID, **changes: Any) -> OutboxEvent:
    event = OutboxEvent(
        tenant_id=TENANT, event_id=f"{event_type}:{run_id}", event_type=event_type,
        aggregate_id=run_id, payload={}, created_at=NOW, delivered_at=None,
    )
    return event.model_copy(update=changes)


def create_run(env: RevEnv, run_id: str = RUN_ID, to_processing: bool = True) -> None:
    command = make_reserve_command(run_id=run_id, idempotency_key=f"req-{run_id}")
    env.quota.reserve_and_create_run(command)
    if to_processing:
        move_to_processing(env, run_id)


def move_to_processing(env: RevEnv, run_id: str = RUN_ID) -> None:
    env.plane.transition_run(
        TransitionRun(
            tenant_id=TENANT, run_id=run_id, expected_state=RunState.WAITING_INPUTS,
            new_state=RunState.PROCESSING, missing_sources=(),
        ),
        event_of("run.processing", run_id),
    )


def make_unit(
    unit_id: str, stage: RunStage = RunStage.NORMALIZE, run_id: str = RUN_ID, **changes: Any
) -> RunUnit:
    key = f"raw/{TENANT}/CNES/2026-08/{unit_id}/manifest.json"
    inputs = {"source_type": "CNES", "file_subtype": "LFCES", "depends_on_unit_ids": (),
              "input_manifests": (ManifestRef(manifest_id=unit_id, manifest_key=key),)}
    if stage is not RunStage.NORMALIZE:
        inputs = {"source_type": None, "file_subtype": None, "input_manifests": (),
                  "depends_on_unit_ids": ("unit-norm",)}
    fields = {
        "tenant_id": TENANT, "run_id": run_id, "unit_id": unit_id, "stage": stage,
        "partition": "all", "state": RunUnitState.PENDING, "attempt": 0, "fencing_token": 0,
        "lease_owner": None, "lease_until": None, "dispatch_id": None, "output_manifests": (),
        "error_code": None, **inputs, **changes,
    }
    return RunUnit(**fields)


def put_units(env: RevEnv, units: tuple[RunUnit, ...], run_id: str = RUN_ID) -> None:
    env.plane.put_run_units(
        PutRunUnits(
            tenant_id=TENANT, run_id=run_id, expected_run_state=RunState.PROCESSING, units=units,
        )
    )


def seed_units(env: RevEnv, count: int, run_id: str = RUN_ID) -> tuple[str, ...]:
    ids = tuple(f"unit-{number:04d}" for number in range(count))
    for unit_id in ids:
        unit = make_unit(unit_id, run_id=run_id)
        attributes = {
            "gsi5pk": f"RUN_ITEMS#{key_component(TENANT)}#{key_component(run_id)}",
            "gsi5sk": f"UNIT#{key_component(unit_id)}",
        }
        item = encode_model(unit, "RUNUNIT", unit_key(TENANT, run_id, unit_id), attributes)
        env.client.put_item(TableName=env.table, Item=item)
    return ids


def reserve_wave(
    env: RevEnv, unit_ids: tuple[str, ...], previous: RunDispatch | None
) -> RunDispatch:
    generation = 1 if previous is None else previous.generation + 1
    return env.plane.reserve_run_dispatch(
        ReserveRunDispatch(
            tenant_id=TENANT, run_id=RUN_ID, wave_id=str(generation) * 16, unit_ids=unit_ids,
            now=env.clock.now(), lease_seconds=LEASE_SECONDS,
        )
    )


def start_wave(env: RevEnv, unit_ids: tuple[str, ...], previous: RunDispatch | None) -> RunDispatch:
    dispatch = reserve_wave(env, unit_ids, previous)
    reference = f"exec-{dispatch.generation}"
    env.plane.bind_run_dispatch(
        BindRunDispatch(
            tenant_id=TENANT, run_id=RUN_ID, dispatch_id=dispatch.dispatch_id,
            execution_ref=reference, now=env.clock.now(), lease_seconds=LEASE_SECONDS,
        )
    )
    env.plane.bind_run_execution(bind_command(env, dispatch, reference, previous))
    return dispatch.model_copy(update={"execution_ref": reference})


def bind_command(
    env: RevEnv, dispatch: RunDispatch, reference: str, previous: RunDispatch | None
) -> RunExecutionBindingCommand:
    state = env.plane.get_run_billing_state(TENANT, RUN_ID)
    return RunExecutionBindingCommand(
        tenant_id=TENANT, run_id=RUN_ID, wave_id=dispatch.wave_id,
        dispatch_id=dispatch.dispatch_id, generation=dispatch.generation,
        execution_ref=reference, unit_ids=dispatch.unit_ids,
        expected_previous_dispatch_id=None if previous is None else previous.dispatch_id,
        expected_previous_execution_ref=None if previous is None else previous.execution_ref,
        expected_entitlement_version=state.authorization.entitlement_version,
        expected_fencing_token=state.fencing_token, bound_at=env.clock.now(),
    )


def claim_unit(
    env: RevEnv, dispatch: RunDispatch, unit_id: str, plane: DynamoDBControlPlane | None = None
) -> RunUnit | None:
    return (plane or env.plane).claim_run_unit(
        ClaimRunUnit(
            tenant_id=TENANT, run_id=RUN_ID, unit_id=unit_id, dispatch_id=dispatch.dispatch_id,
            owner="worker-a", now=env.clock.now(), lease_seconds=60,
        )
    )


def finish_wave(env: RevEnv, dispatch: RunDispatch) -> None:
    for unit_id in dispatch.unit_ids:
        claimed = claim_unit(env, dispatch, unit_id)
        output = ManifestRef(
            manifest_id=f"out-{unit_id}",
            manifest_key=f"raw/{TENANT}/CNES/2026-08/out-{unit_id}/manifest.json",
        )
        env.plane.commit_run_unit(
            CommitRunUnit(
                tenant_id=TENANT, run_id=RUN_ID, unit_id=unit_id,
                dispatch_id=dispatch.dispatch_id, owner="worker-a",
                fencing_token=claimed.fencing_token, output_manifests=(output,),
            ),
            event_of(f"unit.completed.{unit_id}"),
        )
    env.plane.finish_run_dispatch(
        FinishRunDispatch(
            tenant_id=TENANT, run_id=RUN_ID, dispatch_id=dispatch.dispatch_id,
            outcome=DispatchOutcome.SUCCEEDED, finished_at=env.clock.now(),
        )
    )


def revoke_command(env: RevEnv, run_id: str = RUN_ID, **changes: Any) -> RevokeRunCommand:
    state = env.store.get_run_billing_state(TENANT, run_id)
    run = env.store.get_run(TENANT, run_id)
    command = RevokeRunCommand(
        tenant_id=TENANT, run_id=run_id, expected_state=run.state,
        expected_fencing_token=state.fencing_token, reason_code="revoked",
        requested_at=env.clock.now(),
    )
    return replace(command, **changes)


def revocation_event(run_id: str = RUN_ID, **changes: Any) -> OutboxEvent:
    return event_of("run.revocation_requested", run_id, **changes)


def fence(env: RevEnv, run_id: str = RUN_ID) -> RunBillingState:
    return env.store.request_run_revocation(
        revoke_command(env, run_id), revocation_event(run_id)
    )


def cancel_command(
    env: RevEnv, fenced: RunBillingState, limit: int = 98, cursor: str | None = None
) -> CancelRunUnitsCommand:
    return CancelRunUnitsCommand(
        tenant_id=fenced.tenant_id, run_id=fenced.run_id,
        expected_run_fencing_token=fenced.fencing_token, limit=limit, cursor=cursor,
        canceled_at=env.clock.now(),
    )


def cancel_until_done(
    env: RevEnv, fenced: RunBillingState, limit: int = 98
) -> list[CancelRunUnitsResult]:
    results: list[CancelRunUnitsResult] = []
    cursor = None
    while not results or not results[-1].run_canceled:
        results.append(env.store.cancel_run_units(cancel_command(env, fenced, limit, cursor)))
        cursor = results[-1].next_cursor
    return results


def transaction_entities(request: list[dict[str, Any]]) -> set[str]:
    return {
        action["Put"]["Item"]["entity"]["S"] for action in request if "Put" in action
    }


def before_transaction(env: RevEnv, entity: str, action: Callable[[], None]) -> None:
    def hook() -> None:
        if entity in transaction_entities(env.spy.transactions[-1]):
            env.spy.before_transact = None
            action()

    env.spy.before_transact = hook


def reject_transaction() -> None:
    raise cancellation()


def get_raw(env: RevEnv, key: tuple[str, str]) -> dict[str, Any] | None:
    response = env.client.get_item(
        TableName=env.table, Key=item_key(*key), ConsistentRead=True
    )
    return response.get("Item")


def put_run_state(env: RevEnv, state: RunState, run_id: str = RUN_ID) -> None:
    run = env.store.get_run(TENANT, run_id)
    env.client.put_item(TableName=env.table, Item=run_item(run.model_copy(update={"state": state})))


def stored_dispatch(env: RevEnv) -> RunDispatch | None:
    item = get_raw(env, dispatch_key(TENANT, RUN_ID))
    return None if item is None else RunDispatch.model_validate_json(item["payload"]["S"])


def stored_run(env: RevEnv, run_id: str = RUN_ID) -> Run:
    return Run.model_validate_json(get_raw(env, run_entity_key(TENANT, run_id))["payload"]["S"])


def lookup_period(env: RevEnv, run_id: str = RUN_ID) -> datetime:
    item = get_raw(env, run_lookup_key(ACCOUNT, TENANT, run_id))
    return datetime.fromisoformat(json.loads(item["payload"]["S"])["period_start"])


def stored_reservation(env: RevEnv, run_id: str = RUN_ID) -> QuotaReservation:
    key = reservation_key(ACCOUNT, lookup_period(env, run_id), f"res-{run_id}")
    return decode_reservation(get_raw(env, key))[0]


def usage_counters(env: RevEnv, run_id: str = RUN_ID) -> dict[str, int]:
    item = get_raw(env, usage_key(ACCOUNT, lookup_period(env, run_id)))
    return {name: int(value["N"]) for name, value in item.items() if "N" in value}


def units_by_id(env: RevEnv) -> dict[str, RunUnit]:
    return {unit.unit_id: unit for unit in env.plane.list_run_units(TENANT, RUN_ID)}


def three_wave_revocation(env: RevEnv) -> None:
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
    assert len({first.dispatch_id, second.dispatch_id, third.dispatch_id}) == 3
    audited = {key: units_by_id(env)[key] for key in ("unit-norm", "unit-recon")}

    fenced = fence(env)
    active = env.store.get_active_run_dispatch(TENANT, RUN_ID)
    assert (active.dispatch_id, active.execution_ref, active.generation) == (
        third.dispatch_id, "exec-3", 3
    )
    results = cancel_until_done(env, fenced)

    assert results[-1].run_canceled
    assert stored_run(env).state is RunState.CANCELED
    units = units_by_id(env)
    for unit_id in ("unit-mat1", "unit-mat2"):
        assert units[unit_id].state is RunUnitState.CANCELED
        assert (units[unit_id].lease_owner, units[unit_id].lease_until) == (None, None)
    for unit_id, unit in audited.items():
        assert units[unit_id] == unit
        assert units[unit_id].state is RunUnitState.SUCCEEDED
    assert audited["unit-norm"].dispatch_id == first.dispatch_id
    assert audited["unit-recon"].dispatch_id == second.dispatch_id
    dispatch = stored_dispatch(env)
    assert dispatch.dispatch_id == third.dispatch_id
    assert (dispatch.state, dispatch.terminal_outcome) == (
        DispatchState.TERMINAL, DispatchOutcome.CANCELED
    )
    state = env.store.get_run_billing_state(TENANT, RUN_ID)
    assert (state.cancel_requested, state.fencing_token) == (True, 1)
    assert (state.execution_dispatch_id, state.execution_ref) == (third.dispatch_id, "exec-3")
    assert (state.execution_status, state.execution_terminal_outcome) == (
        DispatchState.TERMINAL, DispatchOutcome.CANCELED
    )
    assert stored_reservation(env).status is ReservationStatus.RELEASED
    assert usage_counters(env)["consumed_runs"] == 1
