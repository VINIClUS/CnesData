"""C3: frames do keeper seguidos da admissão do container no mesmo DynamoDB, sem disputa."""
from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

import boto3
import pytest
from moto import mock_aws

from cnes_domain.control_plane.commands import ClaimRunDispatchStart
from cnes_domain.control_plane.dispatch_start import DispatchStartClaim
from cnes_domain.control_plane.enums import DispatchState
from cnes_domain.control_plane.environment_gate import (
    AcquireUnitPermit,
    AcquisitionStatus,
    ClosePromotionFence,
    PermitAcquisition,
    PermitHolder,
)
from cnes_domain.orchestration.mode_canary import (
    ModeCanaryBinding,
    ModeCanaryRecord,
    SyntheticContextRequired,
)
from cnes_infra.aws.mode_canary import ModeCanaryContext, require_seeded_control
from cnes_infra.aws.mode_canary_dispatch import (
    DispatchStep,
    run_dispatch_item,
    start_claim,
    unit_dispatch,
)
from cnes_infra.aws.mode_canary_frames import (
    LedgerState,
    admission,
    canonical_text,
    dispatch_effect,
    effect_id,
    ledger_effect,
    transition,
)
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.control_plane.dynamodb_keys import run_partition, tenant_partition
from packages.cnes_domain.tests.orchestration.test_mode_canary_records import (
    CANARY_ID,
    _control,
    _promotion,
)
from packages.cnes_infra.tests.control_plane.test_dynamodb_adapter import (
    _TABLE_NAME,
    _create_table,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

_TENANT = f"prod-canary-{CANARY_ID}"
_PK = tenant_partition(_TENANT)
_OWNER = "a" * 64
_NOW = datetime(2026, 11, 12, 12, 10, tzinfo=UTC)
_RECOVERY_RUNS = ["recovery-a-aaaaaaaaaaaaaaaa", "recovery-b-aaaaaaaaaaaaaaaa"]
_UNIT_RUN = "unit-aaaaaaaaaaaaaaaa"
_SEEDED_UNITS = [
    {"tenant_id": _TENANT, "run_id": _UNIT_RUN, "unit_id": unit_id, "state": "PENDING",
     "depends_on_unit_ids": depends, "lease_until": None}
    for unit_id, depends in (("n-a", []), ("n-b", []), ("rec", ["n-a", "n-b"]))]


class _Dispatches:
    def __init__(self, dispatch: str, units: tuple[str, ...]) -> None:
        self._dispatch = SimpleNamespace(dispatch_id=dispatch, unit_ids=units)

    def get_run_dispatch(self, tenant_id: str, run_id: str) -> Any:
        return self._dispatch


class _Recorder:
    def __init__(self, client: Any) -> None:
        self.client = client
        self.writes: list[str] = []

    def get_item(self, **kwargs: Any) -> Any:
        return self.client.get_item(**kwargs)

    def put_item(self, **kwargs: Any) -> Any:
        self.writes.append("put_item")
        return self.client.put_item(**kwargs)

    def transact_write_items(self, **kwargs: Any) -> Any:
        self.writes.append("transact_write_items")
        return self.client.transact_write_items(**kwargs)


@pytest.fixture
def client() -> Iterator[Any]:
    with mock_aws():
        dynamo = boto3.client("dynamodb", region_name="us-east-1")
        _create_table(dynamo)
        yield dynamo


def _seed(client: Any, phase: str) -> tuple[ModeCanaryBinding, ModeCanaryRecord]:
    record = _promotion() | {"owner_sha256": _OWNER}
    if phase == "recovery":
        record |= {"phase": "recovery", "mode": "recovery", "scenario": "recovery_resume",
                   "run_ids": _RECOVERY_RUNS}
    text = canonical_text(record)
    control = canonical_text(_control() | {"owner_sha256": _OWNER})
    for sort_key, entity, payload in (
            (f"PROD_CANARY#{_OWNER}#{phase}", "MODECANARYRECORD", text),
            (f"PROD_CANARY_CONTROL#{_OWNER}", "MODECANARYCONTROL", control)):
        client.put_item(TableName=_TABLE_NAME, Item={
            "pk": {"S": _PK}, "sk": {"S": sort_key}, "entity": {"S": entity},
            "payload": {"S": payload}})
    binding = ModeCanaryBinding(_TENANT, tuple(record["run_ids"]), _OWNER,
                                sha256(text.encode()).hexdigest(), phase, "promotion")
    return binding, ModeCanaryRecord.from_mapping(record)


def _payload(client: Any, sort_key: str, pk: str = _PK) -> str | None:
    item = client.get_item(TableName=_TABLE_NAME, Key={"pk": {"S": pk}, "sk": {"S": sort_key}},
                           ConsistentRead=True).get("Item")
    return None if item is None else item["payload"]["S"]


def _state(client: Any, binding: ModeCanaryBinding, record: ModeCanaryRecord,
           effect: str) -> LedgerState:
    control = _payload(client, f"PROD_CANARY_CONTROL#{_OWNER}")
    assert control is not None
    ticket = _payload(client, f"PROD_CANARY_EFFECT#{_OWNER}#{effect}")
    return LedgerState(binding, record, control, ticket)


def _tickets(client: Any) -> list[dict[str, Any]]:
    items = client.query(TableName=_TABLE_NAME, KeyConditionExpression="pk = :pk",
                         ExpressionAttributeValues={":pk": {"S": _PK}})["Items"]
    return [json.loads(item["payload"]["S"]) for item in items
            if item["sk"]["S"].startswith("PROD_CANARY_EFFECT#")]


def _counts(client: Any) -> dict[str, int]:
    control = _payload(client, f"PROD_CANARY_CONTROL#{_OWNER}")
    assert control is not None
    return json.loads(control)["admitted_counts"]


def _context(client: Any, binding: ModeCanaryBinding, dispatch: str,
             units: tuple[str, ...]) -> tuple[ModeCanaryContext, _Recorder]:
    recorder = _Recorder(client)
    dispatches = _Dispatches(dispatch, units)
    return ModeCanaryContext(recorder, _TABLE_NAME, binding, dispatches), recorder


def _keeper_admit(client: Any, binding: ModeCanaryBinding, record: ModeCanaryRecord,
                  effect: Any) -> str:
    request = effect.request_sha256
    expected = effect_id(binding.record_sha256, effect.kind, request)
    plan = admission(_TABLE_NAME, _state(client, binding, record, expected), effect, _NOW)
    if plan.items is not None:
        client.transact_write_items(TransactItems=plan.items)
    return plan.effect_id


def test_dispatch_unit_do_keeper_e_binding_for_do_container_admitem_um_ticket(
        client: Any) -> None:
    binding, record = _seed(client, "unit")
    holder = PermitHolder(_TENANT, "unit-aaaaaaaaaaaaaaaa", "0123456789abcdef")
    units = ("u1", "u2")
    keeper_effect = _keeper_admit(client, binding, record,
                                  dispatch_effect(binding, record, holder, units))
    binding_sk = "PROD_CANARY#0123456789abcdef"
    binding_pk = run_partition(_TENANT, holder.run_id)
    stored = _payload(client, binding_sk, binding_pk)
    context, recorder = _context(client, binding, holder.dispatch_id, units)
    context.binding_for(holder, _NOW)
    assert recorder.writes == []
    assert _counts(client)["unit_tasks"] == len(units)
    assert [ticket["effect_id"] for ticket in _tickets(client)] == [keeper_effect]
    assert stored is not None
    assert _payload(client, binding_sk, binding_pk) == stored


def test_dispatch_unit_repetido_pelo_keeper_nao_gera_frames(client: Any) -> None:
    binding, record = _seed(client, "unit")
    holder = PermitHolder(_TENANT, "unit-aaaaaaaaaaaaaaaa", "0123456789abcdef")
    effect = dispatch_effect(binding, record, holder, ("u1", "u2"))
    first = _keeper_admit(client, binding, record, effect)
    replay = admission(_TABLE_NAME, _state(client, binding, record, first), effect, _NOW)
    assert (replay.effect_id, replay.items) == (first, None)
    assert _counts(client)["unit_tasks"] == 2


def test_frames_do_ledger_do_recovery_e_admissao_do_container_nao_disputam(
        client: Any) -> None:
    binding, record = _seed(client, "recovery")
    resume = ledger_effect("run-recovery", "1" * 64, "2" * 64)
    effect = _keeper_admit(client, binding, record, resume)
    for change in (("UNKNOWN", None), ("SETTLED", "3" * 64)):
        ticket = _payload(client, f"PROD_CANARY_EFFECT#{_OWNER}#{effect}")
        put = transition(_TABLE_NAME, binding, (effect, ticket), change)
        assert put is not None
        client.put_item(**put)
    holder = PermitHolder(_TENANT, _RECOVERY_RUNS[0], "fedcba9876543210")
    context, recorder = _context(client, binding, holder.dispatch_id, ("u3",))
    assert context.admit_effect("run-recovery", "1" * 64, "2" * 64, _NOW) == effect
    assert recorder.writes == []
    context.binding_for(holder, _NOW)
    assert recorder.writes == ["transact_write_items"]
    assert _counts(client) == {"unit_tasks": 1, "recovery_resumes": 1, "recovery_hangs": 0}
    kinds = sorted(ticket["effect_kind"] for ticket in _tickets(client))
    assert kinds == ["run-recovery", "start-execution"]
    [settled] = [t for t in _tickets(client) if t["effect_kind"] == "run-recovery"]
    assert (settled["state"], settled["receipt_sha256"]) == ("SETTLED", "3" * 64)


def test_transicao_repetida_pelo_keeper_nao_gera_frame(client: Any) -> None:
    binding, record = _seed(client, "recovery")
    effect = _keeper_admit(client, binding, record,
                           ledger_effect("run-recovery", "1" * 64, "2" * 64))
    ticket = _payload(client, f"PROD_CANARY_EFFECT#{_OWNER}#{effect}")
    put = transition(_TABLE_NAME, binding, (effect, ticket), ("UNKNOWN", None))
    assert put is not None
    client.put_item(**put)
    again = _payload(client, f"PROD_CANARY_EFFECT#{_OWNER}#{effect}")
    assert transition(_TABLE_NAME, binding, (effect, again), ("UNKNOWN", None)) is None


def test_control_semeado_e_lido_forte_antes_do_recovery_seed(client: Any) -> None:
    _, record = _seed(client, "recovery")
    control = require_seeded_control(client, _TABLE_NAME, record)
    assert (control.owner_sha256, control.admission_state) == (_OWNER, "ACTIVE")


def test_recovery_seed_sem_control_semeado_recusa(client: Any) -> None:
    record = ModeCanaryRecord.from_mapping(_promotion() | {"owner_sha256": "b" * 64})
    with pytest.raises(SyntheticContextRequired, match="reason=control_missing"):
        require_seeded_control(client, _TABLE_NAME, record)


def _native_step(dispatch: Any, state: str, promotion_id: str | None) -> DispatchStep:
    lease = _NOW + timedelta(seconds=90)
    claim = start_claim(dispatch, 1, 0, promotion_id)
    execution = f"arn:aws:states:us-east-2:111122223333:execution:m:{dispatch.dispatch_id}"
    return {"RESERVED": DispatchStep("RESERVED", lease),
            "STARTING": DispatchStep("STARTING", lease, claim),
            "STARTED": DispatchStep("STARTED", lease, claim, execution)}[state]


@pytest.mark.parametrize("state", ["RESERVED", "STARTING", "STARTED"])
def test_dispatch_nativo_do_keeper_e_binding_for_pelo_get_run_dispatch_real_nao_disputam(
        client: Any, state: str) -> None:
    binding, record = _seed(client, "unit")
    dispatch = unit_dispatch(binding, _SEEDED_UNITS, _NOW)
    holder = PermitHolder(dispatch.tenant_id, dispatch.run_id, dispatch.dispatch_id)
    keeper_effect = _keeper_admit(client, binding, record,
                                  dispatch_effect(binding, record, holder, dispatch.unit_ids))
    step = _native_step(dispatch, state, record.promotion_id)
    client.put_item(TableName=_TABLE_NAME, Item=run_dispatch_item(dispatch, step))
    binding_sk = f"PROD_CANARY#{dispatch.dispatch_id}"
    binding_pk = run_partition(_TENANT, holder.run_id)
    stored = _payload(client, binding_sk, binding_pk)
    recorder = _Recorder(client)
    plane = DynamoDBControlPlane(client, _TABLE_NAME, lambda: _NOW)
    ModeCanaryContext(recorder, _TABLE_NAME, binding, plane).binding_for(holder, _NOW)
    assert recorder.writes == []
    assert _counts(client)["unit_tasks"] == len(dispatch.unit_ids) == 2
    assert [ticket["effect_id"] for ticket in _tickets(client)] == [keeper_effect]
    assert stored is not None
    assert _payload(client, binding_sk, binding_pk) == stored


def _under_closed_fence(client: Any, closer: str) -> tuple[Any, Any, PermitAcquisition]:
    binding, _ = _seed(client, "unit")
    plan = unit_dispatch(binding, _SEEDED_UNITS, _NOW)
    plane = DynamoDBControlPlane(client, _TABLE_NAME, lambda: _NOW)
    assert plane.close_promotion_fence(ClosePromotionFence("prod", closer, _NOW))
    reserved = DispatchStep("RESERVED", _NOW + timedelta(seconds=90))
    client.put_item(TableName=_TABLE_NAME, Item=run_dispatch_item(plan, reserved))
    holder = PermitHolder(plan.tenant_id, plan.run_id, plan.dispatch_id)
    return plan, plane, plane.acquire_unit_permit(
        AcquireUnitPermit("prod", holder, _NOW, 90, _OWNER))


def test_dispatch_nativo_do_unit_passa_com_o_fence_fechado_pelo_owner(client: Any) -> None:
    plan, plane, acquired = _under_closed_fence(client, _OWNER)
    assert acquired.status is AcquisitionStatus.ACQUIRED
    assert acquired.permit is not None
    fence = plane.observe_environment_gate("prod").fence
    assert (fence.state.value, fence.promotion_id) == ("CLOSED", _OWNER)
    claim = DispatchStartClaim(**start_claim(plan, acquired.permit.token, fence.version, _OWNER))
    claimed = plane.claim_run_dispatch_start(ClaimRunDispatchStart(
        tenant_id=plan.tenant_id, run_id=plan.run_id, dispatch_id=plan.dispatch_id, claim=claim,
        now=_NOW, lease_seconds=90))
    assert claimed is not None
    assert (claimed.state, claimed.start_claim) == (DispatchState.STARTING, claim)


def test_fence_fechado_por_outra_promocao_recusa_o_permit_do_dispatch_nativo(
        client: Any) -> None:
    _, plane, acquired = _under_closed_fence(client, "b" * 64)
    assert (acquired.status, acquired.permit) == (AcquisitionStatus.FENCE_CLOSED, None)
    assert plane.observe_environment_gate("prod").fence.promotion_id == "b" * 64
