"""Corridas reais do ledger do canary de modo no DynamoDB Local compartilhado."""
from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from threading import Barrier
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast
from uuid import uuid4

import pytest

from cnes_domain.control_plane.environment_gate import PermitHolder
from cnes_domain.orchestration.mode_canary import ModeCanaryBinding, SyntheticContextRequired
from cnes_infra.aws.mode_canary import (
    GENESIS_LEDGER_SHA256,
    ModeCanaryContext,
    canonical_text,
    initialize_control,
)
from cnes_infra.control_plane.dynamodb_keys import tenant_partition
from tests.integration.aws.conftest import _client, _create_table

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

pytestmark = [pytest.mark.dynamodb_local, pytest.mark.s3_integration]

_CANARY_ID = "0123456789abcdef0123456789abcdef"
_TENANT = f"prod-canary-{_CANARY_ID}"
_OWNER = "a" * 64
_RUNS = ("recovery-a-0123456789abcdef", "recovery-b-0123456789abcdef")
_NOW = datetime(2026, 11, 12, 12, 10, tzinfo=UTC)
_BARRIER_SECONDS = 30
_CONTENDERS = 8
_RECORD: dict[str, Any] = {
    "schema": "cnesdata.mode-canary/v2", "activation_sha256": "a" * 64,
    "candidate_sha256": "b" * 64, "source_sha": "d" * 40, "release_id": "r-499",
    "canary_id": _CANARY_ID, "owner_sha256": _OWNER, "promotion_id": "promo-1",
    "promotion_generation": 3, "phase_receipt_sha256": "b" * 64,
    "machine_arn": "arn:aws:states:us-east-1:000000000000:stateMachine:m",
    "revisions": {"unit": "u:1", "recovery": "r:1", "audit": "a:1"}, "run_ids": list(_RUNS),
    "fixture_manifest_sha256": "a" * 64, "seed_manifest_sha256": "b" * 64,
    "fixed_deadline": "2026-11-12T13:00:00Z", "stage_deadline": "2026-11-12T12:30:00Z",
    "canary_deadline": "2026-11-12T12:30:00Z", "phase": "recovery",
    "freeze_policy_sha256": "a" * 64, "expires_at": "2026-11-12T13:00:00Z",
    "context_kind": "promotion", "context_manifest_sha256": "b" * 64,
    "scenario": "recovery_overlap_a", "fault": "none", "mode": "recovery",
}
_CONTROL: dict[str, Any] = {
    "schema": "cnesdata.mode-canary-control/v1", "owner_sha256": _OWNER, "generation": 3,
    "context_kind": "promotion", "context_manifest_sha256": "b" * 64,
    "admission_state": "ACTIVE", "admitted_at": "2026-11-12T12:00:00Z",
    "deadline": "2026-11-12T13:00:00Z", "sealed_at": None,
    "ledger_sha256": GENESIS_LEDGER_SHA256,
    "admitted_counts": {"unit_tasks": 0, "recovery_resumes": 0, "recovery_hangs": 0},
}


@dataclass
class _Canary:
    client: Any
    table: str

    def context(self, dispatches: Any = None) -> ModeCanaryContext:
        digest = sha256(canonical_text(_RECORD).encode()).hexdigest()
        binding = ModeCanaryBinding(_TENANT, _RUNS, _OWNER, digest, "recovery", "promotion")
        return ModeCanaryContext(self.client, self.table, binding, cast("Any", dispatches))

    def payloads(self, prefix: str) -> list[dict[str, Any]]:
        response = self.client.query(
            TableName=self.table, ConsistentRead=True,
            KeyConditionExpression="pk = :pk AND begins_with(sk, :prefix)",
            ExpressionAttributeValues={":pk": {"S": tenant_partition(_TENANT)},
                                       ":prefix": {"S": prefix}},
        )
        return [json.loads(item["payload"]["S"]) for item in response["Items"]]

    def control(self) -> dict[str, Any]:
        return self.payloads("PROD_CANARY_CONTROL#")[0]

    def tickets(self) -> list[dict[str, Any]]:
        return self.payloads(f"PROD_CANARY_EFFECT#{_OWNER}#")


@pytest.fixture
def canary(emulator_endpoints: Any) -> Iterator[_Canary]:
    client = _client("dynamodb", emulator_endpoints.dynamodb)
    table = f"prod499-canary-{uuid4().hex[:12]}"
    _create_table(client, table)
    client.put_item(TableName=table, Item={
        "pk": {"S": tenant_partition(_TENANT)}, "sk": {"S": f"PROD_CANARY#{_OWNER}#recovery"},
        "entity": {"S": "MODECANARY"}, "payload": {"S": canonical_text(_RECORD)},
    })
    initialize_control(client, table, _TENANT, _CONTROL)
    yield _Canary(client, table)
    client.delete_table(TableName=table)


def _race(actions: list[Callable[[], object]]) -> list[object]:
    barrier = Barrier(len(actions), timeout=_BARRIER_SECONDS)

    def run(action: Callable[[], object]) -> object:
        barrier.wait()
        try:
            return action()
        except SyntheticContextRequired as error:
            return error

    with ThreadPoolExecutor(max_workers=len(actions)) as pool:
        return list(pool.map(run, actions))


def _reason(outcome: object) -> str:
    return str(outcome).split()[0] if isinstance(outcome, SyntheticContextRequired) else "ok"


def test_tickets_concorrentes_consomem_contador_exato_sem_ticket_orfao(canary: _Canary) -> None:
    context = canary.context()
    requests = [f"{index:064x}" for index in range(_CONTENDERS)]

    outcomes = _race([
        (lambda request=request: context.admit_effect("run-recovery", request, "c" * 64, _NOW))
        for request in requests
    ])

    reasons = {_reason(outcome) for outcome in outcomes}
    assert reasons <= {"ok", "reason=ledger_contended"}
    admitted = sum(_reason(outcome) == "ok" for outcome in outcomes)
    assert admitted >= 1
    assert len(canary.tickets()) == admitted
    assert canary.control()["admitted_counts"]["recovery_resumes"] == admitted
    for request in requests:
        try:
            context.admit_effect("run-recovery", request, "c" * 64, _NOW)
        except SyntheticContextRequired as error:
            pytest.fail(f"sequential_admission_failed error={error}")
    assert canary.control()["admitted_counts"]["recovery_resumes"] == _CONTENDERS
    with pytest.raises(SyntheticContextRequired, match="reason=budget_exhausted"):
        context.admit_effect("run-recovery", "f" * 64, "c" * 64, _NOW)
    assert len(canary.tickets()) == _CONTENDERS


def test_dois_runs_e_recovery_duplicada_disputam_o_mesmo_dispatch_sem_dobrar(
    canary: _Canary,
) -> None:
    dispatch = SimpleNamespace(dispatch_id="0123456789abcdef", unit_ids=("u1",))
    reader = SimpleNamespace(get_run_dispatch=lambda tenant_id, run_id: dispatch)
    holder = PermitHolder(_TENANT, _RUNS[0], dispatch.dispatch_id)
    contexts = [canary.context(reader) for _ in range(_CONTENDERS)]

    outcomes = _race([
        (lambda context=context: context.binding_for(holder, _NOW)) for context in contexts
    ])

    assert {_reason(outcome) for outcome in outcomes} <= {"ok", "reason=ledger_contended"}
    assert len(canary.tickets()) == 1
    assert canary.control()["admitted_counts"]["unit_tasks"] == 1


def test_seal_concorrente_preserva_ticket_admitido_e_recusa_os_tardios(canary: _Canary) -> None:
    context = canary.context()
    actions: list[Callable[[], object]] = [
        (lambda request=f"{index:064x}": context.admit_effect(
            "native-mutation", request, "c" * 64, _NOW))
        for index in range(_CONTENDERS - 1)
    ]
    actions.append(lambda: context.seal(_NOW))

    outcomes = _race(actions)

    assert _reason(outcomes[-1]) == "ok"
    control = canary.control()
    assert control["admission_state"] == "SEALED"
    admitted = sum(_reason(outcome) == "ok" for outcome in outcomes[:-1])
    assert len(canary.tickets()) == admitted
    assert all(ticket["state"] == "PENDING" for ticket in canary.tickets())
    with pytest.raises(SyntheticContextRequired, match="reason=control_sealed"):
        context.admit_effect("native-mutation", "e" * 64, "c" * 64, _NOW)
