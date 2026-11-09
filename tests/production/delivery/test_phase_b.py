"""Boundary tests for fence drain and Phase B route oracles."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from cnes_domain.control_plane.environment_gate import (
    EnvironmentGateState,
    FenceState,
    PermitHolder,
    PromotionFence,
    ReleaseUnitPermit,
    UnitPermit,
    release_permit,
)
from cnes_domain.ports.environment_gate import ExecutionObservation, ExecutionPresence
from scripts.promotion.environment_gate import (
    DrainError,
    acquire_initial_or_recovery,
    close_and_drain,
)
from scripts.promotion.phase_b import review_phase_b_plan
from scripts.promotion.verify_routes import verify_routes

NOW = datetime(2026, 11, 9, 12, 0, tzinfo=UTC)
COMMON = {
    "repository": "VINIClUS/CnesData",
    "release_id": "r1",
    "source_sha": "a" * 40,
    "candidate_sha256": "b" * 64,
    "canary_id": "c" * 32,
    "activation_sha256": "d" * 64,
    "promotion_id": "r1",
    "promotion_generation": 1,
    "caller": "caller",
}


def _member(dispatch_id: str = "0123456789abcdef") -> dict[str, Any]:
    return {
        "dispatch_id": dispatch_id,
        "request_sha256": "a" * 64,
        "generation": 1,
        "initial_state": "ACKNOWLEDGED",
        "execution_ref": "arn:execution",
        "resolution": "TERMINAL",
        "proof_sha256": hashlib.sha256(
            b'{"dispatch_id":"0123456789abcdef","resolution":"TERMINAL",'
            b'"send_disabled_at":"2026-11-09T12:00:01Z"}'
        ).hexdigest(),
        "observed_at": "2026-11-09T12:00:00Z",
        "send_disabled_at": "2026-11-09T12:00:01Z",
    }


class Probe:
    def probe(self, dispatch_id: str) -> ExecutionObservation:
        return ExecutionObservation(ExecutionPresence.TERMINAL, "arn:execution")


class Source:
    def __init__(self, member: dict[str, Any]) -> None:
        self.member = member

    def freeze_membership(self) -> list[dict[str, Any]]:
        return [self.member]

    def proof_bytes(self, dispatch_id: str) -> bytes:
        assert dispatch_id == self.member["dispatch_id"]
        return (
            b'{"dispatch_id":"0123456789abcdef","resolution":"TERMINAL",'
            b'"send_disabled_at":"2026-11-09T12:00:01Z"}'
        )


class Clock:
    def __init__(self) -> None:
        self.value = 0.0

    def monotonic(self) -> float:
        return self.value

    def sleep(self, seconds: float) -> None:
        self.value += seconds


class Gate:
    def __init__(self, permit: Any = None) -> None:
        self.closed = False
        self.permit = permit
        self.close_calls = 0

    def close_promotion_fence(self, command: object) -> bool:
        self.close_calls += 1
        if self.closed:
            return False
        self.closed = True
        return True

    def observe_environment_gate(self, environment: str) -> EnvironmentGateState:
        fence = PromotionFence(environment, FenceState.CLOSED, "r1", 1, NOW)
        return EnvironmentGateState(environment, fence, self.permit)

    def acquire_unit_permit(self, command: Any) -> Any:
        from cnes_domain.control_plane.environment_gate import AcquisitionStatus, PermitAcquisition

        status = (
            AcquisitionStatus.ACQUIRED
            if command.canary_promotion_id
            else AcquisitionStatus.FENCE_CLOSED
        )
        return PermitAcquisition(
            status, self.permit if status is AcquisitionStatus.ACQUIRED else None
        )


def test_acquire_falha_depois_do_fechamento_e_permit_drena() -> None:
    gate = Gate()
    result = close_and_drain(gate, Probe(), Source(_member()), "prod", "r1", NOW, common=COMMON)
    assert result["fence"]["state"] == "CLOSED"
    assert result["permit"] is None
    assert gate.close_calls == 1


def test_drain_vencido_aborta_sem_phase_b() -> None:
    holder = PermitHolder("tenant", "run", "0123456789abcdef")
    permit = UnitPermit("prod", 1, holder, None, NOW, NOW + timedelta(seconds=300), None)
    gate = Gate(permit=permit)
    with pytest.raises(DrainError, match="deadline_exceeded"):
        close_and_drain(
            gate,
            Probe(),
            Source(_member()),
            "prod",
            "r1",
            NOW,
            expirations=[NOW + timedelta(seconds=1)],
            clock=Clock(),
            common=COMMON,
        )
    assert holder.dispatch_id == "0123456789abcdef"


def test_so_o_canary_ligado_adquire() -> None:
    holder = PermitHolder("tenant", "run", "0123456789abcdef")
    permit = UnitPermit("prod", 1, holder, None, NOW, NOW + timedelta(seconds=300), "r1")
    gate = Gate(permit=permit)
    assert not acquire_initial_or_recovery(gate, holder, NOW)
    assert acquire_initial_or_recovery(gate, holder, NOW, "r1")


def test_update_direto_do_semaforo_e_negado() -> None:
    holder = PermitHolder("tenant", "run", "0123456789abcdef")
    permit = UnitPermit("prod", 1, holder, None, NOW, NOW + timedelta(seconds=300), None)
    command = ReleaseUnitPermit("prod", 2, NOW)
    assert not release_permit(permit, command)


def test_membership_sem_prova_original_recusa() -> None:
    with pytest.raises(DrainError, match="common=original_binding_required"):
        close_and_drain(Gate(), Probe(), lambda: [_member()], "prod", "r1", NOW)


def test_phase_b_recusa_mudanca_de_scheduler_e_revisao_nova() -> None:
    plan = {
        "resource_changes": [
            {
                "mode": "managed",
                "address": "module.processing.aws_sfn_state_machine.processor[0]",
                "change": {"actions": ["update"]},
            },
            {
                "mode": "managed",
                "address": "module.schedulers.aws_scheduler_schedule.recovery",
                "change": {"actions": ["update"]},
            },
        ]
    }
    with pytest.raises(ValueError, match="scheduler"):
        review_phase_b_plan(plan, "old")


def test_ativacao_inicial_usa_oraculo_proprio() -> None:
    policy_before = {"Version": "2012-10-17", "Statement": []}
    policy_after = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Action": sorted(
                    {
                        "states:DescribeStateMachine",
                        "states:StartExecution",
                        "states:DescribeExecution",
                        "states:StopExecution",
                    }
                )
            }
        ],
    }
    plan = {
        "resource_changes": [
            {
                "mode": "managed",
                "address": "module.processing.aws_sfn_state_machine.processor[0]",
                "change": {"actions": ["create"]},
            },
            {
                "mode": "managed",
                "address": "module.runtime_iam.aws_iam_role_policy.api[0]",
                "change": {
                    "actions": ["update"],
                    "before": {"policy": policy_before},
                    "after": {"policy": policy_after},
                },
            },
        ]
    }
    assert review_phase_b_plan(plan, None) == "initial"


def test_ativacao_inicial_confere_arn_da_state_machine_nas_tasks_unit_e_recovery() -> None:
    task_definitions = {
        mode: {
            "environment": {"AWS_STATE_MACHINE_ARN": "arn:aws:states:us-east-2:1:stateMachine:x"}
        }
        for mode in ("unit", "recovery")
    }
    routes = {
        "mode": "initial",
        "state_machine_arn": "arn:aws:states:us-east-2:1:stateMachine:x",
        "unit_revision": "unit:2",
        "recovery_revision": "recovery:1",
        "audit_revision": "audit:1",
        "unit_environment_state_machine_arn": "arn:aws:states:us-east-2:1:stateMachine:x",
        "recovery_environment_state_machine_arn": "arn:aws:states:us-east-2:1:stateMachine:x",
    }
    assert (
        verify_routes(
            routes, routes["state_machine_arn"], "unit:2", "recovery:1", "audit:1", task_definitions
        )
        == routes
    )


def test_rota_mista_mantem_fence_fechado() -> None:
    routes = {
        "mode": "established",
        "state_machine_arn": "arn:aws:states:us-east-2:1:stateMachine:x",
        "unit_revision": "unit:2",
        "recovery_revision": "recovery:2",
        "audit_revision": "audit:1",
        "unit_environment_state_machine_arn": "arn:aws:states:us-east-2:1:stateMachine:x",
        "recovery_environment_state_machine_arn": "arn:aws:states:us-east-2:1:stateMachine:x",
    }
    tasks = {
        mode: {"environment": {"AWS_STATE_MACHINE_ARN": routes["state_machine_arn"]}}
        for mode in ("unit", "recovery")
    }
    with pytest.raises(ValueError, match="recovery_revision"):
        verify_routes(routes, routes["state_machine_arn"], "unit:2", "recovery:1", "audit:1", tasks)
