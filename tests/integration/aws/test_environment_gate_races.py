"""Corridas do gate de ambiente (fence de promoção + semáforo) no DynamoDB Local compartilhado."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from threading import Barrier
from typing import TYPE_CHECKING, Any, cast
from uuid import uuid4

import pytest

from cnes_domain.control_plane.environment_gate import (
    AcquireUnitPermit,
    AcquisitionStatus,
    ClosePromotionFence,
    FenceState,
    PermitAcquisition,
    PermitHolder,
    ReanchorUnitPermit,
    ReleaseUnitPermit,
    UnitPermit,
)
from cnes_domain.orchestration.dispatch_admission import EnvironmentAdmission
from cnes_domain.ports.environment_gate import (
    DeferralReason,
    DispatchDeferred,
    ExecutionObservation,
    ExecutionPresence,
    UnitAdmission,
)
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from tests.integration.aws.conftest import _client, _create_table

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

pytestmark = [pytest.mark.dynamodb_local, pytest.mark.s3_integration]

NOW = datetime(2026, 10, 11, 12, tzinfo=UTC)
LATE = NOW + timedelta(minutes=30)
PROMOTION = "release-race"
_ROUNDS = 12
_CONTENDERS = 8
_BARRIER_SECONDS = 30


@pytest.fixture
def gate(emulator_endpoints: Any) -> Iterator[DynamoDBControlPlane]:
    client = _client("dynamodb", emulator_endpoints.dynamodb)
    table_name = f"prod007-gate-{uuid4().hex[:12]}"
    _create_table(client, table_name)
    yield DynamoDBControlPlane(client, table_name, lambda: NOW)
    client.delete_table(TableName=table_name)


def _holder(index: int) -> PermitHolder:
    return PermitHolder("354130", f"run-{index:02d}", f"{index:016x}")


def _acquire(environment: str, index: int) -> AcquireUnitPermit:
    return AcquireUnitPermit(environment, _holder(index), NOW, 300)


def _race(*actions: Callable[[], object]) -> list[object]:
    barrier = Barrier(len(actions), timeout=_BARRIER_SECONDS)

    def run(action: Callable[[], object]) -> object:
        barrier.wait()
        return action()

    with ThreadPoolExecutor(max_workers=len(actions)) as pool:
        futures = [pool.submit(run, action) for action in actions]
        return [future.result() for future in futures]


def _acquire_against_close(
    gate: DynamoDBControlPlane, environment: str,
) -> tuple[PermitAcquisition, bool]:
    close = ClosePromotionFence(environment, PROMOTION, NOW)
    acquisition, closed = _race(
        lambda: gate.acquire_unit_permit(_acquire(environment, 1)),
        lambda: gate.close_promotion_fence(close),
    )
    return cast("PermitAcquisition", acquisition), cast("bool", closed)


class _AbsentProbe:
    def probe(self, dispatch_id: str) -> ExecutionObservation:
        return ExecutionObservation(ExecutionPresence.ABSENT, None)


def _reanchor_against_absence_release(
    gate: DynamoDBControlPlane, environment: str,
) -> tuple[UnitPermit, bool, bool]:
    permit = cast("UnitPermit", gate.acquire_unit_permit(_acquire(environment, 1)).permit)
    reanchor = ReanchorUnitPermit(environment, permit.token, LATE)
    release = ReleaseUnitPermit(environment, permit.token, LATE, permit.acquired_at)
    reanchored, released = _race(
        lambda: gate.reanchor_unit_permit(reanchor), lambda: gate.release_unit_permit(release),
    )
    return permit, cast("bool", reanchored), cast("bool", released)


def _late_reentry_against_rival(gate: DynamoDBControlPlane, environment: str) -> list[object]:
    gate.acquire_unit_permit(_acquire(environment, 1))
    admission = EnvironmentAdmission(gate, _AbsentProbe(), environment, 300)
    return _race(
        lambda: admission.admit(_holder(1), LATE), lambda: admission.admit(_holder(2), LATE),
    )


def test_acquires_concorrentes_tem_no_maximo_um_vencedor(gate: DynamoDBControlPlane) -> None:
    for round_index in range(_ROUNDS):
        environment = f"race-acquire-{round_index}"
        commands = [_acquire(environment, index) for index in range(1, _CONTENDERS + 1)]

        results = _race(*(lambda command=command: gate.acquire_unit_permit(command)
                          for command in commands))

        acquisitions = [cast("PermitAcquisition", result) for result in results]
        winners = [item for item in acquisitions if item.status is AcquisitionStatus.ACQUIRED]
        holder = gate.observe_environment_gate(environment).permit
        assert len(winners) == 1
        assert holder is not None
        assert holder == winners[0].permit
        assert holder.token == 1
        losers = [item for item in acquisitions if item is not winners[0]]
        assert losers == [PermitAcquisition(AcquisitionStatus.HELD, holder)] * (_CONTENDERS - 1)


def test_fence_em_corrida_com_acquire_nunca_libera_start_sem_bind(
    gate: DynamoDBControlPlane,
) -> None:
    for round_index in range(_ROUNDS):
        environment = f"race-fence-{round_index}"

        acquisition, closed = _acquire_against_close(gate, environment)

        drain = gate.observe_environment_gate(environment)
        late = gate.acquire_unit_permit(_acquire(environment, 2))
        assert closed is True
        assert drain.fence.state is FenceState.CLOSED
        assert late == PermitAcquisition(AcquisitionStatus.FENCE_CLOSED, drain.permit)
        if acquisition.status is AcquisitionStatus.ACQUIRED:
            assert drain.permit == acquisition.permit
            assert drain.permit is not None
            assert drain.permit.execution_ref is None
        else:
            assert acquisition == PermitAcquisition(AcquisitionStatus.FENCE_CLOSED, None)
            assert drain.permit is None


def test_reanchor_em_corrida_com_liberacao_por_ausencia_tem_um_vencedor(
    gate: DynamoDBControlPlane,
) -> None:
    for round_index in range(_ROUNDS):
        environment = f"race-reanchor-{round_index}"

        permit, reanchored, released = _reanchor_against_absence_release(gate, environment)

        current = gate.observe_environment_gate(environment).permit
        assert reanchored is not released
        assert current == (replace(permit, acquired_at=LATE) if reanchored else None)


def test_reentrada_tardia_em_corrida_com_rival_admite_no_maximo_um(
    gate: DynamoDBControlPlane,
) -> None:
    for round_index in range(_ROUNDS):
        environment = f"race-reentry-{round_index}"

        results = _late_reentry_against_rival(gate, environment)

        admitted = [result for result in results if isinstance(result, UnitAdmission)]
        deferred = [result for result in results if not isinstance(result, UnitAdmission)]
        current = gate.observe_environment_gate(environment).permit
        assert len(admitted) == 1, results
        assert deferred == [DispatchDeferred(DeferralReason.PERMIT_HELD)]
        assert current is not None
        assert current.token == admitted[0].token
        assert current.holder == _holder(results.index(admitted[0]) + 1)
