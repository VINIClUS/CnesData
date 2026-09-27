"""Três ondas lógicas, replay do mesmo dispatch e redispatch generation+1 no runtime AWS."""
from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from cnes_domain.control_plane.enums import (
    DispatchOutcome,
    DispatchState,
    RunStage,
    RunState,
    RunUnitState,
)
from cnes_domain.control_plane.errors import ControlPlaneErrorCode, FenceRejected
from tests.integration.aws._doubles import BindFailsOnce
from tests.integration.aws._harness import (
    DATASET,
    active_dispatch,
    build_coordinator,
    drive_dispatch_units,
    execution_name,
    latest_dispatch,
    launch_frozen_cnes_run,
    planned_run,
    recorded_start_requests,
    resume_and_active_dispatch,
    run_manifest_puts,
    stages,
    units_of,
)

if TYPE_CHECKING:
    from tests.integration.aws._harness import AwsTestRuntime

pytestmark = [pytest.mark.dynamodb_local, pytest.mark.s3_integration]


def test_pipeline_inicia_exatamente_tres_ondas_em_ordem(aws_runtime: AwsTestRuntime) -> None:
    run = launch_frozen_cnes_run(aws_runtime)
    normalize = active_dispatch(aws_runtime, run)
    drive_dispatch_units(aws_runtime, normalize)
    reconcile = resume_and_active_dispatch(aws_runtime, run)
    drive_dispatch_units(aws_runtime, reconcile)
    materialize = resume_and_active_dispatch(aws_runtime, run)
    drive_dispatch_units(aws_runtime, materialize)
    result = aws_runtime.processor.coordinator.resume(run.tenant_id, run.run_id)

    requests = recorded_start_requests(aws_runtime, run)
    assert tuple(stages(aws_runtime, run, request.unit_ids) for request in requests) == (
        (RunStage.NORMALIZE,), (RunStage.RECONCILE,), (RunStage.MATERIALIZE,),
    )
    assert tuple(request.dispatch_id for request in requests) == (
        normalize.dispatch_id, reconcile.dispatch_id, materialize.dispatch_id,
    )
    assert len({request.wave_id for request in requests}) == 3
    assert result.state is RunState.PUBLISHED
    assert run_manifest_puts(aws_runtime, run) == 1
    pointer = aws_runtime.api.control_plane.get_dataset_pointer(run.tenant_id, DATASET)
    assert pointer is not None
    assert pointer.version_id == run.run_id


def test_terminal_sem_claim_retry_mantem_wave_e_avanca_dispatch(
    aws_runtime: AwsTestRuntime,
) -> None:
    run = launch_frozen_cnes_run(aws_runtime)
    first = active_dispatch(aws_runtime, run)
    assert {unit.state for unit in units_of(aws_runtime, run)} == {RunUnitState.PENDING}
    aws_runtime.step_functions.set_status(first.execution_ref, "FAILED")

    aws_runtime.processor.services.recovery.run_once(limit=10)

    retry = active_dispatch(aws_runtime, run)
    assert (retry.wave_id, retry.unit_ids) == (first.wave_id, first.unit_ids)
    assert retry.generation == first.generation + 1
    assert retry.dispatch_id != first.dispatch_id
    assert execution_name(retry.execution_ref) == retry.dispatch_id
    replay = next(
        request for request in recorded_start_requests(aws_runtime, run)
        if request.dispatch_id == retry.dispatch_id
    )
    assert aws_runtime.processor.executor.start(replay) == retry.execution_ref
    assert aws_runtime.step_functions.stopped == []


def test_bind_dispatch_falha_cancela_ref_e_recovery_cria_geracao_nova(
    aws_runtime: AwsTestRuntime,
) -> None:
    control_plane = BindFailsOnce(
        aws_runtime.processor.control_plane,
        FenceRejected(ControlPlaneErrorCode.DISPATCH_FENCE_REJECTED),
    )
    coordinator = build_coordinator(aws_runtime, control_plane)
    run = planned_run(aws_runtime)

    with pytest.raises(FenceRejected, match="dispatch_fence_rejected"):
        coordinator.resume(run.tenant_id, run.run_id)

    failed = latest_dispatch(aws_runtime, run)
    [(started_ref, _)] = aws_runtime.step_functions.started
    assert control_plane.fired
    assert aws_runtime.step_functions.stopped == [started_ref]
    assert execution_name(started_ref) == failed.dispatch_id
    assert (failed.state, failed.terminal_outcome, failed.execution_ref) == (
        DispatchState.TERMINAL, DispatchOutcome.CANCELED, None,
    )
    aws_runtime.processor.services.recovery.run_once(limit=10)
    recovered = active_dispatch(aws_runtime, run)
    assert (recovered.wave_id, recovered.generation) == (failed.wave_id, failed.generation + 1)
    assert recovered.dispatch_id != failed.dispatch_id
    assert recovered.state is DispatchState.STARTED
    assert recovered.execution_ref not in {None, started_ref}
