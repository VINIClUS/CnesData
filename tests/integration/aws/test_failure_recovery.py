"""Falhas de fronteira do runtime AWS composto: pointer, outbox, outage e runs degradados."""
from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from botocore.exceptions import ClientError, EndpointConnectionError

from central_api.composition import build_runtime
from central_api.services.serving_access import ServingUnavailable
from cnes_domain.control_plane.enums import DispatchState, RunStage, RunState, RunUnitState
from cnes_domain.control_plane.errors import Conflict
from cnes_domain.outbox_dispatcher import DispatchResult, dispatch_once
from cnes_infra.executor.step_functions import (
    IncompatibleStateMachine,
    ProcessorExecutionUnavailable,
)
from data_processor.composition import build_processor_runtime
from tests.integration.aws._harness import (
    DATASET,
    LOCAL,
    NACIONAL,
    RUN_ID,
    TENANT,
    active_dispatch,
    audit_keys,
    break_raw_source,
    drive_dispatch_units,
    drive_run_to_terminal,
    execution_name,
    launch_frozen_cnes_run,
    new_session,
    pending_events,
    prepare_publication,
    principal,
    publish_version,
    put_planned_run,
    read_run_manifest,
    run_manifest_key,
    run_manifest_puts,
    runtime_values,
    seed_membership,
    serving_key,
    serving_request,
    submit_frozen_raw,
    unit_message,
    units_of,
)

if TYPE_CHECKING:
    from tests.integration.aws._harness import AwsTestRuntime, EmulatorResources

pytestmark = [pytest.mark.dynamodb_local, pytest.mark.s3_integration]

_PUBLISHED = "reconciliation.published"


def _pointer_version(runtime: AwsTestRuntime) -> str | None:
    pointer = runtime.api.control_plane.get_dataset_pointer(TENANT, DATASET)
    return None if pointer is None else pointer.version_id


def test_falha_s3_antes_do_cas_preserva_pointer_anterior(aws_runtime: AwsTestRuntime) -> None:
    old = publish_version(aws_runtime, "run-old")
    request = prepare_publication(aws_runtime, "run-new")
    data_bucket = aws_runtime.resources.data_bucket

    with aws_runtime.s3.failing_puts_after(data_bucket, 1):
        with pytest.raises(ClientError, match="ServiceUnavailable"):
            aws_runtime.processor.publisher.publish(request)

    assert aws_runtime.s3.injected_failures == [(data_bucket, run_manifest_key(request.run))]
    assert _pointer_version(aws_runtime) == old.version.version_id
    assert aws_runtime.api.control_plane.get_run(TENANT, "run-new").state is RunState.PUBLISHING
    assert pending_events(aws_runtime) == ((_PUBLISHED, "run-old"),)


def test_falha_audit_apos_cas_reexecuta_outbox_sem_republicar(
    aws_runtime: AwsTestRuntime,
) -> None:
    request = prepare_publication(aws_runtime, "run-new")
    aws_runtime.processor.publisher.publish(request)
    control_plane, sink = aws_runtime.api.control_plane, aws_runtime.api.audit_sink
    with aws_runtime.s3.failing_audit_put_once():
        first = dispatch_once(control_plane, sink, aws_runtime.clock.now())
    assert (pending_events(aws_runtime), audit_keys(aws_runtime)) == (
        ((_PUBLISHED, "run-new"),), (),
    )

    second = dispatch_once(control_plane, sink, aws_runtime.clock.now())

    assert first == DispatchResult(delivered=0, failed=1)
    assert second == DispatchResult(delivered=1, failed=0)
    assert len(aws_runtime.s3.injected_failures) == 1
    assert pending_events(aws_runtime) == ()
    dated = [key for key in audit_keys(aws_runtime) if not key.startswith("audit/.event-id/")]
    assert len(dated) == 1
    assert dated[0].startswith(f"audit/{TENANT}/")
    assert run_manifest_puts(aws_runtime, request.run) == 1
    version = control_plane.get_dataset_version(TENANT, DATASET, _pointer_version(aws_runtime))
    assert version is not None
    assert version.run_id == "run-new"


def test_dynamodb_indisponivel_falha_fechado(
    aws_runtime: AwsTestRuntime, dynamodb_outage_runtime: AwsTestRuntime,
) -> None:
    seed_membership(aws_runtime, TENANT, "user-1")
    run = launch_frozen_cnes_run(aws_runtime)
    dispatch = active_dispatch(aws_runtime, run)
    publication = prepare_publication(aws_runtime, "run-publish")
    outage = dynamodb_outage_runtime
    message = unit_message(dispatch, dispatch.unit_ids[0], outage.clock.now())

    with pytest.raises(EndpointConnectionError):
        outage.api.services.membership_authorizer.authorize(principal("user-1"), TENANT)
    with pytest.raises(EndpointConnectionError):
        outage.processor.unit_handler.handle(message)
    with pytest.raises(EndpointConnectionError):
        outage.processor.publisher.publish(publication)

    assert {(unit.state, unit.attempt) for unit in units_of(aws_runtime, run)} == {
        (RunUnitState.PENDING, 0),
    }
    assert _pointer_version(aws_runtime) is None
    assert aws_runtime.api.control_plane.get_run(TENANT, "run-publish").state is (
        RunState.PUBLISHING
    )


def test_throttling_do_step_functions_antes_do_start_reexecuta_o_mesmo_dispatch(
    aws_runtime: AwsTestRuntime,
) -> None:
    submit_frozen_raw(aws_runtime)
    run = put_planned_run(aws_runtime, RUN_ID)
    aws_runtime.step_functions.throttle_next_start()

    with pytest.raises(ProcessorExecutionUnavailable, match="ThrottlingException"):
        aws_runtime.api.run_planning.launch(run.tenant_id, run.run_id)

    reserved = active_dispatch(aws_runtime, run)
    assert (reserved.state, reserved.execution_ref) == (DispatchState.RESERVED, None)
    assert aws_runtime.step_functions.started == []
    result = aws_runtime.processor.coordinator.resume(run.tenant_id, run.run_id)
    started = active_dispatch(aws_runtime, run)
    assert (started.dispatch_id, started.generation) == (reserved.dispatch_id, reserved.generation)
    assert (started.state, started.execution_ref) == (DispatchState.STARTED, result.execution_ref)
    assert execution_name(started.execution_ref) == reserved.dispatch_id
    assert aws_runtime.step_functions.stopped == []


def test_falha_terminal_de_unidade_de_reconcile_falha_o_run(aws_runtime: AwsTestRuntime) -> None:
    run = launch_frozen_cnes_run(aws_runtime)
    normalize = active_dispatch(aws_runtime, run)
    committed = drive_dispatch_units(aws_runtime, normalize)
    aws_runtime.api.object_store.delete(committed[0].output_manifests[0].manifest_key)

    final = drive_run_to_terminal(aws_runtime, run)

    reconcile = next(
        unit for unit in units_of(aws_runtime, run) if unit.stage is RunStage.RECONCILE
    )
    assert final.state is RunState.FAILED
    assert (reconcile.state, reconcile.attempt) == (RunUnitState.FAILED_FINAL, 3)
    assert _pointer_version(aws_runtime) is None


def test_serving_ausente_nao_assina_e_falha_fechado(aws_runtime: AwsTestRuntime) -> None:
    seed_membership(aws_runtime, TENANT, "user-1")
    publish_version(aws_runtime, "run-a")
    aws_runtime.api.object_store.delete(serving_key(TENANT, "run-a"))
    request = serving_request("user-1", TENANT, "overview.json")

    with pytest.raises(ServingUnavailable, match="serving_object_unavailable"):
        aws_runtime.api.services.serving_access.grant(request, aws_runtime.clock.now())

    assert aws_runtime.s3.presigned == []


def test_cas_de_pointer_concorrente_tem_um_unico_vencedor(aws_runtime: AwsTestRuntime) -> None:
    first = prepare_publication(aws_runtime, "run-a")
    second = prepare_publication(aws_runtime, "run-b")
    assert (first.expected_version_id, second.expected_version_id) == (None, None)

    winner = aws_runtime.processor.publisher.publish(first)
    with pytest.raises(Conflict, match="pointer_version_conflict"):
        aws_runtime.processor.publisher.publish(second)

    assert _pointer_version(aws_runtime) == winner.version.version_id == "run-a"
    assert aws_runtime.api.control_plane.get_run(TENANT, "run-b").state is RunState.PUBLISHING
    assert pending_events(aws_runtime) == ((_PUBLISHED, "run-a"),)


@pytest.mark.parametrize(
    ("kind", "code"),
    [("express", "workflow_must_be_standard"), ("distributed", "map_must_be_inline")],
)
def test_state_machine_express_ou_distributed_falha_fechado(
    aws_resources: EmulatorResources, invalid_state_machines: dict[str, str],
    kind: str, code: str,
) -> None:
    values = runtime_values(aws_resources, {"AWS_STATE_MACHINE_ARN": invalid_state_machines[kind]})

    with pytest.raises(IncompatibleStateMachine, match=code):
        build_processor_runtime("aws", values, new_session(aws_resources))
    with pytest.raises(IncompatibleStateMachine, match=code):
        build_runtime("aws", values, new_session(aws_resources))


def test_source_obrigatoria_falha_termina_o_run_failed(aws_runtime: AwsTestRuntime) -> None:
    run = launch_frozen_cnes_run(aws_runtime)
    break_raw_source(aws_runtime, LOCAL)

    final = drive_run_to_terminal(aws_runtime, run)

    local = next(
        unit for unit in units_of(aws_runtime, run) if unit.source_type == LOCAL.source_type
    )
    assert final.state is RunState.FAILED
    assert (local.state, local.attempt) == (RunUnitState.FAILED_FINAL, 3)
    assert _pointer_version(aws_runtime) is None
    assert run_manifest_puts(aws_runtime, run) == 0


def test_source_opcional_falha_publica_degradado_com_missing_sources(
    aws_runtime: AwsTestRuntime,
) -> None:
    run = launch_frozen_cnes_run(aws_runtime)
    break_raw_source(aws_runtime, NACIONAL)

    final = drive_run_to_terminal(aws_runtime, run)

    nacional = next(
        unit for unit in units_of(aws_runtime, run) if unit.source_type == NACIONAL.source_type
    )
    assert final.state is RunState.PUBLISHED_DEGRADED
    assert final.missing_sources == (NACIONAL.dependency_key,)
    assert (nacional.state, nacional.attempt) == (RunUnitState.SUCCEEDED_DEGRADED, 3)
    assert _pointer_version(aws_runtime) == run.run_id
    assert read_run_manifest(aws_runtime, run).missing_sources == (NACIONAL.dependency_key,)
