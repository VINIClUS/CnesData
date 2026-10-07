"""Passada limitada de recovery delegada ao coordinator canônico."""
from __future__ import annotations

import json
import logging
from datetime import UTC, datetime, timedelta
from unittest.mock import Mock

import pytest

from cnes_domain.control_plane.entities import Run, RunDependency, RunDispatch
from cnes_domain.control_plane.enums import DispatchState, RunState
from cnes_domain.ports.control_plane import ControlPlanePort
from cnes_domain.ports.processing import (
    ExecutionCallbacks,
    ExecutionPolicyConfig,
    ProcessorExecutorPort,
)
from cnes_infra.executor.step_functions import ProcessorExecutionUnavailable
from cnes_infra.observability import JsonLogFormatter
from data_processor.orchestration.coordinator import (
    CoordinatorDependencies,
    CoordinatorResult,
    PipelineCoordinator,
    RecoveryFailed,
    allow_execution,
    noop_execution_started,
)
from data_processor.orchestration.publisher import DatasetPublisher
from data_processor.recovery import ProcessorRecovery, RecoveryResult

NOW = datetime(2026, 9, 27, 12, tzinfo=UTC)
ONE_MINUTE = timedelta(minutes=1)
EXECUTION_ARN = "arn:aws:states:us-east-1:000000000000:execution:cnesdata-test:2222222222222222"
EVENTS = (
    "processor_recovery_scanned",
    "processor_execution_observed",
    "processor_execution_probe_failed",
    "processor_recovery_completed",
)


def _run(run_id: str, state: RunState = RunState.PROCESSING) -> Run:
    return Run(
        tenant_id="354130", run_id=run_id, competencia="2026-08", dataset_name="cnes",
        state=state,
        dependencies=(
            RunDependency(source_type="CNES_LOCAL", file_subtype="CNES_VINCULO", required=True),
        ),
        missing_sources=(), created_at=NOW,
    )


def _dispatch(state: DispatchState, lease_until: datetime) -> RunDispatch:
    return RunDispatch(
        tenant_id="354130", run_id="r1", wave_id="1111111111111111",
        dispatch_id="2222222222222222", generation=1, unit_ids=("unit-01",), state=state,
        lease_until=lease_until, execution_ref=EXECUTION_ARN,
    )


def _result(state: RunState = RunState.PROCESSING, *, published: bool = False) -> CoordinatorResult:
    return CoordinatorResult(state=state, execution_ref=EXECUTION_ARN, published=published)


def _recovery(runs: tuple[Run, ...], coordinator: Mock) -> tuple[ProcessorRecovery, Mock]:
    control_plane = Mock(spec=ControlPlanePort)
    control_plane.list_recoverable_runs.return_value = runs
    return ProcessorRecovery(control_plane, coordinator, clock=lambda: NOW), control_plane


def test_recovery_lista_no_instante_injetado_e_delega_ao_coordinator() -> None:
    coordinator = Mock(spec=PipelineCoordinator)
    coordinator.recover.return_value = (_result(), _result())
    recovery, control_plane = _recovery(
        (_run("r1", RunState.PROCESSING), _run("r2", RunState.PUBLISHING)), coordinator,
    )

    assert recovery.run_once(10) == RecoveryResult(scanned=2, recovered=2)

    control_plane.list_recoverable_runs.assert_called_once_with(now=NOW, limit=10)
    coordinator.recover.assert_called_once_with(limit=10)


def test_recovery_propaga_falha_do_coordinator() -> None:
    coordinator = Mock(spec=PipelineCoordinator)
    coordinator.recover.side_effect = ProcessorExecutionUnavailable("ThrottlingException")
    recovery, _ = _recovery((_run("r1"),), coordinator)

    with pytest.raises(ProcessorExecutionUnavailable, match="ThrottlingException"):
        recovery.run_once(10)


def test_recovery_nao_avanca_dispatch_enquanto_lease_ativa() -> None:
    coordinator = Mock(spec=PipelineCoordinator)
    coordinator.recover.return_value = ()
    recovery, control_plane = _recovery((_run("r1"),), coordinator)
    control_plane.get_active_run_dispatch.return_value = _dispatch(
        DispatchState.STARTED, NOW + ONE_MINUTE,
    )

    recovery.run_once(limit=10)

    assert [name for name, *_ in control_plane.method_calls] == ["list_recoverable_runs"]
    control_plane.reserve_run_dispatch.assert_not_called()
    coordinator.recover.assert_called_once_with(limit=10)


@pytest.mark.parametrize("limit", [0, -1, 1001])
def test_recovery_rejeita_limite_fora_da_faixa(limit: int) -> None:
    coordinator = Mock(spec=PipelineCoordinator)
    recovery, control_plane = _recovery((), coordinator)

    with pytest.raises(ValueError, match="limit=invalid"):
        recovery.run_once(limit)

    assert control_plane.method_calls == []
    coordinator.recover.assert_not_called()


@pytest.mark.parametrize("limit", [1, 1000])
def test_recovery_aceita_limites_da_faixa(limit: int) -> None:
    coordinator = Mock(spec=PipelineCoordinator)
    coordinator.recover.return_value = ()
    recovery, _ = _recovery((), coordinator)

    assert recovery.run_once(limit) == RecoveryResult(scanned=0, recovered=0)
    coordinator.recover.assert_called_once_with(limit=limit)


def test_recovery_emite_eventos_json_sem_dados_sensiveis(
    caplog: pytest.LogCaptureFixture,
) -> None:
    coordinator = Mock(spec=PipelineCoordinator)
    coordinator.recover.side_effect = [
        (_result(RunState.PUBLISHED, published=True), _result()),
        ProcessorExecutionUnavailable("ThrottlingException"),
    ]
    recovery, _ = _recovery((_run("r1"), _run("r2", RunState.PUBLISHING)), coordinator)
    caplog.set_level(logging.INFO, logger="data_processor.recovery")

    recovery.run_once(10)
    with pytest.raises(ProcessorExecutionUnavailable):
        recovery.run_once(10)

    formatter = JsonLogFormatter("data-processor")
    lines = [json.loads(formatter.format(record)) for record in caplog.records]
    assert [line["event"] for line in lines] == [
        EVENTS[0], EVENTS[1], EVENTS[1], EVENTS[3], EVENTS[0], EVENTS[2],
    ]
    assert lines[0]["scanned"] == 2
    assert lines[0]["run_states"] == {"PROCESSING": 1, "PUBLISHING": 1}
    assert (lines[1]["run_state"], lines[1]["published"]) == ("PUBLISHED", True)
    assert (lines[3]["scanned"], lines[3]["recovered"], lines[3]["failed"]) == (2, 2, 0)
    assert lines[5]["reason"] == "ProcessorExecutionUnavailable"
    rendered = "\n".join(json.dumps(line) for line in lines)
    for forbidden in ("arn:", "serving/", "raw/", "manifest", "https://", "Bearer"):
        assert forbidden not in rendered


def _recovery_com_run_falho() -> tuple[ProcessorRecovery, Mock]:
    control_plane = Mock(spec=ControlPlanePort)
    control_plane.list_recoverable_runs.return_value = (_run("r1"), _run("r2"))
    published = _run("r2", RunState.PUBLISHED)

    def get_run(tenant_id: str, run_id: str) -> Run:
        if run_id == "r1":
            raise ConnectionError("control_plane=unavailable")
        return published

    control_plane.get_run.side_effect = get_run
    coordinator = PipelineCoordinator(
        CoordinatorDependencies(
            control_plane=control_plane, executor=Mock(spec=ProcessorExecutorPort),
            publisher=Mock(spec=DatasetPublisher), clock=lambda: NOW,
        ),
        ExecutionPolicyConfig(2, 300, ExecutionCallbacks(allow_execution, noop_execution_started)),
    )
    return ProcessorRecovery(control_plane, coordinator, clock=lambda: NOW), control_plane


def test_recovery_falha_a_passada_depois_de_retomar_os_demais_runs(
    caplog: pytest.LogCaptureFixture,
) -> None:
    recovery, control_plane = _recovery_com_run_falho()
    caplog.set_level(logging.INFO, logger="data_processor.recovery")

    with pytest.raises(RecoveryFailed, match="failed=1"):
        recovery.run_once(10)

    resumed = [call.args for call in control_plane.get_run.call_args_list]
    assert resumed == [("354130", "r1"), ("354130", "r2")]
    formatter = JsonLogFormatter("data-processor")
    lines = [
        json.loads(formatter.format(record))
        for record in caplog.records if record.name == "data_processor.recovery"
    ]
    assert [line["event"] for line in lines] == [EVENTS[0], EVENTS[1], EVENTS[3]]
    assert (lines[1]["run_state"], lines[1]["published"]) == ("PUBLISHED", False)
    assert (lines[2]["scanned"], lines[2]["recovered"], lines[2]["failed"]) == (2, 1, 1)
    rendered = "\n".join(json.dumps(line) for line in lines)
    for forbidden in ("arn:", "serving/", "raw/", "manifest", "https://", "Bearer"):
        assert forbidden not in rendered
