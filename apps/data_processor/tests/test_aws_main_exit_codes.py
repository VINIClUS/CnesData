"""Classificação AWS do main com parser e entrypoint reais."""
from __future__ import annotations

import io
import json
import logging
from contextlib import nullcontext
from typing import TYPE_CHECKING, Any
from unittest.mock import Mock, patch

import pytest
from botocore.exceptions import ClientError

from cnes_infra.aws.settings import AwsRuntimeConfigurationError
from cnes_infra.billing.settings import BillingConfigurationError
from cnes_infra.executor.step_functions import (
    IncompatibleStateMachine,
    ProcessorExecutionUnavailable,
)
from data_processor.aws_composition import AwsRecoveryRuntime, AwsUnitRuntime
from data_processor.aws_entrypoint import EntrypointConfigurationError
from data_processor.orchestration.coordinator import RecoveryFailed

if TYPE_CHECKING:
    from collections.abc import Callable

_DB_URL = "postgresql+psycopg://offline:offline@127.0.0.1:1/offline"
_EXECUTION_ARN = "arn:aws:states:us-east-1:000000000000:execution:test:run"
_UNIT_ENV = {
    "TENANT_ID": "354130",
    "RUN_ID": "run-01",
    "WAVE_ID": "1111111111111111",
    "DISPATCH_ID": "2222222222222222",
    "UNIT_ID": "unit-01",
    "EXECUTION_OWNER": _EXECUTION_ARN,
    "LEASE_SECONDS": "300",
}


@pytest.fixture(autouse=True)
def _guard_deadline(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "data_processor.main.processor_deadline", nullcontext,
    )


def _runtime() -> Any:
    return AwsUnitRuntime(unit_handler=Mock())


def _recovery_runtime() -> Any:
    return AwsRecoveryRuntime(recovery=Mock())


def _clear_envelope(monkeypatch: pytest.MonkeyPatch) -> None:
    names = (
        "TENANT_ID", "RUN_ID", "WAVE_ID", "DISPATCH_ID", "UNIT_ID",
        "EXECUTION_OWNER", "LEASE_SECONDS",
    )
    for name in names:
        monkeypatch.delenv(name, raising=False)


def _set_unit_envelope(monkeypatch: pytest.MonkeyPatch) -> None:
    for name, value in _UNIT_ENV.items():
        monkeypatch.setenv(name, value)


def _set_worker_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PROFILE", "aws")
    values = {
        "AWS_REGION": "us-east-1", "AWS_CONTROL_PLANE_TABLE": "offline-control-plane",
        "AWS_DATA_BUCKET": "offline-data",
        "AWS_STATE_MACHINE_ARN": "arn:aws:states:us-east-1:000000000000:stateMachine:test",
        "AWS_PROCESSOR_CONTAINER_NAME": "processor", "AWS_PROCESSOR_MAX_CONCURRENCY": "8",
        "AWS_PROCESSOR_LEASE_SECONDS": "3600", "AWS_PROCESSOR_RECOVERY_BATCH_SIZE": "100",
        "BILLING_MODE": "disabled", "BILLING_ENFORCEMENT_MODE": "off",
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)
    monkeypatch.delenv("AWS_PRODUCTION_POLICY", raising=False)
    monkeypatch.delenv("AWS_ENVIRONMENT", raising=False)


async def _run_main(
    monkeypatch: pytest.MonkeyPatch,
    argv: tuple[str, ...],
    runtime: Any = None,
    build_error: BaseException | None = None,
) -> tuple[int, list[dict[str, Any]]]:
    import importlib
    import sys

    _set_worker_settings(monkeypatch)
    monkeypatch.setenv("DB_URL", _DB_URL)
    monkeypatch.setattr(sys, "argv", ["data_processor", *argv])
    app = importlib.import_module("data_processor.main")
    from cnes_infra.observability import configure_json_stdout

    output = io.StringIO()
    root = logging.getLogger()
    original_handlers = root.handlers[:]
    build = Mock(return_value=runtime, side_effect=build_error)
    try:
        with (
            patch.object(
                app, "configure_json_stdout",
                side_effect=lambda service: configure_json_stdout(service, output),
            ),
            patch.object(app, "init_telemetry"),
            patch.object(app, "Session"),
            patch.object(
                app, "build_aws_unit_runtime", build,
            ),
            patch.object(
                app, "build_aws_recovery_runtime", build,
            ),
        ):
            rc = await app.main()
        if argv == ("desconhecido",) or (
            argv == ("recover-once",) and runtime is not None
            and isinstance(runtime, AwsUnitRuntime)
        ):
            build.assert_not_called()
        events = [json.loads(line) for line in output.getvalue().splitlines() if line]
    finally:
        for handler in root.handlers[:]:
            handler.close()
        root.handlers[:] = original_handlers
    return rc, events


def _assert_failure(
    events: list[dict[str, Any]], rc: int, expected_rc: int, expected_type: str,
) -> None:
    failures = [event for event in events if event["event"] == "processor_entrypoint_failed"]
    assert rc == expected_rc
    assert len(failures) == 1
    assert failures[0]["exit_code"] == expected_rc
    assert failures[0]["exception_type"] == expected_type


def _client_error(code: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": "offline"}}, "HeadBucket")


def _terminal_cases() -> list[tuple[Callable[[], BaseException], str]]:
    return [
        (lambda: EntrypointConfigurationError("bad"), "EntrypointConfigurationError"),
        (lambda: AwsRuntimeConfigurationError("bad"), "AwsRuntimeConfigurationError"),
        (lambda: IncompatibleStateMachine("bad"), "IncompatibleStateMachine"),
        (lambda: BillingConfigurationError("bad"), "BillingConfigurationError"),
    ]


def _operational_cases() -> list[tuple[Callable[[], BaseException], str]]:
    return [
        (lambda: RuntimeError("aws=down"), "RuntimeError"),
        (
            lambda: ProcessorExecutionUnavailable("StateMachineDoesNotExist"),
            "ProcessorExecutionUnavailable",
        ),
        (lambda: RecoveryFailed((), 1), "RecoveryFailed"),
        (lambda: ValueError("object_lock=disabled"), "ValueError"),
        (lambda: ValueError("state_machine_arn=invalid"), "ValueError"),
        (lambda: _client_error("NoSuchBucket"), "ClientError"),
        (lambda: _client_error("AccessDenied"), "ClientError"),
    ]


@pytest.mark.asyncio
async def test_modo_desconhecido_rejeita_sem_trabalho(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_envelope(monkeypatch)
    runtime = _runtime()
    rc, events = await _run_main(monkeypatch, ("desconhecido",), runtime)

    runtime.unit_handler.handle.assert_not_called()
    assert (rc, events) == (2, [])


@pytest.mark.asyncio
async def test_envelope_parcial_rejeita_sem_trabalho(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_envelope(monkeypatch)
    monkeypatch.setenv("TENANT_ID", "354130")
    runtime = _runtime()
    rc, events = await _run_main(monkeypatch, ("recover-once",), runtime)

    runtime.unit_handler.handle.assert_not_called()
    assert (rc, events) == (2, [])


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["build", "execution"])
@pytest.mark.parametrize(
    ("error_factory", "exception_type"),
    _terminal_cases(),
    ids=["entrypoint", "aws-runtime", "state-machine", "billing"],
)
async def test_configuracao_terminal_classificada_em_build_e_execucao(
    monkeypatch: pytest.MonkeyPatch,
    phase: str,
    error_factory: Callable[[], BaseException],
    exception_type: str,
) -> None:
    _clear_envelope(monkeypatch)
    runtime = _runtime()
    if phase == "build":
        rc, events = await _run_main(
            monkeypatch, ("recover-once",), build_error=error_factory(),
        )
    else:
        _set_unit_envelope(monkeypatch)
        runtime.unit_handler.handle.side_effect = error_factory()
        rc, events = await _run_main(monkeypatch, (), runtime)

    _assert_failure(events, rc, 2, exception_type)


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["build", "execution"])
@pytest.mark.parametrize(
    ("error_factory", "exception_type"),
    _operational_cases(),
    ids=[
        "runtime", "state-machine-unavailable", "recovery", "object-lock",
        "state-machine-arn", "no-such-bucket", "access-denied",
    ],
)
async def test_falha_operacional_classificada_em_build_e_execucao(
    monkeypatch: pytest.MonkeyPatch,
    phase: str,
    error_factory: Callable[[], BaseException],
    exception_type: str,
) -> None:
    _clear_envelope(monkeypatch)
    runtime = _runtime()
    if phase == "build":
        rc, events = await _run_main(
            monkeypatch, ("recover-once",), build_error=error_factory(),
        )
    elif exception_type == "RecoveryFailed":
        runtime = _recovery_runtime()
        runtime.recovery.run_once.side_effect = error_factory()
        rc, events = await _run_main(monkeypatch, ("recover-once",), runtime)
    else:
        _set_unit_envelope(monkeypatch)
        runtime.unit_handler.handle.side_effect = error_factory()
        rc, events = await _run_main(monkeypatch, (), runtime)

    _assert_failure(events, rc, 1, exception_type)
