"""Oráculos de processo para classificação e deadline do entrypoint AWS."""
from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, cast
from unittest.mock import Mock, patch

import pytest

from data_processor.aws_composition import AwsRecoveryRuntime, AwsUnitRuntime
from data_processor.deadline import DeadlinePolicy

from .test_aws_entrypoint import ENVELOPE, EXPECTED_MESSAGE, NOW

_ROOT = Path(__file__).resolve().parents[3]
_LEASE_SECONDS = 30
_SCHEDULING_TOLERANCE = 1.0
_STARTUP_TIMEOUT = 10.0


def _child_environment(case: str) -> dict[str, str]:
    source_paths = [
        _ROOT / "apps" / "data_processor" / "src",
        _ROOT / "packages" / "cnes_contracts" / "src",
        _ROOT / "packages" / "cnes_domain" / "src",
        _ROOT / "packages" / "cnes_infra" / "src",
    ]
    return {
        "CASE": case,
        "DB_URL": "postgresql+psycopg://offline:offline@127.0.0.1:1/offline",
        "PROFILE": "aws",
        "PYTHONPATH": os.pathsep.join(str(path) for path in source_paths),
        "AWS_EC2_METADATA_DISABLED": "true",
        "AWS_ACCESS_KEY_ID": "offline", "AWS_SECRET_ACCESS_KEY": "offline",
        "PYTHON_DOTENV_DISABLED": "1",
        "AWS_REGION": "us-east-1", "AWS_CONTROL_PLANE_TABLE": "offline-control-plane",
        "AWS_DATA_BUCKET": "offline-data",
        "AWS_STATE_MACHINE_ARN": "arn:aws:states:us-east-1:000000000000:stateMachine:test",
        "AWS_PROCESSOR_CONTAINER_NAME": "processor", "AWS_PROCESSOR_MAX_CONCURRENCY": "8",
        "AWS_PROCESSOR_LEASE_SECONDS": "3600", "AWS_PROCESSOR_RECOVERY_BATCH_SIZE": "100",
        "BILLING_MODE": "disabled", "BILLING_ENFORCEMENT_MODE": "off",
    }


_SHORT_MAIN_CHILD = textwrap.dedent(
    """
    import asyncio
    import importlib
    import os
    import socket
    import sys
    from unittest.mock import Mock, patch

    def deny_network(*_args, **_kwargs):
        raise RuntimeError("network=blocked")

    socket.socket.connect = deny_network
    socket.create_connection = deny_network
    app = importlib.import_module("data_processor.main")
    from data_processor.aws_composition import AwsRecoveryRuntime
    runtime = AwsRecoveryRuntime(recovery=Mock(), processor_lease_seconds=3600)
    case = os.environ["CASE"]
    sys.argv[:] = ["data_processor", "desconhecido" if case == "two" else "recover-once"]
    build_error = RuntimeError("aws=down") if case == "one" else None
    with (
        patch.object(app, "init_telemetry"),
        patch.object(app, "Session"),
        patch.object(
            app, "build_aws_recovery_runtime", side_effect=build_error, return_value=runtime,
        ),
    ):
        raise SystemExit(asyncio.run(app.main()))
    """
)


def _run_short_main(case: str) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(  # noqa: S603
            [sys.executable, "-c", _SHORT_MAIN_CHILD],
            cwd=_ROOT,
            env=_child_environment(case),
            capture_output=True,
            text=True,
            check=False,
            timeout=10.0,
        )
    except subprocess.TimeoutExpired as error:
        stdout_size = len(error.stdout or "")
        stderr_size = len(error.stderr or "")
        raise AssertionError(
            f"subprocess curto excedeu timeout stdout={stdout_size} stderr={stderr_size}"
        ) from error


@pytest.mark.parametrize(("case", "expected"), [("zero", 0), ("one", 1), ("two", 2)])
def test_main_em_subprocesso_converte_retorno_em_exitcode(case: str, expected: int) -> None:
    result = _run_short_main(case)

    assert result.returncode == expected
    if expected in (0, 2):
        assert result.stdout == ""
        assert result.stderr == ""
        return
    events = [json.loads(line) for line in result.stdout.splitlines() if line]
    failures = [event for event in events if event["event"] == "processor_entrypoint_failed"]
    assert len(failures) == 1
    assert failures[0]["exit_code"] == expected
    assert failures[0]["exception_type"] == "RuntimeError"


def _blocked_entrypoint_child(ready: Path, sentinel: Path) -> str:
    return textwrap.dedent(
        f"""
        import time
        import socket
        from pathlib import Path
        from unittest.mock import patch

        def deny_network(*_args, **_kwargs):
            raise RuntimeError("network=blocked")

        socket.socket.connect = deny_network
        socket.create_connection = deny_network
        import asyncio
        import importlib
        import os
        import sys
        from data_processor.aws_composition import AwsUnitRuntime

        class Handler:
            def handle(self, _message):
                Path({str(ready)!r}).write_text("READY", encoding="utf-8")
                time.sleep(31.0)
                Path({str(sentinel)!r}).write_text("continued", encoding="utf-8")

        runtime = AwsUnitRuntime(unit_handler=Handler())
        values = {{
            "TENANT_ID": "354130",
            "RUN_ID": "run-01",
            "WAVE_ID": "1111111111111111",
            "DISPATCH_ID": "2222222222222222",
            "UNIT_ID": "unit-01",
            "EXECUTION_OWNER": "arn:aws:states:us-east-1:000000000000:execution:test:run",
            "LEASE_SECONDS": "30",
        }}
        os.environ.update(values)
        sys.argv[:] = ["data_processor"]
        app = importlib.import_module("data_processor.main")
        with (
            patch.object(app, "init_telemetry"),
            patch.object(app, "Session"),
            patch.object(app, "build_aws_unit_runtime", return_value=runtime),
        ):
            raise SystemExit(asyncio.run(app.main()))
        """
    )


def test_deadline_cancela_e_sai_nao_zero(tmp_path: Path) -> None:
    ready = tmp_path / "ready"
    sentinel = tmp_path / "continued"
    child = _blocked_entrypoint_child(ready, sentinel)
    environment = _child_environment("blocked")
    process = subprocess.Popen(  # noqa: S603
        [sys.executable, "-c", child],
        cwd=_ROOT,
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    try:
        startup_deadline = time.monotonic() + _STARTUP_TIMEOUT
        while not ready.exists() and time.monotonic() < startup_deadline:
            if process.poll() is not None:
                break
            time.sleep(0.02)
        assert ready.exists(), "handler não alcançou READY antes do timeout de startup"
        started = time.monotonic()
        timeout = _LEASE_SECONDS + _SCHEDULING_TOLERANCE
        try:
            returncode = process.wait(timeout=timeout)
        except subprocess.TimeoutExpired as error:
            process.kill()
            process.wait()
            pytest.fail(f"deadline não encerrou o filho em {timeout:.1f}s: {error}")
        elapsed = time.monotonic() - started
        output = process.stdout.read() if process.stdout is not None else ""
        events = [json.loads(line) for line in output.splitlines() if line]
        deadline_events = [
            event for event in events if event["event"] == "processor_deadline_exceeded"
        ]
        assert returncode == 3
        assert elapsed <= timeout
        assert not sentinel.exists()
        assert len(deadline_events) == 1
        assert deadline_events[0]["mode"] == "run-unit"
        assert deadline_events[0]["deadline_seconds"] == _LEASE_SECONDS
        assert set(deadline_events[0]) == {
            "timestamp", "level", "service", "logger", "event", "mode", "deadline_seconds",
        }
    finally:
        if process.poll() is None:
            process.kill()
        process.wait()
        if process.stdout is not None:
            process.stdout.close()


def _record_deadline(monkeypatch: pytest.MonkeyPatch, calls: list[object]) -> None:
    @contextmanager
    def fake_deadline(policy: DeadlinePolicy):
        calls.append(("arm", policy))
        try:
            yield
        finally:
            calls.append(("disarm", policy))

    monkeypatch.setattr("data_processor.main.processor_deadline", fake_deadline)
    monkeypatch.setattr(
        "data_processor.deadline.DeadlineWatchdog",
        Mock(side_effect=AssertionError("deadline=duplicate")),
    )


def _execute_profile(monkeypatch: pytest.MonkeyPatch, runtime: Any) -> int:
    from data_processor import main as app

    values = _child_environment("in-process")
    unit = isinstance(runtime, AwsUnitRuntime)
    if unit:
        values.update(ENVELOPE)
    with (
        patch.dict(os.environ, values, clear=True),
        patch.object(
            sys, "argv", ["data_processor"] if unit else ["data_processor", "recover-once"],
        ),
        patch.object(app, "configure_json_stdout"),
        patch.object(app, "init_telemetry"),
        patch.object(app, "Session"),
        patch.object(app, "build_aws_unit_runtime", return_value=runtime),
        patch.object(app, "build_aws_recovery_runtime", return_value=runtime),
    ):
        monkeypatch.setattr("data_processor.aws_entrypoint._utc_now", lambda: NOW)
        return app._run_aws_profile()


def test_unit_arma_deadline_do_envelope_antes_do_handler(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[Any] = []
    _record_deadline(monkeypatch, calls)
    runtime = AwsUnitRuntime(unit_handler=Mock())
    cast("Any", runtime.unit_handler).handle.side_effect = (
        lambda _: calls.append("handle")
    )

    assert _execute_profile(monkeypatch, runtime) == 0

    assert calls == [
        ("arm", DeadlinePolicy("run-unit", 300)), "handle",
        ("disarm", DeadlinePolicy("run-unit", 300)),
    ]
    cast("Any", runtime.unit_handler).handle.assert_called_once_with(EXPECTED_MESSAGE)


def test_recover_arma_deadline_do_lease_da_composicao_antes_da_passada(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[Any] = []
    _record_deadline(monkeypatch, calls)
    runtime = AwsRecoveryRuntime(recovery=Mock(), processor_lease_seconds=3600)
    cast("Any", runtime.recovery).run_once.side_effect = (
        lambda *, limit: calls.append(("recovery", limit))
    )

    assert _execute_profile(monkeypatch, runtime) == 0

    assert calls == [
        ("arm", DeadlinePolicy("recover-once", 3600)), ("recovery", 100),
        ("disarm", DeadlinePolicy("recover-once", 3600)),
    ]


@pytest.mark.parametrize("raises", [False, True], ids=["sucesso", "erro"])
def test_entrypoint_desarma_deadline_apos_trabalho(
    monkeypatch: pytest.MonkeyPatch, raises: bool,
) -> None:
    calls: list[Any] = []
    _record_deadline(monkeypatch, calls)
    runtime = AwsUnitRuntime(unit_handler=Mock())
    if raises:
        cast("Any", runtime.unit_handler).handle.side_effect = RuntimeError("work=failed")

    assert _execute_profile(monkeypatch, runtime) == (1 if raises else 0)

    assert calls == [
        ("arm", DeadlinePolicy("run-unit", 300)),
        ("disarm", DeadlinePolicy("run-unit", 300)),
    ]
    cast("Any", runtime.unit_handler).handle.assert_called_once_with(EXPECTED_MESSAGE)
