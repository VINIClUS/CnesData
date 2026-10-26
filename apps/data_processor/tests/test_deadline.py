"""Oráculos de processo para classificação e deadline do entrypoint AWS."""
from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
import time
from pathlib import Path

import pytest

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
    }


_SHORT_MAIN_CHILD = textwrap.dedent(
    """
    import asyncio
    import importlib
    import os
    import socket
    import sys
    from types import SimpleNamespace
    from unittest.mock import Mock, patch

    def deny_network(*_args, **_kwargs):
        raise RuntimeError("network=blocked")

    socket.socket.connect = deny_network
    socket.create_connection = deny_network
    app = importlib.import_module("data_processor.main")
    runtime = SimpleNamespace(
        unit_handler=Mock(),
        services=SimpleNamespace(recovery=Mock(), recovery_batch_size=100),
    )
    case = os.environ["CASE"]
    sys.argv[:] = ["data_processor", "desconhecido" if case == "two" else "recover-once"]
    build_error = RuntimeError("aws=down") if case == "one" else None
    with (
        patch.object(app, "init_telemetry"),
        patch.object(app, "Session"),
        patch.object(
            app, "build_processor_runtime", side_effect=build_error, return_value=runtime,
        ),
        patch.object(app, "run_aws_entrypoint", return_value=0) as entrypoint,
    ):
        if case == "two":
            entrypoint.side_effect = None
            app.run_aws_entrypoint = importlib.import_module(
                "data_processor.aws_entrypoint"
            ).run_aws_entrypoint
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
    if expected == 0:
        assert result.stdout == ""
        return
    events = [json.loads(line) for line in result.stdout.splitlines() if line]
    failures = [event for event in events if event["event"] == "processor_entrypoint_failed"]
    assert len(failures) == 1
    assert failures[0]["exit_code"] == expected


def _blocked_entrypoint_child(ready: Path, sentinel: Path) -> str:
    return textwrap.dedent(
        f"""
        import time
        import socket
        from pathlib import Path
        from types import SimpleNamespace
        from unittest.mock import Mock

        def deny_network(*_args, **_kwargs):
            raise RuntimeError("network=blocked")

        socket.socket.connect = deny_network
        socket.create_connection = deny_network
        from cnes_infra.observability import configure_json_stdout
        from data_processor.aws_entrypoint import run_aws_entrypoint

        class Handler:
            def handle(self, _message):
                Path({str(ready)!r}).write_text("READY", encoding="utf-8")
                time.sleep(31.0)
                Path({str(sentinel)!r}).write_text("continued", encoding="utf-8")

        configure_json_stdout("data-processor")
        runtime = SimpleNamespace(
            unit_handler=Handler(),
            services=SimpleNamespace(recovery=Mock(), recovery_batch_size=100),
        )
        values = {{
            "TENANT_ID": "354130",
            "RUN_ID": "run-01",
            "WAVE_ID": "1111111111111111",
            "DISPATCH_ID": "2222222222222222",
            "UNIT_ID": "unit-01",
            "EXECUTION_OWNER": "arn:aws:states:us-east-1:000000000000:execution:test:run",
            "LEASE_SECONDS": "30",
        }}
        run_aws_entrypoint(runtime, values, ())
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
