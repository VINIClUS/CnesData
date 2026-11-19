"""Testes públicos do watchdog e seus oráculos de processo."""
from __future__ import annotations

import errno
import json
import logging
import os
import subprocess
import sys
import textwrap
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

import data_processor.deadline as deadline_module
from data_processor.deadline import DeadlinePolicy, DeadlineWatchdog, processor_deadline

if TYPE_CHECKING:
    from collections.abc import Callable

_ROOT = Path(__file__).resolve().parents[3]
_DEADLINE = 1.0
_TOLERANCE = 1.0
_STARTUP_TIMEOUT = 10.0


def _environment() -> dict[str, str]:
    sources = (
        _ROOT / "apps/data_processor/src",
        _ROOT / "packages/cnes_contracts/src",
        _ROOT / "packages/cnes_domain/src",
        _ROOT / "packages/cnes_infra/src",
    )
    return {
        "PYTHONPATH": os.pathsep.join(str(path) for path in sources),
        "AWS_EC2_METADATA_DISABLED": "true",
        "PYTHONHASHSEED": "0",
    }


def _start_child(source: str, stdout: int | None) -> subprocess.Popen[bytes]:
    return subprocess.Popen(  # noqa: S603
        [sys.executable, "-c", source], cwd=_ROOT, env=_environment(),
        stdout=stdout, stderr=subprocess.DEVNULL,
    )


def _await_marker(process: subprocess.Popen[bytes], marker: Path) -> None:
    limit = time.monotonic() + _STARTUP_TIMEOUT
    while not marker.exists() and time.monotonic() < limit:
        if process.poll() is not None:
            break
        time.sleep(0.01)
    assert marker.exists(), f"marcador ausente: {marker} rc={process.poll()}"


def _await_text_marker(process: subprocess.Popen[bytes], path: Path, marker: str) -> None:
    limit = time.monotonic() + _STARTUP_TIMEOUT
    while time.monotonic() < limit:
        if path.exists() and marker in path.read_text(encoding="utf-8"):
            return
        if process.poll() is not None:
            break
        time.sleep(0.01)
    assert path.exists(), f"marcador ausente: {path} rc={process.poll()}"
    assert marker in path.read_text(encoding="utf-8")


def _wait_deadline(process: subprocess.Popen[bytes]) -> tuple[int, float]:
    started = time.monotonic()
    try:
        result = process.wait(timeout=_DEADLINE + _TOLERANCE)
    except subprocess.TimeoutExpired as error:
        process.kill()
        process.wait()
        raise AssertionError("watchdog não encerrou no orçamento") from error
    return result, time.monotonic() - started


def _cleanup(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is None:
        process.kill()
    process.wait()
    if process.stdout is not None:
        process.stdout.close()


def _socket_guard() -> str:
    source = textwrap.dedent(
        """
        import socket
        def _deny(*_args, **_kwargs):
            raise RuntimeError("network=blocked")
        socket.socket.connect = _deny
        socket.create_connection = _deny
        """
    ).strip()
    lines = source.splitlines()
    return "\n".join([lines[0], *(f"        {line}" for line in lines[1:])])


def _normal_child(ready: Path, continued: Path, finished: Path, exited: Path) -> str:
    return textwrap.dedent(
        f"""
        {_socket_guard()}
        import atexit
        import time
        from pathlib import Path
        from cnes_infra.observability import configure_json_stdout
        from data_processor.deadline import DeadlinePolicy, processor_deadline
        configure_json_stdout("data-processor")
        atexit.register(lambda: Path({str(exited)!r}).write_text("atexit"))
        Path({str(ready)!r}).touch()
        try:
            with processor_deadline(DeadlinePolicy("run-unit", {_DEADLINE})):
                time.sleep(5.0)
                Path({str(continued)!r}).touch()
        finally:
            Path({str(finished)!r}).touch()
        """
    )


def _pipe_child(filled: Path, evidence: Path, continued: Path) -> str:
    return textwrap.dedent(
        f"""
        {_socket_guard()}
        import fcntl
        import json
        import logging
        import os
        import sys
        from pathlib import Path
        from cnes_infra.observability import configure_json_stdout
        from data_processor.deadline import DeadlinePolicy, processor_deadline
        configure_json_stdout("data-processor")
        descriptor = 1
        original = fcntl.fcntl(descriptor, fcntl.F_GETFL)
        fcntl.fcntl(descriptor, fcntl.F_SETFL, original | os.O_NONBLOCK)
        while True:
            try:
                os.write(descriptor, b"x" * 65536)
            except BlockingIOError as error:
                handler = logging.getLogger().handlers[0]
                Path({str(evidence)!r}).write_text(json.dumps({{
                    "errno": error.errno,
                    "stdout_handler": handler.stream is sys.stdout,
                }}), encoding="utf-8")
                break
        fcntl.fcntl(descriptor, fcntl.F_SETFL, original)
        Path({str(filled)!r}).touch()
        with processor_deadline(DeadlinePolicy("run-unit", {_DEADLINE})):
            os.write(descriptor, b"blocked")
            Path({str(continued)!r}).touch()
        """
    )


def _flush_child(flush_marker: Path, continued: Path) -> str:
    return textwrap.dedent(
        f"""
        {_socket_guard()}
        import logging
        import time
        from pathlib import Path
        from cnes_infra.observability import configure_json_stdout
        from data_processor.deadline import DeadlinePolicy, processor_deadline
        class BlockingStream:
            def write(self, value):
                return len(value)
            def flush(self):
                Path({str(flush_marker)!r}).touch()
                time.sleep(5.0)
        configure_json_stdout("data-processor", BlockingStream())
        with processor_deadline(DeadlinePolicy("run-unit", {_DEADLINE})):
            logging.getLogger("boundary").error("trigger")
            Path({str(continued)!r}).touch()
        """
    )


def _success_child(ready: Path, alive: Path) -> str:
    return textwrap.dedent(
        f"""
        {_socket_guard()}
        import time
        from pathlib import Path
        from data_processor.deadline import DeadlinePolicy, processor_deadline
        Path({str(ready)!r}).touch()
        with processor_deadline(DeadlinePolicy("run-unit", {_DEADLINE})):
            pass
        time.sleep({_DEADLINE + 0.2})
        Path({str(alive)!r}).touch()
        """
    )


def _error_child(ready: Path, alive: Path) -> str:
    return textwrap.dedent(
        f"""
        {_socket_guard()}
        import time
        from pathlib import Path
        from data_processor.deadline import DeadlinePolicy, processor_deadline
        Path({str(ready)!r}).touch()
        try:
            with processor_deadline(DeadlinePolicy("run-unit", {_DEADLINE})):
                raise RuntimeError("work=failed")
        except RuntimeError:
            pass
        time.sleep({_DEADLINE + 0.2})
        Path({str(alive)!r}).touch()
        """
    )


def _race_child(trace: Path, ready: Path) -> str:
    return textwrap.dedent(
        f"""
        {_socket_guard()}
        import logging
        import os
        import threading
        import time
        from pathlib import Path
        from cnes_infra.observability import configure_json_stdout
        from data_processor.deadline import DeadlinePolicy, DeadlineWatchdog
        def record(marker):
            with Path({str(trace)!r}).open("a", encoding="utf-8") as stream:
                stream.write(marker + "\\n")
        class BlockingStream:
            def write(self, value):
                record("expiry_emit")
                return len(value)
            def flush(self):
                record("expiry_flush")
                while "disarm_return" not in Path({str(trace)!r}).read_text():
                    time.sleep(0.001)
                time.sleep(5.0)
        configure_json_stdout("data-processor", BlockingStream())
        def hard_exit(code):
            record("fallback")
            os._exit(code)
        watchdog = DeadlineWatchdog(DeadlinePolicy("run-unit", {_DEADLINE}), hard_exit)
        Path({str(ready)!r}).touch()
        Path({str(trace)!r}).touch()
        def disarm_after_expiry():
            while "expiry_flush" not in Path({str(trace)!r}).read_text():
                time.sleep(0.001)
            watchdog.disarm()
            record("disarm_return")
        threading.Thread(target=disarm_after_expiry, daemon=True).start()
        watchdog.arm()
        time.sleep(5.0)
        """
    )


def _captured_timers(
    monkeypatch: pytest.MonkeyPatch, observations: list[str] | None = None,
) -> list[threading.Timer]:
    timers: list[threading.Timer] = []

    def start(timer: threading.Timer) -> None:
        timers.append(timer)
        if observations is not None:
            observations.append("timer-start")

    monkeypatch.setattr(threading.Timer, "start", start)
    return timers


@contextmanager
def _logging_probe(observations: list[str]):
    class TraceStream:
        def write(self, value: str) -> int:
            observations.append("emit")
            return len(value)

        def flush(self) -> None:
            observations.append("flush")

    root = logging.getLogger()
    previous_handlers = root.handlers[:]
    previous_level = root.level
    handler = logging.StreamHandler(TraceStream())
    root.handlers[:] = [handler]
    root.setLevel(logging.INFO)
    try:
        yield
    finally:
        handler.close()
        root.handlers[:] = previous_handlers
        root.setLevel(previous_level)


def test_watchdog_cria_timer_principal_e_fallback_duro(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observations: list[str] = []
    timers = _captured_timers(monkeypatch, observations)
    exits: list[int] = []
    watchdog = DeadlineWatchdog(DeadlinePolicy("run-unit", _DEADLINE), exits.append)

    with _logging_probe(observations):
        watchdog.arm()
        assert len(timers) == 1
        assert timers[0].daemon is True
        assert timers[0].interval == pytest.approx(_DEADLINE - 0.1)
        timers[0].function()

    assert exits == [3]
    assert len(timers) == 2
    assert timers[1].daemon is True
    assert timers[1].interval == pytest.approx(0.1)
    first_io = min(observations.index("emit"), observations.index("flush"))
    assert observations.index("timer-start", 1) < first_io
    exits.clear()
    timers[1].function()
    assert exits == [3]
    watchdog.disarm()


def test_processor_deadline_arma_trabalho_desarma(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    timers = _captured_timers(monkeypatch)
    exits: list[int] = []
    real_watchdog = deadline_module.DeadlineWatchdog

    def factory(policy: DeadlinePolicy) -> DeadlineWatchdog:
        return real_watchdog(policy, exits.append)

    monkeypatch.setattr(deadline_module, "DeadlineWatchdog", factory)
    work: list[str] = []
    with processor_deadline(DeadlinePolicy("run-unit", _DEADLINE)):
        work.append("executed")

    assert work == ["executed"]
    assert exits == []
    assert len(timers) == 1


def test_watchdog_desarma_timer_pendente(monkeypatch: pytest.MonkeyPatch) -> None:
    timers = _captured_timers(monkeypatch)
    cancelled: list[threading.Timer] = []
    monkeypatch.setattr(threading.Timer, "cancel", lambda timer: cancelled.append(timer))
    watchdog = DeadlineWatchdog(DeadlinePolicy("run-unit", _DEADLINE), lambda _: None)

    watchdog.arm()
    assert timers
    assert len(timers) == 1
    watchdog.disarm()

    assert cancelled == timers


def test_deadline_io_normal_emite_evento_unico_e_sai_com_tres(tmp_path: Path) -> None:
    ready = tmp_path / "ready"
    continued = tmp_path / "continued"
    finished = tmp_path / "finished"
    exited = tmp_path / "exited"
    process = _start_child(_normal_child(ready, continued, finished, exited), subprocess.PIPE)
    try:
        _await_marker(process, ready)
        returncode, elapsed = _wait_deadline(process)
        output = process.stdout.read().decode() if process.stdout is not None else ""
        events = [json.loads(line) for line in output.splitlines() if line]
        deadline_events = [
            event for event in events if event["event"] == "processor_deadline_exceeded"
        ]
        assert returncode == 3
        assert elapsed <= _DEADLINE + _TOLERANCE
        assert not continued.exists()
        assert not finished.exists()
        assert not exited.exists()
        assert len(deadline_events) == 1
        assert set(deadline_events[0]) == {
            "timestamp", "level", "service", "logger", "event", "mode",
            "deadline_seconds",
        }
        assert deadline_events[0]["mode"] == "run-unit"
        assert deadline_events[0]["deadline_seconds"] == _DEADLINE
    finally:
        _cleanup(process)


@pytest.mark.skipif(os.name != "posix", reason="pipe POSIX")
def test_deadline_io_stdout_cheio_sai_sem_drenar_antes_do_wait(tmp_path: Path) -> None:
    filled = tmp_path / "filled"
    evidence = tmp_path / "evidence"
    continued = tmp_path / "continued"
    process = _start_child(_pipe_child(filled, evidence, continued), subprocess.PIPE)
    try:
        _await_marker(process, evidence)
        _await_marker(process, filled)
        proof = json.loads(evidence.read_text(encoding="utf-8"))
        assert proof["errno"] in {errno.EAGAIN, errno.EWOULDBLOCK}
        assert proof["stdout_handler"] is True
        returncode, elapsed = _wait_deadline(process)
        assert returncode == 3
        assert elapsed <= _DEADLINE + _TOLERANCE
        assert not continued.exists()
    finally:
        _cleanup(process)


def test_deadline_flush_bloqueado_sai_sem_depender_do_evento(tmp_path: Path) -> None:
    flush_marker = tmp_path / "flush"
    continued = tmp_path / "continued"
    process = _start_child(_flush_child(flush_marker, continued), subprocess.PIPE)
    try:
        _await_marker(process, flush_marker)
        returncode, elapsed = _wait_deadline(process)
        assert returncode == 3
        assert elapsed <= _DEADLINE + _TOLERANCE
        assert not continued.exists()
    finally:
        _cleanup(process)


@pytest.mark.parametrize("child_factory", [_success_child, _error_child], ids=["sucesso", "erro"])
def test_deadline_desarma_e_mantem_processo_vivo(
    tmp_path: Path, child_factory: Callable[[Path, Path], str],
) -> None:
    ready = tmp_path / "ready"
    alive = tmp_path / "alive"
    source = child_factory(ready, alive)
    process = _start_child(source, subprocess.PIPE)
    try:
        _await_marker(process, ready)
        returncode = process.wait(timeout=_DEADLINE + 1.5)
        output = process.stdout.read().decode() if process.stdout is not None else ""
        assert returncode == 0
        assert alive.exists()
        assert "processor_deadline_exceeded" not in output
    finally:
        _cleanup(process)


def test_deadline_expirada_nao_e_revertida_por_disarm(tmp_path: Path) -> None:
    trace = tmp_path / "trace"
    ready = tmp_path / "ready"
    process = _start_child(_race_child(trace, ready), subprocess.PIPE)
    try:
        _await_marker(process, ready)
        _await_text_marker(process, trace, "expiry_flush")
        returncode, elapsed = _wait_deadline(process)
        assert returncode == 3
        assert elapsed <= _DEADLINE + _TOLERANCE
        markers = trace.read_text(encoding="utf-8").splitlines()
        assert markers.index("expiry_emit") < markers.index("expiry_flush")
        assert markers.index("expiry_flush") < markers.index("disarm_return")
        assert markers.index("disarm_return") < markers.index("fallback")
    finally:
        _cleanup(process)
