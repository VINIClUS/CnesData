"""Preflight puro e orçamento único do bootstrap AWS."""
from __future__ import annotations

import importlib
import json
import logging
import os
import subprocess
import sys
import time
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any
from unittest.mock import Mock, patch

import pytest

from cnes_domain.ports.processing import RunUnitMessage
from cnes_infra.aws.settings import AwsRuntimeConfigurationError
from data_processor.aws_composition import AwsRecoveryRuntime, AwsUnitRuntime

if TYPE_CHECKING:
    from collections.abc import Mapping

_ROOT = Path(__file__).resolve().parents[3]
_NOW = datetime(2026, 9, 27, 12, tzinfo=UTC)
_VALUES = {
    "PROFILE": "aws", "AWS_REGION": "us-east-1", "AWS_CONTROL_PLANE_TABLE": "offline-table",
    "AWS_DATA_BUCKET": "offline-data", "AWS_PROCESSOR_CONTAINER_NAME": "processor",
    "AWS_STATE_MACHINE_ARN": "arn:aws:states:us-east-1:000000000000:stateMachine:test",
    "AWS_PROCESSOR_MAX_CONCURRENCY": "8", "AWS_PROCESSOR_LEASE_SECONDS": "3600",
    "AWS_PROCESSOR_RECOVERY_BATCH_SIZE": "100", "AWS_PRODUCTION_POLICY": "false",
    "BILLING_MODE": "disabled", "BILLING_ENFORCEMENT_MODE": "off",
}
_UNIT = {
    "TENANT_ID": "354130", "RUN_ID": "run-01", "UNIT_ID": "unit-01",
    "WAVE_ID": "1111111111111111", "DISPATCH_ID": "2222222222222222",
    "EXECUTION_OWNER": "arn:aws:states:us-east-1:000000000000:execution:test:run",
    "LEASE_SECONDS": "300",
}


def _forbidden(label: str, trace: list[Any]) -> Any:
    def invoke(*_args: Any, **_kwargs: Any) -> Any:
        trace.append(label)
        raise AssertionError(f"forbidden={label}")
    return invoke


@pytest.fixture
def probe(monkeypatch: pytest.MonkeyPatch) -> Any:
    with patch.dict(os.environ, {
        "DB_URL": "postgresql+psycopg://offline:offline@127.0.0.1:1/offline",
        "PYTHON_DOTENV_DISABLED": "1",
    }, clear=True):
        app = importlib.import_module("data_processor.main")
    trace: list[Any] = []
    monkeypatch.setattr("data_processor.deadline.threading.Timer", _forbidden("Timer", trace))
    monkeypatch.setattr("data_processor.deadline.DeadlineWatchdog", _forbidden("watchdog", trace))
    monkeypatch.setattr("data_processor.aws_entrypoint._utc_now", lambda: _NOW)
    return app, trace, monkeypatch


def _invalid(case: str) -> tuple[dict[str, str], list[str]]:
    cases: dict[str, tuple[Mapping[str, str | None], list[str]]] = {
        "unknown": ({}, ["desconhecido"]),
        "partial": ({"TENANT_ID": "354130"}, ["recover-once"]),
        "envelope": (_UNIT | {"LEASE_SECONDS": "29"}, []),
        "unit-settings": (_UNIT | {"AWS_DATA_BUCKET": None}, []),
        "recovery-settings": ({"AWS_PROCESSOR_RECOVERY_BATCH_SIZE": "99"}, ["recover-once"]),
        "billing": ({"BILLING_ENFORCEMENT_MODE": "invalid"}, ["recover-once"]),
    }
    changes, args = cases[case]
    return {key: value for key, value in {**_VALUES, **changes}.items() if value is not None}, args


@pytest.mark.asyncio
@pytest.mark.parametrize("case", [
    "unknown", "partial", "envelope", "unit-settings", "recovery-settings", "billing",
])
async def test_preflight_invalido_sai_2_sem_observabilidade_ou_dependencias(
    probe: Any, case: str, capsys: pytest.CaptureFixture[str], caplog: pytest.LogCaptureFixture,
) -> None:
    app, trace, monkeypatch = probe
    names = ("configure_json_stdout", "init_telemetry", "Session", "build_aws_unit_runtime",
             "build_aws_recovery_runtime", "processor_deadline")
    spies = [Mock(side_effect=_forbidden(name, trace)) for name in names]
    for name, spy in zip(names, spies, strict=True):
        monkeypatch.setattr(app, name, spy)
    lookup = Mock(side_effect=_forbidden("failure-logger", trace))
    values, args = _invalid(case)
    with (patch.dict(os.environ, values, clear=True),
          patch.object(sys, "argv", ["data_processor", *args]),
          patch.object(logging, "getLogger", lookup)):
        rc = await app.main()
    assert (rc, trace) == (2, [])
    assert capsys.readouterr() == ("", "")
    assert caplog.records == []
    for spy in [*spies, lookup]:
        spy.assert_not_called()


def _record_deadline(trace: list[Any]) -> Any:
    @contextmanager
    def context(policy: Any) -> Any:
        trace.append(("arm", policy.mode, policy.seconds))
        try:
            yield
        finally:
            trace.append(("disarm", policy.mode, policy.seconds))
    return context


def _prepare(probe: Any, mode: str, outcome: str) -> Any:
    app, trace, monkeypatch = probe
    def boundary(label: Any, result: Any = None) -> Any:
        def invoke(*_args: Any, **_kwargs: Any) -> Any:
            trace.append(label)
            if (label, outcome) in {("Session", "session-error"), ("handle", "work-error"),
                                    (("recover", 100), "work-error")}:
                raise RuntimeError("aws=down")
            if label == "factory" and outcome == "build-config-error":
                raise AwsRuntimeConfigurationError("settings=invalid")
            return result
        return invoke
    work = Mock(side_effect=boundary("handle" if mode == "run-unit" else ("recover", 100)))
    runtime = AwsUnitRuntime(Mock(handle=work)) if mode == "run-unit" else (
        AwsRecoveryRuntime(Mock(run_once=work), 3600))
    for name, label in (("configure_json_stdout", "configure"), ("init_telemetry", "telemetry"),
                        ("Session", "Session")):
        monkeypatch.setattr(app, name, Mock(side_effect=boundary(label)))
    selected = "build_aws_unit_runtime" if mode == "run-unit" else "build_aws_recovery_runtime"
    monkeypatch.setattr(app, selected, Mock(side_effect=boundary("factory", runtime)))
    other = "build_aws_recovery_runtime" if mode == "run-unit" else "build_aws_unit_runtime"
    monkeypatch.setattr(app, other, _forbidden(other, trace))
    monkeypatch.setattr(app, "processor_deadline", _record_deadline(trace))
    return work


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["run-unit", "recover-once"])
@pytest.mark.parametrize("outcome", [
    "success", "session-error", "build-config-error", "work-error"])
async def test_main_arma_uma_vez_antes_do_bootstrap_e_desarma_apos_resultado(
    probe: Any, mode: str, outcome: str,
) -> None:
    app, trace, _ = probe
    work = _prepare(probe, mode, outcome)
    def failure(event: str, *, extra: dict[str, Any]) -> None:
        assert event == "processor_entrypoint_failed"
        assert set(extra) == {"exit_code"}
        trace.append(("failure", extra["exit_code"], type(sys.exception()).__name__))
    logger = Mock(exception=failure)
    values = _VALUES | (_UNIT if mode == "run-unit" else {})
    with (patch.dict(os.environ, values, clear=True),
          patch.object(sys, "argv", ["data_processor", *([] if mode == "run-unit" else [mode])]),
          patch.object(logging, "getLogger", return_value=logger)):
        rc = await app.main()
    seconds = 300 if mode == "run-unit" else 3600
    expected: list[Any] = [("arm", mode, seconds), "configure", "telemetry", "Session"]
    if outcome != "session-error":
        expected.append("factory")
    if outcome in {"success", "work-error"}:
        expected.append("handle" if mode == "run-unit" else ("recover", 100))
    code = 0 if outcome == "success" else (2 if outcome == "build-config-error" else 1)
    if code:
        error_type = "AwsRuntimeConfigurationError" if code == 2 else "RuntimeError"
        expected.append(("failure", code, error_type))
    expected.append(("disarm", mode, seconds))
    assert (rc, trace) == (code, expected)
    if outcome in {"success", "work-error"} and mode == "run-unit":
        work.assert_called_once_with(RunUnitMessage(
            tenant_id="354130", run_id="run-01", unit_id="unit-01", wave_id="1111111111111111",
            dispatch_id="2222222222222222", owner=_UNIT["EXECUTION_OWNER"], now=_NOW,
            lease_seconds=300,
        ))
    elif outcome in {"success", "work-error"}:
        work.assert_called_once_with(limit=100)
    else:
        work.assert_not_called()


_CHILD_GUARD = '''import asyncio, importlib, json, logging, os, socket, sys, threading, time
from pathlib import Path
from unittest.mock import Mock, patch
BASE = Path.cwd()
CASE, MODE = sys.argv[1:]
NOTE_LOCK = threading.Lock()
def note(kind, **values):
    with NOTE_LOCK:
        path, pending = BASE / "evidence.jsonl", BASE / "pending.jsonl"
        old = path.read_text() if path.exists() else ""
        event = json.dumps({"kind": kind, "time": time.monotonic(), **values}) + "\\n"
        pending.write_text(old + event, encoding="utf-8")
        pending.replace(path)
def deny(*args, **kwargs):
    note("network-forbidden")
    raise RuntimeError("network=blocked")
for name in ("create_connection", "getaddrinfo", "gethostbyname", "gethostbyname_ex"):
    setattr(socket, name, deny)
for name in ("connect", "connect_ex", "sendto"):
    setattr(socket.socket, name, deny)
def audit(event, args):
    if event == "open" and isinstance(args[0], (str, bytes)):
        path = Path(os.fsdecode(args[0]))
        if path.name == ".env" and path.absolute() != BASE / ".env":
            note("dotenv-forbidden")
            raise RuntimeError("dotenv=private")
sys.addaudithook(audit)
def block(phase, **values):
    note("ready", phase=phase, **values)
    time.sleep(10)
    (BASE / "continued").touch()
def load_app():
    note("import-entry")
    return importlib.import_module("data_processor.main")
'''
_IMPORT_CHILD = '''import dotenv
loader = dotenv.load_dotenv
def decoy(*args, **kwargs):
    note("dotenv-call")
    return loader(BASE / ".env", override=False)
dotenv.load_dotenv = decoy
try:
    app = load_app()
except Exception as error:
    note("import-result", error=type(error).__name__, marker="CUT2_DECOY_MARKER" in os.environ,
         db="DB_URL" in os.environ, config="cnes_infra.config" in sys.modules)
    raise SystemExit(20)
note("import-result", error=None, marker="CUT2_DECOY_MARKER" in os.environ,
     db="DB_URL" in os.environ, config="cnes_infra.config" in sys.modules)
from contextlib import nullcontext
def no_timer(*args, **kwargs):
    raise AssertionError("timer=forbidden")
def session():
    note("Session")
    raise RuntimeError("aws=down")
with (patch.object(app, "Session", session), patch.object(app, "init_telemetry"),
      patch.object(app, "processor_deadline", nullcontext),
      patch("data_processor.deadline.threading.Timer", no_timer)):
    sys.argv[:] = ["data_processor", "recover-once"]
    note("returned", rc=asyncio.run(app.main()))
'''
_NATIVE_RUNTIME = '''def instrument(deadline):
    arm, disarm = deadline.DeadlineWatchdog.arm, deadline.DeadlineWatchdog.disarm
    def armed(self):
        arm(self)
        note("arm", mode=self.policy.mode, seconds=self.policy.seconds)
    def disarmed(self):
        disarm(self)
        note("disarm")
    deadline.DeadlineWatchdog.arm, deadline.DeadlineWatchdog.disarm = armed, disarmed
class Closing(logging.NullHandler):
    def close(self):
        block("logging-close")
class Stream:
    def write(self, value):
        try:
            event = json.loads(value)
        except ValueError:
            event = {}
        if (CASE == "telemetry-write" and not getattr(self, "blocked", False)
            and isinstance(event, dict) and (event.get("event"), event.get("logger")) == (
                "otel_disabled OTEL_EXPORTER_OTLP_ENDPOINT not set", "cnes_infra.telemetry")):
            self.blocked = True
            block("telemetry-write", event=event["event"], logger=event["logger"],
                  stream=logging.getLogger().handlers[0].stream is sys.stdout)
        if CASE == "diagnostic" and '"event":"processor_entrypoint_failed"' in value:
            event = json.loads(value)
            note("ready", phase="diagnostic", code=event["exit_code"],
                 error=event["exception_type"], stream=logging.getLogger().handlers[0].stream
                 is sys.stdout)
        return sys.__stdout__.write(value)
    def flush(self):
        return sys.__stdout__.flush()
def session():
    note("Session")
    if CASE == "Session":
        block("Session")
    if CASE == "diagnostic":
        raise RuntimeError("aws=down")
    return Mock(spec_set=["client"], client=client)
def client(service, **kwargs):
    assert service == "dynamodb"
    block("client")
def work(*args, **kwargs):
    note("work", limit=kwargs.get("limit"), count=len(args))
    if CASE == "error":
        raise RuntimeError("aws=down")
def fill_pipe():
    import fcntl
    flags = fcntl.fcntl(1, fcntl.F_GETFL)
    fcntl.fcntl(1, fcntl.F_SETFL, flags | os.O_NONBLOCK)
    try:
        while True:
            os.write(1, b"x" * 65536)
    except BlockingIOError as error:
        note("full-pipe", errno=error.errno)
    fcntl.fcntl(1, fcntl.F_SETFL, flags)
'''
_NATIVE_RUN = '''SEAM_ENTERED = False
def native():
    global SEAM_ENTERED
    app = load_app()
    import data_processor.deadline as deadline
    from cnes_infra.aws.processor_settings import AwsRecoverySettings, AwsUnitSettings
    from cnes_infra.billing.settings import BillingSettings
    from data_processor.aws_composition import AwsExecutionPolicy
    from data_processor.aws_composition import AwsRecoveryRuntime, AwsUnitRuntime
    from data_processor.aws_entrypoint import select_aws_invocation
    seam = getattr(app, "_run_validated_aws")
    instrument(deadline)
    settings_type = AwsUnitSettings if MODE == "run-unit" else AwsRecoverySettings
    args = [] if MODE == "run-unit" else ["recover-once"]
    invocation = select_aws_invocation(os.environ, args)
    settings = settings_type.from_mapping(os.environ)
    policy = AwsExecutionPolicy(BillingSettings.from_mapping(os.environ), lambda *args: None)
    app.Session = session
    if CASE != "telemetry-write":
        app.init_telemetry = lambda *args: None
    if CASE == "logging-close":
        logging.getLogger().handlers[:] = [Closing()]
    if CASE in {"telemetry-write", "diagnostic"}:
        sys.stdout = Stream()
    if CASE == "diagnostic":
        fill_pipe()
    if CASE in {"success", "error"}:
        runtime = AwsUnitRuntime(Mock(handle=work)) if MODE == "run-unit" else (
            AwsRecoveryRuntime(Mock(run_once=work), 3600))
        app.build_aws_unit_runtime = app.build_aws_recovery_runtime = lambda *args: runtime
    SEAM_ENTERED = True
    note("seam-entry")
    rc = seam(invocation, settings, policy, deadline.DeadlinePolicy(MODE, 1.0))
    note("returned", rc=rc)
    if CASE not in {"success", "error"}:
        (BASE / "continued").touch()
    time.sleep(2.0)
    note("alive")
try:
    native()
except Exception as error:
    kind = "setup-error" if not SEAM_ENTERED or isinstance(error, NotImplementedError) else (
        "execution-error")
    note(kind, error=type(error).__name__)
    raise SystemExit(20)
'''

def _child_environment(tmp_path: Path, mode: str) -> dict[str, str]:
    sources = ("apps/data_processor", "packages/cnes_contracts", "packages/cnes_domain",
               "packages/cnes_infra")
    for filename in ("credentials", "config"):
        (tmp_path / filename).write_text("", encoding="utf-8")
    return _VALUES | (_UNIT if mode == "run-unit" else {}) | {
        "PYTHONPATH": os.pathsep.join(str(_ROOT / path / "src") for path in sources),
        "PYTHONDONTWRITEBYTECODE": "1", "PYTHONHASHSEED": "0",
        "AWS_EC2_METADATA_DISABLED": "true", "AWS_ACCESS_KEY_ID": "offline",
        "AWS_SECRET_ACCESS_KEY": "offline",
        "AWS_SHARED_CREDENTIALS_FILE": str(tmp_path / "credentials"),
        "AWS_CONFIG_FILE": str(tmp_path / "config"),
    }


def _read_evidence(tmp_path: Path) -> list[dict[str, Any]]:
    path = tmp_path / "evidence.jsonl"
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


def _await_evidence(process: Any, tmp_path: Path) -> list[dict[str, Any]]:
    limit = time.monotonic() + 10.0
    while time.monotonic() < limit:
        events = _read_evidence(tmp_path)
        if any(event["kind"] in {"ready", "returned", "setup-error"} for event in events):
            return events
        if process.poll() is not None:
            return events
        time.sleep(0.01)
    raise AssertionError("setup_only=startup_timeout")


def _run_child(tmp_path: Path, case: str, mode: str = "recover-once") -> dict[str, Any]:
    source = _CHILD_GUARD + (_IMPORT_CHILD if case == "import" else _NATIVE_RUNTIME + _NATIVE_RUN)
    (tmp_path / ".env").write_text("CUT2_DECOY_MARKER=read\n", encoding="utf-8")
    env = _child_environment(tmp_path, mode)
    if case != "import":
        env["PYTHON_DOTENV_DISABLED"] = "1"
    argv = [sys.executable, "-B", "-c", source, case, mode]
    process = subprocess.Popen(  # noqa: S603
        argv, cwd=tmp_path, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    proof: dict[str, Any] = {"case": case, "mode": mode, "env": env, "argv": argv}
    try:
        events = _await_evidence(process, tmp_path)
        deadline = 5.0 if any(event["kind"] == "returned" for event in events) else 2.0
        try:
            process.wait(timeout=deadline)
        except subprocess.TimeoutExpired:
            proof["timeout"] = True
        return proof
    finally:
        proof["killed"] = process.poll() is None
        if proof["killed"]:
            process.kill()
        process.wait(timeout=2.0)
        stdout, stderr = process.communicate(timeout=2.0)
        for stream in (process.stdout, process.stderr):
            if stream is not None:
                stream.close()
        proof.update(rc=process.returncode, events=_read_evidence(tmp_path), reaped=True,
                     closed=all(s is None or s.closed for s in (process.stdout, process.stderr)))
        proof.update(stdout=stdout.decode(), stderr=stderr.decode())
        (tmp_path / "native-result.json").write_text(json.dumps(proof), encoding="utf-8")


def _native_proof(proof: dict[str, Any]) -> list[dict[str, Any]]:
    assert (proof["reaped"], proof["closed"]) == (True, True)
    events = proof["events"]
    setup = [event for event in events if event["kind"] == "setup-error"]
    assert not setup, f"SETUP_ONLY private-seam pending: {setup}"
    assert not any(e["kind"] == "execution-error" for e in events), events
    assert not any(e["kind"] in {"network-forbidden", "dotenv-forbidden"} for e in events)
    assert not proof.get("timeout"), f"UNRESOLVED native timeout: {events}"
    return events


def _assert_blocked(proof: dict[str, Any], tmp_path: Path) -> list[dict[str, Any]]:
    events = _native_proof(proof)
    arms = [event for event in events if event["kind"] == "arm"]
    ready = [event for event in events if event["kind"] == "ready"]
    assert (proof["rc"], proof["killed"]) == (3, False)
    assert len(arms) == len(ready) == 1
    assert (arms[0]["mode"], arms[0]["seconds"]) == ("recover-once", 1.0)
    assert arms[0]["time"] <= ready[0]["time"]
    assert 0 <= time.monotonic() - arms[0]["time"] <= 2.0
    assert not any(e["kind"] in {"disarm", "returned", "alive"} for e in events)
    assert not (tmp_path / "continued").exists()
    return events


def test_import_aws_novo_independe_de_db_url_e_dotenv_privado(tmp_path: Path) -> None:
    proof = _run_child(tmp_path, "import")
    events = proof["events"]
    result = next(event for event in events if event["kind"] == "import-result")
    assert {key: result[key] for key in ("error", "marker", "db", "config")} == {
        "error": None, "marker": False, "db": False, "config": False,
    }
    assert not any(e["kind"] in {"dotenv-call", "dotenv-forbidden", "network-forbidden"}
                   for e in events)
    assert (proof["rc"], proof["reaped"], proof["closed"]) == (0, True, True)
    assert [e["rc"] for e in events if e["kind"] == "returned"] == [1]
    assert sum(e["kind"] == "Session" for e in events) == 1
    failures = [json.loads(line) for line in proof["stdout"].splitlines()]
    assert len(failures) == 1
    assert (failures[0]["event"], failures[0]["exit_code"], failures[0]["exception_type"]) == (
        "processor_entrypoint_failed", 1, "RuntimeError")
    assert proof["stderr"] == ""


@pytest.mark.parametrize("case", ["logging-close", "telemetry-write", "Session", "client"])
def test_bootstrap_bloqueado_encerra_no_deadline_unico(tmp_path: Path, case: str) -> None:
    proof = _run_child(tmp_path, case)
    events = _assert_blocked(proof, tmp_path)
    ready = next(e for e in events if e["kind"] == "ready")
    assert ready["phase"] == case
    if case == "telemetry-write":
        assert (ready["event"], ready["logger"], ready["stream"]) == (
            "otel_disabled OTEL_EXPORTER_OTLP_ENDPOINT not set", "cnes_infra.telemetry", True)


def test_relato_de_falha_bloqueado_permanece_no_deadline(tmp_path: Path) -> None:
    events = _assert_blocked(_run_child(tmp_path, "diagnostic"), tmp_path)
    assert next(e["errno"] for e in events if e["kind"] == "full-pipe") in {11, 35}
    ready = next(e for e in events if e["kind"] == "ready")
    assert (ready["phase"], ready["code"], ready["error"], ready["stream"]) == (
        "diagnostic", 1, "RuntimeError", True)


@pytest.mark.parametrize("mode", ["run-unit", "recover-once"])
@pytest.mark.parametrize("case", ["success", "error"])
def test_retorno_normal_nao_deixa_deadline_fatal_pendente(
    tmp_path: Path, mode: str, case: str,
) -> None:
    proof = _run_child(tmp_path, case, mode)
    events = _native_proof(proof)
    assert (proof["rc"], proof["killed"]) == (0, False)
    assert [e["kind"] for e in events] == [
        "import-entry", "seam-entry", "arm", "Session", "work", "disarm", "returned", "alive",
    ]
    arm, work, returned, alive = (events[index] for index in (2, 4, 6, 7))
    assert (arm["mode"], arm["seconds"]) == (mode, 1.0)
    assert (work["limit"], work["count"]) == ((100, 0) if mode == "recover-once" else (None, 1))
    assert returned["rc"] == (1 if case == "error" else 0)
    assert alive["time"] - returned["time"] >= 2.0
    failures = [json.loads(line) for line in proof["stdout"].splitlines()]
    assert len(failures) == (1 if case == "error" else 0)
    if failures:
        assert (failures[0]["event"], failures[0]["exit_code"], failures[0]["exception_type"]) == (
            "processor_entrypoint_failed", 1, "RuntimeError")
    assert proof["stderr"] == ""
