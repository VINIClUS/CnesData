"""Fronteiras de processo para exercitar o gate Bash com relógio proporcional."""

import ctypes
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

TIME_SCALE = 0.02
_EXTERNAL_DEADLINE = 25.0

_UV = """import json, os, shutil, signal, sys, time
from pathlib import Path
args = sys.argv[1:]
phase = "startup"
def emit(event, **extra):
    with open(os.environ["GATE_EVENTS"], "a") as output:
        output.write(json.dumps({"phase": phase, "event": event,
                                 "observed_monotonic": time.monotonic(), **extra}) + "\\n")
    print(f"boundary_phase={phase} boundary_event={event}", flush=True)
def interrupted(number, frame):
    emit("int", signal=number)
    raise SystemExit(130)
signal.signal(signal.SIGINT, interrupted)
if "scripts/gen_openapi.py" in args:
    phase = "openapi"
    output = Path(args[args.index("--output") + 1])
    shutil.copyfile("docs/contracts/openapi.json", output)
    emit("complete", output=str(output), argv=args)
elif "scripts/gen_contracts.py" in args:
    phase = "contracts"
    output = Path(args[args.index("--output") + 1])
    shutil.copytree("docs/contracts/schemas", output, dirs_exist_ok=True)
    emit("complete", output=str(output), argv=args)
elif "ruff" in args or "pyright" in args or "alembic" in args:
    phase = "migration" if "alembic" in args else ("ruff" if "ruff" in args else "pyright")
    emit("complete", argv=args)
elif "pytest" in args:
    phase = "packages" if "packages/cnes_domain" in args else "apps"
    emit("start", argv=args)
    case = os.environ["GATE_CASE"]
    if case == phase + "_hang":
        while True:
            time.sleep(60)
    if phase == "packages" and case == "packages_fail":
        emit("failure")
        raise SystemExit(7)
    if phase == "packages" and case == "finite":
        time.sleep(520 * float(os.environ["GATE_SCALE"]))
    if phase == "packages" and case == "finite_tail":
        time.sleep(10 * float(os.environ["GATE_SCALE"]))
        emit("work")
        time.sleep(520 * float(os.environ["GATE_SCALE"]))
        emit("summary")
    reports = [value.removeprefix("--cov-report=xml:") for value in args
               if value.startswith("--cov-report=xml:")]
    for report in reports:
        Path(report).write_text('<coverage synthetic="true"/>\\n')
    if phase == "packages" and case == "finite_tail":
        emit("coverage", reports=reports)
        emit("tail")
        time.sleep(130 * float(os.environ["GATE_SCALE"]))
        emit("process_exit")
    emit("complete", reports=reports)
else:
    emit("unknown", argv=args)
    raise SystemExit(92)
"""

_TIMEOUT = """import json, os, sys
args = sys.argv[1:]
index = next(index for index, value in enumerate(args) if value.replace(".", "", 1).isdigit())
scaled = list(args)
scaled[index] = str(float(args[index]) * float(os.environ["GATE_SCALE"]))
with open(os.environ["GATE_TIMEOUTS"], "a") as output:
    output.write(json.dumps({"original_argv": args, "scaled_argv": scaled}) + "\\n")
os.execv(os.environ["REAL_GNU_TIMEOUT"], [os.environ["REAL_GNU_TIMEOUT"], *scaled])
"""


@dataclass(frozen=True)
class GateRun:
    returncode: int
    deadline_expired: bool
    elapsed: float
    events: tuple[str, ...]
    reports: tuple[str, ...]
    temporary_outputs: tuple[str, ...]
    surviving_pids: tuple[int, ...]
    stdout: str
    pytest_argv: tuple[tuple[str, ...], ...]
    timeout_argv: tuple[tuple[str, ...], ...]
    wall_bounds: tuple[float, float]
    phase_durations: tuple[tuple[str, float], ...]


def _session_members(session: int) -> dict[int, int]:
    members = {}
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            fields = (entry / "stat").read_text().rsplit(")", 1)[1].split()
            if int(fields[3]) == session:
                members[int(entry.name)] = int(fields[2])
        except (FileNotFoundError, ProcessLookupError):
            continue
    return members


@contextmanager
def _adopt_orphans() -> Iterator[None]:
    if not Path("/proc/self/stat").is_file():
        raise RuntimeError("prerequisite=linux_process_sessions")
    libc = ctypes.CDLL(None, use_errno=True)
    previous = ctypes.c_int()
    if libc.prctl(37, ctypes.byref(previous), 0, 0, 0) != 0:
        raise OSError(ctypes.get_errno(), "prerequisite=subreaper_query")
    if libc.prctl(36, 1, 0, 0, 0) != 0:
        raise OSError(ctypes.get_errno(), "prerequisite=subreaper_enable")
    try:
        yield
    finally:
        if libc.prctl(36, previous.value, 0, 0, 0) != 0:
            raise OSError(ctypes.get_errno(), "subreaper_restore")


def _cleanup_session(process: subprocess.Popen[str]) -> tuple[int, ...]:
    started = time.monotonic()
    while time.monotonic() - started < 3:
        process.poll()
        members = _session_members(process.pid)
        if not members:
            return ()
        termination = signal.SIGTERM if time.monotonic() - started < 0.3 else signal.SIGKILL
        for group in set(members.values()):
            try:
                os.killpg(group, termination)
            except ProcessLookupError:
                pass
        for pid in members.keys() - {process.pid}:
            try:
                os.waitpid(pid, os.WNOHANG)
            except ChildProcessError:
                pass
        time.sleep(0.01)
    return tuple(_session_members(process.pid))


def _tools() -> tuple[str, str, dict[str, str]]:
    bash = shutil.which("bash")
    if bash is None:
        raise RuntimeError("prerequisite=bash")
    versions = {
        "bash": subprocess.check_output(  # noqa: S603 - discovered local tool
            [bash, "--version"],
            text=True,
        ).splitlines()[0]
    }
    for name in ("timeout", "gtimeout", "gnutimeout"):
        timeout = shutil.which(name)
        if timeout is None:
            continue
        versions[name] = subprocess.check_output(  # noqa: S603 - discovered local tool
            [timeout, "--version"],
            text=True,
        ).splitlines()[0]
        if "GNU coreutils" in versions[name]:
            versions["selected_timeout_path"] = timeout
            return bash, timeout, versions
    raise RuntimeError("prerequisite=gnu_timeout")


def _workspace(root: Path) -> tuple[Path, dict[str, str]]:
    work = root / "gate"
    for name in ("scripts", "bin", "temp", "docs/contracts/schemas", "packages/cnes_infra"):
        (work / name).mkdir(parents=True)
    source = Path(
        os.environ.get(
            "S3_GATE_SCRIPT",
            str(Path(__file__).resolve().parents[2] / "scripts/ci_python_gate.sh"),
        )
    )
    shutil.copyfile(source, work / "scripts/ci_python_gate.sh")
    (work / "docs/contracts/openapi.json").write_text('{"synthetic":"openapi"}\n')
    (work / "docs/contracts/schemas/fixture.json").write_text('{"synthetic":"schema"}\n')
    for name, body in (("uv", _UV), ("timeout", _TIMEOUT)):
        executable = work / "bin" / name
        executable.write_text(f"#!{sys.executable}\n{body}")
        executable.chmod(0o755)
    environment = os.environ.copy()
    environment.update(
        PATH=str(work / "bin") + os.pathsep + environment["PATH"],
        TMPDIR=str(work / "temp"),
        GATE_EVENTS=str(root / "events.jsonl"),
        GATE_TIMEOUTS=str(root / "timeouts.jsonl"),
        GATE_SCALE=str(TIME_SCALE),
    )
    return work, environment


def _records(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


def _phase_durations(rows: list[dict[str, Any]]) -> tuple[tuple[str, float], ...]:
    phases: dict[str, list[float]] = {}
    for row in rows:
        if row["phase"] in {"packages", "apps"}:
            phases.setdefault(row["phase"], []).append(row["observed_monotonic"])
    return tuple((phase, times[-1] - times[0]) for phase, times in phases.items())


def run_gate(root: Path, scenario: str) -> GateRun:
    """Args: Diretório temporário e cenário da fronteira UV.
    Returns: Resultado do Bash, fases, artefatos e limpeza de descendentes.
    Raises: Erros de pré-requisito ou processo sintético sobrevivente.
    """
    bash, timeout, versions = _tools()
    work, environment = _workspace(root)
    environment.update(GATE_CASE=scenario, REAL_GNU_TIMEOUT=timeout)
    expired = False
    wall_started = time.time()
    started = time.monotonic()
    with _adopt_orphans():
        process = subprocess.Popen(  # noqa: S603 - local Bash and script under test
            [bash, "scripts/ci_python_gate.sh"],
            cwd=work,
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            start_new_session=True,
        )
        try:
            output, _ = process.communicate(timeout=_EXTERNAL_DEADLINE)
        except subprocess.TimeoutExpired:
            expired = True
            output = ""
        finally:
            survivors = _cleanup_session(process)
        assert not survivors, f"synthetic_processes_survived={survivors}"
        if expired:
            output, _ = process.communicate(timeout=1)
    rows = _records(root / "events.jsonl")
    assert process.returncode is not None
    result = GateRun(
        process.returncode,
        expired,
        time.monotonic() - started,
        tuple(f"{row['phase']}:{row['event']}" for row in rows),
        tuple(path.name for path in work.glob("coverage-*.xml")),
        tuple(row["output"] for row in rows if "output" in row),
        survivors,
        output,
        tuple(tuple(row["argv"]) for row in rows if row["event"] == "start"),
        tuple(tuple(row["original_argv"]) for row in _records(root / "timeouts.jsonl")),
        (wall_started, time.time()),
        _phase_durations(rows),
    )
    _save_result(root, result, process.pid, versions)
    return result


def _save_result(root: Path, result: GateRun, session: int, versions: dict[str, str]) -> None:
    evidence = {
        **asdict(result),
        "pid_session": session,
        "versions": versions,
        "time_scale": TIME_SCALE,
        "outer_deadline": _EXTERNAL_DEADLINE,
        "timeout_calls": _records(root / "timeouts.jsonl"),
    }
    (root / "result.json").write_text(json.dumps(evidence, indent=2) + "\n")
