"""Portas reais de processo, credenciais de sessão e laço do supervisor."""

import json
import os
import select
import subprocess
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from tests.integration.aws_capability._profiles import PROFILES
from tests.integration.aws_capability._safety import SafetyFailure
from tests.integration.aws_capability._service_contracts import ServicePorts
from tests.integration.aws_capability.contracts import (
    Outcome,
    Policy,
    Ports,
    Processes,
    RequestBudget,
    RunSpec,
)
from tests.integration.aws_capability.lifecycle import janitor, monitor, parse, refusal, run
from tests.integration.aws_capability.services import AwsCommands, AwsServices

ACK_TIMEOUT = 15.0
ACK_LIMIT = 65536
COMMAND_TIMEOUT = 25.0
MARGIN_SECONDS = 15.0
CYCLE_SECONDS = 1.0
CREDENTIAL_KEYS = ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN")
PROFILE_KEYS = ("AWS_PROFILE", "AWS_DEFAULT_PROFILE")
AUTHORIZED_REGIONS = frozenset({"us-east-2"})


def _spawn(argv: list[str], **options: Any) -> Any:
    return subprocess.Popen(argv, **options)  # noqa: S603


@dataclass
class SystemProcesses:
    popen: Callable[..., Any] = _spawn
    system: Any = os
    selector: Any = select
    clock: Callable[[], float] = time.monotonic
    out: Any = None
    children: dict[int, Any] = field(default_factory=dict, init=False)
    sent: set[int] = field(default_factory=set, init=False)

    def start(self, argv: Sequence[str], options: Mapping[str, object]) -> int:
        child = self.popen(
            list(argv),
            **{
                **options,
                "shell": False,
                "close_fds": True,
                "start_new_session": True,
                "stdin": subprocess.DEVNULL,
                "stdout": subprocess.PIPE,
                "stderr": subprocess.DEVNULL,
            },
        )
        self.children[child.pid] = child
        return int(child.pid)

    def alive(self, pid: int) -> bool:
        if pid in self.children and self._reaped(pid):
            return False
        try:
            self.system.kill(pid, 0)
        except PermissionError:
            return True
        except OSError:
            return False
        return True

    def receive(self, pid: int) -> Mapping[str, object] | None:
        child = self.children.get(pid)
        if child is None or child.stdout is None:
            return None
        line = self._read_line(child.stdout.fileno())
        try:
            message = json.loads(line) if line else None
        except ValueError:
            return None
        return message if isinstance(message, dict) else None

    def send(self, pid: int, message: Mapping[str, object]) -> None:
        if pid in self.sent:
            return
        self.sent.add(pid)
        stream = self.out if self.out is not None else sys.stdout
        try:
            stream.write(json.dumps(message, sort_keys=True) + "\n")
            stream.flush()
        except (OSError, ValueError):
            return

    def _reaped(self, pid: int) -> bool:
        try:
            finished, _ = self.system.waitpid(pid, self.system.WNOHANG)
        except ChildProcessError:
            return False
        if finished != pid:
            return False
        self.children.pop(pid, None)
        return True

    def _read_line(self, descriptor: int) -> bytes:
        deadline = self.clock() + ACK_TIMEOUT
        buffer = b""
        while b"\n" not in buffer:
            remaining = deadline - self.clock()
            if remaining <= 0 or len(buffer) >= ACK_LIMIT:
                return b""
            ready, _, _ = self.selector.select([descriptor], [], [], remaining)
            chunk = self.system.read(descriptor, 4096) if ready else b""
            if not chunk:
                return b""
            buffer += chunk
        return buffer.split(b"\n", 1)[0]


def require_session_credentials(environment: Mapping[str, str]) -> None:
    if any(key in environment for key in PROFILE_KEYS):
        raise SafetyFailure("profile_credentials_forbidden")
    if not all(environment.get(key) for key in CREDENTIAL_KEYS):
        raise SafetyFailure("session_credentials_required")


def build_ports(
    spec: RunSpec,
    environment: Mapping[str, str],
    processes: Processes,
    clock: Callable[[], float],
) -> Ports:
    profile = PROFILES.get(spec.probe)
    if profile is None:
        raise SafetyFailure("invalid_profile")
    commands = AwsCommands(spec.region, dict(environment), COMMAND_TIMEOUT)
    services = AwsServices(spec, ServicePorts(commands, clock))
    policy = Policy(spec.account, AUTHORIZED_REGIONS, profile.limits)
    budget = RequestBudget(profile.limits, spec.deadline, clock)
    return Ports(services, processes, clock, policy, budget)


def supervise(
    spec: RunSpec, ports: Ports, worker_pid: int, sleep: Callable[[float], None]
) -> Outcome:
    limit = spec.deadline + ports.policy.limits.cleanup_seconds + MARGIN_SECONDS
    while True:
        outcome = monitor(spec, ports, worker_pid)
        if outcome.status != "waiting":
            return outcome
        if ports.now() >= limit:
            return Outcome("incomplete", "supervisor_timeout")
        sleep(CYCLE_SECONDS)


def emit_line(line: str) -> None:
    try:
        sys.stdout.write(line + "\n")
        sys.stdout.flush()
    except (OSError, ValueError):
        return


@dataclass(frozen=True)
class Runtime:
    processes: Processes
    clock: Callable[[], float]
    sleep: Callable[[float], None]
    emit: Callable[[str], None]
    build: Callable[..., Ports]


def default_runtime() -> Runtime:
    return Runtime(SystemProcesses(), time.time, time.sleep, emit_line, build_ports)


def render(outcome: Outcome) -> str:
    remaining = (
        None
        if outcome.remaining is None
        else [{"service": ref.service, "name": ref.name} for ref in outcome.remaining]
    )
    document = {
        "status": outcome.status,
        "reason": outcome.reason,
        "remaining": remaining,
        "evidence": outcome.evidence,
    }
    return json.dumps(document, sort_keys=True, default=str)


def dispatch(
    argv: Sequence[str], environment: Mapping[str, str], runtime: Runtime
) -> tuple[str, Outcome]:
    command, spec, worker_pid = parse(argv)
    require_session_credentials(environment)
    ports = runtime.build(spec, environment, runtime.processes, runtime.clock)
    if command == "run":
        return command, run(spec, ports)
    if command == "janitor":
        return command, janitor(spec, ports)
    if worker_pid is None or worker_pid <= 0:
        return command, refusal("invalid_worker_pid")
    return command, supervise(spec, ports, worker_pid, runtime.sleep)


def main(argv: Sequence[str], environment: Mapping[str, str], runtime: Runtime) -> int:
    command = ""
    try:
        command, outcome = dispatch(argv, environment, runtime)
    except SafetyFailure as error:
        outcome = refusal(error.reason)
    except Exception as error:
        outcome = Outcome("incomplete", "unexpected_error", None, {"error": type(error).__name__})
    runtime.emit(render(outcome))
    success = ("stopped", "succeeded") if command == "monitor" else ("succeeded",)
    return 0 if outcome.status in success else 1
