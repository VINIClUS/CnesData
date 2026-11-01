"""Fronteiras falsas de processo, os, select, relógio e saída para o runtime de produção."""

import subprocess
from collections.abc import Mapping
from dataclasses import dataclass, field

from tests.integration.aws_capability._b1_testing import operator_environment


@dataclass
class FakeStream:
    descriptor: int = 77

    def fileno(self) -> int:
        return self.descriptor

    def close(self) -> None:
        pass


@dataclass
class FakeChild:
    pid: int = 4242
    stdout: FakeStream | None = field(default_factory=FakeStream)


@dataclass
class FakePopen:
    calls: list[tuple[list[str], dict[str, object]]] = field(default_factory=list)
    child: FakeChild = field(default_factory=FakeChild)
    error: OSError | None = None

    def __call__(self, argv: list[str], **options: object) -> FakeChild:
        self.calls.append((argv, options))
        if self.error is not None:
            raise self.error
        return self.child


@dataclass
class FakeSystem:
    WNOHANG: int = 1
    live: set[int] = field(default_factory=set)
    exited_children: set[int] = field(default_factory=set)
    foreign: set[int] = field(default_factory=set)
    denied: set[int] = field(default_factory=set)
    chunks: list[bytes] = field(default_factory=list)
    kills: list[tuple[int, int]] = field(default_factory=list)
    waits: list[tuple[int, int]] = field(default_factory=list)

    def kill(self, pid: int, signal: int) -> None:
        self.kills.append((pid, signal))
        if pid in self.denied:
            raise PermissionError
        if pid not in self.live:
            raise ProcessLookupError

    def waitpid(self, pid: int, flags: int) -> tuple[int, int]:
        self.waits.append((pid, flags))
        if pid in self.foreign:
            raise ChildProcessError
        return (pid, 0) if pid in self.exited_children else (0, 0)

    def read(self, descriptor: int, size: int) -> bytes:
        return self.chunks.pop(0) if self.chunks else b""


@dataclass
class FakeSelector:
    system: FakeSystem
    timeouts: list[float] = field(default_factory=list)
    silent: bool = False

    def select(
        self, readable: list[int], writable: list[int], errors: list[int], timeout: float
    ) -> tuple[list[int], list[int], list[int]]:
        self.timeouts.append(timeout)
        if self.silent or not self.system.chunks:
            return [], [], []
        return readable, [], []


@dataclass
class FakeClock:
    current: float = 1000.0
    step: float = 0.0

    def __call__(self) -> float:
        self.current += self.step
        return self.current


class ClosedOut:
    def __init__(self, error: BaseException) -> None:
        self.error = error

    def write(self, text: str) -> int:
        raise self.error

    def flush(self) -> None:
        raise self.error


def environment() -> Mapping[str, str]:
    return {
        **operator_environment(),
        "AWS_ACCESS_KEY_ID": "dummy-key-do-not-log",
        "AWS_SECRET_ACCESS_KEY": "dummy-secret-do-not-log",
        "AWS_SESSION_TOKEN": "dummy-token-do-not-log",
        "PATH": "/usr/bin",
    }


DEVNULL = subprocess.DEVNULL
PIPE = subprocess.PIPE
