"""Humano e listener sintéticos: lê o handoff, loga uma vez e segue os redirects silenciosos."""

from collections.abc import Callable
from dataclasses import dataclass, field

from tests.integration.aws_capability._login_fake_testing import FakeIdp, query_of
from tests.integration.aws_capability._login_handoff import handoff_path
from tests.integration.aws_capability._login_ports import Callback
from tests.integration.aws_capability.contracts import RunSpec


@dataclass
class FakeListener:
    idp: FakeIdp
    spec: RunSpec
    advance: Callable[[float], None]
    human_seconds: float = 40.0
    silent_seconds: float = 1.0
    wrong_state_at: int | None = None
    error_at: int | None = None
    steps: int = 0
    pending: str | None = None
    handoff_text: str | None = None
    finished: str | None = None
    closed: bool = False
    redirects: list[str] = field(default_factory=list)
    waits: list[float] = field(default_factory=list)

    def factory(self) -> "FakeListener":
        return self

    def wait(self, timeout: float) -> Callback | None:
        self.waits.append(timeout)
        first = self.steps == 0
        seconds = self.human_seconds if first else self.silent_seconds
        if first:
            self.handoff_text = handoff_path(self.spec).read_text()
            self.pending = self.handoff_text.splitlines()[0].removeprefix("url=")
        if seconds > timeout or (first and not self._login_ok()):
            self.advance(timeout)
            return None
        self.advance(seconds)
        self.steps += 1
        if self.pending is None:
            return None
        query = query_of(self.pending)
        if self.error_at == self.steps:
            return Callback(error="access_denied")
        code = self.idp.authorize(query)
        state = "wrong" if self.wrong_state_at == self.steps else query["state"]
        return Callback(code, state)

    def _login_ok(self) -> bool:
        values = dict(line.split("=", 1) for line in (self.handoff_text or "").splitlines())
        pool = self.idp.pool
        return any(
            row["email"] == values.get("username") and row["password"] == values.get("password")
            for row in pool.users.values()
        )

    def redirect(self, location: str) -> None:
        self.redirects.append(location)
        self.pending = location

    def finish(self, message: str) -> None:
        self.finished = message

    def close(self) -> None:
        self.closed = True
