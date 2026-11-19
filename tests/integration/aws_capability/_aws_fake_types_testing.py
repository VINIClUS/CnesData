"""Tipos e respostas básicas da fronteira de comandos com estado."""

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime

from tests.integration.aws_capability._service_contracts import ApiError, Reply

OPERATOR = "operator"
ACCESS_KEY = "ASIA_FAKE_SESSION_KEY_CANARY"
SECRET = "FAKE_SESSION_SECRET_CANARY"  # noqa: S105
TOKEN = "FAKE_SESSION_TOKEN_CANARY"  # noqa: S105


def denied(code: str = "AccessDenied", message: str = "") -> Reply:
    details = {"Code": code, "Message": message} if message else {"Code": code}
    return Reply(False, {}, ApiError(code, details, "service"))


def linger(table: dict[str, int], key: str, polls: int) -> None:
    if polls > 0:
        table[key] = polls


def still_visible(table: dict[str, int], key: str) -> bool:
    remaining = table.get(key, 0)
    if remaining <= 0:
        return False
    table[key] = remaining - 1
    return True


def good(data: Mapping[str, object] | None = None) -> Reply:
    return Reply(True, data or {}, None)


def iso(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_iso(value: object) -> float:
    return datetime.strptime(str(value), "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC).timestamp()


@dataclass
class FakeVersion:
    key: str
    version_id: str
    hold: str | None = None
    until: float = 0.0


@dataclass
class FakeBucket:
    tags: dict[str, str] = field(default_factory=dict)
    versions: list[FakeVersion] = field(default_factory=list)
    policy: Mapping[str, object] | None = None
    policy_effective: int = 0


@dataclass
class FakeRole:
    tags: dict[str, str]
    trust: object
    policies: dict[str, tuple[frozenset[str], int, object]] = field(default_factory=dict)
    pending_trust: tuple[int, object] | None = None

    def trust_at(self, counter: int) -> object:
        if self.pending_trust is not None and counter >= self.pending_trust[0]:
            return self.pending_trust[1]
        return self.trust

    def newest_trust(self) -> object:
        return self.trust if self.pending_trust is None else self.pending_trust[1]
