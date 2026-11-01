"""Tipos e respostas básicas da fronteira de comandos com estado."""

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime

from tests.integration.aws_capability._service_contracts import ApiError, Reply

OPERATOR = "operator"
ACCESS_KEY = "ASIA_FAKE_SESSION_KEY_CANARY"
SECRET = "FAKE_SESSION_SECRET_CANARY"  # noqa: S105
TOKEN = "FAKE_SESSION_TOKEN_CANARY"  # noqa: S105


def denied(code: str = "AccessDenied") -> Reply:
    return Reply(False, {}, ApiError(code, {"Code": code}, "service"))


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


@dataclass
class FakeRole:
    tags: dict[str, str]
    trust: object
    policies: dict[str, tuple[frozenset[str], int, object]] = field(default_factory=dict)
