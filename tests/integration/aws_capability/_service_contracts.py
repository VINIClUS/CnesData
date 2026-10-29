"""Resultados privados e porta de comandos dos serviços de capacidade."""

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal, Protocol

if TYPE_CHECKING:
    from tests.integration.aws_capability.cli import Request
    from tests.integration.aws_capability.contracts import RequestContext


@dataclass(frozen=True)
class ApiError:
    code: str = field(repr=False)
    details: Mapping[str, object] = field(repr=False)
    origin: Literal["service", "local"]


@dataclass(frozen=True)
class Reply:
    ok: bool
    data: Mapping[str, object] = field(repr=False)
    error: ApiError | None


class CommandPort(Protocol):
    def call(self, request: "Request", context: "RequestContext") -> Reply: ...


@dataclass(frozen=True)
class ServicePorts:
    commands: CommandPort = field(repr=False)
    now: Callable[[], float]
