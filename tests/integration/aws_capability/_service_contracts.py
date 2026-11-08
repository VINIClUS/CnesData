"""Resultados privados e porta de comandos dos serviços de capacidade."""

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal, Protocol

import httpx

from tests.integration.aws_capability._login_ports import DnsPort, ListenerPort
from tests.integration.aws_capability._operator_inputs import OperatorInputs

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


def no_pause(seconds: float) -> None:
    return None


SessionFactory = Callable[[Mapping[str, str]], CommandPort]


@dataclass(frozen=True)
class RegistryLogin:
    registry: str
    username: str = field(repr=False)
    token: str = field(repr=False)


class ImagePort(Protocol):
    def push(
        self, image: str, target: str, login: RegistryLogin, context: "RequestContext"
    ) -> str: ...


@dataclass(frozen=True)
class ServicePorts:
    commands: CommandPort = field(repr=False)
    now: Callable[[], float]
    pause: Callable[[float], None] = no_pause
    session: SessionFactory | None = field(default=None, repr=False)
    operator: OperatorInputs | None = None
    http: httpx.Client | None = field(default=None, repr=False)
    listener: Callable[[], ListenerPort] | None = field(default=None, repr=False)
    dns: DnsPort | None = field(default=None, repr=False)
    images: ImagePort | None = field(default=None, repr=False)
    image: str | None = None
