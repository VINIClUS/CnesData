"""Portas de fronteira do login: callback local, DNS e constantes do client público."""

from dataclasses import dataclass, field
from typing import Protocol

LISTEN_PORT = 53682
CALLBACK_URL = f"http://localhost:{LISTEN_PORT}/callback"
LOGOUT_URL = f"http://localhost:{LISTEN_PORT}/"
API_ORIGIN = "https://api.cnesdata.com.br"
API_SCOPE = f"{API_ORIGIN}/api.access"
SCOPES = ("openid", "email", "profile", API_SCOPE)


@dataclass(frozen=True)
class Callback:
    code: str | None = field(default=None, repr=False)
    state: str | None = field(default=None, repr=False)
    error: str | None = None


class ListenerPort(Protocol):
    def wait(self, timeout: float) -> Callback | None: ...
    def redirect(self, location: str) -> None: ...
    def finish(self, message: str) -> None: ...
    def close(self) -> None: ...


class DnsPort(Protocol):
    def resolvers(self) -> tuple[str, ...]: ...
    def addresses(self, host: str) -> tuple[str, ...]: ...
    def exchange(self, server: str, packet: bytes) -> bytes | None: ...
