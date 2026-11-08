"""Listener real do callback em 127.0.0.1 e ::1; sem lógica além de enfileirar."""

import queue
import socket
import threading
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Protocol, cast
from urllib.parse import parse_qs, urlsplit

from tests.integration.aws_capability._login_oauth import label
from tests.integration.aws_capability._login_ports import LISTEN_PORT, Callback, ListenerPort
from tests.integration.aws_capability.contracts import ServiceFailure

HOSTS = ("127.0.0.1", "::1")
ANSWER_SECONDS = 900.0
TEXT = "text/plain; charset=utf-8"


@dataclass
class Exchange:
    callbacks: "queue.Queue[Callback]" = field(default_factory=queue.Queue)
    answers: "queue.Queue[tuple[int, str | None, str]]" = field(default_factory=queue.Queue)


class Server(Protocol):
    exchange: Any

    def serve_forever(self) -> None: ...
    def shutdown(self) -> None: ...
    def server_close(self) -> None: ...


def parse_callback(target: str) -> Callback | None:
    parts = urlsplit(target)
    if parts.path != "/callback":
        return None
    query = {key: values[0] for key, values in parse_qs(parts.query).items()}
    error = query.get("error")
    return Callback(query.get("code"), query.get("state"), None if error is None else label(error))


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        callback = parse_callback(self.path)
        if callback is None:
            self.send_response(404)
            self.send_header("Content-Type", TEXT)
            self.end_headers()
            return
        exchange: Exchange = cast("_Server", self.server).exchange
        exchange.callbacks.put(callback)
        status, location, body = exchange.answers.get(timeout=ANSWER_SECONDS)
        self.send_response(status)
        if location is not None:
            self.send_header("Location", location)
        else:
            self.send_header("Content-Type", TEXT)
        self.end_headers()
        self.wfile.write(body.encode("utf-8"))

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
        return None


class _Server(ThreadingHTTPServer):
    daemon_threads = True
    exchange: Any = None


class _Server6(_Server):
    address_family = socket.AF_INET6

    def server_bind(self) -> None:
        self.socket.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
        super().server_bind()


def _bind(host: str, port: int) -> Server:
    return (_Server6 if ":" in host else _Server)((host, port), _Handler)


class LoopbackListener:
    def __init__(self, servers: Sequence[Server], threads: Sequence[threading.Thread]) -> None:
        self.servers = servers
        self.threads = threads
        self.exchange = Exchange()

    def wait(self, timeout: float) -> Callback | None:
        try:
            return self.exchange.callbacks.get(timeout=max(timeout, 0.0))
        except queue.Empty:
            return None

    def redirect(self, location: str) -> None:
        self.exchange.answers.put((302, location, ""))

    def finish(self, message: str) -> None:
        self.exchange.answers.put((200, None, message))

    def close(self) -> None:
        for server in self.servers:
            server.shutdown()
            server.server_close()


def open_listener(bind: Callable[[str, int], Server] = _bind) -> LoopbackListener:
    servers: list[Server] = []
    try:
        servers.extend(bind(host, LISTEN_PORT) for host in HOSTS)
    except OSError:
        for server in servers:
            server.server_close()
        raise
    listener = LoopbackListener(servers, [])
    for server in servers:
        server.exchange = listener.exchange
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        listener.threads = [*listener.threads, thread]
    return listener


def listener_factory(
    bind: Callable[[str, int], Server] = _bind,
) -> Callable[[], ListenerPort]:
    def build() -> ListenerPort:
        try:
            return open_listener(bind)
        except OSError:
            raise ServiceFailure("probe=listener_unavailable") from None

    return build
