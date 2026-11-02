"""Adaptador real do DNS: UDP/53 em IPv4 e getaddrinfo, sem lógica além de E/S."""

import ipaddress
import socket
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

DNS_PORT = 53
QUERY_TIMEOUT = 1.0
MAX_REPLY = 4096


def parse_resolvers(text: str) -> tuple[str, ...]:
    """Returns: IPv4 das linhas nameserver, na ordem e sem repetição."""
    found: list[str] = []
    for line in text.splitlines():
        parts = line.split()
        if len(parts) >= 2 and parts[0] == "nameserver" and _is_ipv4(parts[1]):
            found.append(parts[1])
    return tuple(dict.fromkeys(found))


def _is_ipv4(text: str) -> bool:
    try:
        ipaddress.IPv4Address(text)
    except ValueError:
        return False
    return True


def open_udp() -> socket.socket:
    return socket.socket(socket.AF_INET, socket.SOCK_DGRAM)


def lookup(host: str) -> list[Any]:
    return socket.getaddrinfo(host, DNS_PORT, socket.AF_INET, socket.SOCK_DGRAM)


@dataclass(frozen=True)
class UdpDns:
    timeout: float = QUERY_TIMEOUT
    resolv_conf: Path = Path("/etc/resolv.conf")
    open_socket: Callable[[], Any] = field(default=open_udp, repr=False)
    resolve: Callable[[str], list[Any]] = field(default=lookup, repr=False)
    clock: Callable[[], float] = field(default=time.monotonic, repr=False)

    def resolvers(self) -> tuple[str, ...]:
        try:
            return parse_resolvers(self.resolv_conf.read_text(encoding="utf-8"))
        except OSError:
            return ()

    def addresses(self, host: str) -> tuple[str, ...]:
        try:
            infos = self.resolve(host)
        except OSError:
            return ()
        return tuple(dict.fromkeys(str(info[4][0]) for info in infos))

    def exchange(self, server: str, packet: bytes) -> bytes | None:
        try:
            with self.open_socket() as sock:
                sock.settimeout(self.timeout)
                sock.sendto(packet, (server, DNS_PORT))
                return self._receive(sock, server)
        except OSError:
            return None

    def _receive(self, sock: Any, server: str) -> bytes | None:
        deadline = self.clock() + self.timeout
        while (remaining := deadline - self.clock()) > 0:
            sock.settimeout(remaining)
            data, source = sock.recvfrom(MAX_REPLY)
            if source[0] == server:
                return data
        return None
