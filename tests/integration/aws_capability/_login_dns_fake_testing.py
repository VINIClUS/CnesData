"""DNS sintético: resolver recursivo e NS autoritativos que publicam no relógio do lab."""

import struct
from collections.abc import Callable
from dataclasses import dataclass, field

ZONE = "auth.us-east-2.amazoncognito.com"
RESOLVER = "127.0.0.53"
HOSTS = {
    "ns-1.awsdns-01.net": "192.0.2.1",
    "ns-2.awsdns-02.org": "192.0.2.2",
    "ns-3.awsdns-03.co.uk": "192.0.2.3",
    "ns-4.awsdns-04.com": "192.0.2.4",
}
DOMAIN_ADDRESS = bytes([198, 51, 100, 7])
TYPE_A = 1
TYPE_NS = 2


class EarlyLookup(BaseException):
    pass


@dataclass(frozen=True)
class Query:
    server: str
    name: str
    qtype: int
    recursion: bool


def _qname(name: str) -> bytes:
    return b"".join(bytes([len(p)]) + p.encode() for p in name.split(".")) + b"\x00"


def _parse(packet: bytes) -> tuple[int, bool, str, int]:
    txid, flags = struct.unpack_from(">HH", packet, 0)
    labels, offset = [], 12
    while packet[offset]:
        labels.append(packet[offset + 1 : offset + 1 + packet[offset]].decode())
        offset += 1 + packet[offset]
    qtype = struct.unpack_from(">H", packet, offset + 1)[0]
    return txid, bool(flags & 0x0100), ".".join(labels), qtype


def build_reply(
    query: tuple[int, bool, str, int],
    rcode: int,
    authoritative: bool,
    records: list[tuple[str, int, bytes]],
) -> bytes:
    txid, recursion, name, qtype = query
    flags = 0x8000 | (0x0400 if authoritative else 0) | (0x0180 if recursion else 0) | rcode
    header = struct.pack(">HHHHHH", txid, flags, 1, len(records), 0, 0)
    question = _qname(name) + struct.pack(">HH", qtype, 1)
    answers = b"".join(
        _qname(owner) + struct.pack(">HHIH", rtype, 1, 60, len(data)) + data
        for owner, rtype, data in records
    )
    return header + question + answers


@dataclass
class FakeDns:
    now: Callable[[], float]
    advance: Callable[[float], None]
    latency: float = 0.05
    timeout: float = 1.0
    resolver_list: tuple[str, ...] = (RESOLVER,)
    hosts: dict[str, str] = field(default_factory=lambda: dict(HOSTS))
    publish_delay: dict[str, float] = field(default_factory=dict)
    silent: set[str] = field(default_factory=set)
    drop_every: int = 0
    zone_answer: str = "ok"
    probe_answer: dict[str, str] = field(default_factory=dict)
    script: dict[str, list[str]] = field(default_factory=dict)
    target: str | None = None
    created_at: float | None = None
    log: list[Query] = field(default_factory=list)
    counts: dict[str, int] = field(default_factory=dict)
    address_lookups: list[str] = field(default_factory=list)
    events: list[str] = field(default_factory=list)

    def create(self, name: str) -> None:
        self.target, self.created_at = name.lower(), self.now()
        self.events.append("create")

    def authorities(self) -> set[str]:
        return set(self.hosts.values())

    def published(self, server: str) -> bool:
        if self.target is None or self.created_at is None:
            return False
        return self.now() >= self.created_at + self.publish_delay.get(server, 0.0)

    def fully_published(self) -> bool:
        return all(self.published(server) for server in self.authorities())

    def resolvers(self) -> tuple[str, ...]:
        return self.resolver_list

    def addresses(self, host: str) -> tuple[str, ...]:
        if self.target is not None and host.lower() == self.target:
            raise EarlyLookup("dns=address_lookup_of_target")
        self.address_lookups.append(host)
        self.advance(self.latency)
        address = self.hosts.get(host.lower())
        return (address,) if address else ()

    def exchange(self, server: str, packet: bytes) -> bytes | None:
        query = _parse(packet)
        _, recursion, name, qtype = query
        self.log.append(Query(server, name, qtype, recursion))
        self.events.append("query")
        if name == self.target and (server not in self.authorities() or recursion):
            raise EarlyLookup(f"dns=target_outside_authorities recursion={recursion}")
        if server in self.silent and server not in self.authorities():
            return self._drop()
        if server in self.resolver_list:
            return self._recursive(query)
        if server in self.authorities():
            return self._authoritative(server, query)
        return self._drop()

    def _drop(self) -> None:
        self.advance(self.timeout)

    def _recursive(self, query: tuple[int, bool, str, int]) -> bytes | None:
        name = query[2]
        if name != ZONE or self.zone_answer == "nxdomain":
            return self._answer(query, 3, False, [])
        if self.zone_answer == "drop":
            return self._drop()
        owner = "other." + ZONE if self.zone_answer == "other_owner" else ZONE
        records = (
            []
            if self.zone_answer == "empty"
            else [(owner, TYPE_NS, _qname(host)) for host in self.hosts]
        )
        return self._answer(query, 0, False, records)

    def _authoritative(self, server: str, query: tuple[int, bool, str, int]) -> bytes | None:
        self.counts[server] = self.counts.get(server, 0) + 1
        dropped = self.drop_every and self.counts[server] % self.drop_every == 0
        if server in self.silent or dropped:
            return self._drop()
        name = query[2]
        if name != self.target:
            return self._probe(server, query)
        steps = self.script.get(server)
        mode = steps.pop(0) if steps else ("a" if self.published(server) else "nx")
        return self._target_reply(mode, query)

    def _probe(self, server: str, query: tuple[int, bool, str, int]) -> bytes | None:
        mode = self.probe_answer.get(server, "nx")
        if mode == "drop":
            return self._drop()
        return self._answer(query, 3, mode == "nx", [])

    def _target_reply(self, mode: str, query: tuple[int, bool, str, int]) -> bytes | None:
        name = query[2]
        replies = {
            "a": (0, True, [(name, TYPE_A, DOMAIN_ADDRESS)]),
            "nx": (3, True, []),
            "plain": (0, False, [(name, TYPE_A, DOMAIN_ADDRESS)]),
            "other": (0, True, [("other." + name, TYPE_A, DOMAIN_ADDRESS)]),
            "nodata": (0, True, []),
        }
        if mode not in replies:
            return self._drop()
        rcode, authoritative, records = replies[mode]
        return self._answer(query, rcode, authoritative, records)

    def _answer(
        self,
        query: tuple[int, bool, str, int],
        rcode: int,
        authoritative: bool,
        records: list[tuple[str, int, bytes]],
    ) -> bytes:
        self.advance(self.latency)
        return build_reply(query, rcode, authoritative, records)
