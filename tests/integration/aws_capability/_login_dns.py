"""Gate de publicação do domain: o nome novo só vai aos NS autoritativos, sem recursão."""

import secrets
from dataclasses import dataclass, field

from tests.integration.aws_capability._login_dns_wire import (
    RCODE_NOERROR,
    RCODE_NXDOMAIN,
    TYPE_A,
    TYPE_NS,
    Answer,
    decode_reply,
    encode_query,
    normalize,
)
from tests.integration.aws_capability._login_ports import DnsPort
from tests.integration.aws_capability.contracts import RequestContext

CONFIRMATIONS = 2
DISCOVERY_TRIES = 3
PROBE_PREFIX = "cnesdata-cap-probe-"


@dataclass(frozen=True)
class Dns:
    port: DnsPort = field(repr=False)
    context: RequestContext

    def ask(self, server: str, name: str, qtype: int, recursion: bool) -> Answer | None:
        """Returns: resposta decodificada ou None em timeout e pacote alheio."""
        self.context.budget.acquire(self.context.lane)
        txid = secrets.randbits(16)
        reply = self.port.exchange(server, encode_query(txid, name, qtype, recursion))
        return None if reply is None else decode_reply(reply, txid, name, qtype)

    def addresses(self, host: str) -> tuple[str, ...]:
        """Returns: endereços IPv4 do host de um NS, consumindo orçamento."""
        self.context.budget.acquire(self.context.lane)
        return self.port.addresses(host)


@dataclass(frozen=True)
class Discovery:
    code: str
    servers: tuple[str, ...] = field(default=(), repr=False)


def zone_of(region: str) -> str:
    return f"auth.{region}.amazoncognito.com"


def discover(dns: Dns, zone: str) -> Discovery:
    """Returns: NS IPv4 da zona provados autoritativos, ou o código da falha."""
    resolvers = dns.port.resolvers()
    if not resolvers:
        return Discovery("no_resolver")
    hosts = _zone_hosts(dns, resolvers, normalize(zone))
    if not hosts:
        return Discovery("no_nameservers")
    servers = _addresses(dns, hosts)
    if servers is None:
        return Discovery("unaddressed")
    probe = f"{PROBE_PREFIX}{secrets.token_hex(4)}.{normalize(zone)}"
    if not all(_denies(dns, server, probe) for server in servers):
        return Discovery("unverified")
    return Discovery("ok", servers)


def _zone_hosts(dns: Dns, resolvers: tuple[str, ...], zone: str) -> tuple[str, ...]:
    for resolver in resolvers:
        answer = _answered(dns, resolver, zone)
        hosts = () if answer is None else _ns_hosts(answer, zone)
        if hosts:
            return hosts
    return ()


def _answered(dns: Dns, resolver: str, zone: str) -> Answer | None:
    for _ in range(DISCOVERY_TRIES):
        answer = dns.ask(resolver, zone, TYPE_NS, True)
        if answer is not None:
            return answer
    return None


def _ns_hosts(answer: Answer, zone: str) -> tuple[str, ...]:
    if answer.rcode != RCODE_NOERROR:
        return ()
    names = {r.data for r in answer.records if r.rtype == TYPE_NS and r.name == zone and r.data}
    return tuple(sorted(names))


def _addresses(dns: Dns, hosts: tuple[str, ...]) -> tuple[str, ...] | None:
    found: set[str] = set()
    for host in hosts:
        addresses = dns.addresses(host)
        if not addresses:
            return None
        found.update(addresses)
    return tuple(sorted(found))


def _denies(dns: Dns, server: str, probe: str) -> bool:
    for _ in range(DISCOVERY_TRIES):
        answer = dns.ask(server, probe, TYPE_A, False)
        if answer is not None and answer.authoritative and answer.rcode == RCODE_NXDOMAIN:
            return True
    return False


def _publishes(answer: Answer, name: str) -> bool:
    if not answer.authoritative or answer.rcode != RCODE_NOERROR:
        return False
    return any(r.rtype == TYPE_A and r.name == normalize(name) for r in answer.records)


@dataclass
class Gate:
    dns: Dns = field(repr=False)
    servers: tuple[str, ...] = field(repr=False)
    name: str = field(repr=False)
    confirmations: dict[str, int] = field(default_factory=dict, repr=False)

    def step(self) -> bool:
        """Returns: se cada NS confirmou o A do nome em CONFIRMATIONS passos seguidos."""
        for server in self.servers:
            if self.confirmations.get(server, 0) >= CONFIRMATIONS:
                continue
            answer = self.dns.ask(server, self.name, TYPE_A, False)
            if answer is not None:
                done = _publishes(answer, self.name)
                self.confirmations[server] = self.confirmations.get(server, 0) + 1 if done else 0
        return bool(self.servers) and all(
            self.confirmations.get(server, 0) >= CONFIRMATIONS for server in self.servers
        )
