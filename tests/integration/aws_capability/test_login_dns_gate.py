"""Descoberta dos NS autoritativos e gate de publicação sobre o DNS sintético."""

from typing import Any

import pytest

from tests.integration.aws_capability._login_dns import (
    CONFIRMATIONS,
    DISCOVERY_TRIES,
    Discovery,
    Dns,
    Gate,
    discover,
    zone_of,
)
from tests.integration.aws_capability._login_dns_fake_testing import (
    HOSTS,
    RESOLVER,
    ZONE,
    FakeDns,
)
from tests.integration.aws_capability._login_dns_wire import TYPE_A, TYPE_NS
from tests.integration.aws_capability.contracts import Limits, RequestBudget, RequestContext

TARGET = "cnesdata-cap-0a1b2c3d.auth.us-east-2.amazoncognito.com"
SERVERS = tuple(sorted(HOSTS.values()))


class Clock:
    def __init__(self) -> None:
        self.value = 1000.0

    def now(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


def world(**options: Any) -> tuple[FakeDns, Dns, Clock]:
    clock = Clock()
    fake = FakeDns(clock.now, clock.advance, **options)
    budget = RequestBudget(Limits(400, 8, 2, 30, 10), 1_000_000.0, clock.now)
    return fake, Dns(fake, RequestContext(budget, "work")), clock


def test_zona_do_dominio_gerenciado_segue_a_regiao() -> None:
    assert zone_of("us-east-2") == ZONE


def test_descobre_os_ns_pelo_resolver_e_prova_cada_um_como_autoritativo() -> None:
    fake, dns, _ = world()
    discovery = discover(dns, ZONE)
    assert discovery == Discovery("ok", SERVERS)
    to_resolver = [q for q in fake.log if q.server == RESOLVER]
    assert [(q.name, q.qtype, q.recursion) for q in to_resolver] == [(ZONE, TYPE_NS, True)]
    probes = [q for q in fake.log if q.server != RESOLVER]
    assert sorted(q.server for q in probes) == list(SERVERS)
    assert all(not q.recursion and q.qtype == TYPE_A for q in probes)
    assert all(q.name.endswith("." + ZONE) and q.name != TARGET for q in probes)
    assert len({q.name for q in probes}) == 1
    assert sorted(fake.address_lookups) == sorted(HOSTS)


def test_nome_da_sonda_e_aleatorio_a_cada_descoberta() -> None:
    fake, dns, _ = world()
    discover(dns, ZONE)
    discover(dns, ZONE)
    names = {q.name for q in fake.log if q.server != RESOLVER}
    assert len(names) == 2


def test_sem_resolver_configurado_falha_fechado_sem_consultar_nada() -> None:
    fake, dns, _ = world(resolver_list=())
    assert discover(dns, ZONE) == Discovery("no_resolver")
    assert fake.log == []


@pytest.mark.parametrize("zone_answer", ["drop", "nxdomain", "empty", "other_owner"])
def test_zona_sem_ns_utilizavel_vira_no_nameservers(zone_answer: str) -> None:
    fake, dns, _ = world(zone_answer=zone_answer)
    assert discover(dns, ZONE) == Discovery("no_nameservers")
    assert all(q.server == RESOLVER for q in fake.log)
    expected = DISCOVERY_TRIES if zone_answer == "drop" else 1
    assert len(fake.log) == expected


def test_tenta_o_proximo_resolver_quando_o_primeiro_nao_responde() -> None:
    fake, dns, _ = world(resolver_list=("203.0.113.9", RESOLVER), silent={"203.0.113.9"})
    assert discover(dns, ZONE).code == "ok"
    first = [q for q in fake.log if q.server == "203.0.113.9"]
    assert len(first) == DISCOVERY_TRIES


def test_ns_sem_endereco_ipv4_vira_unaddressed() -> None:
    fake, dns, _ = world()
    missing = Dns(_MissingAddress(fake, "ns-3.awsdns-03.co.uk"), dns.context)
    assert discover(missing, ZONE) == Discovery("unaddressed")
    assert all(q.server == RESOLVER for q in fake.log)


class _MissingAddress:
    def __init__(self, inner: FakeDns, host: str) -> None:
        self.inner, self.host = inner, host

    def resolvers(self) -> tuple[str, ...]:
        return self.inner.resolvers()

    def addresses(self, host: str) -> tuple[str, ...]:
        return () if host == self.host else self.inner.addresses(host)

    def exchange(self, server: str, packet: bytes) -> bytes | None:
        return self.inner.exchange(server, packet)


@pytest.mark.parametrize("mode", ["plain", "drop"])
def test_ns_que_nao_nega_com_autoridade_vira_unverified(mode: str) -> None:
    fake, dns, _ = world(probe_answer={"192.0.2.2": mode})
    assert discover(dns, ZONE) == Discovery("unverified")
    tries = [q for q in fake.log if q.server == "192.0.2.2"]
    assert len(tries) == DISCOVERY_TRIES


def test_sonda_com_perda_pontual_ainda_verifica() -> None:
    fake, dns, _ = world(drop_every=2)
    fake.counts = dict.fromkeys(SERVERS, 1)
    assert discover(dns, ZONE).code == "ok"
    assert all(fake.counts[server] == 3 for server in SERVERS)


def gate_for(fake: FakeDns, dns: Dns) -> Gate:
    fake.create(TARGET)
    return Gate(dns, SERVERS, TARGET)


def test_gate_abre_so_apos_todos_os_ns_confirmarem_em_passos_seguidos() -> None:
    delays = {"192.0.2.1": 0.0, "192.0.2.2": 10.0, "192.0.2.3": 20.0, "192.0.2.4": 30.0}
    fake, dns, clock = world(publish_delay=delays)
    gate = gate_for(fake, dns)
    opened = []
    for _ in range(12):
        opened.append(gate.step())
        clock.advance(5.0)
    assert opened.index(True) == 7
    assert all(opened[7:])


def test_ns_confirmado_nao_e_consultado_de_novo() -> None:
    fake, dns, clock = world(publish_delay={"192.0.2.4": 20.0})
    gate = gate_for(fake, dns)
    for _ in range(10):
        gate.step()
        clock.advance(5.0)
    per_server = {s: sum(q.server == s and q.name == TARGET for q in fake.log) for s in SERVERS}
    assert per_server["192.0.2.1"] == CONFIRMATIONS
    assert per_server["192.0.2.4"] > CONFIRMATIONS


@pytest.mark.parametrize("negative", ["nx", "nodata", "plain", "other"])
def test_resposta_que_nao_e_a_autoritativo_do_nome_exato_zera_a_confirmacao(negative: str) -> None:
    fake, dns, _ = world(script={"192.0.2.3": ["a", negative, "a", "a"]})
    gate = gate_for(fake, dns)
    assert [gate.step() for _ in range(4)] == [False, False, False, True]


def test_timeout_nao_confirma_nem_zera() -> None:
    fake, dns, _ = world(script={"192.0.2.3": ["a", "drop", "a"]})
    gate = gate_for(fake, dns)
    assert [gate.step() for _ in range(3)] == [False, False, True]


def test_ns_mudo_mantem_o_gate_fechado() -> None:
    fake, dns, _ = world(silent={"192.0.2.2"})
    gate = gate_for(fake, dns)
    assert not any(gate.step() for _ in range(8))


def test_nome_novo_so_vai_aos_ns_autoritativos_sem_recursao() -> None:
    fake, dns, clock = world(publish_delay={"192.0.2.2": 12.0})
    discovery = discover(dns, ZONE)
    fake.create(TARGET)
    gate = Gate(dns, discovery.servers, TARGET)
    while not gate.step():
        clock.advance(5.0)
    target = [q for q in fake.log if q.name == TARGET]
    assert target
    assert {q.server for q in target} == set(SERVERS)
    assert not any(q.recursion for q in target)
    assert all(q.qtype == TYPE_A for q in target)


def test_cada_consulta_e_cada_endereco_consomem_orcamento() -> None:
    fake, dns, _ = world()
    discover(dns, ZONE)
    gate = gate_for(fake, dns)
    gate.step()
    used = dns.context.budget.work_used
    assert used == len(fake.log) + len(fake.address_lookups)


def test_resposta_com_txid_trocado_conta_como_sem_resposta() -> None:
    fake, dns, _ = world()
    swapped = _SwapTxid(fake)
    assert Dns(swapped, dns.context).ask(RESOLVER, ZONE, TYPE_NS, True) is None


class _SwapTxid:
    def __init__(self, inner: FakeDns) -> None:
        self.inner = inner

    def resolvers(self) -> tuple[str, ...]:
        return self.inner.resolvers()

    def addresses(self, host: str) -> tuple[str, ...]:
        return self.inner.addresses(host)

    def exchange(self, server: str, packet: bytes) -> bytes | None:
        reply = self.inner.exchange(server, packet)
        return None if reply is None else bytes([reply[0] ^ 0xFF]) + reply[1:]


def test_repr_nao_expoe_servidores_nem_o_nome() -> None:
    fake, dns, _ = world()
    discovery = discover(dns, ZONE)
    gate = gate_for(fake, dns)
    text = repr(discovery) + repr(gate) + repr(dns)
    assert "192.0.2" not in text
    assert TARGET not in text


def test_gate_sem_servidores_nunca_abre() -> None:
    fake, dns, _ = world()
    fake.create(TARGET)
    assert Gate(dns, (), TARGET).step() is False
