"""Adaptador UDP real com socket e resolução sintéticos, e a cerca offline do conftest."""

import socket
import time
from pathlib import Path

import pytest

from tests.integration.aws_capability import _login_dns_udp
from tests.integration.aws_capability._login_dns_udp import (
    DNS_PORT,
    QUERY_TIMEOUT,
    UdpDns,
    open_udp,
    parse_resolvers,
)

SERVER = "192.0.2.1"


class FakeSocket:
    def __init__(self, replies: list[object], clock: list[float] | None = None) -> None:
        self.replies = replies
        self.clock = clock
        self.sent: list[tuple[bytes, tuple[str, int]]] = []
        self.timeouts: list[float] = []
        self.closed = False

    def __enter__(self) -> "FakeSocket":
        return self

    def __exit__(self, *args: object) -> None:
        self.closed = True

    def settimeout(self, value: float) -> None:
        self.timeouts.append(value)

    def sendto(self, data: bytes, address: tuple[str, int]) -> int:
        self.sent.append((data, address))
        return len(data)

    def recvfrom(self, size: int) -> tuple[bytes, tuple[str, int]]:
        if self.clock is not None:
            self.clock[0] += 0.4
        if not self.replies:
            raise TimeoutError
        item = self.replies.pop(0)
        if isinstance(item, BaseException):
            raise item
        assert isinstance(item, tuple)
        return item


def adapter(sock: FakeSocket, clock: list[float] | None = None) -> UdpDns:
    ticks = clock if clock is not None else [0.0]
    return UdpDns(open_socket=lambda: sock, clock=lambda: ticks[0])


def test_envia_o_pacote_a_porta_53_do_servidor_e_devolve_a_resposta() -> None:
    sock = FakeSocket([(b"reply", (SERVER, DNS_PORT))])
    assert adapter(sock).exchange(SERVER, b"query") == b"reply"
    assert sock.sent == [(b"query", (SERVER, DNS_PORT))]
    assert sock.timeouts[0] == QUERY_TIMEOUT
    assert sock.closed


def test_ignora_datagrama_de_outra_origem_e_aceita_o_do_servidor() -> None:
    sock = FakeSocket([(b"spoof", ("203.0.113.9", DNS_PORT)), (b"reply", (SERVER, DNS_PORT))])
    assert adapter(sock).exchange(SERVER, b"query") == b"reply"


def test_datagramas_alheios_ate_o_prazo_viram_none() -> None:
    clock = [0.0]
    foreign = [(b"spoof", ("203.0.113.9", DNS_PORT))] * 10
    sock = FakeSocket(list(foreign), clock)
    assert adapter(sock, clock).exchange(SERVER, b"query") is None
    assert clock[0] <= QUERY_TIMEOUT + 0.4
    assert all(0 < value <= QUERY_TIMEOUT for value in sock.timeouts)


@pytest.mark.parametrize("failure", [TimeoutError(), OSError("unreachable")])
def test_timeout_ou_erro_de_socket_vira_none(failure: BaseException) -> None:
    sock = FakeSocket([failure])
    assert adapter(sock).exchange(SERVER, b"query") is None
    assert sock.closed


def test_falha_ao_abrir_socket_vira_none() -> None:
    def broken() -> FakeSocket:
        raise OSError("no socket")

    assert UdpDns(open_socket=broken).exchange(SERVER, b"query") is None


def test_resolvers_le_so_linhas_nameserver_ipv4_sem_repetir() -> None:
    text = (
        "# comentario\nnameserver 127.0.0.53\noptions edns0 trust-ad\n"
        "nameserver ::1\nnameserver 1.1.1.1 extra\nnameserver 127.0.0.53\n"
        "search lan\nnameserver\nnameserver 999.1.1.1\n  nameserver 8.8.8.8\n"
    )
    assert parse_resolvers(text) == ("127.0.0.53", "1.1.1.1", "8.8.8.8")


def test_resolvers_vem_do_arquivo_configurado(tmp_path: Path) -> None:
    conf = tmp_path / "resolv.conf"
    conf.write_text("nameserver 10.0.0.1\n", encoding="utf-8")
    assert UdpDns(resolv_conf=conf).resolvers() == ("10.0.0.1",)
    assert UdpDns(resolv_conf=tmp_path / "missing").resolvers() == ()


def test_addresses_pede_ipv4_udp_e_remove_repetidos() -> None:
    calls: list[str] = []

    def resolve(host: str) -> list[object]:
        calls.append(host)
        info = (socket.AF_INET, socket.SOCK_DGRAM, 17, "", ("192.0.2.4", DNS_PORT))
        other = (socket.AF_INET, socket.SOCK_DGRAM, 17, "", ("192.0.2.5", DNS_PORT))
        return [info, other, info]

    assert UdpDns(resolve=resolve).addresses("ns-1.example") == ("192.0.2.4", "192.0.2.5")
    assert calls == ["ns-1.example"]


def test_addresses_com_erro_de_resolucao_vira_vazio() -> None:
    def resolve(host: str) -> list[object]:
        raise socket.gaierror("no name")

    assert UdpDns(resolve=resolve).addresses("ns-1.example") == ()


def test_lookup_padrao_pede_ipv4_udp_na_porta_53(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[tuple[object, ...]] = []
    monkeypatch.setattr(socket, "getaddrinfo", lambda *args: seen.append(args) or [])
    assert _login_dns_udp.lookup("ns-1.example") == []
    assert seen == [("ns-1.example", DNS_PORT, socket.AF_INET, socket.SOCK_DGRAM)]


def test_socket_padrao_e_udp_ipv4() -> None:
    with open_udp() as sock:
        assert (sock.family, sock.type) == (socket.AF_INET, socket.SOCK_DGRAM)


def test_padroes_do_adaptador_real() -> None:
    real = UdpDns()
    assert real.timeout == QUERY_TIMEOUT == 1.0
    assert real.resolv_conf == Path("/etc/resolv.conf")
    assert real.open_socket is open_udp
    assert real.resolve is _login_dns_udp.lookup
    assert real.clock is time.monotonic


def test_conftest_proibe_udp_e_resolucao_reais() -> None:
    with pytest.raises(pytest.fail.Exception, match="real_io_forbidden"):
        socket.getaddrinfo("localhost", DNS_PORT)
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        with pytest.raises(pytest.fail.Exception, match="real_io_forbidden"):
            sock.sendto(b"x", ("127.0.0.1", 9))
        with pytest.raises(pytest.fail.Exception, match="real_io_forbidden"):
            sock.recvfrom(16)
