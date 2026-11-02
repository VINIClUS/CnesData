"""Adaptadores reais do login finos, ligados somente ao perfil cognito-login."""

import logging
from dataclasses import dataclass, field, replace

import httpx
import pytest

from tests.integration.aws_capability import _production as production
from tests.integration.aws_capability._login_dns_udp import UdpDns
from tests.integration.aws_capability._login_listener import (
    LoopbackListener,
    listener_factory,
    open_listener,
    parse_callback,
)
from tests.integration.aws_capability._login_ports import LISTEN_PORT, Callback
from tests.integration.aws_capability._production_testing import FakeClock
from tests.integration.aws_capability._testing import Lab
from tests.integration.aws_capability.contracts import ServiceFailure
from tests.integration.aws_capability.test_production import environment, processes


@dataclass
class StubServer:
    host: str
    port: int
    stopped: bool = False
    closed: bool = False
    serving: bool = False
    exchange: object = field(default=None, repr=False)

    def serve_forever(self) -> None:
        self.serving = True

    def shutdown(self) -> None:
        if not self.serving:
            raise RuntimeError("shutdown=blocks_forever_when_not_serving")
        self.stopped = True

    def server_close(self) -> None:
        self.closed = True


def test_callback_so_existe_em_caminho_exato_e_so_leva_code_state_e_error() -> None:
    assert parse_callback("/callback?code=C&state=S&extra=1") == Callback("C", "S", None)
    assert parse_callback("/callback?error=access_denied&state=S") == Callback(
        None, "S", "access_denied"
    )
    assert parse_callback("/callback") == Callback(None, None, None)
    assert parse_callback("/outro?code=C") is None
    assert parse_callback("/callback/x?code=C") is None


def test_error_longo_ou_com_url_vira_rotulo_curto() -> None:
    callback = parse_callback("/callback?error=http%3A%2F%2Fx%3Fa%3D1")
    assert callback is not None
    assert callback.error == "other"


def test_listener_liga_127_0_0_1_e_ipv6_loopback_na_porta_fixa() -> None:
    bound: list[tuple[str, int]] = []

    def bind(host: str, port: int) -> StubServer:
        bound.append((host, port))
        return StubServer(host, port)

    listener = open_listener(bind)
    assert bound == [("127.0.0.1", LISTEN_PORT), ("::1", LISTEN_PORT)]
    for thread in listener.threads:
        thread.join(timeout=2)
    listener.close()


def test_falha_ao_ligar_um_dos_enderecos_fecha_o_outro_e_vira_service_failure() -> None:
    servers: list[StubServer] = []

    def bind(host: str, port: int) -> StubServer:
        if host == "::1":
            raise OSError("detalhe")
        servers.append(StubServer(host, port))
        return servers[-1]

    with pytest.raises(ServiceFailure) as error:
        listener_factory(bind)()
    assert error.value.args == ("probe=listener_unavailable",)
    assert servers[0].closed is True
    assert servers[0].stopped is False


def test_espera_sem_chamada_devolve_none_e_close_para_os_servidores() -> None:
    servers = [StubServer("a", 1), StubServer("b", 1)]
    for server in servers:
        server.serving = True
    listener = LoopbackListener(servers, [])
    assert listener.wait(0.01) is None
    listener.close()
    assert all(server.stopped and server.closed for server in servers)


def test_respostas_seguem_a_ordem_redirect_depois_pagina() -> None:
    listener = LoopbackListener([], [])
    listener.redirect("https://x/authorize")
    listener.finish("fim")
    assert listener.exchange.answers.get_nowait() == (302, "https://x/authorize", "")
    assert listener.exchange.answers.get_nowait() == (200, None, "fim")


def test_cliente_http_nao_loga_urls_com_query(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)
    transport = httpx.MockTransport(lambda request: httpx.Response(200, json={}))
    client = production.build_http_client(transport)
    client.get("https://x/oauth2/authorize?code=SEGREDO&state=ESTADO")
    assert "SEGREDO" not in caplog.text
    assert "ESTADO" not in caplog.text
    assert client.follow_redirects is False


def test_ports_reais_so_ligam_http_listener_e_dns_no_perfil_de_login(lab: Lab) -> None:
    boundary, *_ = processes()
    login = production.build_ports(
        replace(lab.spec, probe="cognito-login"), environment(), boundary, FakeClock()
    )
    other = production.build_ports(
        replace(lab.spec, probe="ddb-conditions"), environment(), boundary, FakeClock()
    )
    assert isinstance(login.services, production.AwsServices)
    assert isinstance(login.services.ports.http, httpx.Client)
    assert callable(login.services.ports.listener)
    assert isinstance(other.services, production.AwsServices)
    assert other.services.ports.http is None
    assert other.services.ports.listener is None
    assert other.services.ports.dns is None
    assert login.services.ports.dns == UdpDns()
