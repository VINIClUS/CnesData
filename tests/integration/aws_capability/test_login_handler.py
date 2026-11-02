"""Handler real do callback sem socket: logging, 404, content-type, V6ONLY e primeiro valor."""

import socket
from io import BytesIO
from typing import Any, cast

import pytest

from tests.integration.aws_capability import _login_listener as listener
from tests.integration.aws_capability._login_chain import LOGIN_SECONDS


class StubServer:
    def __init__(self) -> None:
        self.exchange = listener.Exchange()


def serve(target: str) -> tuple[bytes, listener.Exchange]:
    server = StubServer()
    handler = listener._Handler.__new__(listener._Handler)
    handler.rfile = BytesIO(f"GET {target} HTTP/1.1\r\n\r\n".encode())
    handler.wfile = BytesIO()
    handler.client_address = ("127.0.0.1", 1)
    handler.server = cast("Any", server)
    handler.close_connection = True
    server.exchange.answers.put((200, None, "fim"))
    handler.handle_one_request()
    return handler.wfile.getvalue(), server.exchange


def test_log_nao_escreve_a_linha_de_requisicao_com_code_e_state(
    capsys: pytest.CaptureFixture[str],
) -> None:
    serve("/callback?code=CODE-CANARY&state=STATE-CANARY")
    captured = capsys.readouterr()
    assert captured.err == ""
    assert captured.out == ""


def test_caminho_desconhecido_responde_404_text_plain_sem_enfileirar() -> None:
    raw, exchange = serve("/outro?code=C")
    assert raw.startswith(b"HTTP/1.0 404")
    assert b"Content-Type: text/plain; charset=utf-8" in raw
    assert exchange.callbacks.empty()
    assert exchange.answers.qsize() == 1


def test_callback_valido_enfileira_e_responde_a_pagina_final_text_plain() -> None:
    raw, exchange = serve("/callback?code=C&state=S")
    assert raw.startswith(b"HTTP/1.0 200")
    assert b"Content-Type: text/plain; charset=utf-8" in raw
    assert raw.endswith(b"fim")
    assert exchange.callbacks.get_nowait().code == "C"


def test_redirect_envia_location_sem_corpo() -> None:
    server = StubServer()
    handler = listener._Handler.__new__(listener._Handler)
    handler.rfile = BytesIO(b"GET /callback?code=C&state=S HTTP/1.1\r\n\r\n")
    handler.wfile = BytesIO()
    handler.client_address = ("127.0.0.1", 1)
    handler.server = cast("Any", server)
    handler.close_connection = True
    server.exchange.answers.put((302, "https://x/next", ""))
    handler.handle_one_request()
    raw = handler.wfile.getvalue()
    assert raw.startswith(b"HTTP/1.0 302")
    assert b"Location: https://x/next" in raw


def test_ipv6_so_aceita_ipv6(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[Any, ...]] = []

    class Sock:
        def setsockopt(self, *args: Any) -> None:
            calls.append(args)

    server = listener._Server6.__new__(listener._Server6)
    server.socket = cast("Any", Sock())
    monkeypatch.setattr(listener.ThreadingHTTPServer, "server_bind", lambda self: None)
    server.server_bind()
    assert calls == [(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)]


def test_espera_pela_resposta_cobre_o_login_inteiro() -> None:
    assert listener.ANSWER_SECONDS > LOGIN_SECONDS


def test_primeiro_valor_de_parametro_repetido_vence() -> None:
    callback = listener.parse_callback("/callback?code=A&code=B&state=S1&state=S2")
    assert callback == listener.Callback("A", "S1", None)
