"""Tests for the proxy-aware rate-limit key."""
from unittest.mock import MagicMock

import pytest

from central_api.ratelimit import client_ip, is_trusted_proxy_peer


def _request(xff: str | None, host: str = "10.0.0.9") -> MagicMock:
    req = MagicMock()
    req.headers = {} if xff is None else {"X-Forwarded-For": xff}
    req.client.host = host
    return req


def test_ignora_x_forwarded_for_de_peer_nao_confiavel(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TRUSTED_PROXY_CIDRS", raising=False)
    assert client_ip(_request("1.1.1.1, 203.0.113.7")) == "10.0.0.9"


def test_usa_ultimo_hop_nao_confiavel_atras_de_proxy(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRUSTED_PROXY_CIDRS", "172.16.0.0/12,10.0.0.0/8")
    req = _request("203.0.113.7, 172.20.0.5, 172.20.0.2", host="172.20.0.2")
    assert client_ip(req) == "203.0.113.7"


def test_ignora_xff_quando_allowlist_vazia(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRUSTED_PROXY_CIDRS", "")
    req = _request("203.0.113.7", host="172.20.0.2")
    assert client_ip(req) == "172.20.0.2"


def test_hop_malformado_encerra_a_varredura(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRUSTED_PROXY_CIDRS", "172.16.0.0/12")
    req = _request("not-an-ip, 172.20.0.5, 172.20.0.2", host="172.20.0.2")
    assert client_ip(req) == "not-an-ip"


def test_ignora_entrada_de_cidr_malformada_na_allowlist(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRUSTED_PROXY_CIDRS", "not-a-cidr, 172.16.0.0/12")
    req = _request("203.0.113.7, 172.20.0.2", host="172.20.0.2")
    assert client_ip(req) == "203.0.113.7"


def test_ignora_xff_quando_peer_nao_e_ip(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRUSTED_PROXY_CIDRS", "172.16.0.0/12")
    req = _request("203.0.113.7", host="testclient")
    assert client_ip(req) == "testclient"


def test_usa_endereco_do_socket_sem_header(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TRUSTED_PROXY_CIDRS", raising=False)
    assert client_ip(_request(None)) == "10.0.0.9"


def test_header_vazio_cai_no_socket(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRUSTED_PROXY_CIDRS", "172.16.0.0/12")
    assert client_ip(_request(" , ", host="172.20.0.2")) == "172.20.0.2"


def test_handler_429_inclui_retry_after_da_janela() -> None:
    from slowapi.errors import RateLimitExceeded

    from central_api.ratelimit import rate_limit_handler

    exc = RateLimitExceeded.__new__(RateLimitExceeded)
    exc.limit = MagicMock()
    exc.limit.limit.get_expiry.return_value = 60
    resp = rate_limit_handler(_request(None), exc)
    assert resp.status_code == 429
    assert resp.headers["Retry-After"] == "60"


def test_is_trusted_proxy_peer_aceita_peer_na_allowlist(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRUSTED_PROXY_CIDRS", "172.16.0.0/12")
    assert is_trusted_proxy_peer(_request(None, host="172.20.0.2")) is True


def test_is_trusted_proxy_peer_rejeita_peer_fora_da_allowlist(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TRUSTED_PROXY_CIDRS", "172.16.0.0/12")
    assert is_trusted_proxy_peer(_request(None, host="203.0.113.7")) is False


def test_is_trusted_proxy_peer_rejeita_tudo_sem_allowlist(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TRUSTED_PROXY_CIDRS", raising=False)
    assert is_trusted_proxy_peer(_request(None, host="172.20.0.2")) is False
