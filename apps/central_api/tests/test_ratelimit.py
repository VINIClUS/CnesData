"""Tests for the proxy-aware rate-limit key."""
from unittest.mock import MagicMock

from central_api.ratelimit import client_ip


def _request(xff: str | None, host: str = "10.0.0.9") -> MagicMock:
    req = MagicMock()
    req.headers = {} if xff is None else {"X-Forwarded-For": xff}
    req.client.host = host
    return req


def test_usa_ultimo_hop_do_x_forwarded_for() -> None:
    assert client_ip(_request("1.1.1.1, 203.0.113.7")) == "203.0.113.7"


def test_usa_endereco_do_socket_sem_header() -> None:
    assert client_ip(_request(None)) == "10.0.0.9"


def test_header_vazio_cai_no_socket() -> None:
    assert client_ip(_request(" , ")) == "10.0.0.9"


def test_handler_429_inclui_retry_after_da_janela() -> None:
    from slowapi.errors import RateLimitExceeded

    from central_api.ratelimit import rate_limit_handler

    exc = RateLimitExceeded.__new__(RateLimitExceeded)
    exc.limit = MagicMock()
    exc.limit.limit.get_expiry.return_value = 60
    resp = rate_limit_handler(_request(None), exc)
    assert resp.status_code == 429
    assert resp.headers["Retry-After"] == "60"
