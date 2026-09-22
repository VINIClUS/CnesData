"""Testes das rotas de login local, logout e identidade por cookie de sessão."""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from slowapi.errors import RateLimitExceeded

from central_api.ratelimit import limiter, rate_limit_handler
from central_api.routes import local_auth
from cnes_infra.auth.local_auth import (
    AuthenticatedPrincipal,
    AuthenticationRejected,
    AuthRejectionCode,
)

_PASSWORD = "correct-horse-battery"  # noqa: S105
_PRINCIPAL = AuthenticatedPrincipal(
    user_id="user-1",
    email="gestor@epitacio.sp.gov.br",
    tenant_id="354130",
    role="gestor",
)


class _FakeService:
    def __init__(self) -> None:
        self.issued_for: AuthenticatedPrincipal | None = None
        self.revoked_token: str | None = None
        self._sessions: dict[str, AuthenticatedPrincipal] = {}
        self._reject_auth = False
        self._reject_session = False

    def reject_authenticate(self) -> None:
        self._reject_auth = True

    def reject_session(self) -> None:
        self._reject_session = True

    def authenticate(self, email: str, password: str) -> AuthenticatedPrincipal:
        if self._reject_auth:
            raise AuthenticationRejected(AuthRejectionCode.INVALID_CREDENTIALS)
        return _PRINCIPAL

    def issue_session(self, principal: AuthenticatedPrincipal) -> str:
        self.issued_for = principal
        token = "raw-session-token"  # noqa: S105
        self._sessions[token] = principal
        return token

    def resolve_session(self, token: str) -> AuthenticatedPrincipal:
        if self._reject_session or token not in self._sessions:
            raise AuthenticationRejected(AuthRejectionCode.SESSION_INVALID)
        return self._sessions[token]

    def revoke_session(self, token: str) -> None:
        self.revoked_token = token
        self._sessions.pop(token, None)


def _build(
    service: _FakeService | None,
    *,
    base_url: str = "http://testserver",
    peer: str = "testclient",
) -> TestClient:
    app = FastAPI()
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, rate_limit_handler)
    app.include_router(local_auth.router)
    if service is not None:
        app.dependency_overrides[local_auth.get_local_auth_service] = lambda: service
    return TestClient(app, base_url=base_url, client=(peer, 50000))


@pytest.fixture(autouse=True)
def _reset_limiter() -> None:
    limiter.reset()


def test_login_valido_retorna_principal_e_cookie_de_sessao() -> None:
    client = _build(_FakeService())

    response = client.post(
        "/api/v1/auth/local/login", json={"email": "g@x.com", "password": _PASSWORD}
    )

    assert response.status_code == 200
    assert response.json() == {
        "user_id": "user-1",
        "email": "gestor@epitacio.sp.gov.br",
        "tenant_id": "354130",
        "role": "gestor",
    }
    cookie_header = response.headers["set-cookie"]
    assert "HttpOnly" in cookie_header
    assert "samesite=lax" in cookie_header.lower()
    assert "Path=/" in cookie_header
    assert f"Max-Age={local_auth.SESSION_TTL_SECONDS}" in cookie_header


def test_login_nao_aceita_tenant_do_browser() -> None:
    service = _FakeService()
    client = _build(service)

    response = client.post(
        "/api/v1/auth/local/login",
        json={"email": "g@x.com", "password": _PASSWORD, "tenant_id": "354130"},
    )

    assert response.status_code == 422
    assert service.issued_for is None


def test_hash_e_salt_nunca_sao_retornados() -> None:
    client = _build(_FakeService())

    login_response = client.post(
        "/api/v1/auth/local/login", json={"email": "g@x.com", "password": _PASSWORD}
    )
    me_response = client.get("/api/v1/auth/me")

    assert set(login_response.json()) == {"user_id", "email", "tenant_id", "role"}
    assert "password_hash" not in login_response.text
    assert "salt" not in login_response.text
    assert set(me_response.json()) == {"user_id", "email", "tenant_id", "role"}
    assert "password_hash" not in me_response.text
    assert "salt" not in me_response.text


def test_login_rejeita_credenciais_invalidas() -> None:
    service = _FakeService()
    service.reject_authenticate()
    client = _build(service)

    response = client.post(
        "/api/v1/auth/local/login", json={"email": "g@x.com", "password": _PASSWORD}
    )

    assert response.status_code == 401
    assert response.json()["detail"] == "invalid_credentials"


def test_login_rejeita_senha_fora_do_tamanho() -> None:
    client = _build(_FakeService())

    response = client.post(
        "/api/v1/auth/local/login", json={"email": "g@x.com", "password": "curta"}
    )

    assert response.status_code == 422


def test_me_retorna_principal_da_sessao() -> None:
    client = _build(_FakeService())
    client.post("/api/v1/auth/local/login", json={"email": "g@x.com", "password": _PASSWORD})

    response = client.get("/api/v1/auth/me")

    assert response.status_code == 200
    assert response.json()["user_id"] == "user-1"


def test_me_rejeita_requisicao_sem_cookie() -> None:
    client = _build(_FakeService())

    response = client.get("/api/v1/auth/me")

    assert response.status_code == 401
    assert response.json()["detail"] == "session_required"


def test_me_rejeita_sessao_invalida() -> None:
    service = _FakeService()
    client = _build(service)
    client.post("/api/v1/auth/local/login", json={"email": "g@x.com", "password": _PASSWORD})
    service.reject_session()

    response = client.get("/api/v1/auth/me")

    assert response.status_code == 401
    assert response.json()["detail"] == "session_invalid"


def test_logout_revoga_sessao_e_limpa_cookie() -> None:
    service = _FakeService()
    client = _build(service)
    client.post("/api/v1/auth/local/login", json={"email": "g@x.com", "password": _PASSWORD})

    response = client.post("/api/v1/auth/logout")

    assert response.status_code == 204
    assert service.revoked_token == "raw-session-token"  # noqa: S105
    assert 'cnesdata_session=""' in response.headers["set-cookie"]


def test_logout_sem_cookie_e_idempotente() -> None:
    service = _FakeService()
    client = _build(service)

    response = client.post("/api/v1/auth/logout")

    assert response.status_code == 204
    assert service.revoked_token is None


def test_rotas_falham_fechadas_sem_composicao() -> None:
    client = _build(None)

    login = client.post(
        "/api/v1/auth/local/login", json={"email": "g@x.com", "password": _PASSWORD}
    )
    logout = client.post("/api/v1/auth/logout")

    assert login.status_code == 503
    assert login.json()["detail"] == "local_auth_not_configured"
    assert logout.status_code == 503


def test_me_falha_fechada_sem_composicao_com_cookie_presente() -> None:
    app = FastAPI()
    app.include_router(local_auth.router)
    client = TestClient(app, cookies={local_auth.SESSION_COOKIE_NAME: "any-token"})

    response = client.get("/api/v1/auth/me")

    assert response.status_code == 503
    assert response.json()["detail"] == "local_auth_not_configured"


def test_cookie_de_sessao_e_secure_sob_https() -> None:
    client = _build(_FakeService(), base_url="https://testserver")

    response = client.post(
        "/api/v1/auth/local/login", json={"email": "g@x.com", "password": _PASSWORD}
    )

    assert "Secure" in response.headers["set-cookie"]


def test_login_com_rate_limit_excedido() -> None:
    client = _build(_FakeService())

    for _ in range(5):
        client.post(
            "/api/v1/auth/local/login", json={"email": "g@x.com", "password": _PASSWORD}
        )

    response = client.post(
        "/api/v1/auth/local/login", json={"email": "g@x.com", "password": _PASSWORD}
    )

    assert response.status_code == 429
    assert response.json()["detail"] == "rate_limited"


def test_login_rate_limit_separa_clientes_atras_de_proxy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TRUSTED_PROXY_CIDRS", "172.16.0.0/12")
    client = _build(_FakeService(), peer="172.20.0.2")

    for _ in range(5):
        client.post(
            "/api/v1/auth/local/login",
            json={"email": "g@x.com", "password": _PASSWORD},
            headers={"X-Forwarded-For": "203.0.113.1, 172.20.0.2"},
        )

    response = client.post(
        "/api/v1/auth/local/login",
        json={"email": "g@x.com", "password": _PASSWORD},
        headers={"X-Forwarded-For": "203.0.113.2, 172.20.0.2"},
    )

    assert response.status_code == 200


def test_login_rate_limit_nao_separa_clientes_sem_proxy_confiavel() -> None:
    """Sem TRUSTED_PROXY_CIDRS, XFF é ignorado — os dois IPs forjados compartilham o limite."""
    client = _build(_FakeService(), peer="172.20.0.2")

    for _ in range(5):
        client.post(
            "/api/v1/auth/local/login",
            json={"email": "g@x.com", "password": _PASSWORD},
            headers={"X-Forwarded-For": "203.0.113.1, 172.20.0.2"},
        )

    response = client.post(
        "/api/v1/auth/local/login",
        json={"email": "g@x.com", "password": _PASSWORD},
        headers={"X-Forwarded-For": "203.0.113.2, 172.20.0.2"},
    )

    assert response.status_code == 429


def test_cookie_de_sessao_secure_com_x_forwarded_proto_https(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TRUST_X_FORWARDED_PROTO", "true")
    client = _build(_FakeService())

    response = client.post(
        "/api/v1/auth/local/login",
        json={"email": "g@x.com", "password": _PASSWORD},
        headers={"X-Forwarded-Proto": "https"},
    )

    assert "Secure" in response.headers["set-cookie"]


def test_cookie_sem_secure_com_x_forwarded_proto_http(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TRUST_X_FORWARDED_PROTO", "true")
    client = _build(_FakeService())

    response = client.post(
        "/api/v1/auth/local/login",
        json={"email": "g@x.com", "password": _PASSWORD},
        headers={"X-Forwarded-Proto": "http"},
    )

    assert "Secure" not in response.headers["set-cookie"]


def test_cookie_seguro_nao_confia_x_forwarded_proto_por_padrao(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cookie não deve ter Secure por padrão mesmo com X-Forwarded-Proto: https sem configuração."""
    monkeypatch.delenv("TRUST_X_FORWARDED_PROTO", raising=False)
    client = _build(_FakeService())

    response = client.post(
        "/api/v1/auth/local/login",
        json={"email": "g@x.com", "password": _PASSWORD},
        headers={"X-Forwarded-Proto": "https"},
    )

    assert "Secure" not in response.headers["set-cookie"]
