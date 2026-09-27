"""Testes do OidcVerifier — discovery genérica, cache de JWKS e claims estritos."""
import time
from collections.abc import Iterator
from typing import Any

import httpx
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from jose import jwt as jose_jwt
from jose.utils import base64url_encode

from cnes_infra.auth.jwt import TokenInvalid
from cnes_infra.auth.oidc import OidcPrincipal, OidcVerifier

ISSUER = "https://idp.example.test"
AUDIENCE = "cnesdata-dashboard"
DISCOVERY = f"{ISSUER}/.well-known/openid-configuration"
JWKS = f"{ISSUER}/keys"


def _b64uint(value: int) -> str:
    raw = value.to_bytes((value.bit_length() + 7) // 8, "big")
    return base64url_encode(raw).decode().rstrip("=")


def _keypair(kid: str) -> tuple[dict[str, Any], str]:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    numbers = key.public_key().public_numbers()
    jwk = {
        "kty": "RSA", "kid": kid, "use": "sig", "alg": "RS256",
        "n": _b64uint(numbers.n), "e": _b64uint(numbers.e),
    }
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()
    return jwk, pem


_K1 = _keypair("k1")
_K2 = _keypair("k2")


def _claims(**overrides: Any) -> dict[str, Any]:
    now = int(time.time())
    claims = {
        "iss": ISSUER, "aud": AUDIENCE, "sub": "user-1",
        "email": "gestor@example.test", "name": "Gestor",
        "exp": now + 300, "iat": now,
    }
    claims.update(overrides)
    return {name: value for name, value in claims.items() if value is not None}


def _signed_token(
    pem: str = _K1[1], kid: str | None = "k1", extra: dict[str, Any] | None = None,
) -> str:
    headers = {} if kid is None else {"kid": kid}
    return jose_jwt.encode(_claims(**(extra or {})), pem, algorithm="RS256", headers=headers)


def _mock_discovery(httpx_mock, **overrides: Any) -> None:
    body = {"issuer": ISSUER, "jwks_uri": JWKS} | overrides
    httpx_mock.add_response(url=DISCOVERY, json=body)


def _mock_provider(httpx_mock, *keys: dict[str, Any]) -> None:
    _mock_discovery(httpx_mock)
    httpx_mock.add_response(url=JWKS, json={"keys": list(keys or (_K1[0],))})


@pytest.fixture
def client() -> Iterator[httpx.Client]:
    with httpx.Client() as http:
        yield http


def _verifier(client: httpx.Client, **kwargs: Any) -> OidcVerifier:
    return OidcVerifier(issuer=kwargs.pop("issuer", ISSUER), audience=AUDIENCE, client=client,
                        **kwargs)


def _requests_to(httpx_mock, url: str) -> int:
    return sum(1 for request in httpx_mock.get_requests() if str(request.url) == url)


def test_descobre_jwks_uri_e_valida_token_generico(httpx_mock, client) -> None:
    _mock_provider(httpx_mock)
    principal = _verifier(client).verify(_signed_token())
    assert principal == OidcPrincipal(
        issuer=ISSUER, subject="user-1", email="gestor@example.test", display_name="Gestor",
    )


def test_rejeita_discovery_com_issuer_divergente(httpx_mock, client) -> None:
    _mock_discovery(httpx_mock, issuer="https://evil.example")
    with pytest.raises(TokenInvalid, match=r"^discovery_issuer_mismatch$"):
        _verifier(client).verify(_signed_token())


def test_nao_interpreta_claim_de_tenant(httpx_mock, client) -> None:
    _mock_provider(httpx_mock)
    principal = _verifier(client).verify(_signed_token(extra={"tenant_id": "tenant-b"}))
    assert not hasattr(principal, "tenant_id")
    assert "tenant-b" not in repr(principal)


def test_preserva_issuer_literal_com_barra_final(httpx_mock, client) -> None:
    issuer = f"{ISSUER}/"
    _mock_discovery(httpx_mock, issuer=issuer)
    httpx_mock.add_response(url=JWKS, json={"keys": [_K1[0]]})
    principal = _verifier(client, issuer=issuer).verify(_signed_token(extra={"iss": issuer}))
    assert principal.issuer == issuer


def test_rejeita_jwks_uri_sem_https(httpx_mock, client) -> None:
    _mock_discovery(httpx_mock, jwks_uri="http://idp.example.test/keys")
    with pytest.raises(TokenInvalid, match=r"^jwks_uri_not_https$"):
        _verifier(client).verify(_signed_token())


def test_rejeita_discovery_sem_jwks_uri(httpx_mock, client) -> None:
    _mock_discovery(httpx_mock, jwks_uri=None)
    with pytest.raises(TokenInvalid, match=r"^discovery_no_jwks_uri$"):
        _verifier(client).verify(_signed_token())


def test_rejeita_token_expirado(httpx_mock, client) -> None:
    _mock_provider(httpx_mock)
    token = _signed_token(extra={"exp": int(time.time()) - 10})
    with pytest.raises(TokenInvalid, match=r"^expired$"):
        _verifier(client).verify(token)


def test_rejeita_audience_errada(httpx_mock, client) -> None:
    _mock_provider(httpx_mock)
    with pytest.raises(TokenInvalid, match=r"^audience$"):
        _verifier(client).verify(_signed_token(extra={"aud": "outro-app"}))


def test_rejeita_issuer_errado(httpx_mock, client) -> None:
    _mock_provider(httpx_mock)
    with pytest.raises(TokenInvalid, match=r"^issuer$"):
        _verifier(client).verify(_signed_token(extra={"iss": "https://evil.example"}))


@pytest.mark.parametrize("claim", ["exp", "iat"])
def test_rejeita_token_sem_claim_temporal(httpx_mock, client, claim: str) -> None:
    _mock_provider(httpx_mock)
    with pytest.raises(TokenInvalid, match=r"^claims$"):
        _verifier(client).verify(_signed_token(extra={claim: None}))


def test_rejeita_token_ainda_nao_valido(httpx_mock, client) -> None:
    _mock_provider(httpx_mock)
    token = _signed_token(extra={"nbf": int(time.time()) + 3600})
    with pytest.raises(TokenInvalid, match=r"^claims$"):
        _verifier(client).verify(token)


@pytest.mark.parametrize("subject", [None, "", "   "])
def test_rejeita_sub_ausente_ou_vazio(httpx_mock, client, subject: str | None) -> None:
    _mock_provider(httpx_mock)
    with pytest.raises(TokenInvalid, match=r"^subject_required$"):
        _verifier(client).verify(_signed_token(extra={"sub": subject}))


def test_rejeita_assinatura_de_outra_chave(httpx_mock, client) -> None:
    _mock_provider(httpx_mock)
    with pytest.raises(TokenInvalid, match=r"^signature$"):
        _verifier(client).verify(_signed_token(pem=_K2[1], kid="k1"))


def test_rejeita_header_malformado(client) -> None:
    with pytest.raises(TokenInvalid, match=r"^malformed_header$"):
        _verifier(client).verify("not-a-jwt")


def test_rejeita_token_sem_kid(client) -> None:
    with pytest.raises(TokenInvalid, match=r"^missing_kid$"):
        _verifier(client).verify(_signed_token(kid=None))


def test_rejeita_algoritmo_hs256(client) -> None:
    token = jose_jwt.encode(_claims(), "segredo", algorithm="HS256", headers={"kid": "k1"})
    with pytest.raises(TokenInvalid, match=r"^unsupported_algorithm$"):
        _verifier(client).verify(token)


def test_rejeita_algoritmo_none(client) -> None:
    header = base64url_encode(b'{"alg":"none","kid":"k1"}').decode()
    body = base64url_encode(b'{"sub":"user-1"}').decode()
    with pytest.raises(TokenInvalid, match=r"^unsupported_algorithm$"):
        _verifier(client).verify(f"{header}.{body}.")


def test_recarrega_jwks_uma_vez_para_kid_desconhecido(httpx_mock, client) -> None:
    _mock_provider(httpx_mock)
    httpx_mock.add_response(url=JWKS, json={"keys": [_K1[0], _K2[0]]})
    verifier = _verifier(client)
    verifier.verify(_signed_token())
    principal = verifier.verify(_signed_token(pem=_K2[1], kid="k2"))
    assert principal.subject == "user-1"
    assert _requests_to(httpx_mock, JWKS) == 2
    assert _requests_to(httpx_mock, DISCOVERY) == 1


def test_rejeita_kid_desconhecido_apos_um_refresh(httpx_mock, client) -> None:
    _mock_provider(httpx_mock)
    httpx_mock.add_response(url=JWKS, json={"keys": [_K1[0]]})
    with pytest.raises(TokenInvalid, match=r"^unknown_kid$"):
        _verifier(client).verify(_signed_token(pem=_K2[1], kid="k2"))
    assert _requests_to(httpx_mock, JWKS) == 2


def test_falha_quando_discovery_inacessivel_sem_cache(httpx_mock, client) -> None:
    httpx_mock.add_exception(httpx.ConnectError("boom"), url=DISCOVERY)
    with pytest.raises(TokenInvalid, match=r"^discovery_unreachable$"):
        _verifier(client).verify(_signed_token())


def test_falha_quando_jwks_inacessivel_sem_cache(httpx_mock, client) -> None:
    _mock_discovery(httpx_mock)
    httpx_mock.add_response(url=JWKS, status_code=503, text=f"erro em {JWKS}")
    with pytest.raises(TokenInvalid, match=r"^jwks_unreachable$"):
        _verifier(client).verify(_signed_token())


def test_usa_cache_fresco_sem_nova_requisicao(httpx_mock, client) -> None:
    _mock_provider(httpx_mock)
    verifier = _verifier(client)
    verifier.verify(_signed_token())
    verifier.verify(_signed_token())
    assert _requests_to(httpx_mock, JWKS) == 1


def test_usa_cache_quando_jwks_inacessivel(httpx_mock, client) -> None:
    _mock_provider(httpx_mock)
    httpx_mock.add_exception(httpx.ConnectError("boom"), url=JWKS)
    verifier = _verifier(client, cache_ttl_seconds=0)
    verifier.verify(_signed_token())
    principal = verifier.verify(_signed_token())
    assert principal.subject == "user-1"


def test_erro_nao_expoe_url_token_nem_causa(httpx_mock, client) -> None:
    _mock_discovery(httpx_mock)
    httpx_mock.add_response(url=JWKS, status_code=500, text="corpo-secreto")
    token = _signed_token()
    with pytest.raises(TokenInvalid) as exc:
        _verifier(client).verify(token)
    message = str(exc.value)
    assert ISSUER not in message
    assert token not in message
    assert "corpo-secreto" not in message
    assert exc.value.__cause__ is None
    assert exc.value.__suppress_context__


@pytest.mark.parametrize("value", [None, "", "  ", 123])
def test_ignora_email_e_nome_invalidos(httpx_mock, client, value: Any) -> None:
    _mock_provider(httpx_mock)
    principal = _verifier(client).verify(_signed_token(extra={"email": value, "name": value}))
    assert principal.email is None
    assert principal.display_name is None
