"""Tests for JWKSValidator — fetch + cache + verify."""
import time
from typing import Any

import httpx
import pytest
from jose import jwt as jose_jwt
from jose.utils import base64url_encode

from cnes_infra.auth.jwt import JWKSValidator, TokenInvalid

_ISS = "https://kc.local/realms/cnesdata"
_AUD = "cnesdata-dashboard"


def _make_keypair() -> tuple[dict[str, Any], dict[str, Any]]:
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pub = key.public_key()
    nums = pub.public_numbers()

    def _b64uint(v: int) -> str:
        b = v.to_bytes((v.bit_length() + 7) // 8, "big")
        return base64url_encode(b).decode().rstrip("=")

    jwk = {
        "kty": "RSA", "kid": "k1", "use": "sig", "alg": "RS256",
        "n": _b64uint(nums.n), "e": _b64uint(nums.e),
    }
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()
    return jwk, {"kid": "k1", "pem": pem}


def _make_token(
    priv: dict, *, iss: str = _ISS, aud: str = _AUD,
    sub: str = "user-1", exp_offset: int = 300,
) -> str:
    return jose_jwt.encode(
        {
            "iss": iss, "aud": aud, "sub": sub, "email": "g@m",
            "exp": int(time.time()) + exp_offset,
            "iat": int(time.time()),
        },
        priv["pem"], algorithm="RS256", headers={"kid": priv["kid"]},
    )


def test_valida_token_assinado_corretamente(httpx_mock) -> None:
    jwk, priv = _make_keypair()
    httpx_mock.add_response(
        url=f"{_ISS}/.well-known/jwks.json", json={"keys": [jwk]},
    )
    v = JWKSValidator(issuer=_ISS, audience=_AUD)
    token = _make_token(priv)
    claims = v.verify(token)
    assert claims["sub"] == "user-1"
    assert claims["email"] == "g@m"


def test_rejeita_token_expirado(httpx_mock) -> None:
    jwk, priv = _make_keypair()
    httpx_mock.add_response(
        url=f"{_ISS}/.well-known/jwks.json", json={"keys": [jwk]},
    )
    v = JWKSValidator(issuer=_ISS, audience=_AUD)
    token = _make_token(priv, exp_offset=-10)
    with pytest.raises(TokenInvalid, match="expired"):
        v.verify(token)


def test_rejeita_audience_errada(httpx_mock) -> None:
    jwk, priv = _make_keypair()
    httpx_mock.add_response(
        url=f"{_ISS}/.well-known/jwks.json", json={"keys": [jwk]},
    )
    v = JWKSValidator(issuer=_ISS, audience=_AUD)
    token = _make_token(priv, aud="other-app")
    with pytest.raises(TokenInvalid, match="audience"):
        v.verify(token)


def test_rejeita_issuer_errado(httpx_mock) -> None:
    jwk, priv = _make_keypair()
    httpx_mock.add_response(
        url=f"{_ISS}/.well-known/jwks.json", json={"keys": [jwk]},
    )
    v = JWKSValidator(issuer=_ISS, audience=_AUD)
    token = _make_token(priv, iss="https://evil.local")
    with pytest.raises(TokenInvalid, match="issuer"):
        v.verify(token)


def test_cacheia_jwks_por_ttl(httpx_mock) -> None:
    jwk, priv = _make_keypair()
    httpx_mock.add_response(
        url=f"{_ISS}/.well-known/jwks.json", json={"keys": [jwk]},
    )
    v = JWKSValidator(issuer=_ISS, audience=_AUD, jwks_ttl_seconds=60)
    v.verify(_make_token(priv))
    v.verify(_make_token(priv))
    assert len(httpx_mock.get_requests()) == 1


def test_rejeita_token_com_kid_desconhecido(httpx_mock) -> None:
    jwk, _priv = _make_keypair()
    _other_jwk, other_priv = _make_keypair()
    other_priv["kid"] = "k2"
    httpx_mock.add_response(
        url=f"{_ISS}/.well-known/jwks.json", json={"keys": [jwk]},
        is_reusable=True,
    )
    v = JWKSValidator(issuer=_ISS, audience=_AUD)
    token = _make_token(other_priv)
    with pytest.raises(TokenInvalid, match="unknown_kid"):
        v.verify(token)


def test_rejeita_token_com_header_malformado() -> None:
    v = JWKSValidator(issuer=_ISS, audience=_AUD)
    with pytest.raises(TokenInvalid, match="malformed_header"):
        v.verify("not-a-jwt")


def test_rejeita_token_sem_kid(httpx_mock) -> None:
    _jwk, priv = _make_keypair()
    v = JWKSValidator(issuer=_ISS, audience=_AUD)
    token = jose_jwt.encode(
        {"iss": _ISS, "aud": _AUD, "sub": "s", "exp": int(time.time()) + 60},
        priv["pem"], algorithm="RS256",
    )
    with pytest.raises(TokenInvalid, match="missing_kid"):
        v.verify(token)


def test_rejeita_assinatura_invalida(httpx_mock) -> None:
    jwk, _priv = _make_keypair()
    _other_jwk, other_priv = _make_keypair()
    httpx_mock.add_response(
        url=f"{_ISS}/.well-known/jwks.json", json={"keys": [jwk]},
        is_reusable=True,
    )
    v = JWKSValidator(issuer=_ISS, audience=_AUD)
    token = _make_token(other_priv)
    with pytest.raises(TokenInvalid, match="signature"):
        v.verify(token)


def test_rejeita_claim_nbf_no_futuro(httpx_mock) -> None:
    jwk, priv = _make_keypair()
    httpx_mock.add_response(
        url=f"{_ISS}/.well-known/jwks.json", json={"keys": [jwk]},
    )
    v = JWKSValidator(issuer=_ISS, audience=_AUD)
    token = jose_jwt.encode(
        {
            "iss": _ISS, "aud": _AUD, "sub": "s",
            "exp": int(time.time()) + 600,
            "nbf": int(time.time()) + 3600,
        },
        priv["pem"], algorithm="RS256", headers={"kid": priv["kid"]},
    )
    with pytest.raises(TokenInvalid, match="claims"):
        v.verify(token)


def test_falha_quando_jwks_inacessivel(httpx_mock) -> None:
    httpx_mock.add_exception(httpx.ConnectError("boom"))
    v = JWKSValidator(issuer=_ISS, audience=_AUD)
    _jwk, priv = _make_keypair()
    token = _make_token(priv)
    with pytest.raises(TokenInvalid, match="jwks_unreachable"):
        v.verify(token)


def test_usa_cache_quando_refetch_falha(httpx_mock) -> None:
    jwk, _priv = _make_keypair()
    _other_jwk, other_priv = _make_keypair()
    other_priv["kid"] = "k2"
    httpx_mock.add_response(
        url=f"{_ISS}/.well-known/jwks.json", json={"keys": [jwk]},
    )
    v = JWKSValidator(issuer=_ISS, audience=_AUD)
    v.verify(_make_token({"kid": "k1", "pem": _priv["pem"]}))
    httpx_mock.add_exception(httpx.ConnectError("boom"))
    with pytest.raises(TokenInvalid, match="unknown_kid"):
        v.verify(_make_token(other_priv))


def test_recarrega_jwks_apos_kid_miss(httpx_mock) -> None:
    jwk1, priv1 = _make_keypair()
    jwk2, priv2 = _make_keypair()
    jwk2["kid"] = "k2"
    priv2["kid"] = "k2"
    httpx_mock.add_response(
        url=f"{_ISS}/.well-known/jwks.json", json={"keys": [jwk1]},
    )
    httpx_mock.add_response(
        url=f"{_ISS}/.well-known/jwks.json", json={"keys": [jwk1, jwk2]},
    )
    v = JWKSValidator(issuer=_ISS, audience=_AUD, jwks_ttl_seconds=3600)
    v.verify(_make_token(priv1))
    claims = v.verify(_make_token(priv2))
    assert claims["sub"] == "user-1"
    assert len(httpx_mock.get_requests()) == 2


def test_http_status_4xx_propaga_como_token_invalid(httpx_mock) -> None:
    httpx_mock.add_response(
        url=f"{_ISS}/.well-known/jwks.json",
        status_code=503,
    )
    v = JWKSValidator(issuer=_ISS, audience=_AUD)
    _jwk, priv = _make_keypair()
    token = _make_token(priv)
    with pytest.raises(TokenInvalid, match="jwks_unreachable"):
        v.verify(token)
