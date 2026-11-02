"""Porta HTTP, rótulos OAuth, prontidão e veredito do verifier, com repr sem segredos."""

import time
from collections.abc import Callable

import httpx
import pytest
from jose import jwt

from tests.integration.aws_capability._login_chain import Chain, make_plan
from tests.integration.aws_capability._login_claims import (
    aud_label,
    claim_names,
    make_verifier,
    scope_has_api,
    verdict,
)
from tests.integration.aws_capability._login_fake_testing import _PRIVATE, JWK, KID
from tests.integration.aws_capability._login_http import Http, HttpReply
from tests.integration.aws_capability._login_oauth import (
    READY_ATTEMPTS,
    Grant,
    TokenReply,
    await_ready,
    exchange_code,
    label,
)
from tests.integration.aws_capability._login_pkce import Endpoint
from tests.integration.aws_capability._login_ports import Callback
from tests.integration.aws_capability.contracts import (
    BudgetExhausted,
    Limits,
    RequestBudget,
    RequestContext,
)

ENDPOINT = Endpoint("https://d.auth.us-east-2.amazoncognito.com", "CLIENT_CANARY")


def context(calls: int = 100) -> RequestContext:
    budget = RequestBudget(Limits(calls, 8, 2, 30, 10), 5000.0, lambda: 1000.0)
    return RequestContext(budget, "work")


def http_for(handler, calls: int = 100) -> tuple[Http, RequestContext]:
    ctx = context(calls)
    return Http(httpx.Client(transport=httpx.MockTransport(handler)), ctx), ctx


def test_cada_chamada_http_consome_orcamento_de_trabalho() -> None:
    http, ctx = http_for(lambda request: httpx.Response(200, json={"a": 1}))
    assert http.get("https://x/y") == HttpReply(200, {"a": 1})
    http.post_form("https://x/z", {"k": "v"})
    assert ctx.budget.work_used == 2


def test_orcamento_esgotado_interrompe_antes_da_chamada() -> None:
    http, _ = http_for(lambda request: httpx.Response(200, json={}), calls=0)
    with pytest.raises(BudgetExhausted):
        http.get("https://x/y")


def test_erro_de_transporte_vira_status_zero_sem_corpo() -> None:
    def boom(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("SECRET-DETAIL")

    http, _ = http_for(boom)
    reply = http.get("https://x/y")
    assert reply == HttpReply(0)
    assert "SECRET-DETAIL" not in repr(reply)


@pytest.mark.parametrize("text", ["<html>", "[1,2]", ""])
def test_corpo_que_nao_e_objeto_json_vira_vazio(text: str) -> None:
    http, _ = http_for(lambda request: httpx.Response(200, text=text))
    assert http.get("https://x/y").body == {}


def test_repr_da_resposta_http_nao_mostra_o_corpo() -> None:
    reply = HttpReply(200, {"access_token": "TOKEN-CANARY"})
    assert "TOKEN-CANARY" not in repr(reply)


@pytest.mark.parametrize(
    ("value", "expected"),
    [("invalid_grant", "invalid_grant"), (None, "none"), ("http://x?c=1", "other"), (5, "other")],
)
def test_rotulo_de_erro_oauth_so_aceita_palavra_curta(value: object, expected: str) -> None:
    assert label(value) == expected


def test_exchange_nunca_expoe_tokens_nem_codigo_no_repr() -> None:
    def ok(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200, json={"access_token": "ACCESS-C", "id_token": "ID-C", "refresh_token": "REFRESH-C"}
        )

    http, _ = http_for(ok)
    reply = exchange_code(http, ENDPOINT, Grant("CODE-CANARY", "VERIFIER-CANARY"))
    text = repr(reply)
    assert reply.access == "ACCESS-C"
    assert not any(s in text for s in ("ACCESS-C", "ID-C", "REFRESH-C", "CODE-CANARY"))
    assert repr(TokenReply(400, "invalid_grant")) == "TokenReply(status=400, error='invalid_grant')"


def test_erro_do_endpoint_nunca_vem_do_corpo_livre() -> None:
    http, _ = http_for(lambda r: httpx.Response(400, json={"error": "Bad thing: SECRET"}))
    assert exchange_code(http, ENDPOINT, Grant("c", "v")).error == "other"


def published_after(checks: int) -> tuple[list[int], Callable[[], bool]]:
    calls = [0]

    def published() -> bool:
        calls[0] += 1
        return calls[0] > checks

    return calls, published


def test_prontidao_para_no_primeiro_200_e_registra_tentativas() -> None:
    answers = iter([404, 403, 200])
    http, _ = http_for(lambda r: httpx.Response(next(answers), text="x"))
    pauses: list[float] = []
    assert await_ready(http, ENDPOINT, pauses.append, lambda: True) == (3, 1, True)
    assert pauses == [5.0, 5.0]


def test_prontidao_e_limitada_a_36_tentativas_com_35_pausas() -> None:
    http, ctx = http_for(lambda r: httpx.Response(404, text="x"))
    pauses: list[float] = []
    assert await_ready(http, ENDPOINT, pauses.append, lambda: True) == (READY_ATTEMPTS, 1, False)
    assert len(pauses) == READY_ATTEMPTS - 1
    assert ctx.budget.work_used == READY_ATTEMPTS


def test_prontidao_nao_busca_a_pagina_antes_do_dns_publicar() -> None:
    gets: list[int] = []
    calls, published = published_after(3)

    def handler(request: httpx.Request) -> httpx.Response:
        gets.append(calls[0])
        return httpx.Response(200 if len(gets) == 2 else 404, text="x")

    http, _ = http_for(handler)
    pauses: list[float] = []
    assert await_ready(http, ENDPOINT, pauses.append, published) == (5, 4, True)
    assert gets == [4, 4]
    assert calls == [4]
    assert pauses == [5.0] * 4


def test_dns_que_nunca_publica_esgota_as_tentativas_sem_http() -> None:
    gets: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        gets.append(str(request.url))
        return httpx.Response(200, text="x")

    http, ctx = http_for(handler)
    pauses: list[float] = []
    result = await_ready(http, ENDPOINT, pauses.append, lambda: False)
    assert result == (READY_ATTEMPTS, None, False)
    assert gets == []
    assert ctx.budget.work_used == 0
    assert len(pauses) == READY_ATTEMPTS - 1


def test_dns_publicado_na_ultima_tentativa_ainda_tenta_uma_vez_a_pagina() -> None:
    _, published = published_after(READY_ATTEMPTS - 1)
    http, ctx = http_for(lambda r: httpx.Response(200, text="x"))
    result = await_ready(http, ENDPOINT, lambda seconds: None, published)
    assert result == (READY_ATTEMPTS, READY_ATTEMPTS, True)
    assert ctx.budget.work_used == 1


def test_repr_de_plano_cadeia_e_callback_nao_mostra_state_nem_codigo() -> None:
    plan = make_plan(ENDPOINT)
    chain = Chain("ok", {"a": "CODE-CANARY"})
    callback = Callback("CODE-CANARY", plan.auths["a"].state)
    text = repr(plan) + repr(chain) + repr(callback)
    assert "CODE-CANARY" not in text
    assert plan.auths["a"].state not in text
    assert plan.urls["a"] not in text


ISSUER = "https://cognito-idp.us-east-2.amazonaws.com/us-east-2_P"


def signed(**overrides: object) -> str:
    now = int(time.time())
    claims: dict[str, object] = {
        "sub": "s", "iss": ISSUER, "aud": "https://api.cnesdata.com.br",
        "iat": now, "exp": now + 600,
    }
    claims.update(overrides)
    return jwt.encode(claims, _PRIVATE, "RS256", headers={"kid": KID})


def verifier():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("openid-configuration"):
            return httpx.Response(200, json={"issuer": ISSUER, "jwks_uri": ISSUER + "/jwks"})
        return httpx.Response(200, json={"keys": [JWK]})

    return make_verifier(ISSUER, httpx.Client(transport=httpx.MockTransport(handler)))


@pytest.mark.parametrize(
    ("token", "expected"),
    [
        (signed(), "ok"),
        (signed(aud="outro"), "audience"),
        (signed(iss="https://evil"), "issuer"),
        (signed(exp=1), "expired"),
        ("lixo", "malformed_header"),
        (None, "no_token"),
    ],
)
def test_veredito_do_verifier_vira_codigo_curto(token: str | None, expected: str) -> None:
    assert verdict(verifier(), token) == expected


def test_token_sem_aud_e_audience() -> None:
    now = int(time.time())
    claims = {"sub": "s", "iss": ISSUER, "iat": now, "exp": now + 600}
    token = jwt.encode(claims, _PRIVATE, "RS256", headers={"kid": KID})
    assert verdict(verifier(), token) == "audience"


def test_rotulos_de_claims_nunca_devolvem_valores_arbitrarios() -> None:
    assert aud_label({"aud": "https://api.cnesdata.com.br"}) == "https://api.cnesdata.com.br"
    assert aud_label({"aud": "SEGREDO"}) == "other"
    assert aud_label({"aud": ["a"]}) == "other"
    assert aud_label({}) == "absent"
    assert scope_has_api({"scope": "openid https://api.cnesdata.com.br/api.access"}) is True
    assert scope_has_api({"scope": "openid"}) is False
    assert claim_names({"token_use": 1, "Bad Name!": 2, "sub": 3}) == ["sub", "token_use"]
