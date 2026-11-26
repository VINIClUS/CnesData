"""Sonda do closed-ready: corpo malformado da API entra no retry e vira closed_ready_probe=api."""

from __future__ import annotations

import json

import pytest

from scripts.promotion import closed_ready_probe as probe
from tests.production.delivery.test_closed_ready_probe_http import (
    RATE,
    Response,
    _api,
    _listing,
    _sent,
)

NAME = "cnesdata-closed-ready-7-1"


def test_corpo_malformado_e_repetido_e_o_seguinte_valido_e_aceito() -> None:
    api, opener, clock = _api(Response(b"{", **RATE), Response(_listing(9), **RATE))
    assert api.artifacts(NAME) == [{"id": 9}]
    assert len(opener.requests) == 2
    assert clock.sleeps == [probe.API_RETRY_SECONDS]


@pytest.mark.parametrize("body", [b"{", b"[]", b"{}", b'{"artifacts": 3}'],
                         ids=["json", "lista", "chave", "tipo"])
def test_corpo_sempre_fora_da_forma_vira_closed_ready_probe_api(body: bytes) -> None:
    api, opener, _ = _api(*(Response(body, **RATE) for _ in range(probe.API_ATTEMPTS)))
    with pytest.raises(ValueError, match=r"^closed_ready_probe=api$"):
        api.artifacts(NAME)
    assert len(opener.requests) == probe.API_ATTEMPTS


def test_lista_de_jobs_sem_a_chave_vira_closed_ready_probe_api() -> None:
    api, _, _ = _api(*(Response(b'{"total_count": 0}', **RATE)
                       for _ in range(probe.API_ATTEMPTS)))
    with pytest.raises(ValueError, match=r"^closed_ready_probe=api$"):
        api.jobs()


def test_corpo_malformado_nao_entra_no_cache_do_get_condicional() -> None:
    api, opener, _ = _api(Response(b"{", ETag='"bad"', **RATE),
                          Response(_listing(4), ETag='"v2"', **RATE),
                          Response(json.dumps({"artifacts": []}).encode(), **RATE))
    assert api.artifacts(NAME) == [{"id": 4}]
    assert "if-none-match" not in _sent(opener, 1)
    api.artifacts(NAME)
    assert _sent(opener, 2)["if-none-match"] == '"v2"'
