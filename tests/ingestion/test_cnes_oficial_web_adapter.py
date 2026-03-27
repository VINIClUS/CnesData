"""test_cnes_oficial_web_adapter.py — Testes unitários do adapter HTTP DATASUS."""

from unittest.mock import MagicMock

import requests

from ingestion.cnes_oficial_web_adapter import (
    CnesOficialWebAdapter,
    STATUS_CONFIRMADO,
    STATUS_INDISPONIVEL,
    STATUS_LAG,
)

_CNES = "0985333"


def _sessao_com_resposta(status_code: int) -> requests.Session:
    sessao = MagicMock(spec=requests.Session)
    resposta = MagicMock()
    resposta.status_code = status_code
    sessao.get.return_value = resposta
    sessao.headers = {}
    return sessao


def test_http_200_retorna_status_lag():
    adapter = CnesOficialWebAdapter(session=_sessao_com_resposta(200))
    assert adapter.verificar_estabelecimento(_CNES) == STATUS_LAG


def test_http_404_retorna_status_confirmado():
    adapter = CnesOficialWebAdapter(session=_sessao_com_resposta(404))
    assert adapter.verificar_estabelecimento(_CNES) == STATUS_CONFIRMADO


def test_http_204_retorna_status_confirmado():
    adapter = CnesOficialWebAdapter(session=_sessao_com_resposta(204))
    assert adapter.verificar_estabelecimento(_CNES) == STATUS_CONFIRMADO
