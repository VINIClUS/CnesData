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
