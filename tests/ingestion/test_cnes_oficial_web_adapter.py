"""test_cnes_oficial_web_adapter.py — Testes unitários do adapter HTTP DATASUS."""

from unittest.mock import MagicMock

import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_none

from ingestion.cnes_oficial_web_adapter import (
    CnesOficialWebAdapter,
    STATUS_CONFIRMADO,
    STATUS_INDISPONIVEL,
    STATUS_LAG,
    _ServidorIndisponivel,
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


def test_timeout_retorna_status_indisponivel():
    sessao = MagicMock(spec=requests.Session)
    sessao.get.side_effect = requests.Timeout()
    sessao.headers = {}
    adapter = CnesOficialWebAdapter(session=sessao)
    assert adapter.verificar_estabelecimento(_CNES) == STATUS_INDISPONIVEL


def test_connection_error_retorna_status_indisponivel():
    sessao = MagicMock(spec=requests.Session)
    sessao.get.side_effect = requests.ConnectionError()
    sessao.headers = {}
    adapter = CnesOficialWebAdapter(session=sessao)
    assert adapter.verificar_estabelecimento(_CNES) == STATUS_INDISPONIVEL


def _adapter_com_retry_rapido(sessao: requests.Session) -> CnesOficialWebAdapter:
    adapter = CnesOficialWebAdapter(session=sessao)
    retry_rapido = retry(
        stop=stop_after_attempt(3),
        wait=wait_none(),
        retry=retry_if_exception_type(_ServidorIndisponivel),
        reraise=False,
    )
    func_original = type(adapter)._chamar_com_retry.__wrapped__
    adapter._chamar_com_retry = retry_rapido(
        lambda cnes, s=adapter: func_original(s, cnes)
    )
    return adapter


def test_http_503_exaustao_retorna_status_indisponivel():
    adapter = _adapter_com_retry_rapido(_sessao_com_resposta(503))
    assert adapter.verificar_estabelecimento(_CNES) == STATUS_INDISPONIVEL


def test_http_503_seguido_200_retorna_status_lag():
    sessao = MagicMock(spec=requests.Session)
    resp_503 = MagicMock()
    resp_503.status_code = 503
    resp_200 = MagicMock()
    resp_200.status_code = 200
    sessao.get.side_effect = [resp_503, resp_200]
    sessao.headers = {}
    adapter = _adapter_com_retry_rapido(sessao)
    assert adapter.verificar_estabelecimento(_CNES) == STATUS_LAG
