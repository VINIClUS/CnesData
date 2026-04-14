"""test_cnes_oficial_web_adapter.py — Testes unitários do adapter HTTP DATASUS."""

from unittest.mock import MagicMock

import requests

from cnes_domain.pipeline.circuit_breaker import CircuitBreaker
from cnes_infra.ingestion.cnes_oficial_web_adapter import (
    STATUS_CONFIRMADO,
    STATUS_INDISPONIVEL,
    STATUS_LAG,
    CnesOficialWebAdapter,
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


def test_http_503_abre_circuit_breaker_apos_threshold():
    sessao = MagicMock(spec=requests.Session)
    resp = MagicMock()
    resp.status_code = 503
    sessao.get.return_value = resp
    sessao.headers = {}
    breaker = CircuitBreaker(failure_threshold=3, service_name="DATASUS")
    adapter = CnesOficialWebAdapter(session=sessao, circuit_breaker=breaker)
    for _ in range(3):
        result = adapter.verificar_estabelecimento(_CNES)
    assert result == STATUS_INDISPONIVEL
    assert breaker.is_open


def test_circuito_aberto_retorna_indisponivel_sem_chamada_http():
    sessao = MagicMock(spec=requests.Session)
    sessao.headers = {}
    breaker = CircuitBreaker(failure_threshold=1, service_name="DATASUS")
    # Abrir o circuito manualmente via uma falha inicial
    resp = MagicMock()
    resp.status_code = 503
    sessao.get.return_value = resp
    adapter = CnesOficialWebAdapter(session=sessao, circuit_breaker=breaker)
    adapter.verificar_estabelecimento(_CNES)
    assert breaker.is_open
    # Segunda chamada deve ser bloqueada sem acionar o HTTP
    sessao.get.side_effect = Exception("nao_deve_ser_chamado")
    result = adapter.verificar_estabelecimento(_CNES)
    assert result == STATUS_INDISPONIVEL
