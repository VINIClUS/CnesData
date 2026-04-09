"""Testes do CircuitBreaker."""
import pytest

from pipeline.circuit_breaker import CircuitBreaker, CircuitBreakerAberto


def _falhar():
    raise RuntimeError("falha_simulada")


def _ok():
    return "ok"


def test_circuito_fechado_em_sucesso():
    breaker = CircuitBreaker(failure_threshold=3)
    resultado = breaker.call(_ok)
    assert resultado == "ok"
    assert not breaker.is_open


def test_circuito_abre_apos_threshold():
    breaker = CircuitBreaker(failure_threshold=3)
    for _ in range(3):
        with pytest.raises(RuntimeError):
            breaker.call(_falhar)
    assert breaker.is_open


def test_chamada_bloqueada_com_circuito_aberto():
    breaker = CircuitBreaker(failure_threshold=1)
    with pytest.raises(RuntimeError):
        breaker.call(_falhar)
    assert breaker.is_open
    with pytest.raises(CircuitBreakerAberto):
        breaker.call(_ok)


def test_falhas_consecutivas_resetam_em_sucesso():
    breaker = CircuitBreaker(failure_threshold=5)
    with pytest.raises(RuntimeError):
        breaker.call(_falhar)
    breaker.call(_ok)
    assert not breaker.is_open


def test_circuito_nao_abre_antes_do_threshold():
    breaker = CircuitBreaker(failure_threshold=3)
    for _ in range(2):
        with pytest.raises(RuntimeError):
            breaker.call(_falhar)
    assert not breaker.is_open
