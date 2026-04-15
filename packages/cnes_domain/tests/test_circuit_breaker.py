"""Testes do CircuitBreaker com backoff exponencial e HALF-OPEN."""
from unittest.mock import MagicMock, patch

import pytest

from cnes_domain.pipeline.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerAberto,
)

MODULE = "cnes_domain.pipeline.circuit_breaker.time"


def _fn_ok(val: str = "ok") -> str:
    return val


def _fn_erro() -> None:
    raise ConnectionError("timeout")


class TestCircuitBreaker:
    def test_sucesso_reseta_falhas(self) -> None:
        cb = CircuitBreaker(failure_threshold=3)
        with pytest.raises(ConnectionError):
            cb.call(_fn_erro)
        assert cb._falhas_consecutivas == 1

        resultado = cb.call(_fn_ok)
        assert resultado == "ok"
        assert cb._falhas_consecutivas == 0

    def test_abre_apos_threshold(self) -> None:
        cb = CircuitBreaker(failure_threshold=3)
        for _ in range(3):
            with pytest.raises(ConnectionError):
                cb.call(_fn_erro)

        assert cb.is_open is True
        with pytest.raises(CircuitBreakerAberto, match="service=external"):
            cb.call(_fn_ok)

    @patch(MODULE)
    def test_backoff_exponencial(self, mock_time: MagicMock) -> None:
        mock_time.monotonic.return_value = 0.0
        mock_time.sleep = MagicMock()
        cb = CircuitBreaker(
            failure_threshold=3, base_delay=1.0, max_delay=30.0,
        )

        with pytest.raises(ConnectionError):
            cb.call(_fn_erro)
        mock_time.sleep.assert_called_once_with(1.0)

        mock_time.sleep.reset_mock()
        with pytest.raises(ConnectionError):
            cb.call(_fn_erro)
        mock_time.sleep.assert_called_once_with(2.0)

        mock_time.sleep.reset_mock()
        with pytest.raises(ConnectionError):
            cb.call(_fn_erro)
        assert cb.is_open is True
        mock_time.sleep.assert_not_called()

    @patch(MODULE)
    def test_half_open_permite_tentativa_apos_reset(
        self, mock_time: MagicMock,
    ) -> None:
        mock_time.sleep = MagicMock()
        mock_time.monotonic.return_value = 100.0
        cb = CircuitBreaker(
            failure_threshold=2, reset_after=60.0,
        )

        for _ in range(2):
            with pytest.raises(ConnectionError):
                cb.call(_fn_erro)
        assert cb.is_open is True
        assert cb._aberto_em == 100.0

        mock_time.monotonic.return_value = 161.0
        resultado = cb.call(_fn_ok)
        assert resultado == "ok"
        assert cb.is_open is False
        assert cb._falhas_consecutivas == 0

    @patch(MODULE)
    def test_half_open_fecha_se_probe_falha(
        self, mock_time: MagicMock,
    ) -> None:
        mock_time.sleep = MagicMock()
        mock_time.monotonic.return_value = 100.0
        cb = CircuitBreaker(
            failure_threshold=1, reset_after=60.0,
        )

        with pytest.raises(ConnectionError):
            cb.call(_fn_erro)
        assert cb.is_open is True

        mock_time.monotonic.return_value = 161.0
        with pytest.raises(ConnectionError):
            cb.call(_fn_erro)
        assert cb.is_open is True
        assert cb._aberto_em == 161.0

    def test_backward_compatible_defaults(self) -> None:
        cb = CircuitBreaker()
        assert cb._threshold == 3
        assert cb._service == "external"
        assert cb._base_delay == 1.0
        assert cb._max_delay == 30.0
        assert cb._reset_after == 60.0
        assert cb.is_open is False

        resultado = cb.call(_fn_ok, "valor")
        assert resultado == "valor"
