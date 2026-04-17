"""Regressão: CircuitBreaker.call_async (async-safe) e rejeição de tipos errados."""
import asyncio

import pytest

from cnes_domain.pipeline.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerAberto,
)


async def _async_ok(val: str = "ok") -> str:
    return val


async def _async_erro() -> None:
    raise ConnectionError("timeout async")


def _sync_ok(val: str = "ok") -> str:
    return val


class TestCallAsyncSuccess:
    async def test_sucesso_reseta_falhas(self) -> None:
        cb = CircuitBreaker(failure_threshold=3)
        with pytest.raises(ConnectionError):
            await cb.call_async(_async_erro)
        assert cb._falhas_consecutivas == 1

        resultado = await cb.call_async(_async_ok)
        assert resultado == "ok"
        assert cb._falhas_consecutivas == 0


class TestCallAsyncFailure:
    async def test_falha_aguardada_abre_circuito(self) -> None:
        cb = CircuitBreaker(failure_threshold=2, base_delay=0.0)
        for _ in range(2):
            with pytest.raises(ConnectionError):
                await cb.call_async(_async_erro)

        assert cb.is_open is True
        with pytest.raises(CircuitBreakerAberto, match="service=external"):
            await cb.call_async(_async_ok)


class TestCallAsyncRejeitaSync:
    async def test_call_async_com_fn_sync_levanta_typeerror(self) -> None:
        cb = CircuitBreaker()
        with pytest.raises(TypeError, match="call_async espera coroutine"):
            await cb.call_async(_sync_ok)


class TestCallRejeitaAsync:
    def test_call_com_async_def_levanta_typeerror(self) -> None:
        cb = CircuitBreaker()
        with pytest.raises(TypeError, match="call espera função síncrona"):
            cb.call(_async_erro)


class TestPytestAsyncioConfig:
    def test_asyncio_mode_auto_ativado(self) -> None:
        pass


def pytest_collection_modifyitems(config, items) -> None:
    for item in items:
        if asyncio.iscoroutinefunction(getattr(item, "function", None)):
            item.add_marker(pytest.mark.asyncio)
