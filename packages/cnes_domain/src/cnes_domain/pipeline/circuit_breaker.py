"""CircuitBreaker — falha-rápida para chamadas externas instáveis."""
import asyncio
import inspect
import logging
import threading
import time
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")

_MAX_BACKOFF_EXPONENTE: int = 30


class CircuitBreakerAberto(Exception):
    pass


class CircuitBreaker:
    """CLOSED → OPEN após N falhas; HALF-OPEN após reset_after segundos.

    Args:
        failure_threshold: Falhas consecutivas para abrir o circuito.
        service_name: Nome do serviço (para logging).
        base_delay: Delay base em segundos para backoff exponencial.
        max_delay: Delay máximo em segundos.
        reset_after: Segundos até transição OPEN → HALF-OPEN.
    """

    def __init__(
        self,
        failure_threshold: int = 3,
        service_name: str = "external",
        base_delay: float = 1.0,
        max_delay: float = 30.0,
        reset_after: float = 60.0,
    ) -> None:
        self._threshold = failure_threshold
        self._service = service_name
        self._base_delay = base_delay
        self._max_delay = max_delay
        self._reset_after = reset_after
        self._falhas_consecutivas: int = 0
        self._aberto: bool = False
        self._aberto_em: float | None = None
        self._lock = threading.Lock()

    @property
    def is_open(self) -> bool:
        return self._aberto

    def call(self, fn: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """Executa fn síncrona protegida pelo circuit breaker.

        Args:
            fn: Função síncrona a executar. Use `call_async` para corrotinas.

        Raises:
            CircuitBreakerAberto: Se o circuito estiver aberto.
            TypeError: Se fn for `async def`.
        """
        if inspect.iscoroutinefunction(fn):
            raise TypeError(
                "call espera função síncrona; use call_async para corrotinas"
            )
        self._gate_pre_call()
        try:
            resultado = fn(*args, **kwargs)
        except Exception:
            delay = self._registrar_falha()
            if delay > 0:
                time.sleep(delay)
            raise
        self._registrar_sucesso()
        return resultado

    async def call_async(
        self, fn: Callable[..., Awaitable[T]], *args: Any, **kwargs: Any,
    ) -> T:
        """Executa corrotina protegida pelo circuit breaker.

        Args:
            fn: `async def` a executar. Use `call` para funções síncronas.

        Raises:
            CircuitBreakerAberto: Se o circuito estiver aberto.
            TypeError: Se fn não for `async def`.
        """
        if not inspect.iscoroutinefunction(fn):
            raise TypeError(
                "call_async espera coroutine function; use call para fn sync"
            )
        self._gate_pre_call()
        try:
            resultado = await fn(*args, **kwargs)
        except Exception:
            delay = self._registrar_falha()
            if delay > 0:
                await asyncio.sleep(delay)
            raise
        self._registrar_sucesso()
        return resultado

    def _gate_pre_call(self) -> None:
        with self._lock:
            if not self._aberto:
                return
            if self._should_half_open():
                logger.info(
                    "circuit_breaker=HALF_OPEN service=%s",
                    self._service,
                )
                self._aberto = False
                self._falhas_consecutivas = 0
                return
        logger.warning(
            "circuit_breaker=OPEN service=%s chamada_bloqueada",
            self._service,
        )
        raise CircuitBreakerAberto(f"service={self._service}")

    def _registrar_sucesso(self) -> None:
        with self._lock:
            self._falhas_consecutivas = 0

    def _registrar_falha(self) -> float:
        with self._lock:
            self._falhas_consecutivas += 1
            consecutivas = self._falhas_consecutivas
            expoente = min(consecutivas - 1, _MAX_BACKOFF_EXPONENTE)
            delay = min(self._base_delay * (2 ** expoente), self._max_delay)
            abriu_agora = consecutivas >= self._threshold
            if abriu_agora:
                self._aberto = True
                self._aberto_em = time.monotonic()
        logger.warning(
            "circuit_breaker_falha service=%s consecutivas=%d delay=%.1fs",
            self._service,
            consecutivas,
            delay,
        )
        if abriu_agora:
            logger.error(
                "circuit_breaker=OPEN service=%s threshold=%d",
                self._service,
                self._threshold,
            )
            return 0.0
        return delay

    def _should_half_open(self) -> bool:
        if self._aberto_em is None:
            return False
        return (time.monotonic() - self._aberto_em) >= self._reset_after
