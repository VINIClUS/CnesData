"""CircuitBreaker — falha-rápida para chamadas externas instáveis."""
import logging
import time
from collections.abc import Callable
from typing import Any

logger = logging.getLogger(__name__)


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

    @property
    def is_open(self) -> bool:
        return self._aberto

    def call(self, fn: Callable, *args: Any, **kwargs: Any) -> Any:
        """Executa fn protegida pelo circuit breaker.

        Args:
            fn: Função a executar.

        Raises:
            CircuitBreakerAberto: Se o circuito estiver aberto.
        """
        if self._aberto:
            if self._should_half_open():
                logger.info(
                    "circuit_breaker=HALF_OPEN service=%s",
                    self._service,
                )
                self._aberto = False
                self._falhas_consecutivas = 0
            else:
                logger.warning(
                    "circuit_breaker=OPEN service=%s chamada_bloqueada",
                    self._service,
                )
                raise CircuitBreakerAberto(f"service={self._service}")
        try:
            resultado = fn(*args, **kwargs)
            self._falhas_consecutivas = 0
            return resultado
        except Exception:
            self._falhas_consecutivas += 1
            delay = min(
                self._base_delay * (2 ** (self._falhas_consecutivas - 1)),
                self._max_delay,
            )
            logger.warning(
                "circuit_breaker_falha service=%s consecutivas=%d delay=%.1fs",
                self._service,
                self._falhas_consecutivas,
                delay,
            )
            if self._falhas_consecutivas >= self._threshold:
                self._aberto = True
                self._aberto_em = time.monotonic()
                logger.error(
                    "circuit_breaker=OPEN service=%s threshold=%d",
                    self._service,
                    self._threshold,
                )
            else:
                time.sleep(delay)
            raise

    def _should_half_open(self) -> bool:
        if self._aberto_em is None:
            return False
        return (time.monotonic() - self._aberto_em) >= self._reset_after
