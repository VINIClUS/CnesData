"""CircuitBreaker — falha-rápida para chamadas externas instáveis."""
import logging
from collections.abc import Callable
from typing import Any

logger = logging.getLogger(__name__)


class CircuitBreakerAberto(Exception):
    """Disparado quando o circuit breaker está aberto e a chamada é bloqueada."""


class CircuitBreaker:
    """CLOSED → OPEN após N falhas consecutivas. Sem estado HALF-OPEN.

    Args:
        failure_threshold: Número de falhas consecutivas para abrir o circuito.
        service_name: Nome do serviço (para logging).
    """

    def __init__(self, failure_threshold: int = 3, service_name: str = "external") -> None:
        self._threshold = failure_threshold
        self._service = service_name
        self._falhas_consecutivas: int = 0
        self._aberto: bool = False

    @property
    def is_open(self) -> bool:
        return self._aberto

    def call(self, fn: Callable, *args: Any, **kwargs: Any) -> Any:
        """Executa fn protegida pelo circuit breaker.

        Args:
            fn: Função a executar.
            *args: Argumentos posicionais para fn.
            **kwargs: Argumentos keyword para fn.

        Returns:
            Resultado de fn, ou None se o circuito estiver aberto.

        Raises:
            CircuitBreakerAberto: Se o circuito estiver aberto.
        """
        if self._aberto:
            logger.warning("circuit_breaker=OPEN service=%s chamada_bloqueada", self._service)
            raise CircuitBreakerAberto(f"service={self._service}")
        try:
            resultado = fn(*args, **kwargs)
            self._falhas_consecutivas = 0
            return resultado
        except Exception as exc:
            self._falhas_consecutivas += 1
            logger.warning(
                "circuit_breaker_falha service=%s consecutivas=%d threshold=%d err=%s",
                self._service,
                self._falhas_consecutivas,
                self._threshold,
                exc,
            )
            if self._falhas_consecutivas >= self._threshold:
                self._aberto = True
                logger.error(
                    "circuit_breaker=OPEN service=%s threshold_atingido=%d",
                    self._service,
                    self._threshold,
                )
            raise
