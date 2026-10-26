"""Watchdog de orçamento para uma execução do processor."""
from __future__ import annotations

import logging
import os
import threading
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable, Generator


@dataclass(frozen=True, slots=True)
class DeadlinePolicy:
    """Política de prazo.
    Args:
        mode: Modo de execução.
        seconds: Orçamento em segundos.
    """

    mode: str
    seconds: float


@dataclass(slots=True)
class DeadlineWatchdog:
    """Watchdog de prazo.
    Args:
        policy: Política de prazo.
        exit_process: Callback para encerrar o processo.
    """

    policy: DeadlinePolicy
    exit_process: Callable[[int], None] = os._exit
    _timer: threading.Timer | None = field(default=None, init=False)
    _expired: bool = field(default=False, init=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    def arm(self) -> None:
        """Arma o watchdog.
        Returns:
            None.
        """
        with self._lock:
            self._expired = False
            timer = threading.Timer(self.policy.seconds - 0.1, self._expire)
            timer.daemon = True
            self._timer = timer
            timer.start()

    def disarm(self) -> None:
        """Desarma o watchdog pendente.
        Returns:
            None.
        """
        with self._lock:
            if self._expired or self._timer is None:
                return
            timer = self._timer
            self._timer = None
            timer.cancel()

    def _expire(self) -> None:
        with self._lock:
            if self._expired:
                return
            self._expired = True
            fallback = threading.Timer(0.1, self._fallback_exit)
            fallback.daemon = True
            self._timer = fallback
            fallback.start()
        logging.getLogger(__name__).error(
            "processor_deadline_exceeded",
            extra={
                "mode": self.policy.mode,
                "deadline_seconds": self.policy.seconds,
            },
        )
        self.exit_process(3)

    def _fallback_exit(self) -> None:
        self.exit_process(3)


@contextmanager
def processor_deadline(policy: DeadlinePolicy) -> Generator[None]:
    """Contexto com prazo.
    Args:
        policy: Política de prazo.
    Yields:
        None durante o bloco protegido.
    """
    watchdog = DeadlineWatchdog(policy)
    watchdog.arm()
    try:
        yield
    finally:
        watchdog.disarm()
