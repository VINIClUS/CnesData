"""Circuit breakers para I/O em maquinas municipais."""

import logging
import shutil
from pathlib import Path

logger = logging.getLogger(__name__)


class InsufficientDiskError(RuntimeError):
    pass


class SpoolLimitExceeded(RuntimeError):
    pass


def pre_flight_check(
    target_dir: Path, min_free_mb: int = 500,
) -> None:
    """Verifica espaco livre em disco antes da extracao.

    Args:
        target_dir: Diretorio alvo para verificacao.
        min_free_mb: Minimo de MB livres exigido.

    Raises:
        InsufficientDiskError: Se espaco livre < min_free_mb.
    """
    usage = shutil.disk_usage(target_dir)
    free_mb = usage.free // (1024 * 1024)
    if free_mb < min_free_mb:
        raise InsufficientDiskError(
            f"free_mb={free_mb} min_required={min_free_mb} "
            f"path={target_dir}"
        )
    logger.info("pre_flight_ok free_mb=%d min=%d", free_mb, min_free_mb)


class SpoolGuard:
    """Rastreia bytes escritos e impede estouro de limite."""

    def __init__(self, max_bytes: int) -> None:
        self._max = max_bytes
        self._total = 0

    @property
    def total_bytes(self) -> int:
        return self._total

    def track(self, n_bytes: int) -> None:
        """Registra bytes escritos e dispara se limite excedido.

        Args:
            n_bytes: Quantidade de bytes escritos.

        Raises:
            SpoolLimitExceeded: Se total acumulado > max_bytes.
        """
        self._total += n_bytes
        if self._total > self._max:
            raise SpoolLimitExceeded(
                f"spool_limit_exceeded written={self._total} "
                f"max={self._max}"
            )

    def reset(self) -> None:
        """Zera contagem de bytes acumulados."""
        self._total = 0
