"""Bounded processor recovery pass delegated to the canonical coordinator."""
from __future__ import annotations

import logging
from collections import Counter
from dataclasses import dataclass
from typing import TYPE_CHECKING

from data_processor.orchestration.coordinator import RecoveryFailed

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import datetime

    from cnes_domain.ports.control_plane import ControlPlanePort
    from data_processor.orchestration.coordinator import CoordinatorResult, PipelineCoordinator

logger = logging.getLogger(__name__)

_MAX_LIMIT = 1000


@dataclass(frozen=True, slots=True)
class RecoveryResult:
    scanned: int
    recovered: int


def _log_pass(results: tuple[CoordinatorResult, ...], scanned: int, failed: int) -> None:
    for result in results:
        logger.info(
            "processor_execution_observed",
            extra={"run_state": str(result.state), "published": result.published},
        )
    logger.info(
        "processor_recovery_completed",
        extra={"scanned": scanned, "recovered": len(results), "failed": failed},
    )


class ProcessorRecovery:
    def __init__(
        self, control_plane: ControlPlanePort, coordinator: PipelineCoordinator,
        clock: Callable[[], datetime],
    ) -> None:
        self._control_plane = control_plane
        self._coordinator = coordinator
        self._clock = clock

    def run_once(self, limit: int) -> RecoveryResult:
        """Args: limit: máximo de runs da passada, entre 1 e 1000.
        Returns: Runs candidatos lidos e runs revalidados pelo coordinator.
        Raises: ValueError para limite inválido; RecoveryFailed se algum run da passada
            falhou; erros do control plane e do coordinator.
        """
        if not 1 <= limit <= _MAX_LIMIT:
            raise ValueError("limit=invalid")
        runs = self._control_plane.list_recoverable_runs(now=self._clock(), limit=limit)
        # Structured extras, not key=value text: CloudWatch metric filters read JSON fields.
        states = Counter(str(run.state) for run in runs)
        logger.info(
            "processor_recovery_scanned", extra={"scanned": len(runs), "run_states": dict(states)},
        )
        try:
            results = self._coordinator.recover(limit=limit)
        except RecoveryFailed as error:
            _log_pass(error.results, len(runs), error.failed)
            raise
        except Exception as error:
            logger.error("processor_execution_probe_failed", extra={"reason": type(error).__name__})
            raise
        _log_pass(results, len(runs), 0)
        return RecoveryResult(scanned=len(runs), recovered=len(results))


__all__ = ["ProcessorRecovery", "RecoveryResult"]
