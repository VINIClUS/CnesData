"""One-shot watcher — avalia thresholds, sinaliza flag global."""

import logging

from sqlalchemy.engine import Engine

from batch_watcher.config import (
    AGE_THRESHOLD_DAYS,
    SIZE_THRESHOLD_MB,
)
from cnes_infra.storage.batch_trigger import (
    Thresholds,
    evaluate_and_open,
)

logger = logging.getLogger(__name__)

_MB = 1024 * 1024


def run_once(engine: Engine) -> int:
    """Executa 1 ciclo. Retorna exit code (0=ok, 1=erro)."""
    thresholds = Thresholds(
        size_bytes=SIZE_THRESHOLD_MB * _MB,
        age_days=AGE_THRESHOLD_DAYS,
    )
    try:
        state = evaluate_and_open(engine, thresholds)
    except Exception:
        logger.exception("watcher_failure")
        return 1
    pending_mb = (state.pending_bytes or 0) / _MB
    oldest = (
        state.oldest_completed_at.isoformat()
        if state.oldest_completed_at else "none"
    )
    logger.info(
        "watcher_tick status=%s pending_mb=%.1f oldest=%s reason=%s",
        state.status, pending_mb, oldest, state.reason or "none",
    )
    return 0
