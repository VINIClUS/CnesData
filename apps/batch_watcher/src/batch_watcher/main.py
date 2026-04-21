"""Entrypoint CLI do batch_watcher."""

import logging
import sys

from sqlalchemy import create_engine

from batch_watcher.watcher import run_once
from cnes_infra import config as infra_cfg
from cnes_infra.telemetry import init_telemetry


def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-5s %(name)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )


def main() -> int:
    _setup_logging()
    init_telemetry("batch-watcher")
    engine = create_engine(infra_cfg.DB_URL)
    try:
        return run_once(engine)
    finally:
        engine.dispose()


if __name__ == "__main__":  # pragma: no cover - entrypoint
    sys.exit(main())
