"""Ponto de entrada do dump_agent — daemon de streaming."""
import asyncio
import logging
import os
import random
import sys
import uuid
from logging.handlers import RotatingFileHandler
from pathlib import Path

from dump_agent.worker.consumer import run_worker

fmt = logging.Formatter(
    "%(asctime)s %(levelname)-5s %(name)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

_LOGS_DIR = Path(os.getenv("DUMP_LOGS_DIR", "logs"))
_LOG_FILE = _LOGS_DIR / "dump_agent.log"
_MAX_JITTER = float(os.getenv("DUMP_MAX_JITTER_SECONDS", "1800"))


def _setup_logging(verbose: bool = False) -> None:
    _LOGS_DIR.mkdir(parents=True, exist_ok=True)
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.DEBUG if verbose else logging.INFO)
    console.setFormatter(fmt)
    arquivo = RotatingFileHandler(
        _LOG_FILE,
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    arquivo.setLevel(logging.DEBUG)
    arquivo.setFormatter(fmt)
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.addHandler(console)
    root.addHandler(arquivo)


async def main() -> int:
    verbose = "--verbose" in sys.argv or "-v" in sys.argv
    _setup_logging(verbose)

    api_url = os.getenv("API_BASE_URL", "http://localhost:8000")
    machine_id = os.getenv("MACHINE_ID", str(uuid.uuid4())[:8])

    startup_jitter = random.uniform(0, _MAX_JITTER)
    logging.getLogger(__name__).info(
        "startup_jitter=%.1fs machine_id=%s", startup_jitter, machine_id,
    )
    await asyncio.sleep(startup_jitter)

    await run_worker(api_url, machine_id, _MAX_JITTER)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
