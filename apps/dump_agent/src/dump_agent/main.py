"""Ponto de entrada do dump_agent — daemon de streaming."""
import asyncio
import faulthandler
import logging
import os
import random
import sys
from logging.handlers import RotatingFileHandler

from dump_agent.platform_runtime import (
    acquire_single_instance_lock,
    install_shutdown_handler,
    logs_dir,
    resolve_machine_id,
)
from dump_agent.worker.consumer import run_worker

logger = logging.getLogger(__name__)

fmt = logging.Formatter(
    "%(asctime)s %(levelname)-5s %(name)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

_MAX_JITTER = float(os.getenv("DUMP_MAX_JITTER_SECONDS", "1800"))


def _setup_logging(verbose: bool = False) -> None:
    target_dir = logs_dir()
    log_file = target_dir / "dump_agent.log"
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.DEBUG if verbose else logging.INFO)
    console.setFormatter(fmt)
    arquivo = RotatingFileHandler(
        log_file,
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


async def _async_main(
    api_url: str,
    machine_id: str,
    stop_event: asyncio.Event,
) -> int:
    startup_jitter = random.uniform(0, _MAX_JITTER)
    logger.info(
        "startup_jitter=%.1fs machine_id=%s",
        startup_jitter, machine_id,
    )
    try:
        await asyncio.wait_for(
            stop_event.wait(), timeout=startup_jitter,
        )
        return 0
    except TimeoutError:
        pass
    await run_worker(
        api_base_url=api_url,
        machine_id=machine_id,
        stop_event=stop_event,
        jitter_max=_MAX_JITTER,
    )
    return 0


def main_sync() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, OSError):
        pass
    faulthandler.enable()

    verbose = "--verbose" in sys.argv or "-v" in sys.argv
    _setup_logging(verbose)

    api_url = os.getenv("API_BASE_URL", "http://localhost:8000")
    machine_id = resolve_machine_id()
    logger.info("machine_id_resolved machine_id=%s", machine_id)

    stop_event = asyncio.Event()

    def _on_stop() -> None:
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = None
        if loop is not None and loop.is_running():
            loop.call_soon_threadsafe(stop_event.set)
        else:
            stop_event.set()

    install_shutdown_handler(_on_stop)

    with acquire_single_instance_lock("dump_agent"):
        return asyncio.run(
            _async_main(api_url, machine_id, stop_event),
        )


if __name__ == "__main__":
    sys.exit(main_sync())
