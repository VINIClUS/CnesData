"""Ponto de entrada do data_processor."""
from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal
import sys
from datetime import UTC, datetime
from logging.handlers import RotatingFileHandler
from typing import TYPE_CHECKING

from sqlalchemy import create_engine

from cnes_domain.outbox_dispatcher import dispatch_once
from cnes_infra import config
from cnes_infra.storage.rls import install_rls_listener
from cnes_infra.storage.s3_presigned import S3PresignedStorage, build_s3_client
from cnes_infra.telemetry import init_telemetry
from data_processor.consumer import run_processor

if TYPE_CHECKING:
    from collections.abc import Callable

    from cnes_domain.ports.audit import AuditSinkPort
    from cnes_domain.ports.control_plane import ControlPlanePort
    from cnes_domain.ports.object_storage import ObjectStoragePort
    from data_processor.orchestration.coordinator import PipelineCoordinator

fmt = logging.Formatter(
    "%(asctime)s %(levelname)-5s %(name)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def _setup_logging(verbose: bool = False) -> None:
    config.LOGS_DIR.mkdir(parents=True, exist_ok=True)
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.DEBUG if verbose else logging.INFO)
    console.setFormatter(fmt)
    arquivo = RotatingFileHandler(
        config.LOG_FILE,
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


def _create_storage() -> ObjectStoragePort:
    client = build_s3_client(
        config.S3_REGION, config.S3_ENDPOINT_URL or None, config.S3_ADDRESSING_STYLE,
    )
    return S3PresignedStorage(client)


def _profile_is_local() -> bool:
    return os.environ.get("PROFILE", "").strip().lower() == "local"


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _install_shutdown_handler(shutdown: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        with contextlib.suppress(NotImplementedError, RuntimeError):
            loop.add_signal_handler(sig, shutdown.set)


async def _recover_tick(coordinator: PipelineCoordinator) -> None:
    try:
        results = coordinator.recover()
    except Exception:
        logging.getLogger(__name__).exception("local_profile_recover_tick_error")
        return
    if results:
        logging.getLogger(__name__).info("local_profile_recover_tick runs=%d", len(results))


def _audit_tick(control_plane: ControlPlanePort, audit_sink: AuditSinkPort) -> None:
    result = dispatch_once(control_plane, audit_sink, _utc_now())
    if result.delivered or result.failed:
        logging.getLogger(__name__).info(
            "local_profile_audit_tick delivered=%d failed=%d",
            result.delivered,
            result.failed,
        )


async def _poll_until_shutdown(
    coordinator: PipelineCoordinator,
    shutdown: asyncio.Event,
    interval: float,
    audit_tick: Callable[[], None] | None = None,
) -> None:
    while not shutdown.is_set():
        await _recover_tick(coordinator)
        if audit_tick is not None:
            try:
                audit_tick()
            except Exception:
                logging.getLogger(__name__).exception("local_profile_audit_tick_error")
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(shutdown.wait(), timeout=interval)


async def _run_local_profile() -> None:
    """Compõe o runtime local e mantém o processo vivo drenando runs recuperáveis."""
    from cnes_domain.profiles import parse_profile
    from data_processor.composition import build_local_processor_runtime
    from data_processor.config import POLL_INTERVAL

    settings = parse_profile(os.environ)
    runtime = build_local_processor_runtime(settings, _utc_now)
    logging.getLogger(__name__).info(
        "local_profile_composed tenant_id=%s data_dir=%s",
        settings.tenant_id, settings.data_dir,
    )
    shutdown = asyncio.Event()
    _install_shutdown_handler(shutdown)
    try:
        await _poll_until_shutdown(
            runtime.coordinator,
            shutdown,
            POLL_INTERVAL,
            audit_tick=lambda: _audit_tick(runtime.control_plane, runtime.audit_sink),
        )
    finally:
        runtime.executor.close()


async def main() -> int:
    verbose = "--verbose" in sys.argv or "-v" in sys.argv
    _setup_logging(verbose)
    init_telemetry("data-processor")

    if _profile_is_local():
        await _run_local_profile()
        return 0

    engine = create_engine(config.DB_URL)
    install_rls_listener(engine)
    storage = _create_storage()

    await run_processor(engine, storage)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
