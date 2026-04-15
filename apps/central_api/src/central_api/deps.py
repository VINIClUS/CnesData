"""Dependências compartilhadas da API (engine, settings, reaper)."""

import asyncio
import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from cnes_domain.ports.object_storage import (
    NullObjectStoragePort,
    ObjectStoragePort,
)
from cnes_infra import config
from cnes_infra.storage.job_queue import reap_expired_leases
from cnes_infra.storage.rls import install_rls_listener
from cnes_infra.telemetry import instrument_engine

logger = logging.getLogger(__name__)

_engine: Engine | None = None
_object_storage: ObjectStoragePort | None = None
_REAPER_INTERVAL = 60


def get_engine() -> Engine:
    global _engine
    if _engine is None:
        _engine = create_engine(config.DB_URL)
    return _engine


def get_object_storage() -> ObjectStoragePort:
    global _object_storage
    if _object_storage is None:
        try:
            from cnes_infra.storage.object_storage import (
                MinioObjectStorage,
            )
            _object_storage = MinioObjectStorage(
                endpoint=config.MINIO_ENDPOINT,
                access_key=config.MINIO_ACCESS_KEY,
                secret_key=config.MINIO_SECRET_KEY,
                secure=config.MINIO_SECURE,
            )
        except Exception:
            logger.warning("minio_unavailable using_null_storage")
            _object_storage = NullObjectStoragePort()
    return _object_storage


async def _lease_reaper_loop(engine: Engine) -> None:
    loop = asyncio.get_running_loop()
    while True:
        await asyncio.sleep(_REAPER_INTERVAL)
        try:
            count = await loop.run_in_executor(
                None, reap_expired_leases, engine,
            )
            if count > 0:
                logger.info("leases_reaped count=%d", count)
        except Exception:
            logger.exception("reaper_error")


@asynccontextmanager
async def lifespan(app: object) -> AsyncGenerator[None]:
    global _engine
    _engine = create_engine(config.DB_URL)
    install_rls_listener(_engine)
    instrument_engine(_engine)
    reaper = asyncio.create_task(_lease_reaper_loop(_engine))
    yield
    reaper.cancel()
    if _engine is not None:
        _engine.dispose()
        _engine = None
