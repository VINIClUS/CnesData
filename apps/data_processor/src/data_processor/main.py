"""Ponto de entrada do data_processor."""
import asyncio
import logging
import sys
from logging.handlers import RotatingFileHandler

from cnes_domain.ports.object_storage import (
    NullObjectStoragePort,
    ObjectStoragePort,
)
from cnes_infra import config
from cnes_infra.telemetry import init_telemetry
from sqlalchemy import create_engine

from data_processor.consumer import run_processor

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
    try:
        from cnes_infra.storage.object_storage import (
            MinioObjectStorage,
        )
        return MinioObjectStorage(
            endpoint=config.MINIO_ENDPOINT,
            access_key=config.MINIO_ACCESS_KEY,
            secret_key=config.MINIO_SECRET_KEY,
            secure=config.MINIO_SECURE,
        )
    except Exception:
        logging.getLogger(__name__).warning(
            "minio_unavailable using_null_storage"
        )
        return NullObjectStoragePort()


async def main() -> int:
    verbose = "--verbose" in sys.argv or "-v" in sys.argv
    _setup_logging(verbose)
    init_telemetry("data-processor")

    engine = create_engine(config.DB_URL)
    storage = _create_storage()

    await run_processor(engine, storage)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
