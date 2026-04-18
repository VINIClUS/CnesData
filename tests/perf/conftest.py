"""Fixtures de perf — containers Firebird/Postgres session-scoped."""
import logging
import shutil
import subprocess
import sys
import time
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

_PG_PERF_URL = (
    "postgresql+psycopg://cnesdata:cnesdata_perf@localhost:5434/cnesdata_perf"
)
_FB_HOST = "localhost"
_FB_PORT = 3051
_FB_DB = "CNES.FDB"


def _docker_bin() -> str | None:
    return shutil.which("docker")


def _docker_compose_ok() -> bool:
    return _docker_bin() is not None


def _wait_pg(timeout: float = 60.0) -> bool:
    deadline = time.monotonic() + timeout
    engine = create_engine(_PG_PERF_URL)
    try:
        while time.monotonic() < deadline:
            try:
                with engine.connect() as con:
                    con.execute(text("SELECT 1"))
                return True
            except Exception:
                time.sleep(1)
        return False
    finally:
        engine.dispose()


@pytest.fixture(scope="session")
def perf_stack():
    docker = _docker_bin()
    if not docker:
        pytest.skip("docker não disponível — perf fixtures indisponíveis")
    subprocess.run(  # noqa: S603
        [docker, "compose", "-f", "docker-compose.perf.yml", "up", "-d"],
        check=True,
    )
    if not _wait_pg():
        subprocess.run(  # noqa: S603
            [docker, "compose", "-f", "docker-compose.perf.yml", "down"],
            check=False,
        )
        pytest.fail("postgres_perf não subiu em 60s")
    yield
    subprocess.run(  # noqa: S603
        [docker, "compose", "-f", "docker-compose.perf.yml", "down"],
        check=False,
    )


@pytest.fixture(scope="session")
def pg_perf_engine(perf_stack):
    from alembic import command
    from alembic.config import Config
    cfg = Config()
    cfg.set_main_option("script_location", "cnes_infra:alembic")
    cfg.set_main_option("sqlalchemy.url", _PG_PERF_URL)
    command.upgrade(cfg, "head")
    engine = create_engine(_PG_PERF_URL)
    yield engine
    engine.dispose()


@pytest.fixture(scope="session")
def fb_perf_dsn(perf_stack):
    script = Path("scripts/seed_firebird_fixture.py")
    subprocess.run(  # noqa: S603
        [sys.executable, str(script), "--n-profs", "100000"],
        check=True,
    )
    return f"{_FB_HOST}/{_FB_PORT}:{_FB_DB}"
