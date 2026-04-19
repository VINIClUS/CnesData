"""Firebird connection helper for dump_agent."""

import logging
import os

import fdb

from dump_agent.platform_runtime import fbclient_dll_path

logger = logging.getLogger(__name__)


def conectar_firebird() -> fdb.Connection:
    dll_path = fbclient_dll_path()
    fdb.load_api(str(dll_path))

    db_host = os.getenv("DB_HOST", "localhost")
    db_path = os.environ["DB_PATH"]
    dsn = f"{db_host}:{db_path}"

    con = fdb.connect(
        dsn=dsn,
        user=os.getenv("DB_USER", "SYSDBA"),
        password=os.environ["DB_PASSWORD"],
        charset="WIN1252",
    )
    logger.info("firebird_connected dsn=%s", dsn)
    return con
