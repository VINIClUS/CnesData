"""Firebird connection helper for dump_agent."""

import logging
import os
from pathlib import Path

import fdb

logger = logging.getLogger(__name__)


def conectar_firebird() -> fdb.Connection:
    dll_path = Path(os.environ["FIREBIRD_DLL"])
    if not dll_path.exists():
        raise FileNotFoundError(f"dll_path={dll_path}")
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
