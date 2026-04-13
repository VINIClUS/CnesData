"""Configuração do subsistema SIHD2 (AIH)."""

import os

from cnes_infra.config import DB_HOST, DB_PASSWORD, DB_USER, FIREBIRD_DLL

SIHD_DB_PATH: str = os.getenv(
    "SIHD_DB_PATH", r"C:\Datasus\SIHD2\BDSIHD2.GDB",
)
SIHD_DB_HOST: str = os.getenv("SIHD_DB_HOST", DB_HOST)
SIHD_DB_USER: str = os.getenv("SIHD_DB_USER", DB_USER)
SIHD_DB_PASSWORD: str = os.getenv("SIHD_DB_PASSWORD", DB_PASSWORD)
SIHD_DB_DSN: str = f"{SIHD_DB_HOST}:{SIHD_DB_PATH}"
SIHD_FIREBIRD_DLL: str = os.getenv("SIHD_FIREBIRD_DLL", FIREBIRD_DLL)
