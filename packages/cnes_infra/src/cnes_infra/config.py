"""Configuração centralizada — lê .env e expõe atributos tipados."""

import os
from functools import lru_cache
from pathlib import Path
from urllib.parse import quote, urlparse, urlunparse

from cnes_domain.config import (
    _RE_CNPJ_14,
    _RE_COD_MUN_6,
    _RE_IBGE7,
    validar_formato,
)
from dotenv import load_dotenv


def _find_project_root() -> Path:
    path = Path(__file__).resolve().parent
    while path != path.parent:
        if (path / "pyproject.toml").exists() and (path / "packages").exists():
            return path
        path = path.parent
    return Path.cwd()


RAIZ_PROJETO = _find_project_root()

load_dotenv(RAIZ_PROJETO / ".env", override=False, encoding="utf-8")


def _exigir(nome: str) -> str:
    valor = os.getenv(nome)
    if not valor:
        raise EnvironmentError(
            f"Variável de ambiente obrigatória '{nome}' não encontrada. "
            f"Verifique o arquivo .env na raiz do projeto."
        )
    return valor


def _exigir_inteiro(nome: str, padrao: int) -> int:
    valor = os.getenv(nome, str(padrao))
    try:
        return int(valor)
    except ValueError:
        raise EnvironmentError(f"variavel={nome} valor='{valor}' tipo_esperado=int")


def _sanitizar_db_url(raw: str) -> str:
    parsed = urlparse(raw)
    scheme = "postgresql+psycopg"
    usuario = quote(parsed.username or "", safe="")
    senha = quote(parsed.password or "", safe="")
    netloc = f"{usuario}:{senha}@{parsed.hostname}"
    if parsed.port:
        netloc += f":{parsed.port}"
    return urlunparse(parsed._replace(scheme=scheme, netloc=netloc))


DB_HOST: str = os.getenv("DB_HOST", "localhost")
DB_USER: str = os.getenv("DB_USER", "SYSDBA")

COD_MUN_IBGE: str = validar_formato(
    "COD_MUN_IBGE", _exigir("COD_MUN_IBGE"), _RE_COD_MUN_6
)
ID_MUNICIPIO_IBGE7: str = validar_formato(
    "ID_MUNICIPIO_IBGE7", _exigir("ID_MUNICIPIO_IBGE7"), _RE_IBGE7
)
CNPJ_MANTENEDORA: str = validar_formato(
    "CNPJ_MANTENEDORA", _exigir("CNPJ_MANTENEDORA"), _RE_CNPJ_14
)

DB_URL: str = _sanitizar_db_url(_exigir("DB_URL"))
CACHE_DIR: Path = RAIZ_PROJETO / os.getenv("CACHE_DIR", "data/cache")

_output_dir = os.getenv("OUTPUT_DIR", "data/processed")
_output_filename = os.getenv("OUTPUT_FILENAME", "Relatorio_Profissionais_CNES.csv")
OUTPUT_PATH: Path = RAIZ_PROJETO / _output_dir / _output_filename

COMPETENCIA_ANO: int = _exigir_inteiro("COMPETENCIA_ANO", 2026)
COMPETENCIA_MES: int = _exigir_inteiro("COMPETENCIA_MES", 1)

LOGS_DIR: Path = RAIZ_PROJETO / "logs"
LOG_FILE: Path = LOGS_DIR / "cnes_exporter.log"

DLQ_THRESHOLD: float = float(os.getenv("DLQ_THRESHOLD", "0.05"))

API_HOST: str = os.getenv("API_HOST", "0.0.0.0")
API_PORT: int = _exigir_inteiro("API_PORT", 8000)

MINIO_ENDPOINT: str = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_SECURE: bool = os.getenv("MINIO_SECURE", "false").lower() == "true"
MINIO_BUCKET: str = os.getenv("MINIO_BUCKET", "cnesdata-landing")
MAX_JITTER_SECONDS: float = float(
    os.getenv("MAX_JITTER_SECONDS", "1800"),
)


@lru_cache(maxsize=1)
def _firebird_db_path() -> str:
    return _exigir("DB_PATH")


@lru_cache(maxsize=1)
def _firebird_db_password() -> str:
    return _exigir("DB_PASSWORD")


@lru_cache(maxsize=1)
def _firebird_dll() -> str:
    return _exigir("FIREBIRD_DLL")


@lru_cache(maxsize=1)
def _gcp_project_id() -> str:
    return _exigir("GCP_PROJECT_ID")


def _minio_access_key() -> str:
    return os.getenv("MINIO_ACCESS_KEY", "minioadmin")


def _minio_secret_key() -> str:
    return os.getenv("MINIO_SECRET_KEY", "minioadmin")


_LAZY_ATTRS: dict[str, object] = {
    "DB_PATH": _firebird_db_path,
    "DB_PASSWORD": _firebird_db_password,
    "DB_DSN": lambda: f"{DB_HOST}:{_firebird_db_path()}",
    "FIREBIRD_DLL": _firebird_dll,
    "GCP_PROJECT_ID": _gcp_project_id,
    "MINIO_ACCESS_KEY": _minio_access_key,
    "MINIO_SECRET_KEY": _minio_secret_key,
}


def __getattr__(name: str) -> object:
    if name in _LAZY_ATTRS:
        return _LAZY_ATTRS[name]()
    raise AttributeError(f"module 'cnes_infra.config' has no attribute '{name}'")
