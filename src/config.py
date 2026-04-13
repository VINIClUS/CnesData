"""config.py — Configuração centralizada (Single Source of Truth)."""

import os
import re
from functools import lru_cache
from pathlib import Path
from urllib.parse import quote, urlparse, urlunparse

from dotenv import load_dotenv

RAIZ_PROJETO = Path(__file__).parent.parent

load_dotenv(RAIZ_PROJETO / ".env", override=False, encoding="utf-8")

_RE_COD_MUN_6: re.Pattern[str] = re.compile(r"^\d{6}$")
_RE_IBGE7: re.Pattern[str] = re.compile(r"^\d{7}$")
_RE_CNPJ_14: re.Pattern[str] = re.compile(r"^\d{14}$")


def _exigir(nome: str) -> str:
    valor = os.getenv(nome)
    if not valor:
        raise EnvironmentError(
            f"Variável de ambiente obrigatória '{nome}' não encontrada. "
            f"Verifique o arquivo .env na raiz do projeto."
        )
    return valor


def _validar_formato(nome: str, valor: str, padrao: re.Pattern[str]) -> str:
    if not padrao.match(valor):
        raise EnvironmentError(f"variavel={nome} valor='{valor}' formato_invalido")
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

COD_MUN_IBGE: str = _validar_formato(
    "COD_MUN_IBGE", _exigir("COD_MUN_IBGE"), _RE_COD_MUN_6
)
ID_MUNICIPIO_IBGE7: str = _validar_formato(
    "ID_MUNICIPIO_IBGE7", _exigir("ID_MUNICIPIO_IBGE7"), _RE_IBGE7
)
CNPJ_MANTENEDORA: str = _validar_formato(
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


_LAZY_ATTRS: dict[str, object] = {
    "DB_PATH": _firebird_db_path,
    "DB_PASSWORD": _firebird_db_password,
    "DB_DSN": lambda: f"{DB_HOST}:{_firebird_db_path()}",
    "FIREBIRD_DLL": _firebird_dll,
    "GCP_PROJECT_ID": _gcp_project_id,
}


def __getattr__(name: str) -> object:
    if name in _LAZY_ATTRS:
        return _LAZY_ATTRS[name]()
    raise AttributeError(f"module 'config' has no attribute '{name}'")
