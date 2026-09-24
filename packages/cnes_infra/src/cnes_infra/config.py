"""Configuração centralizada — lê .env e expõe atributos tipados."""

import os
from functools import lru_cache
from pathlib import Path
from urllib.parse import quote, urlparse, urlunparse

from dotenv import load_dotenv

from cnes_domain.config import (
    _RE_CNPJ_14,
    _RE_COD_MUN_6,
    _RE_IBGE7,
    validar_formato,
)


def _find_project_root() -> Path:
    path = Path(__file__).resolve().parent
    while path != path.parent:
        if (path / "pyproject.toml").exists() and (path / "packages").exists():
            return path
        path = path.parent
    return Path.cwd()  # pragma: no cover - nunca ocorre com pacote instalado


RAIZ_PROJETO = _find_project_root()

load_dotenv(RAIZ_PROJETO / ".env", override=False, encoding="utf-8")


def _exigir(nome: str) -> str:
    valor = os.getenv(nome)
    if not valor:
        raise OSError(
            f"Variável de ambiente obrigatória '{nome}' não encontrada. "
            f"Verifique o arquivo .env na raiz do projeto."
        )
    return valor


def _exigir_inteiro(nome: str, padrao: int) -> int:
    valor = os.getenv(nome, str(padrao))
    try:
        return int(valor)
    except ValueError as err:
        raise OSError(
            f"variavel={nome} valor='{valor}' tipo_esperado=int",
        ) from err


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

S3_REGION: str = os.getenv("S3_REGION", "sa-east-1")
S3_BUCKET: str = os.getenv("S3_BUCKET", "cnesdata-landing")
# Vazio = resolvedor padrão do boto3 (S3 real). Preenchido em dev/CI para
# apontar para AIStor/LocalStack. "" != None: boto3.client(endpoint_url="")
# tentaria assinar contra um host vazio, por isso consumidores devem
# normalizar com `S3_ENDPOINT_URL or None`.
S3_ENDPOINT_URL: str = os.getenv("S3_ENDPOINT_URL", "")
# Host embutido nas URLs presigned entregues ao edge agent — pode divergir
# do endpoint interno (ex.: alias de rede Docker "minio:9000", inalcançável
# fora do host). Default: mesmo valor de S3_ENDPOINT_URL (comportamento
# anterior, sem split). Porta MINIO_PUBLIC_ENDPOINT (PR #230, H9 —
# confirmado empiricamente: todo agente fora da rede Docker do VPS precisa
# disso setado para um host que ele alcança).
S3_PUBLIC_ENDPOINT_URL: str = os.getenv("S3_PUBLIC_ENDPOINT_URL") or S3_ENDPOINT_URL
S3_ADDRESSING_STYLE: str = os.getenv("S3_ADDRESSING_STYLE", "auto")
MAX_JITTER_SECONDS: float = float(
    os.getenv("MAX_JITTER_SECONDS", "1800"),
)

AUTH_REQUIRED: str = os.environ.get("AUTH_REQUIRED", "required")
ADMIN_TOKEN: str = os.environ.get("ADMIN_TOKEN", "").strip()
CORS_ALLOWED_ORIGINS: str = os.environ.get("CORS_ALLOWED_ORIGINS", "")
LEADS_RATE_LIMIT: str = os.environ.get("LEADS_RATE_LIMIT", "5/minute")
DASHBOARD_OIDC_ISSUER: str = os.environ.get("DASHBOARD_OIDC_ISSUER", "")
DASHBOARD_OIDC_AUDIENCE: str = os.environ.get(
    "DASHBOARD_OIDC_AUDIENCE", "cnesdata-dashboard",
)

AUTH_CA_CERT_PATH: str = os.environ.get("AUTH_CA_CERT_PATH", "")
AUTH_CA_KEY_PATH: str = os.environ.get("AUTH_CA_KEY_PATH", "")
AUTH_DEVICE_VERIFICATION_URI: str = os.environ.get(
    "AUTH_DEVICE_VERIFICATION_URI", "",
)
AUTH_DEVICE_CODE_TTL: int = _exigir_inteiro("AUTH_DEVICE_CODE_TTL", 600)
AUTH_ACCESS_TOKEN_TTL: int = _exigir_inteiro("AUTH_ACCESS_TOKEN_TTL", 300)
AUTH_CERT_TTL_DAYS: int = _exigir_inteiro("AUTH_CERT_TTL_DAYS", 90)
# Fail-closed: only an explicit "false" (local stack without Caddy) disables it.
AGENT_MTLS_REQUIRED: bool = (
    os.environ.get("AGENT_MTLS_REQUIRED", "true").strip().lower() != "false"
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


_LAZY_ATTRS: dict[str, object] = {
    "DB_PATH": _firebird_db_path,
    "DB_PASSWORD": _firebird_db_password,
    "DB_DSN": lambda: f"{DB_HOST}:{_firebird_db_path()}",
    "FIREBIRD_DLL": _firebird_dll,
    "GCP_PROJECT_ID": _gcp_project_id,
    "COD_MUN_IBGE": lambda: validar_formato(
        "COD_MUN_IBGE", _exigir("COD_MUN_IBGE"), _RE_COD_MUN_6,
    ),
    "ID_MUNICIPIO_IBGE7": lambda: validar_formato(
        "ID_MUNICIPIO_IBGE7", _exigir("ID_MUNICIPIO_IBGE7"), _RE_IBGE7,
    ),
    "CNPJ_MANTENEDORA": lambda: validar_formato(
        "CNPJ_MANTENEDORA", _exigir("CNPJ_MANTENEDORA"), _RE_CNPJ_14,
    ),
}


def __getattr__(name: str) -> object:
    if name in _LAZY_ATTRS:
        return _LAZY_ATTRS[name]()
    raise AttributeError(f"module 'cnes_infra.config' has no attribute '{name}'")
