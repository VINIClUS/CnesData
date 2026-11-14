"""Configuração centralizada — lê config.json do host, .env e expõe atributos tipados."""

import json
import os
import re
import stat
from collections.abc import Callable, Mapping, MutableMapping
from functools import lru_cache
from pathlib import Path
from typing import cast
from urllib.parse import quote, urlparse, urlunparse

from dotenv import load_dotenv

from cnes_domain.config import (
    _RE_CNPJ_14,  # pyright: ignore[reportPrivateUsage]
    _RE_COD_MUN_6,  # pyright: ignore[reportPrivateUsage]
    _RE_IBGE7,  # pyright: ignore[reportPrivateUsage]
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
CONFIG_JSON = Path("/run/config/config.json")
_CONFIG_JSON_LIMITE = 65536
_CREDENCIAL = "AWS_SHARED_CREDENTIALS_FILE"
_CHAVE_CONFIG = re.compile(r"[A-Z_][A-Z0-9_]*")
# The host gate admits only AWS_SHARED_CREDENTIALS_FILE among these names in the compose env; the
# owner-pinned file must not reopen credential, endpoint or loader authority through os.environ.
_CHAVES_DE_AUTORIDADE = frozenset({
    "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_SECURITY_TOKEN",
    "AWS_PROFILE", "AWS_DEFAULT_PROFILE", "AWS_CONFIG_FILE", _CREDENCIAL, "AWS_ROLE_ARN",
    "AWS_ROLE_SESSION_NAME", "AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_CA_BUNDLE", "AWS_DATA_PATH",
    "DYNAMODB_ENDPOINT_URL",
})
_PREFIXOS_DE_AUTORIDADE = (
    "AWS_CONTAINER_", "AWS_EC2_METADATA_", "AWS_ENDPOINT_URL", "DOCKER_", "SSH_", "LD_", "PYTHON",
)


def carregar_config_json(caminho: Path, ambiente: MutableMapping[str, str]) -> None:
    """Args: caminho: config.json montado pelo host gate; ambiente: os.environ ou cópia.
    Raises: OSError: arquivo presente fora do contrato, valor divergente do ambiente ou
    AWS_SHARED_CREDENTIALS_FILE sem arquivo regular legível; o ambiente fica intacto.
    """
    bruto = _ler_config(caminho)
    if bruto is None:
        return
    valores = _valores_config(bruto)
    for chave, valor in sorted(valores.items()):
        if ambiente.get(chave, valor) != valor:
            raise OSError(f"config_json=conflict key={chave}")
    _exigir_arquivo_de_credencial(ambiente)
    ambiente.update(valores)


def _ler_config(caminho: Path) -> bytes | None:
    try:
        with caminho.open("rb") as arquivo:
            bruto = arquivo.read(_CONFIG_JSON_LIMITE + 1)
    except FileNotFoundError:
        return None
    except OSError as err:
        raise OSError(f"config_json=unreadable path={caminho}") from err
    if len(bruto) > _CONFIG_JSON_LIMITE:
        raise OSError(f"config_json=too_large limit={_CONFIG_JSON_LIMITE}")
    return bruto


def _sem_duplicatas(pares: list[tuple[str, object]]) -> dict[str, object]:
    valores: dict[str, object] = {}
    for chave, valor in pares:
        if chave in valores:
            raise OSError(f"config_json=duplicate_key key={chave}")
        valores[chave] = valor
    return valores


def _valores_config(bruto: bytes) -> dict[str, str]:
    try:
        documento: object = json.loads(bruto.decode("utf-8"), object_pairs_hook=_sem_duplicatas)
    except (UnicodeDecodeError, json.JSONDecodeError) as err:
        raise OSError("config_json=invalid_json") from err
    if not isinstance(documento, dict):
        raise OSError("config_json=not_object")
    pares = cast("dict[str, object]", documento)
    return {chave: _valor_config(chave, valor) for chave, valor in pares.items()}


def _valor_config(chave: str, valor: object) -> str:
    if _CHAVE_CONFIG.fullmatch(chave) is None:
        raise OSError(f"config_json=invalid_key key={chave}")
    if chave in _CHAVES_DE_AUTORIDADE or chave.startswith(_PREFIXOS_DE_AUTORIDADE):
        raise OSError(f"config_json=credential_authority key={chave}")
    if not isinstance(valor, str) or "\x00" in valor:
        raise OSError(f"config_json=invalid_value key={chave}")
    return valor


def _exigir_arquivo_de_credencial(ambiente: Mapping[str, str]) -> None:
    caminho = Path(ambiente.get(_CREDENCIAL, ""))
    try:
        regular = caminho.is_absolute() and stat.S_ISREG(caminho.stat().st_mode)
        if regular:
            caminho.open("rb").close()
    except (FileNotFoundError, NotADirectoryError):
        regular = False
    except OSError as err:
        raise OSError(f"credentials_file=unreadable key={_CREDENCIAL}") from err
    if not regular:
        raise OSError(f"credentials_file=missing key={_CREDENCIAL}")


carregar_config_json(CONFIG_JSON, os.environ)
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


_LAZY_ATTRS: dict[str, Callable[[], object]] = {
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
