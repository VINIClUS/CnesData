"""Resolução da configuração de banco para migrações."""
import os
from urllib.parse import quote, urlparse, urlunparse

from dotenv import load_dotenv


def sanitize_database_url(raw: str) -> str:
    parsed = urlparse(raw)
    scheme = "postgresql+psycopg"
    usuario = quote(parsed.username or "", safe="")
    senha = quote(parsed.password or "", safe="")
    netloc = f"{usuario}:{senha}@{parsed.hostname}"
    if parsed.port:
        netloc += f":{parsed.port}"
    return urlunparse(parsed._replace(scheme=scheme, netloc=netloc))

def resolve_database_url(override: str | None) -> str:
    if override:
        return override
    load_dotenv(override=False, encoding="utf-8")
    value = os.getenv("DB_URL")
    if not value:
        raise OSError("env_missing key=DB_URL")
    return sanitize_database_url(value)
