"""Fixtures de integração — conecta à stack Docker já rodando.

Uso: docker compose up -d → pytest -m postgres tests/integration/
"""
import json
import os
import urllib.request

import pytest
from sqlalchemy import create_engine


def _is_healthy(url: str) -> bool:
    try:
        resp = urllib.request.urlopen(
            f"{url}/api/v1/system/health", timeout=5,
        )
        body = json.loads(resp.read())
        return body.get("status") == "ok"
    except Exception:
        return False


@pytest.fixture(scope="session")
def api_url():
    url = os.getenv("API_TEST_URL", "http://localhost:8000")
    if not _is_healthy(url):
        pytest.skip(
            f"central-api indisponível em {url}; "
            "rode 'docker compose up -d' primeiro",
        )
    return url


@pytest.fixture(scope="session")
def pg_engine():
    url = os.getenv(
        "PG_TEST_URL",
        "postgresql+psycopg://cnesdata:cnesdata_test@localhost:5433/cnesdata_test",
    )
    engine = create_engine(url)
    try:
        with engine.connect() as con:
            con.execute(
                __import__("sqlalchemy").text("SELECT 1"),
            )
    except Exception:
        pytest.skip(
            f"postgres indisponível em {url}; "
            "rode 'docker compose up -d' primeiro",
        )
    return engine
