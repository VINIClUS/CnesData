"""Testes de saúde dos serviços Docker."""
import json
import urllib.request

import pytest
from sqlalchemy import text

pytestmark = pytest.mark.postgres


def test_health_endpoint_retorna_ok(api_url):
    resp = urllib.request.urlopen(f"{api_url}/api/v1/system/health")
    body = json.loads(resp.read())
    assert body["status"] == "ok"
    assert body["db_connected"] is True


def test_openapi_schema_valido(api_url):
    resp = urllib.request.urlopen(f"{api_url}/openapi.json")
    schema = json.loads(resp.read())
    assert "paths" in schema
    assert "/api/v1/system/health" in schema["paths"]
    assert "/api/v1/jobs/acquire" in schema["paths"]


def test_postgres_gold_schema_existe(pg_engine):
    with pg_engine.connect() as con:
        result = con.execute(
            text(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'gold'"
            ),
        )
        tables = {row[0] for row in result}
    assert "dim_profissional" in tables
    assert "dim_estabelecimento" in tables
    assert "fato_vinculo" in tables


def test_postgres_landing_schema_existe(pg_engine):
    with pg_engine.connect() as con:
        result = con.execute(
            text(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'landing'"
            ),
        )
        tables = {row[0] for row in result}
    assert "raw_payload" in tables


def test_job_queue_tabela_existe(pg_engine):
    with pg_engine.connect() as con:
        result = con.execute(
            text(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'queue' "
                "AND table_name = 'jobs'"
            ),
        )
        assert result.first() is not None
