"""QueryCounterMiddleware behaviour tests."""
from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from central_api.middleware import (
    QueryCounterMiddleware,
    increment_query_count,
)


def _build_app(n_queries: int) -> FastAPI:
    app = FastAPI()
    app.add_middleware(QueryCounterMiddleware)

    @app.get("/sim")
    def handler():
        for _ in range(n_queries):
            increment_query_count()
        return {"ok": True}

    return app


def test_conta_queries_no_header_x_query_count():
    client = TestClient(_build_app(7))
    r = client.get("/sim")
    assert r.status_code == 200
    assert r.headers["X-Query-Count"] == "7"
    assert "X-Query-Count-Warn" not in r.headers


def test_flag_quando_excede_threshold_15():
    client = TestClient(_build_app(20))
    r = client.get("/sim")
    assert r.status_code == 200
    assert r.headers["X-Query-Count"] == "20"
    assert r.headers["X-Query-Count-Warn"] == "threshold-exceeded"


def test_counter_reseta_entre_requests():
    client = TestClient(_build_app(5))
    r1 = client.get("/sim")
    r2 = client.get("/sim")
    assert r1.headers["X-Query-Count"] == "5"
    assert r2.headers["X-Query-Count"] == "5"
