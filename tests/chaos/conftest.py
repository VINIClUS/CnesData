"""Chaos injection fixtures for boundary failures."""
from __future__ import annotations

from contextlib import contextmanager
from itertools import count
from typing import TYPE_CHECKING, Literal

import pytest

if TYPE_CHECKING:
    from collections.abc import Iterator


@pytest.fixture
def inject_db_failure(monkeypatch):
    @contextmanager
    def injector(
        mode: Literal["timeout", "deadlock", "connection_refused"],
        calls_before_fail: int = 0,
    ) -> Iterator[None]:
        try:
            from sqlalchemy.engine import Connection
            from sqlalchemy.exc import OperationalError
        except ImportError:
            pytest.skip("sqlalchemy not installed")

        call_count = count()
        orig = Connection.execute

        def wrapped(self, *args, **kwargs):
            n = next(call_count)
            if n >= calls_before_fail:
                if mode == "timeout":
                    raise OperationalError("timeout", {}, Exception())
                if mode == "deadlock":
                    raise OperationalError("deadlock", {}, Exception())
                raise OperationalError("conn_refused", {}, Exception())
            return orig(self, *args, **kwargs)

        monkeypatch.setattr(Connection, "execute", wrapped)
        yield
    return injector


@pytest.fixture
def inject_minio_failure(monkeypatch):
    @contextmanager
    def injector(mode: Literal["put_timeout", "get_500", "auth_fail"]) -> Iterator[None]:
        def failing_presign(*args, **kwargs):
            if mode == "put_timeout":
                raise TimeoutError("minio_put_timeout")
            if mode == "get_500":
                raise RuntimeError("minio_500")
            raise PermissionError("minio_auth_fail")

        try:
            from minio import Minio
            monkeypatch.setattr(Minio, "presigned_put_object", failing_presign)
        except ImportError:
            pytest.skip("minio not installed")
        yield
    return injector


@pytest.fixture
def inject_http_failure(monkeypatch):
    @contextmanager
    def injector(mode: Literal["5xx", "timeout", "conn_reset"]) -> Iterator[None]:
        try:
            import httpx
        except ImportError:
            pytest.skip("httpx not installed")

        async def fake_request(self, *args, **kwargs):
            if mode == "5xx":
                return httpx.Response(502, text="upstream down")
            if mode == "timeout":
                raise httpx.TimeoutException("http_timeout")
            raise httpx.ConnectError("conn_reset")

        monkeypatch.setattr(httpx.AsyncClient, "request", fake_request)
        yield
    return injector
