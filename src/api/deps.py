"""Dependências compartilhadas da API (engine, settings)."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

import config

_engine: Engine | None = None


def get_engine() -> Engine:
    global _engine
    if _engine is None:
        _engine = create_engine(config.DB_URL)
    return _engine


@asynccontextmanager
async def lifespan(app: object) -> AsyncGenerator[None, None]:
    global _engine
    _engine = create_engine(config.DB_URL)
    yield
    if _engine is not None:
        _engine.dispose()
        _engine = None
