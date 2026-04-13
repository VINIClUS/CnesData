"""Factory da aplicação FastAPI."""

from fastapi import FastAPI

from api.deps import lifespan
from api.routes import health, ingest, jobs


def create_app() -> FastAPI:
    """Cria e configura a aplicação FastAPI."""
    app = FastAPI(
        title="CnesData Processing Engine",
        version="1.0.0",
        lifespan=lifespan,
    )
    app.include_router(ingest.router, prefix="/api/v1")
    app.include_router(jobs.router, prefix="/api/v1")
    app.include_router(health.router, prefix="/api/v1")
    return app
