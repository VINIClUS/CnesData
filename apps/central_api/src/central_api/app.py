"""Factory da aplicação FastAPI."""

from fastapi import FastAPI

from central_api.deps import lifespan
from central_api.middleware import TenantMiddleware
from central_api.routes import admin, agents, health, jobs
from cnes_infra.telemetry import init_telemetry

init_telemetry("central-api")


def create_app() -> FastAPI:
    """Cria e configura a aplicação FastAPI."""
    app = FastAPI(
        title="CnesData Processing Engine",
        version="1.0.0",
        lifespan=lifespan,
    )
    app.add_middleware(TenantMiddleware)
    app.include_router(jobs.router, prefix="/api/v1")
    app.include_router(health.router, prefix="/api/v1")
    app.include_router(admin.router, prefix="/api/v1")
    app.include_router(agents.router, prefix="/api/v1")
    return app
