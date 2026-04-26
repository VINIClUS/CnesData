"""Factory da aplicação FastAPI."""

from fastapi import FastAPI
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

from central_api.deps import lifespan
from central_api.middleware import (
    AuthMiddleware,
    QueryCounterMiddleware,
    TenantMiddleware,
)
from central_api.routes import (
    access_requests,
    admin,
    agents,
    dashboard,
    extractions,
    health,
    jobs,
    oauth,
)
from cnes_infra.telemetry import init_telemetry

init_telemetry("central-api")


def create_app() -> FastAPI:
    """Cria e configura a aplicação FastAPI."""
    app = FastAPI(
        title="CnesData Processing Engine",
        version="1.0.0",
        lifespan=lifespan,
    )
    app.add_middleware(QueryCounterMiddleware)
    app.add_middleware(TenantMiddleware)
    app.add_middleware(AuthMiddleware)
    app.state.limiter = oauth.limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
    app.include_router(jobs.router, prefix="/api/v1")
    app.include_router(health.router, prefix="/api/v1")
    app.include_router(admin.router, prefix="/api/v1")
    app.include_router(agents.router, prefix="/api/v1")
    app.include_router(extractions.router, prefix="/api/v1")
    app.include_router(dashboard.router, prefix="/api/v1/dashboard")
    app.include_router(
        access_requests.router,
        prefix="/api/v1/dashboard/access-requests",
    )
    app.include_router(oauth.router)
    return app
