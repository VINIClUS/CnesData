"""Factory da aplicação FastAPI."""

import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from slowapi.errors import RateLimitExceeded

from central_api.deps import lifespan
from central_api.middleware import AuthMiddleware, QueryCounterMiddleware
from central_api.ratelimit import limiter, rate_limit_handler
from central_api.routes import (
    access_requests,
    admin,
    agents,
    billing,
    billing_admin,
    dashboard,
    extractions,
    health,
    jobs,
    local_auth,
    oauth,
    overview,
    provision,
    provision_rotate,
    public_leads,
    raw_admin,
    raw_jobs,
    raw_manifests,
    serving,
    stripe_webhook,
    tenants,
)
from cnes_domain.profiles import RuntimeProfile
from cnes_infra import config
from cnes_infra.auth.errors import OAuthError
from cnes_infra.aws.settings import production_policy_enabled
from cnes_infra.telemetry import init_telemetry

if TYPE_CHECKING:
    from starlette.types import ExceptionHandler

init_telemetry("central-api")

_CORS_HEADERS = ("Authorization", "Content-Type", "X-Tenant-Id")
_CORS_MAX_AGE_SECONDS = 600


@dataclass(frozen=True, slots=True)
class CorsPolicy:
    allow_origins: tuple[str, ...]
    allow_methods: tuple[str, ...]
    allow_headers: tuple[str, ...]
    allow_credentials: bool
    max_age: int


_PRODUCTION_CORS = CorsPolicy(
    allow_origins=("https://cnesdata.com.br",),
    allow_methods=("GET", "POST"),
    allow_headers=_CORS_HEADERS,
    allow_credentials=False,
    max_age=_CORS_MAX_AGE_SECONDS,
)


async def _oauth_error_handler(
    _request: Request, exc: OAuthError,
) -> JSONResponse:
    body: dict[str, object] = {"error": exc.code}
    if exc.description:
        body["error_description"] = exc.description
    body.update(exc.extra)
    return JSONResponse(status_code=exc.status_code, content=body)


def cors_origins() -> list[str]:
    """Explicit allow-list from CORS_ALLOWED_ORIGINS (comma-separated); never a wildcard."""
    origins = (o.strip() for o in config.CORS_ALLOWED_ORIGINS.split(","))
    return [o for o in origins if o and o != "*"]


def cors_policy(values: Mapping[str, str]) -> CorsPolicy:
    """Args: values: Variáveis ambientais disponíveis.
    Returns: Política exata sob PROFILE=aws com AWS_PRODUCTION_POLICY=true; senão a atual.
    Raises: AwsRuntimeConfigurationError: AWS_PRODUCTION_POLICY inválido sob PROFILE=aws.
    """
    if _aws_profile(values) and production_policy_enabled(values):
        return _PRODUCTION_CORS
    return CorsPolicy(
        allow_origins=tuple(cors_origins()),
        allow_methods=("GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"),
        allow_headers=_CORS_HEADERS,
        allow_credentials=True,
        max_age=_CORS_MAX_AGE_SECONDS,
    )


def _aws_profile(values: Mapping[str, str]) -> bool:
    return values.get("PROFILE", "").strip().lower() == RuntimeProfile.AWS


def _install_cors(app: FastAPI, policy: CorsPolicy) -> None:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=policy.allow_origins,
        allow_credentials=policy.allow_credentials,
        allow_methods=policy.allow_methods,
        allow_headers=policy.allow_headers,
        max_age=policy.max_age,
    )


def create_app() -> FastAPI:
    """Cria e configura a aplicação FastAPI."""
    policy = cors_policy(os.environ)
    app = FastAPI(
        title="CnesData Processing Engine",
        version="1.0.0",
        lifespan=lifespan,
    )
    app.add_middleware(QueryCounterMiddleware)
    app.add_middleware(AuthMiddleware)
    _install_cors(app, policy)
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, cast("ExceptionHandler", rate_limit_handler))
    app.add_exception_handler(OAuthError, cast("ExceptionHandler", _oauth_error_handler))
    app.include_router(jobs.router, prefix="/api/v1")
    app.include_router(health.router, prefix="/api/v1")
    app.include_router(admin.router, prefix="/api/v1")
    app.include_router(agents.router, prefix="/api/v1")
    app.include_router(extractions.router, prefix="/api/v1")
    app.include_router(dashboard.router, prefix="/api/v1/dashboard")
    app.include_router(overview.router, prefix="/api/v1/dashboard")
    app.include_router(
        access_requests.router,
        prefix="/api/v1/dashboard/access-requests",
    )
    app.include_router(public_leads.router, prefix="/api/v1/public")
    app.include_router(oauth.router)
    app.include_router(oauth.activation_router, prefix="/api/v1")
    app.include_router(provision.router)
    app.include_router(provision_rotate.router)
    app.include_router(raw_jobs.router)
    app.include_router(raw_admin.router)
    app.include_router(raw_manifests.router)
    app.include_router(local_auth.router)
    app.include_router(serving.router)
    app.include_router(billing.router)
    app.include_router(tenants.router)
    app.include_router(billing_admin.router)
    app.include_router(stripe_webhook.router)
    return app
