"""Middlewares: AuthMiddleware, QueryCounterMiddleware."""
import logging
from contextvars import ContextVar
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from uuid import UUID

from starlette.concurrency import run_in_threadpool
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from central_api.auth.aws_oidc import TenantAccessDenied
from cnes_domain.tenant import set_tenant_id
from cnes_infra.auth import TokenInvalid

if TYPE_CHECKING:
    from cnes_infra.auth.oidc import OidcPrincipal, OidcVerifier

logger = logging.getLogger(__name__)

_HEALTH_PATH = "/api/v1/system/health"
_TENANT_SCOPED_PREFIX = "/api/v1/dashboard/"

_query_count: ContextVar[list[int] | None] = ContextVar(
    "query_count", default=None,
)


@dataclass
class AuthenticatedUser:
    user_id: UUID
    email: str
    display_name: str | None
    role: str
    tenant_ids: list[str]


class AuthMiddleware(BaseHTTPMiddleware):

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint,
    ) -> Response:
        path = request.url.path
        if path.startswith(("/oauth/", "/provision/", "/api/v1/public/")):
            return await call_next(request)
        verifier = getattr(request.app.state, "oidc_verifier", None)
        if verifier is not None:
            return await self._dispatch_oidc(request, call_next, verifier)
        header = request.headers.get("Authorization", "")
        if not header.lower().startswith("bearer "):
            return await call_next(request)
        validator = getattr(request.app.state, "jwt_validator", None)
        if validator is None:
            return await call_next(request)
        token = header[7:].strip()
        try:
            claims = validator.verify(token)
        except TokenInvalid as e:
            return await self._on_invalid(request, call_next, e)
        return await self._populate_user(request, call_next, claims)

    async def _on_invalid(
        self, request: Request, call_next: RequestResponseEndpoint, err: TokenInvalid,
    ) -> Response:
        mode = getattr(request.app.state, "auth_required", "required")
        logger.warning("auth_token_invalid reason=%s mode=%s", err, mode)
        if mode == "optional":
            return await call_next(request)
        return JSONResponse(
            status_code=401, content={"detail": "token_invalid"},
        )

    async def _populate_user(
        self, request: Request, call_next: RequestResponseEndpoint, claims: dict[str, Any],
    ) -> Response:
        repo = request.app.state.dashboard_repo
        user = repo.upsert_user(
            oidc_subject=claims["sub"],
            oidc_issuer=claims["iss"],
            email=claims.get("email", ""),
            display_name=claims.get("name"),
        )
        request.state.user = AuthenticatedUser(
            user_id=user.user_id, email=user.email,
            display_name=user.display_name, role=user.role,
            tenant_ids=list(user.tenant_ids),
        )
        return await call_next(request)

    async def _dispatch_oidc(
        self, request: Request, call_next: RequestResponseEndpoint, verifier: "OidcVerifier",
    ) -> Response:
        path = request.url.path
        if path == _HEALTH_PATH:
            return await call_next(request)
        tenant_scoped = path.startswith(_TENANT_SCOPED_PREFIX)
        header = request.headers.get("Authorization", "")
        if not header.lower().startswith("bearer "):
            if tenant_scoped:
                return _deny(401, "auth_required")
            return await call_next(request)
        try:
            principal = await run_in_threadpool(verifier.verify, header[7:].strip())
        except TokenInvalid as e:
            logger.warning("oidc_token_invalid reason=%s", e)
            return _deny(401, "token_invalid")
        request.state.principal = principal
        if tenant_scoped:
            return await self._authorize_tenant(request, call_next, principal)
        return await call_next(request)

    async def _authorize_tenant(
        self, request: Request, call_next: RequestResponseEndpoint, principal: "OidcPrincipal",
    ) -> Response:
        requested = request.headers.get("X-Tenant-Id", "").strip()
        if not requested:
            return _deny(400, "tenant_header_required")
        authorizer = request.app.state.runtime.services.membership_authorizer
        try:
            authorized = await run_in_threadpool(authorizer.authorize, principal, requested)
        except TenantAccessDenied as e:
            logger.warning("tenant_access_denied code=%s", e.code)
            return _deny(403, "tenant_not_allowed")
        request.state.authorized_tenant = authorized
        set_tenant_id(authorized.tenant_id)
        return await call_next(request)


def _deny(status_code: int, detail: str) -> JSONResponse:
    return JSONResponse(status_code=status_code, content={"detail": detail})


class QueryCounterMiddleware(BaseHTTPMiddleware):

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint,
    ) -> Response:
        counter = [0]
        _query_count.set(counter)
        response = await call_next(request)
        count = counter[0]
        response.headers["X-Query-Count"] = str(count)
        if count > 15:
            response.headers["X-Query-Count-Warn"] = "threshold-exceeded"
        return response


def increment_query_count() -> None:
    counter = _query_count.get()
    if counter is not None:
        counter[0] += 1
