"""Middlewares: TenantMiddleware, AuthMiddleware, QueryCounterMiddleware."""
import logging
from contextvars import ContextVar
from dataclasses import dataclass
from uuid import UUID

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from cnes_domain.tenant import set_tenant_id
from cnes_infra.auth import TokenInvalid

logger = logging.getLogger(__name__)

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


class TenantMiddleware(BaseHTTPMiddleware):

    async def dispatch(
        self, request: Request, call_next: object,
    ) -> Response:
        tid = request.headers.get("X-Tenant-Id")
        if tid:
            set_tenant_id(tid)
        return await call_next(request)


class AuthMiddleware(BaseHTTPMiddleware):

    async def dispatch(
        self, request: Request, call_next: object,
    ) -> Response:
        path = request.url.path
        if path.startswith(("/oauth/", "/provision/")):
            return await call_next(request)
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
        self, request: Request, call_next: object, err: TokenInvalid,
    ) -> Response:
        mode = getattr(request.app.state, "auth_required", "required")
        logger.warning("auth_token_invalid reason=%s mode=%s", err, mode)
        if mode == "optional":
            return await call_next(request)
        return JSONResponse(
            status_code=401, content={"detail": "token_invalid"},
        )

    async def _populate_user(
        self, request: Request, call_next: object, claims: dict,
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


class QueryCounterMiddleware(BaseHTTPMiddleware):

    async def dispatch(
        self, request: Request, call_next: object,
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
