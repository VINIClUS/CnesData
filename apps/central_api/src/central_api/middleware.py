"""TenantMiddleware — extrai tenant_id do header X-Tenant-Id."""

from contextvars import ContextVar

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from cnes_domain.tenant import set_tenant_id

_query_count: ContextVar[list[int] | None] = ContextVar(
    "query_count", default=None,
)


class TenantMiddleware(BaseHTTPMiddleware):

    async def dispatch(
        self, request: Request, call_next: object,
    ) -> Response:
        tid = request.headers.get("X-Tenant-Id")
        if tid:
            set_tenant_id(tid)
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
