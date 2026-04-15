"""TenantMiddleware — extrai tenant_id do header X-Tenant-Id."""

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from cnes_domain.tenant import set_tenant_id


class TenantMiddleware(BaseHTTPMiddleware):

    async def dispatch(
        self, request: Request, call_next: object,
    ) -> Response:
        tid = request.headers.get("X-Tenant-Id")
        if tid:
            set_tenant_id(tid)
        return await call_next(request)
