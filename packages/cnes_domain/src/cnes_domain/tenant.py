"""Tenant context — ContextVar para isolamento multi-tenant."""

from contextvars import ContextVar

tenant_id_ctx: ContextVar[str] = ContextVar("tenant_id")


def get_tenant_id() -> str:
    try:
        return tenant_id_ctx.get()
    except LookupError:
        raise RuntimeError("tenant_id not set in context")


def set_tenant_id(tid: str) -> None:
    tenant_id_ctx.set(tid)
