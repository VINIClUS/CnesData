"""Tenant context — ContextVar para isolamento multi-tenant."""

from contextvars import ContextVar

tenant_id_ctx: ContextVar[str] = ContextVar("tenant_id")


def get_tenant_id() -> str:
    try:
        return tenant_id_ctx.get()
    except LookupError as err:
        raise RuntimeError("tenant_id not set in context") from err


def set_tenant_id(tid: str) -> None:
    tenant_id_ctx.set(tid)


class InvalidTenantError(ValueError):
    pass


def validate_tenant_id(tid: str) -> None:
    if not isinstance(tid, str) or len(tid) != 6 or not tid.isdigit():
        raise InvalidTenantError(f"tenant_invalid value={tid!r}")
