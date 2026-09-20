"""Helper for emitting schema-compliant HTTP 422 validation errors."""

from fastapi import HTTPException


def validation_error(msg: str, loc: list[str] | None = None) -> HTTPException:
    """422 HTTPException whose `detail` matches FastAPI's own auto-documented
    HTTPValidationError shape (list of {loc, msg, type}), not a bare string.

    Application code that rejects a request with 422 outside Pydantic's own
    validation must match that same shape — every route's OpenAPI schema
    documents 422 as HTTPValidationError regardless of who raises it. A bare
    string `detail` breaks OpenAPI-generated clients that decode strictly
    (see H11, docs/edge-agent-audit-2026-09-20.md: the Go edge agent's generated
    client crashed unmarshaling a string into that list, discarding the
    real error message under a useless secondary parse-failure log line).

    Args:
        msg: Human-readable error message.
        loc: JSON-pointer-style location, e.g. ["body", "intent"]. Defaults
            to ["body"].

    Returns:
        HTTPException ready to raise.
    """
    return HTTPException(
        status_code=422,
        detail=[{"loc": loc or ["body"], "msg": msg, "type": "value_error"}],
    )
