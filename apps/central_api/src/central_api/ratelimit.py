"""Shared slowapi limiter keyed by the client IP behind the edge proxy."""
from fastapi import Request
from fastapi.responses import JSONResponse
from slowapi import Limiter
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address


def client_ip(request: Request) -> str:
    """Return the originating X-Forwarded-For hop or the socket address."""
    forwarded = request.headers.get("X-Forwarded-For", "")
    if forwarded:
        return forwarded.split(",", 1)[0].strip() or get_remote_address(request)
    return get_remote_address(request)


def rate_limit_handler(_request: Request, exc: RateLimitExceeded) -> JSONResponse:
    """429 with Retry-After = window length of the exceeded limit (seconds)."""
    retry_after = exc.limit.limit.get_expiry()
    return JSONResponse(
        status_code=429,
        content={"detail": "rate_limited", "retry_after": retry_after},
        headers={"Retry-After": str(retry_after)},
    )


limiter = Limiter(key_func=client_ip)
