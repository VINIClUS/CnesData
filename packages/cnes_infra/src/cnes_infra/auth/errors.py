"""OAuthError: RFC 6749 §5.2 / RFC 8628 §3.5 error body shape."""
from __future__ import annotations


class OAuthError(Exception):
    """Raised by OAuth + provision routes; rendered by app exception handler.

    Args:
        code: RFC 6749 error code (e.g. "invalid_grant", "slow_down").
        description: human-readable error_description (optional).
        status_code: HTTP status (default 400; 401 for invalid_token).
        extra: extra body fields (e.g. {"interval": 10} for slow_down).
    """

    def __init__(
        self,
        code: str,
        *,
        description: str | None = None,
        status_code: int = 400,
        extra: dict | None = None,
    ) -> None:
        super().__init__(code)
        self.code = code
        self.description = description
        self.status_code = status_code
        self.extra = extra or {}
