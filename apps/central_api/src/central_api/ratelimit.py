"""Shared slowapi limiter keyed by the client IP behind the edge proxy."""
import ipaddress
import os
from functools import lru_cache

from fastapi import Request
from fastapi.responses import JSONResponse
from slowapi import Limiter
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

_IpNetwork = ipaddress.IPv4Network | ipaddress.IPv6Network


@lru_cache
def _parse_networks(raw: str) -> tuple[_IpNetwork, ...]:
    """Cache key is always the raw env string, never a fresh os.getenv() read."""
    networks = []
    for raw_entry in raw.split(","):
        entry = raw_entry.strip()
        if not entry:
            continue
        try:
            networks.append(ipaddress.ip_network(entry, strict=False))
        except ValueError:
            continue
    return tuple(networks)


def _is_trusted(addr: str, networks: tuple[_IpNetwork, ...]) -> bool:
    try:
        ip = ipaddress.ip_address(addr)
    except ValueError:
        return False
    return any(ip in network for network in networks)


def _trusted_networks() -> tuple[_IpNetwork, ...]:
    return _parse_networks(os.getenv("TRUSTED_PROXY_CIDRS", ""))


def is_trusted_proxy_peer(request: Request) -> bool:
    """Returns: True se o peer do socket está em TRUSTED_PROXY_CIDRS."""
    return _is_trusted(get_remote_address(request), _trusted_networks())


def client_ip(request: Request) -> str:
    """Return the client IP, trusting X-Forwarded-For only from an allowlisted proxy peer."""
    peer = get_remote_address(request)
    networks = _trusted_networks()
    if not networks or not _is_trusted(peer, networks):
        return peer
    forwarded = request.headers.get("X-Forwarded-For", "")
    hops = [hop.strip() for hop in reversed(forwarded.split(",")) if hop.strip()]
    for hop in hops:
        if not _is_trusted(hop, networks):
            return hop
    return peer


def rate_limit_handler(_request: Request, exc: RateLimitExceeded) -> JSONResponse:
    """429 with Retry-After = window length of the exceeded limit (seconds)."""
    retry_after = exc.limit.limit.get_expiry()
    return JSONResponse(
        status_code=429,
        content={"detail": "rate_limited", "retry_after": retry_after},
        headers={"Retry-After": str(retry_after)},
    )


limiter = Limiter(key_func=client_ip)
