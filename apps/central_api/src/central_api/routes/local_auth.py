"""Rotas de login local, logout e identidade com sessão por cookie HttpOnly."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import BaseModel, ConfigDict, Field

from central_api.ratelimit import limiter

from cnes_infra.auth.local_auth import (
    SESSION_TTL_SECONDS,
    AuthenticatedPrincipal,
    AuthenticationRejected,
    LocalAuthService,
)
from cnes_infra.auth.local_credentials import MAX_PASSWORD_LENGTH, MIN_PASSWORD_LENGTH


router = APIRouter(prefix="/api/v1/auth", tags=["auth-local"])
SESSION_COOKIE_NAME = "cnesdata_session"
_COOKIE_PATH = "/"
_COOKIE_SAMESITE = "lax"


class LoginRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    email: str = Field(min_length=3, max_length=320)
    password: str = Field(min_length=MIN_PASSWORD_LENGTH, max_length=MAX_PASSWORD_LENGTH)


class PrincipalResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    user_id: str
    email: str
    tenant_id: str
    role: str


def get_local_auth_service() -> LocalAuthService:
    """Falha fechado até a composição fornecer a autenticação local."""

    raise HTTPException(status_code=503, detail="local_auth_not_configured")


def _to_response(principal: AuthenticatedPrincipal) -> PrincipalResponse:
    return PrincipalResponse(
        user_id=principal.user_id,
        email=principal.email,
        tenant_id=principal.tenant_id,
        role=principal.role,
    )


def _is_secure_transport(request: Request) -> bool:
    """Detect HTTPS from direct TLS or X-Forwarded-Proto behind a trusted proxy."""
    import os
    trust_proxy = os.getenv("TRUST_X_FORWARDED_PROTO", "false").lower() == "true"
    if trust_proxy:
        forwarded_proto = request.headers.get("x-forwarded-proto", "")
        if forwarded_proto:
            return forwarded_proto == "https"
    return request.url.scheme == "https"


def _set_session_cookie(response: Response, request: Request, token: str) -> None:
    response.set_cookie(
        key=SESSION_COOKIE_NAME,
        value=token,
        httponly=True,
        samesite=_COOKIE_SAMESITE,
        secure=_is_secure_transport(request),
        path=_COOKIE_PATH,
        max_age=SESSION_TTL_SECONDS,
    )


def _require_session_token(request: Request) -> str:
    token = request.cookies.get(SESSION_COOKIE_NAME)
    if token is None:
        raise HTTPException(status_code=401, detail="session_required")
    return token


@router.post("/local/login", response_model=PrincipalResponse)
@limiter.limit("5/minute")
def login(
    body: LoginRequest,
    request: Request,
    response: Response,
    service: LocalAuthService = Depends(get_local_auth_service),
) -> PrincipalResponse:
    try:
        principal = service.authenticate(body.email, body.password)
    except AuthenticationRejected as error:
        raise HTTPException(status_code=401, detail="invalid_credentials") from error
    token = service.issue_session(principal)
    _set_session_cookie(response, request, token)
    return _to_response(principal)


@router.post("/logout", status_code=204)
def logout(
    request: Request,
    response: Response,
    service: LocalAuthService = Depends(get_local_auth_service),
) -> Response:
    token = request.cookies.get(SESSION_COOKIE_NAME)
    if token is not None:
        service.revoke_session(token)
    response.delete_cookie(
        key=SESSION_COOKIE_NAME,
        path=_COOKIE_PATH,
        httponly=True,
        samesite=_COOKIE_SAMESITE,
    )
    response.status_code = 204
    return response


@router.get("/me", response_model=PrincipalResponse)
def me(
    token: str = Depends(_require_session_token),
    service: LocalAuthService = Depends(get_local_auth_service),
) -> PrincipalResponse:
    try:
        principal = service.resolve_session(token)
    except AuthenticationRejected as error:
        raise HTTPException(status_code=401, detail="session_invalid") from error
    return _to_response(principal)


__all__ = ["SESSION_COOKIE_NAME", "LoginRequest", "PrincipalResponse", "router"]
