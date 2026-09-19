"""Autenticação local por senha e resolução de membership OIDC, tenant fixo pelo profile."""

from __future__ import annotations

import hmac
import logging
from dataclasses import dataclass, field
from datetime import timedelta
from enum import StrEnum
from typing import TYPE_CHECKING

from cnes_infra.auth.local_credentials import (
    MAX_PASSWORD_LENGTH,
    MIN_PASSWORD_LENGTH,
    SALT_BYTES,
    SCRYPT_DKLEN,
    LocalCredentialStore,
    SessionRecord,
    generate_session_token,
    hash_password,
    hash_session_token,
    normalize_email,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from datetime import datetime
    from typing import Any

    from cnes_domain.control_plane.entities import Membership
    from cnes_domain.ports.control_plane import ControlPlanePort
    from cnes_domain.profiles import ProfileSettings

logger = logging.getLogger(__name__)

SESSION_TTL_SECONDS = 43_200
_DUMMY_SALT = bytes(SALT_BYTES)
_DUMMY_HASH = bytes(SCRYPT_DKLEN)


class AuthRejectionCode(StrEnum):
    INVALID_CREDENTIALS = "invalid_credentials"
    PASSWORD_LENGTH = "password_length"  # noqa: S105
    USER_DISABLED = "user_disabled"
    MEMBERSHIP_MISSING = "membership_missing"
    SESSION_INVALID = "session_invalid"
    SESSION_EXPIRED = "session_expired"
    TENANT_CLAIM_REJECTED = "tenant_claim_rejected"
    INVALID_CLAIMS = "invalid_claims"


class AuthenticationRejected(Exception):
    def __init__(self, code: AuthRejectionCode) -> None:
        super().__init__(code)
        self.code = code


@dataclass(frozen=True, slots=True)
class AuthenticatedPrincipal:
    user_id: str
    email: str
    tenant_id: str
    role: str


@dataclass(frozen=True, slots=True)
class LocalAuthDependencies:
    credentials: LocalCredentialStore
    control_plane: ControlPlanePort
    settings: ProfileSettings
    hasher: Callable[[str, bytes], bytes] = field(default=hash_password)


def _principal_for(
    user_id: str, email: str, settings: ProfileSettings, membership: Membership
) -> AuthenticatedPrincipal:
    return AuthenticatedPrincipal(
        user_id=user_id,
        email=email,
        tenant_id=settings.tenant_id,
        role=membership.role,
    )


def _require_membership(
    control_plane: ControlPlanePort, settings: ProfileSettings, user_id: str
) -> Membership:
    membership = control_plane.get_membership(settings.tenant_id, user_id)
    if membership is None:
        raise AuthenticationRejected(AuthRejectionCode.MEMBERSHIP_MISSING)
    return membership


class LocalAuthService:
    """Autentica por senha e emite/resolve sessões opacas com tenant fixo pelo profile."""

    def __init__(self, dependencies: LocalAuthDependencies, clock: Callable[[], datetime]) -> None:
        self._deps = dependencies
        self._clock = clock

    def authenticate(self, email: str, password: str) -> AuthenticatedPrincipal:
        if not (MIN_PASSWORD_LENGTH <= len(password) <= MAX_PASSWORD_LENGTH):
            raise AuthenticationRejected(AuthRejectionCode.PASSWORD_LENGTH)
        record = self._deps.credentials.find_user_by_email(normalize_email(email))
        if record is None:
            hmac.compare_digest(self._deps.hasher(password, _DUMMY_SALT), _DUMMY_HASH)
            raise AuthenticationRejected(AuthRejectionCode.INVALID_CREDENTIALS)
        candidate = self._deps.hasher(password, record.salt)
        if not hmac.compare_digest(candidate, record.password_hash):
            raise AuthenticationRejected(AuthRejectionCode.INVALID_CREDENTIALS)
        if record.disabled_at is not None:
            raise AuthenticationRejected(AuthRejectionCode.USER_DISABLED)
        membership = _require_membership(
            self._deps.control_plane, self._deps.settings, record.user_id
        )
        logger.info(
            "local_login_succeeded user_id=%s tenant_id=%s",
            record.user_id,
            self._deps.settings.tenant_id,
        )
        return _principal_for(record.user_id, record.email, self._deps.settings, membership)

    def issue_session(self, principal: AuthenticatedPrincipal) -> str:
        token = generate_session_token()
        now = self._clock()
        expires_at = now + timedelta(seconds=SESSION_TTL_SECONDS)
        session = SessionRecord(
            session_hash=hash_session_token(token),
            user_id=principal.user_id,
            tenant_id=self._deps.settings.tenant_id,
            expires_at=expires_at,
        )
        self._deps.credentials.put_session(session, now)
        return token

    def resolve_session(self, token: str) -> AuthenticatedPrincipal:
        session = self._deps.credentials.find_session(hash_session_token(token))
        if session is None:
            raise AuthenticationRejected(AuthRejectionCode.SESSION_INVALID)
        if session.tenant_id != self._deps.settings.tenant_id:
            self._deps.credentials.delete_session(session.session_hash)
            raise AuthenticationRejected(AuthRejectionCode.SESSION_INVALID)
        if session.expires_at <= self._clock():
            self._deps.credentials.delete_session(session.session_hash)
            raise AuthenticationRejected(AuthRejectionCode.SESSION_EXPIRED)
        record = self._deps.credentials.find_user_by_id(session.user_id)
        if record is None or record.disabled_at is not None:
            raise AuthenticationRejected(AuthRejectionCode.SESSION_INVALID)
        membership = _require_membership(
            self._deps.control_plane, self._deps.settings, record.user_id
        )
        return _principal_for(record.user_id, record.email, self._deps.settings, membership)

    def revoke_session(self, token: str) -> None:
        self._deps.credentials.delete_session(hash_session_token(token))


@dataclass(frozen=True, slots=True)
class OidcMembershipResolver:
    control_plane: ControlPlanePort
    settings: ProfileSettings

    def resolve(self, claims: Mapping[str, Any]) -> AuthenticatedPrincipal:
        subject = str(claims.get("sub") or "").strip()
        email = normalize_email(str(claims.get("email") or ""))
        if not subject or not email:
            raise AuthenticationRejected(AuthRejectionCode.INVALID_CLAIMS)
        claimed_tenant = claims.get("tenant_id") or claims.get("tid")
        if claimed_tenant is not None and str(claimed_tenant) != self.settings.tenant_id:
            raise AuthenticationRejected(AuthRejectionCode.TENANT_CLAIM_REJECTED)
        membership = _require_membership(self.control_plane, self.settings, subject)
        return _principal_for(subject, email, self.settings, membership)


__all__ = [
    "MAX_PASSWORD_LENGTH",
    "MIN_PASSWORD_LENGTH",
    "SESSION_TTL_SECONDS",
    "AuthRejectionCode",
    "AuthenticatedPrincipal",
    "AuthenticationRejected",
    "LocalAuthDependencies",
    "LocalAuthService",
    "OidcMembershipResolver",
]
