"""Autorização de tenant no perfil AWS pela membership canônica."""
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from cnes_domain.control_plane.entities import Membership
    from cnes_domain.ports.control_plane import ControlPlanePort
    from cnes_infra.auth.oidc import OidcPrincipal


class MembershipCandidateSource(Protocol):
    def list_candidates(self, user_id: str) -> tuple[str, ...]: ...


class TenantAccessDenied(Exception):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


@dataclass(frozen=True, slots=True)
class AuthorizedTenant:
    tenant_id: str
    user_id: str
    role: str


class MembershipAuthorizer:
    def __init__(
        self, control_plane: "ControlPlanePort", candidates: MembershipCandidateSource,
    ) -> None:
        self._control_plane = control_plane
        self._candidates = candidates

    def authorize(self, principal: "OidcPrincipal", requested_tenant_id: str) -> AuthorizedTenant:
        """Returns: tenant autorizado pela membership relida na chave base.

        Raises:
            TenantAccessDenied: tenant em branco ou membership ausente/divergente.
        """
        tenant_id = requested_tenant_id.strip()
        if not tenant_id:
            raise TenantAccessDenied("tenant_required")
        membership = self._control_plane.get_membership(tenant_id, principal.subject)
        if membership is None or not _matches(membership, tenant_id, principal):
            raise TenantAccessDenied("membership_not_active")
        return AuthorizedTenant(
            tenant_id=membership.tenant_id, user_id=membership.user_id, role=membership.role,
        )

    def list_authorized(self, principal: "OidcPrincipal") -> tuple[AuthorizedTenant, ...]:
        """Returns: tenants cujos candidatos do índice sobrevivem à revalidação na base."""
        tenant_ids = dict.fromkeys(self._candidates.list_candidates(principal.subject))
        grants = (self._authorized_or_none(principal, tenant_id) for tenant_id in tenant_ids)
        return tuple(grant for grant in grants if grant is not None)

    def _authorized_or_none(
        self, principal: "OidcPrincipal", tenant_id: str,
    ) -> AuthorizedTenant | None:
        try:
            return self.authorize(principal, tenant_id)
        except TenantAccessDenied:
            return None


def _matches(membership: "Membership", tenant_id: str, principal: "OidcPrincipal") -> bool:
    return (
        membership.tenant_id == tenant_id
        and membership.user_id == principal.subject
        and membership.oidc_issuer in (None, principal.issuer)
    )
