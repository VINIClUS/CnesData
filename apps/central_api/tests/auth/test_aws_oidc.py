"""Testes do MembershipAuthorizer — autorização só pela membership na chave base."""
from datetime import UTC, datetime
from unittest.mock import Mock

import pytest
from botocore.exceptions import ClientError

from central_api.auth.aws_oidc import AuthorizedTenant, MembershipAuthorizer, TenantAccessDenied
from cnes_domain.control_plane.entities import Membership
from cnes_infra.auth.oidc import OidcPrincipal

_ISSUER = "https://idp.example.test"
_NOW = datetime(2026, 9, 27, 12, tzinfo=UTC)


def _principal(subject: str = "user-1", issuer: str = _ISSUER) -> OidcPrincipal:
    return OidcPrincipal(issuer=issuer, subject=subject, email=None, display_name=None)


def _membership(
    tenant_id: str, user_id: str, oidc_issuer: str | None = None,
) -> Membership:
    return Membership(
        tenant_id=tenant_id, user_id=user_id, role="gestor", created_at=_NOW,
        oidc_issuer=oidc_issuer,
    )


def _denied_code(authorizer: MembershipAuthorizer, tenant_id: str) -> str:
    with pytest.raises(TenantAccessDenied) as exc:
        authorizer.authorize(_principal(), tenant_id)
    return exc.value.code


def test_autoriza_por_membership_na_chave_canonica() -> None:
    control_plane = Mock()
    control_plane.get_membership.return_value = _membership("tenant-a", "user-1")
    result = MembershipAuthorizer(control_plane, Mock()).authorize(_principal(), "tenant-a")
    assert result == AuthorizedTenant(tenant_id="tenant-a", user_id="user-1", role="gestor")
    control_plane.get_membership.assert_called_once_with("tenant-a", "user-1")


def test_rejeita_tenant_de_claim_e_request_sem_membership() -> None:
    control_plane = Mock()
    control_plane.get_membership.return_value = None
    authorizer = MembershipAuthorizer(control_plane, Mock())
    assert _denied_code(authorizer, "tenant-b") == "membership_not_active"


@pytest.mark.parametrize(
    "membership",
    [_membership("tenant-x", "user-1"), _membership("tenant-a", "user-2")],
    ids=["tenant_divergente", "usuario_divergente"],
)
def test_rejeita_membership_divergente_da_chave(membership: Membership) -> None:
    control_plane = Mock()
    control_plane.get_membership.return_value = membership
    authorizer = MembershipAuthorizer(control_plane, Mock())
    assert _denied_code(authorizer, "tenant-a") == "membership_not_active"


def test_rejeita_membership_de_outro_issuer() -> None:
    control_plane = Mock()
    control_plane.get_membership.return_value = _membership(
        "tenant-a", "user-1", oidc_issuer="https://outro-idp.example.test",
    )
    authorizer = MembershipAuthorizer(control_plane, Mock())
    assert _denied_code(authorizer, "tenant-a") == "membership_not_active"


def test_autoriza_membership_vinculada_ao_mesmo_issuer() -> None:
    control_plane = Mock()
    control_plane.get_membership.return_value = _membership(
        "tenant-a", "user-1", oidc_issuer=_ISSUER,
    )
    result = MembershipAuthorizer(control_plane, Mock()).authorize(_principal(), "tenant-a")
    assert result.tenant_id == "tenant-a"


@pytest.mark.parametrize("tenant_id", ["", "   "])
def test_rejeita_tenant_em_branco_sem_acessar_storage(tenant_id: str) -> None:
    control_plane = Mock()
    authorizer = MembershipAuthorizer(control_plane, Mock())
    assert _denied_code(authorizer, tenant_id) == "tenant_required"
    control_plane.get_membership.assert_not_called()


def test_remove_candidato_stale_do_gsi_apos_revalidar_base() -> None:
    control_plane = Mock()
    candidates = Mock()
    candidates.list_candidates.return_value = ("tenant-a", "tenant-b")
    control_plane.get_membership.side_effect = [_membership("tenant-a", "user-1"), None]
    result = MembershipAuthorizer(control_plane, candidates).list_authorized(_principal())
    assert tuple(item.tenant_id for item in result) == ("tenant-a",)
    candidates.list_candidates.assert_called_once_with("user-1")


def test_revalida_duplicata_do_gsi_uma_unica_vez() -> None:
    control_plane = Mock()
    candidates = Mock()
    candidates.list_candidates.return_value = ("tenant-a", "tenant-a")
    control_plane.get_membership.return_value = _membership("tenant-a", "user-1")
    result = MembershipAuthorizer(control_plane, candidates).list_authorized(_principal())
    assert len(result) == 1
    control_plane.get_membership.assert_called_once_with("tenant-a", "user-1")


def test_propaga_erro_do_storage_ao_listar() -> None:
    control_plane = Mock()
    candidates = Mock()
    candidates.list_candidates.return_value = ("tenant-a",)
    control_plane.get_membership.side_effect = ClientError(
        {"Error": {"Code": "InternalServerError"}}, "GetItem",
    )
    with pytest.raises(ClientError):
        MembershipAuthorizer(control_plane, candidates).list_authorized(_principal())
