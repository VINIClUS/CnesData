"""Testes de autenticação local via LocalAuthService e resolução de membership OIDC."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pytest

from cnes_domain.control_plane.entities import Membership
from cnes_domain.profiles import ProfileSettings
from cnes_infra.auth.local_auth import (
    AuthenticationRejected,
    AuthRejectionCode,
    LocalAuthDependencies,
    LocalAuthService,
    OidcMembershipResolver,
)
from cnes_infra.auth.local_credentials import (
    SALT_BYTES,
    LocalCredentialStore,
    build_user,
    hash_password,
)
from packages.cnes_infra.tests.contracts.clock import MutableClock

if TYPE_CHECKING:
    from pathlib import Path

_TENANT = "354130"
_PASSWORD = "correct-horse-battery"  # noqa: S105


class _FakeControlPlane:
    def __init__(self) -> None:
        self._memberships: dict[tuple[str, str], Membership] = {}

    def add_membership(self, tenant_id: str, user_id: str, role: str = "gestor") -> None:
        self._memberships[(tenant_id, user_id)] = Membership(
            tenant_id=tenant_id,
            user_id=user_id,
            role=role,
            created_at=datetime(2026, 7, 1, tzinfo=UTC),
        )

    def get_membership(self, tenant_id: str, user_id: str) -> Membership | None:
        return self._memberships.get((tenant_id, user_id))


class _RecordingHasher:
    def __init__(self) -> None:
        self.calls: list[tuple[str, bytes]] = []

    def __call__(self, password: str, salt: bytes) -> bytes:
        self.calls.append((password, salt))
        return hash_password(password, salt)


@dataclass(slots=True)
class _AuthContext:
    credentials: LocalCredentialStore
    control_plane: _FakeControlPlane
    settings: ProfileSettings
    clock: MutableClock
    hasher: _RecordingHasher
    database_path: Path

    @property
    def service(self) -> LocalAuthService:
        dependencies = LocalAuthDependencies(
            credentials=self.credentials,
            control_plane=self.control_plane,
            settings=self.settings,
            hasher=self.hasher,
        )
        return LocalAuthService(dependencies, self.clock.now)


@pytest.fixture
def auth_context(tmp_path: Path) -> _AuthContext:
    database_path = tmp_path / "state" / "cnesdata.sqlite3"
    credentials = LocalCredentialStore(database_path)
    credentials.initialize()
    return _AuthContext(
        credentials=credentials,
        control_plane=_FakeControlPlane(),
        settings=ProfileSettings(tenant_id=_TENANT),
        clock=MutableClock(datetime(2026, 7, 15, 12, tzinfo=UTC)),
        hasher=_RecordingHasher(),
        database_path=database_path,
    )


def _seed_user(
    context: _AuthContext, email: str = "gestor@epitacio.sp.gov.br"
) -> str:
    user = build_user("user-1", email, _PASSWORD, context.clock.now())
    context.credentials.put_user(user)
    return user.user_id


def test_dependencias_usam_hash_password_real_por_padrao(auth_context: _AuthContext) -> None:
    user_id = _seed_user(auth_context)
    auth_context.control_plane.add_membership(auth_context.settings.tenant_id, user_id)
    deps = LocalAuthDependencies(
        credentials=auth_context.credentials,
        control_plane=auth_context.control_plane,
        settings=auth_context.settings,
    )
    service_with_real_hasher = LocalAuthService(deps, auth_context.clock.now)

    principal = service_with_real_hasher.authenticate("gestor@epitacio.sp.gov.br", _PASSWORD)

    assert principal.user_id == user_id


@pytest.mark.parametrize("password", ["curta12345", "x" * 129], ids=["muito_curta", "muito_longa"])
def test_autenticacao_rejeita_senha_fora_do_tamanho(
    auth_context: _AuthContext, password: str
) -> None:
    with pytest.raises(AuthenticationRejected) as exc:
        auth_context.service.authenticate("qualquer@x.com", password)

    assert exc.value.code == AuthRejectionCode.PASSWORD_LENGTH
    assert auth_context.hasher.calls == []


@pytest.mark.parametrize("length", [12, 128], ids=["limite_minimo", "limite_maximo"])
def test_autenticacao_aceita_senha_nos_limites_inclusivos(
    auth_context: _AuthContext, length: int
) -> None:
    password = "x" * length
    user = build_user(
        "user-1", "gestor@epitacio.sp.gov.br", password, auth_context.clock.now()
    )
    auth_context.credentials.put_user(user)
    auth_context.control_plane.add_membership(auth_context.settings.tenant_id, user.user_id)

    principal = auth_context.service.authenticate("gestor@epitacio.sp.gov.br", password)

    assert principal.user_id == user.user_id


def test_autenticacao_rejeita_senha_incorreta(auth_context: _AuthContext) -> None:
    user_id = _seed_user(auth_context)
    auth_context.control_plane.add_membership(auth_context.settings.tenant_id, user_id)

    with pytest.raises(AuthenticationRejected) as exc:
        auth_context.service.authenticate("gestor@epitacio.sp.gov.br", "senha-errada-1234")

    assert exc.value.code == AuthRejectionCode.INVALID_CREDENTIALS


def test_autenticacao_rejeita_usuario_desabilitado(auth_context: _AuthContext) -> None:
    user = build_user(
        "user-1", "gestor@epitacio.sp.gov.br", _PASSWORD, auth_context.clock.now()
    )
    disabled = type(user)(
        user_id=user.user_id,
        email=user.email,
        password_hash=user.password_hash,
        salt=user.salt,
        created_at=user.created_at,
        disabled_at=auth_context.clock.now(),
    )
    auth_context.credentials.put_user(disabled)
    auth_context.control_plane.add_membership(auth_context.settings.tenant_id, user.user_id)

    with pytest.raises(AuthenticationRejected) as exc:
        auth_context.service.authenticate("gestor@epitacio.sp.gov.br", _PASSWORD)

    assert exc.value.code == AuthRejectionCode.USER_DISABLED
    assert len(auth_context.hasher.calls) == 1


def test_autenticacao_rejeita_usuario_sem_membership(auth_context: _AuthContext) -> None:
    _seed_user(auth_context)

    with pytest.raises(AuthenticationRejected) as exc:
        auth_context.service.authenticate("gestor@epitacio.sp.gov.br", _PASSWORD)

    assert exc.value.code == AuthRejectionCode.MEMBERSHIP_MISSING


def test_autenticacao_de_usuario_desconhecido_executa_hash_dummy(
    auth_context: _AuthContext,
) -> None:
    with pytest.raises(AuthenticationRejected) as exc:
        auth_context.service.authenticate("fantasma@x.com", _PASSWORD)

    assert exc.value.code == AuthRejectionCode.INVALID_CREDENTIALS
    assert len(auth_context.hasher.calls) == 1
    _, salt = auth_context.hasher.calls[0]
    assert len(salt) == SALT_BYTES


def test_usuario_desconhecido_e_senha_incorreta_chamam_hasher_o_mesmo_numero_de_vezes(
    auth_context: _AuthContext,
) -> None:
    user_id = _seed_user(auth_context)
    auth_context.control_plane.add_membership(auth_context.settings.tenant_id, user_id)

    with pytest.raises(AuthenticationRejected):
        auth_context.service.authenticate("fantasma@x.com", _PASSWORD)
    unknown_calls = len(auth_context.hasher.calls)

    auth_context.hasher.calls.clear()
    with pytest.raises(AuthenticationRejected):
        auth_context.service.authenticate("gestor@epitacio.sp.gov.br", "senha-errada-1234")
    wrong_password_calls = len(auth_context.hasher.calls)

    assert unknown_calls == wrong_password_calls == 1


def test_autenticacao_nunca_registra_a_senha_no_erro(auth_context: _AuthContext) -> None:
    with pytest.raises(AuthenticationRejected) as exc:
        auth_context.service.authenticate("fantasma@x.com", _PASSWORD)

    assert _PASSWORD not in str(exc.value)
    assert _PASSWORD not in repr(exc.value)


def test_sessao_emitida_persiste_apenas_o_hash(auth_context: _AuthContext) -> None:
    import sqlite3

    user_id = _seed_user(auth_context)
    auth_context.control_plane.add_membership(auth_context.settings.tenant_id, user_id)
    principal = auth_context.service.authenticate("gestor@epitacio.sp.gov.br", _PASSWORD)

    token = auth_context.service.issue_session(principal)

    connection = sqlite3.connect(auth_context.database_path)
    rows = connection.execute(
        "SELECT session_hash, user_id, expires_at FROM local_sessions"
    ).fetchall()
    connection.close()
    assert len(rows) == 1
    session_hash, stored_user_id, _ = rows[0]
    assert stored_user_id == user_id
    assert session_hash != token
    assert token not in session_hash


def test_emissao_de_sessao_remove_sessoes_expiradas(auth_context: _AuthContext) -> None:
    user_id = _seed_user(auth_context)
    auth_context.control_plane.add_membership(auth_context.settings.tenant_id, user_id)
    principal = auth_context.service.authenticate("gestor@epitacio.sp.gov.br", _PASSWORD)
    stale_token = auth_context.service.issue_session(principal)

    auth_context.clock.advance(timedelta(seconds=100_000))
    auth_context.service.issue_session(principal)

    with pytest.raises(AuthenticationRejected) as exc:
        auth_context.service.resolve_session(stale_token)
    assert exc.value.code == AuthRejectionCode.SESSION_INVALID


def test_sessao_valida_resolve_principal_com_membership(auth_context: _AuthContext) -> None:
    user_id = _seed_user(auth_context)
    auth_context.control_plane.add_membership(
        auth_context.settings.tenant_id, user_id, role="operador"
    )
    principal = auth_context.service.authenticate("gestor@epitacio.sp.gov.br", _PASSWORD)
    token = auth_context.service.issue_session(principal)

    resolved = auth_context.service.resolve_session(token)

    assert resolved == principal


def test_sessao_expirada_e_rejeitada(auth_context: _AuthContext) -> None:
    from cnes_infra.auth.local_auth import SESSION_TTL_SECONDS

    user_id = _seed_user(auth_context)
    auth_context.control_plane.add_membership(auth_context.settings.tenant_id, user_id)
    principal = auth_context.service.authenticate("gestor@epitacio.sp.gov.br", _PASSWORD)
    token = auth_context.service.issue_session(principal)

    auth_context.clock.advance(timedelta(seconds=SESSION_TTL_SECONDS + 1))

    with pytest.raises(AuthenticationRejected) as exc:
        auth_context.service.resolve_session(token)
    assert exc.value.code == AuthRejectionCode.SESSION_EXPIRED


def test_sessao_desconhecida_e_rejeitada(auth_context: _AuthContext) -> None:
    with pytest.raises(AuthenticationRejected) as exc:
        auth_context.service.resolve_session("token-nunca-emitido")
    assert exc.value.code == AuthRejectionCode.SESSION_INVALID


def test_sessao_de_usuario_desabilitado_e_rejeitada(auth_context: _AuthContext) -> None:
    user_id = _seed_user(auth_context)
    auth_context.control_plane.add_membership(auth_context.settings.tenant_id, user_id)
    principal = auth_context.service.authenticate("gestor@epitacio.sp.gov.br", _PASSWORD)
    token = auth_context.service.issue_session(principal)

    disabled = build_user(
        "user-1", "gestor@epitacio.sp.gov.br", _PASSWORD, auth_context.clock.now()
    )
    disabled = type(disabled)(
        user_id=disabled.user_id,
        email=disabled.email,
        password_hash=disabled.password_hash,
        salt=disabled.salt,
        created_at=disabled.created_at,
        disabled_at=auth_context.clock.now(),
    )
    auth_context.credentials.put_user(disabled)

    with pytest.raises(AuthenticationRejected) as exc:
        auth_context.service.resolve_session(token)
    assert exc.value.code == AuthRejectionCode.SESSION_INVALID


def test_sessao_de_usuario_removido_e_rejeitada(auth_context: _AuthContext) -> None:
    import sqlite3

    user_id = _seed_user(auth_context)
    auth_context.control_plane.add_membership(auth_context.settings.tenant_id, user_id)
    principal = auth_context.service.authenticate("gestor@epitacio.sp.gov.br", _PASSWORD)
    token = auth_context.service.issue_session(principal)

    connection = sqlite3.connect(auth_context.database_path)
    connection.execute("DELETE FROM local_users WHERE user_id = ?", (user_id,))
    connection.commit()
    connection.close()

    with pytest.raises(AuthenticationRejected) as exc:
        auth_context.service.resolve_session(token)
    assert exc.value.code == AuthRejectionCode.SESSION_INVALID


def test_sessao_rejeitada_quando_membership_e_revogada(auth_context: _AuthContext) -> None:
    user_id = _seed_user(auth_context)
    auth_context.control_plane.add_membership(auth_context.settings.tenant_id, user_id)
    principal = auth_context.service.authenticate("gestor@epitacio.sp.gov.br", _PASSWORD)
    token = auth_context.service.issue_session(principal)

    auth_context.control_plane._memberships.clear()

    with pytest.raises(AuthenticationRejected) as exc:
        auth_context.service.resolve_session(token)
    assert exc.value.code == AuthRejectionCode.MEMBERSHIP_MISSING


def test_logout_revoga_a_sessao_de_forma_idempotente(auth_context: _AuthContext) -> None:
    user_id = _seed_user(auth_context)
    auth_context.control_plane.add_membership(auth_context.settings.tenant_id, user_id)
    principal = auth_context.service.authenticate("gestor@epitacio.sp.gov.br", _PASSWORD)
    token = auth_context.service.issue_session(principal)

    auth_context.service.revoke_session(token)
    auth_context.service.revoke_session(token)

    with pytest.raises(AuthenticationRejected) as exc:
        auth_context.service.resolve_session(token)
    assert exc.value.code == AuthRejectionCode.SESSION_INVALID


def test_resolve_oidc_retorna_principal_com_tenant_do_profile(
    auth_context: _AuthContext,
) -> None:
    auth_context.control_plane.add_membership(
        auth_context.settings.tenant_id, "oidc-subject-1", role="gestor"
    )
    resolver = OidcMembershipResolver(
        control_plane=auth_context.control_plane, settings=auth_context.settings
    )

    principal = resolver.resolve({"sub": "oidc-subject-1", "email": "  A@B.COM  "})

    assert principal.tenant_id == auth_context.settings.tenant_id
    assert principal.user_id == "oidc-subject-1"
    assert principal.email == "a@b.com"


def test_resolve_oidc_ignora_claim_de_tenant_coincidente(auth_context: _AuthContext) -> None:
    auth_context.control_plane.add_membership(auth_context.settings.tenant_id, "oidc-subject-1")
    resolver = OidcMembershipResolver(
        control_plane=auth_context.control_plane, settings=auth_context.settings
    )

    principal = resolver.resolve(
        {
            "sub": "oidc-subject-1",
            "email": "a@b.com",
            "tenant_id": auth_context.settings.tenant_id,
        }
    )

    assert principal.tenant_id == auth_context.settings.tenant_id


def test_resolve_oidc_rejeita_tenant_divergente_nas_claims(
    auth_context: _AuthContext,
) -> None:
    resolver = OidcMembershipResolver(
        control_plane=auth_context.control_plane, settings=auth_context.settings
    )

    with pytest.raises(AuthenticationRejected) as exc:
        resolver.resolve({"sub": "oidc-subject-1", "email": "a@b.com", "tenant_id": "999999"})

    assert exc.value.code == AuthRejectionCode.TENANT_CLAIM_REJECTED


@pytest.mark.parametrize(
    "claims",
    [{"email": "a@b.com"}, {"sub": "oidc-subject-1"}, {"sub": "", "email": ""}],
    ids=["sem_sub", "sem_email", "vazios"],
)
def test_resolve_oidc_rejeita_claims_incompletas(
    auth_context: _AuthContext, claims: dict[str, str]
) -> None:
    resolver = OidcMembershipResolver(
        control_plane=auth_context.control_plane, settings=auth_context.settings
    )

    with pytest.raises(AuthenticationRejected) as exc:
        resolver.resolve(claims)

    assert exc.value.code == AuthRejectionCode.INVALID_CLAIMS


def test_resolve_oidc_rejeita_sem_membership(auth_context: _AuthContext) -> None:
    resolver = OidcMembershipResolver(
        control_plane=auth_context.control_plane, settings=auth_context.settings
    )

    with pytest.raises(AuthenticationRejected) as exc:
        resolver.resolve({"sub": "oidc-subject-1", "email": "a@b.com"})

    assert exc.value.code == AuthRejectionCode.MEMBERSHIP_MISSING
