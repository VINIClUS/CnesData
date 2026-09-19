"""Testes de autenticação local via LocalAuthService e resolução de membership OIDC."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta

import pytest

from cnes_domain.control_plane.entities import Membership
from cnes_domain.profiles import ProfileSettings
from cnes_infra.auth.local_auth import (
    AuthenticatedPrincipal,
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
    generate_salt,
    hash_password,
    normalize_email,
)
from packages.cnes_infra.tests.contracts.clock import MutableClock

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


@pytest.fixture
def clock() -> MutableClock:
    return MutableClock(datetime(2026, 7, 15, 12, tzinfo=UTC))


@pytest.fixture
def credentials(tmp_path) -> LocalCredentialStore:
    store = LocalCredentialStore(tmp_path / "state" / "cnesdata.sqlite3")
    store.initialize()
    return store


@pytest.fixture
def control_plane() -> _FakeControlPlane:
    return _FakeControlPlane()


@pytest.fixture
def settings() -> ProfileSettings:
    return ProfileSettings(tenant_id=_TENANT)


@pytest.fixture
def hasher() -> _RecordingHasher:
    return _RecordingHasher()


@pytest.fixture
def deps(credentials, control_plane, settings, hasher) -> LocalAuthDependencies:
    return LocalAuthDependencies(
        credentials=credentials,
        control_plane=control_plane,
        settings=settings,
        hasher=hasher,
    )


@pytest.fixture
def service(deps, clock) -> LocalAuthService:
    return LocalAuthService(deps, clock.now)


def _seed_user(
    credentials: LocalCredentialStore, clock: MutableClock, email: str = "gestor@epitacio.sp.gov.br"
) -> str:
    user = build_user("user-1", email, _PASSWORD, clock.now())
    credentials.put_user(user)
    return user.user_id


# --- local_credentials.py: hashing e normalização ---


def test_dependencias_usam_hash_password_real_por_padrao(
    credentials, control_plane, settings, clock
) -> None:
    user_id = _seed_user(credentials, clock)
    control_plane.add_membership(settings.tenant_id, user_id)
    deps = LocalAuthDependencies(
        credentials=credentials, control_plane=control_plane, settings=settings
    )
    service_with_real_hasher = LocalAuthService(deps, clock.now)

    principal = service_with_real_hasher.authenticate("gestor@epitacio.sp.gov.br", _PASSWORD)

    assert principal.user_id == user_id


@pytest.mark.parametrize("password", ["curta12345", "x" * 129], ids=["muito_curta", "muito_longa"])
def test_autenticacao_rejeita_senha_fora_do_tamanho(service, hasher, password: str) -> None:
    with pytest.raises(AuthenticationRejected) as exc:
        service.authenticate("qualquer@x.com", password)

    assert exc.value.code == AuthRejectionCode.PASSWORD_LENGTH
    assert hasher.calls == []


@pytest.mark.parametrize("length", [12, 128], ids=["limite_minimo", "limite_maximo"])
def test_autenticacao_aceita_senha_nos_limites_inclusivos(
    service, credentials, control_plane, clock, settings, length: int
) -> None:
    password = "x" * length
    user = build_user("user-1", "gestor@epitacio.sp.gov.br", password, clock.now())
    credentials.put_user(user)
    control_plane.add_membership(settings.tenant_id, user.user_id)

    principal = service.authenticate("gestor@epitacio.sp.gov.br", password)

    assert principal.user_id == user.user_id


def test_autenticacao_rejeita_senha_incorreta(
    service, credentials, control_plane, clock, settings
) -> None:
    user_id = _seed_user(credentials, clock)
    control_plane.add_membership(settings.tenant_id, user_id)

    with pytest.raises(AuthenticationRejected) as exc:
        service.authenticate("gestor@epitacio.sp.gov.br", "senha-errada-1234")

    assert exc.value.code == AuthRejectionCode.INVALID_CREDENTIALS


def test_autenticacao_rejeita_usuario_desabilitado(
    service, credentials, control_plane, clock, settings, hasher
) -> None:
    user = build_user("user-1", "gestor@epitacio.sp.gov.br", _PASSWORD, clock.now())
    disabled = type(user)(
        user_id=user.user_id,
        email=user.email,
        password_hash=user.password_hash,
        salt=user.salt,
        created_at=user.created_at,
        disabled_at=clock.now(),
    )
    credentials.put_user(disabled)
    control_plane.add_membership(settings.tenant_id, user.user_id)

    with pytest.raises(AuthenticationRejected) as exc:
        service.authenticate("gestor@epitacio.sp.gov.br", _PASSWORD)

    assert exc.value.code == AuthRejectionCode.USER_DISABLED
    assert len(hasher.calls) == 1


def test_autenticacao_rejeita_usuario_sem_membership(service, credentials, clock) -> None:
    _seed_user(credentials, clock)

    with pytest.raises(AuthenticationRejected) as exc:
        service.authenticate("gestor@epitacio.sp.gov.br", _PASSWORD)

    assert exc.value.code == AuthRejectionCode.MEMBERSHIP_MISSING


def test_autenticacao_de_usuario_desconhecido_executa_hash_dummy(service, hasher) -> None:
    with pytest.raises(AuthenticationRejected) as exc:
        service.authenticate("fantasma@x.com", _PASSWORD)

    assert exc.value.code == AuthRejectionCode.INVALID_CREDENTIALS
    assert len(hasher.calls) == 1
    _, salt = hasher.calls[0]
    assert len(salt) == SALT_BYTES


def test_usuario_desconhecido_e_senha_incorreta_chamam_hasher_o_mesmo_numero_de_vezes(
    service,
    credentials,
    control_plane,
    clock,
    settings,
    hasher,
) -> None:
    user_id = _seed_user(credentials, clock)
    control_plane.add_membership(settings.tenant_id, user_id)

    with pytest.raises(AuthenticationRejected):
        service.authenticate("fantasma@x.com", _PASSWORD)
    unknown_calls = len(hasher.calls)

    hasher.calls.clear()
    with pytest.raises(AuthenticationRejected):
        service.authenticate("gestor@epitacio.sp.gov.br", "senha-errada-1234")
    wrong_password_calls = len(hasher.calls)

    assert unknown_calls == wrong_password_calls == 1


def test_autenticacao_nunca_registra_a_senha_no_erro(service) -> None:
    with pytest.raises(AuthenticationRejected) as exc:
        service.authenticate("fantasma@x.com", _PASSWORD)

    assert _PASSWORD not in str(exc.value)
    assert _PASSWORD not in repr(exc.value)


# --- sessões ---


def test_sessao_emitida_persiste_apenas_o_hash(
    service, credentials, control_plane, clock, settings, tmp_path
) -> None:
    import sqlite3

    user_id = _seed_user(credentials, clock)
    control_plane.add_membership(settings.tenant_id, user_id)
    principal = service.authenticate("gestor@epitacio.sp.gov.br", _PASSWORD)

    token = service.issue_session(principal)

    connection = sqlite3.connect(tmp_path / "state" / "cnesdata.sqlite3")
    rows = connection.execute(
        "SELECT session_hash, user_id, expires_at FROM local_sessions"
    ).fetchall()
    connection.close()
    assert len(rows) == 1
    session_hash, stored_user_id, _ = rows[0]
    assert stored_user_id == user_id
    assert session_hash != token
    assert token not in session_hash


def test_emissao_de_sessao_remove_sessoes_expiradas(
    service, credentials, control_plane, clock, settings
) -> None:
    user_id = _seed_user(credentials, clock)
    control_plane.add_membership(settings.tenant_id, user_id)
    principal = service.authenticate("gestor@epitacio.sp.gov.br", _PASSWORD)
    stale_token = service.issue_session(principal)

    clock.advance(timedelta(seconds=100_000))
    service.issue_session(principal)

    with pytest.raises(AuthenticationRejected) as exc:
        service.resolve_session(stale_token)
    assert exc.value.code == AuthRejectionCode.SESSION_INVALID


def test_sessao_valida_resolve_principal_com_membership(
    service, credentials, control_plane, clock, settings
) -> None:
    user_id = _seed_user(credentials, clock)
    control_plane.add_membership(settings.tenant_id, user_id, role="operador")
    principal = service.authenticate("gestor@epitacio.sp.gov.br", _PASSWORD)
    token = service.issue_session(principal)

    resolved = service.resolve_session(token)

    assert resolved == principal


def test_sessao_expirada_e_rejeitada(service, credentials, control_plane, clock, settings) -> None:
    from cnes_infra.auth.local_auth import SESSION_TTL_SECONDS

    user_id = _seed_user(credentials, clock)
    control_plane.add_membership(settings.tenant_id, user_id)
    principal = service.authenticate("gestor@epitacio.sp.gov.br", _PASSWORD)
    token = service.issue_session(principal)

    clock.advance(timedelta(seconds=SESSION_TTL_SECONDS + 1))

    with pytest.raises(AuthenticationRejected) as exc:
        service.resolve_session(token)
    assert exc.value.code == AuthRejectionCode.SESSION_EXPIRED


def test_sessao_desconhecida_e_rejeitada(service) -> None:
    with pytest.raises(AuthenticationRejected) as exc:
        service.resolve_session("token-nunca-emitido")
    assert exc.value.code == AuthRejectionCode.SESSION_INVALID


def test_sessao_de_usuario_desabilitado_e_rejeitada(
    service, credentials, control_plane, clock, settings
) -> None:
    user_id = _seed_user(credentials, clock)
    control_plane.add_membership(settings.tenant_id, user_id)
    principal = service.authenticate("gestor@epitacio.sp.gov.br", _PASSWORD)
    token = service.issue_session(principal)

    disabled = build_user("user-1", "gestor@epitacio.sp.gov.br", _PASSWORD, clock.now())
    disabled = type(disabled)(
        user_id=disabled.user_id,
        email=disabled.email,
        password_hash=disabled.password_hash,
        salt=disabled.salt,
        created_at=disabled.created_at,
        disabled_at=clock.now(),
    )
    credentials.put_user(disabled)

    with pytest.raises(AuthenticationRejected) as exc:
        service.resolve_session(token)
    assert exc.value.code == AuthRejectionCode.SESSION_INVALID


def test_sessao_de_usuario_removido_e_rejeitada(
    service, credentials, control_plane, clock, settings, tmp_path
) -> None:
    import sqlite3

    user_id = _seed_user(credentials, clock)
    control_plane.add_membership(settings.tenant_id, user_id)
    principal = service.authenticate("gestor@epitacio.sp.gov.br", _PASSWORD)
    token = service.issue_session(principal)

    connection = sqlite3.connect(tmp_path / "state" / "cnesdata.sqlite3")
    connection.execute("DELETE FROM local_users WHERE user_id = ?", (user_id,))
    connection.commit()
    connection.close()

    with pytest.raises(AuthenticationRejected) as exc:
        service.resolve_session(token)
    assert exc.value.code == AuthRejectionCode.SESSION_INVALID


def test_sessao_rejeitada_quando_membership_e_revogada(
    service, credentials, control_plane, clock, settings
) -> None:
    user_id = _seed_user(credentials, clock)
    control_plane.add_membership(settings.tenant_id, user_id)
    principal = service.authenticate("gestor@epitacio.sp.gov.br", _PASSWORD)
    token = service.issue_session(principal)

    control_plane._memberships.clear()

    with pytest.raises(AuthenticationRejected) as exc:
        service.resolve_session(token)
    assert exc.value.code == AuthRejectionCode.MEMBERSHIP_MISSING


def test_logout_revoga_a_sessao_de_forma_idempotente(
    service, credentials, control_plane, clock, settings
) -> None:
    user_id = _seed_user(credentials, clock)
    control_plane.add_membership(settings.tenant_id, user_id)
    principal = service.authenticate("gestor@epitacio.sp.gov.br", _PASSWORD)
    token = service.issue_session(principal)

    service.revoke_session(token)
    service.revoke_session(token)

    with pytest.raises(AuthenticationRejected) as exc:
        service.resolve_session(token)
    assert exc.value.code == AuthRejectionCode.SESSION_INVALID


# --- OidcMembershipResolver ---


def test_resolve_oidc_retorna_principal_com_tenant_do_profile(control_plane, settings) -> None:
    control_plane.add_membership(settings.tenant_id, "oidc-subject-1", role="gestor")
    resolver = OidcMembershipResolver(control_plane=control_plane, settings=settings)

    principal = resolver.resolve({"sub": "oidc-subject-1", "email": "a@b.com"})

    assert principal.tenant_id == settings.tenant_id
    assert principal.user_id == "oidc-subject-1"


def test_resolve_oidc_ignora_claim_de_tenant_coincidente(control_plane, settings) -> None:
    control_plane.add_membership(settings.tenant_id, "oidc-subject-1")
    resolver = OidcMembershipResolver(control_plane=control_plane, settings=settings)

    principal = resolver.resolve(
        {"sub": "oidc-subject-1", "email": "a@b.com", "tenant_id": settings.tenant_id}
    )

    assert principal.tenant_id == settings.tenant_id


def test_resolve_oidc_rejeita_tenant_divergente_nas_claims(control_plane, settings) -> None:
    resolver = OidcMembershipResolver(control_plane=control_plane, settings=settings)

    with pytest.raises(AuthenticationRejected) as exc:
        resolver.resolve({"sub": "oidc-subject-1", "email": "a@b.com", "tenant_id": "999999"})

    assert exc.value.code == AuthRejectionCode.TENANT_CLAIM_REJECTED


@pytest.mark.parametrize(
    "claims",
    [{"email": "a@b.com"}, {"sub": "oidc-subject-1"}, {"sub": "", "email": ""}],
    ids=["sem_sub", "sem_email", "vazios"],
)
def test_resolve_oidc_rejeita_claims_incompletas(control_plane, settings, claims) -> None:
    resolver = OidcMembershipResolver(control_plane=control_plane, settings=settings)

    with pytest.raises(AuthenticationRejected) as exc:
        resolver.resolve(claims)

    assert exc.value.code == AuthRejectionCode.INVALID_CLAIMS


def test_resolve_oidc_rejeita_sem_membership(control_plane, settings) -> None:
    resolver = OidcMembershipResolver(control_plane=control_plane, settings=settings)

    with pytest.raises(AuthenticationRejected) as exc:
        resolver.resolve({"sub": "oidc-subject-1", "email": "a@b.com"})

    assert exc.value.code == AuthRejectionCode.MEMBERSHIP_MISSING


# --- LocalCredentialStore: schema compartilhado com o control plane ---


