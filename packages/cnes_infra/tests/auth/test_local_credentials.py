"""Testes de hashing de senha, criação de usuários e inicialização do credential store."""

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


def test_hash_password_usa_scrypt_com_parametros_fixos() -> None:
    salt = generate_salt()
    expected = hashlib.scrypt(_PASSWORD.encode(), salt=salt, n=2**14, r=8, p=1, dklen=32)
    assert hash_password(_PASSWORD, salt) == expected


def test_normaliza_email_com_espacos_e_maiusculas() -> None:
    assert normalize_email("  Gestor@Epitacio.SP.GOV.BR  ") == "gestor@epitacio.sp.gov.br"


@pytest.mark.parametrize("password", ["curta12345", "x" * 129], ids=["muito_curta", "muito_longa"])
def test_build_user_rejeita_senha_fora_do_tamanho(password: str) -> None:
    from cnes_infra.auth.local_credentials import CredentialRejected

    with pytest.raises(CredentialRejected):
        build_user("user-1", "a@b.com", password, datetime(2026, 7, 1, tzinfo=UTC))


def test_build_user_gera_salt_de_dezesseis_bytes() -> None:
    user = build_user("user-1", "a@b.com", _PASSWORD, datetime(2026, 7, 1, tzinfo=UTC))
    assert len(user.salt) == SALT_BYTES


@pytest.mark.parametrize("length", [12, 128], ids=["limite_minimo", "limite_maximo"])
def test_build_user_aceita_senha_nos_limites_inclusivos(length: int) -> None:
    user = build_user("user-1", "a@b.com", "x" * length, datetime(2026, 7, 1, tzinfo=UTC))
    assert user.user_id == "user-1"


def test_inicializa_schema_e_idempotente_no_banco_do_control_plane(tmp_path) -> None:
    from cnes_infra.control_plane.sqlite_adapter import SQLiteControlPlane

    database_path = tmp_path / "state" / "cnesdata.sqlite3"
    control_plane = SQLiteControlPlane(database_path, lambda: datetime(2026, 7, 15, tzinfo=UTC))
    control_plane.initialize()

    store = LocalCredentialStore(database_path)
    store.initialize()
    store.initialize()

    user = build_user("user-1", "a@b.com", _PASSWORD, datetime(2026, 7, 1, tzinfo=UTC))
    store.put_user(user)
    assert store.find_user_by_email("a@b.com") == user
