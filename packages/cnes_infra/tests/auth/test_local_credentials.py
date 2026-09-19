"""Testes de hashing de senha, criação de usuários e inicialização do credential store."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime

import pytest

from cnes_infra.auth.local_credentials import (
    SALT_BYTES,
    LocalCredentialStore,
    build_user,
    generate_salt,
    hash_password,
    normalize_email,
)

_PASSWORD = "correct-horse-battery"  # noqa: S105


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
