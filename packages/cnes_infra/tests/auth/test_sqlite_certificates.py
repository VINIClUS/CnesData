"""Persistência e falhas do registro de certificados em SQLite real."""

import sqlite3
from contextlib import closing
from pathlib import Path

import pytest

from cnes_domain.auth.certificates import CertificateStoreUnavailable
from cnes_infra.auth.sqlite_certificates import SQLiteCertificateRegister
from cnes_infra.control_plane.sqlite_adapter import SQLiteControlPlane
from packages.cnes_infra.tests.auth.test_certificate_register_contract import (
    _FINGERPRINT,
    _NOW,
    _certificate,
    _key,
)


def _open_register(path: Path) -> tuple[SQLiteControlPlane, SQLiteCertificateRegister]:
    plane = SQLiteControlPlane(path, lambda: _NOW)
    plane.initialize()
    return plane, SQLiteCertificateRegister(plane)


def test_reabertura_le_registro_revogado_e_preserva_outro_serial(tmp_path: Path) -> None:
    path = tmp_path / "control.sqlite3"
    _, register = _open_register(path)
    register.record(_certificate())
    register.record(_certificate(serial="serial-new"))
    assert register.revoke(_key(), _NOW) is True

    _, reopened = _open_register(path)

    assert reopened.get(_key()) == _certificate(revoked_at=_NOW)
    assert reopened.is_active(_key(), _FINGERPRINT, _NOW) is False
    assert reopened.is_active(_key(serial="serial-new"), _FINGERPRINT, _NOW) is True


def test_initialize_adiciona_tabela_ao_arquivo_existente(tmp_path: Path) -> None:
    path = tmp_path / "old-control.sqlite3"
    with closing(sqlite3.connect(path)) as connection, connection:
        connection.execute("CREATE TABLE legacy_marker (value TEXT NOT NULL)")
        connection.execute("INSERT INTO legacy_marker (value) VALUES (?)", ("preserved",))

    _, register = _open_register(path)
    register.record(_certificate())

    assert register.get(_key()) == _certificate()
    with closing(sqlite3.connect(path)) as connection, connection:
        assert connection.execute("SELECT value FROM legacy_marker").fetchone() == ("preserved",)


def test_banco_ocupado_levanta_erro_publico_sem_gravar(tmp_path: Path) -> None:
    path = tmp_path / "control.sqlite3"
    _, register = _open_register(path)
    with closing(sqlite3.connect(path)) as lock, lock:
        lock.execute("BEGIN IMMEDIATE")
        with pytest.raises(CertificateStoreUnavailable) as failure:
            register.record(_certificate())
        assert failure.value.code == "certificate_store_unavailable"
        assert str(failure.value) == "code=certificate_store_unavailable"
    assert register.get(_key()) is None


def test_banco_ocupado_durante_revogacao_preserva_registro_ativo(tmp_path: Path) -> None:
    path = tmp_path / "control.sqlite3"
    _, register = _open_register(path)
    original = _certificate()
    register.record(original)

    with closing(sqlite3.connect(path)) as lock, lock:
        lock.execute("BEGIN IMMEDIATE")
        with pytest.raises(CertificateStoreUnavailable) as failure:
            register.revoke(_key(), _NOW)
        assert failure.value.code == "certificate_store_unavailable"
        assert str(failure.value) == "code=certificate_store_unavailable"

    assert register.get(_key()) == original
    assert register.is_active(_key(), _FINGERPRINT, _NOW) is True


def test_falha_de_leitura_nao_vira_inativo(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    plane, register = _open_register(tmp_path / "control.sqlite3")
    register.record(_certificate())

    def failing_connect() -> sqlite3.Connection:
        raise sqlite3.OperationalError("disk I/O error")

    monkeypatch.setattr(plane, "_connect", failing_connect)
    with pytest.raises(CertificateStoreUnavailable) as failure:
        register.is_active(_key(), _FINGERPRINT, _NOW)
    assert failure.value.code == "certificate_store_unavailable"
    assert str(failure.value) == "code=certificate_store_unavailable"
