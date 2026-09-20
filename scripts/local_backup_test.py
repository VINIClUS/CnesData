"""Testes de create_backup/restore_backup: atomicidade, hash, tenant, exclusões."""
from __future__ import annotations

import io
import os
import sqlite3
import tarfile
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pytest

from scripts.local_backup import (
    BackupRejected,
    RestoreRejected,
    create_backup,
    restore_backup,
)

if TYPE_CHECKING:
    from pathlib import Path

_NOW = datetime(2026, 1, 15, 12, tzinfo=UTC)


def _seed_state_db(path: Path, tenant_id: str = "354130") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    try:
        connection.execute("CREATE TABLE tenants (tenant_id TEXT PRIMARY KEY, data TEXT)")
        connection.execute(
            "INSERT INTO tenants VALUES (?, ?)", (tenant_id, f'{{"tenant_id": "{tenant_id}"}}')
        )
        connection.commit()
    finally:
        connection.close()


def _seed_data_dir(data_dir: Path) -> None:
    (data_dir / "objects" / "raw").mkdir(parents=True)
    (data_dir / "objects" / "raw" / "a.parquet").write_bytes(b"payload-a")
    (data_dir / "audit" / "354130" / "2026" / "01" / "15").mkdir(parents=True)
    events_log = data_dir / "audit" / "354130" / "2026" / "01" / "15" / "events.jsonl"
    events_log.write_bytes(b'{"event_id": "e1"}\n')
    (data_dir / "audit" / "index.sqlite3").write_bytes(b"derivable-index")
    (data_dir / "audit" / ".sink.lock").write_bytes(b"")


def _prepared_dirs(tmp_path: Path) -> tuple[Path, Path]:
    data_dir = tmp_path / "data"
    state_db = data_dir / "state" / "cnesdata.sqlite3"
    _seed_state_db(state_db)
    _seed_data_dir(data_dir)
    return state_db, data_dir


def test_create_backup_grava_manifest_com_hash_e_tamanho_por_arquivo(tmp_path: Path) -> None:
    state_db, data_dir = _prepared_dirs(tmp_path)
    target = tmp_path / "backup.tar"

    manifest = create_backup(state_db, data_dir, target, _NOW)

    assert manifest.tenant_id == "354130"
    assert manifest.created_at == _NOW
    paths = {file.path for file in manifest.files}
    assert "state/cnesdata.sqlite3" in paths
    assert "objects/raw/a.parquet" in paths
    assert any(path.endswith("events.jsonl") for path in paths)
    assert "audit/index.sqlite3" not in paths
    assert not any(path.endswith(".sink.lock") for path in paths)
    parquet_file = next(f for f in manifest.files if f.path == "objects/raw/a.parquet")
    assert parquet_file.size_bytes == len(b"payload-a")


def test_create_backup_so_produz_arquivo_alvo_completo(tmp_path: Path) -> None:
    state_db, data_dir = _prepared_dirs(tmp_path)
    target = tmp_path / "backup.tar"

    create_backup(state_db, data_dir, target, _NOW)

    assert target.exists()
    assert list(tmp_path.glob(".backup.tar*")) == []
    with tarfile.open(target, "r") as tar:
        assert "state/cnesdata.sqlite3" in tar.getnames()


def test_create_backup_rejeita_state_db_sem_tenant(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    state_db = data_dir / "state" / "cnesdata.sqlite3"
    state_db.parent.mkdir(parents=True)
    connection = sqlite3.connect(state_db)
    connection.execute("CREATE TABLE tenants (tenant_id TEXT PRIMARY KEY, data TEXT)")
    connection.commit()
    connection.close()
    _seed_data_dir(data_dir)

    with pytest.raises(BackupRejected, match="tenant_count_invalid"):
        create_backup(state_db, data_dir, tmp_path / "backup.tar", _NOW)


def test_restore_backup_recompoe_state_db_e_objetos(tmp_path: Path) -> None:
    state_db, data_dir = _prepared_dirs(tmp_path)
    target = tmp_path / "backup.tar"
    create_backup(state_db, data_dir, target, _NOW)
    restore_state_db = tmp_path / "restored" / "state" / "cnesdata.sqlite3"
    restore_data_dir = tmp_path / "restored"

    restore_backup(target, restore_state_db, restore_data_dir)

    assert restore_state_db.exists()
    connection = sqlite3.connect(restore_state_db)
    try:
        rows = connection.execute("SELECT tenant_id FROM tenants").fetchall()
    finally:
        connection.close()
    assert rows == [("354130",)]
    restored_parquet = restore_data_dir / "objects" / "raw" / "a.parquet"
    assert restored_parquet.read_bytes() == b"payload-a"
    assert not (restore_data_dir / "audit" / "index.sqlite3").exists()


def test_restore_backup_recusa_alvo_state_db_existente(tmp_path: Path) -> None:
    state_db, data_dir = _prepared_dirs(tmp_path)
    target = tmp_path / "backup.tar"
    create_backup(state_db, data_dir, target, _NOW)
    existing_state_db = tmp_path / "restored" / "state" / "cnesdata.sqlite3"
    _seed_state_db(existing_state_db)

    with pytest.raises(RestoreRejected, match="target_not_empty"):
        restore_backup(target, existing_state_db, tmp_path / "restored")


def test_restore_backup_recusa_objects_dir_nao_vazio(tmp_path: Path) -> None:
    state_db, data_dir = _prepared_dirs(tmp_path)
    target = tmp_path / "backup.tar"
    create_backup(state_db, data_dir, target, _NOW)
    restore_data_dir = tmp_path / "restored"
    (restore_data_dir / "objects").mkdir(parents=True)
    (restore_data_dir / "objects" / "leftover.txt").write_bytes(b"x")

    with pytest.raises(RestoreRejected, match="target_not_empty"):
        restore_backup(target, restore_data_dir / "state" / "cnesdata.sqlite3", restore_data_dir)


def test_restore_backup_recusa_hash_corrompido_sem_escrever_nada(tmp_path: Path) -> None:
    state_db, data_dir = _prepared_dirs(tmp_path)
    target = tmp_path / "backup.tar"
    create_backup(state_db, data_dir, target, _NOW)
    corrupted = tmp_path / "corrupted.tar"
    with tarfile.open(target, "r") as source_tar, tarfile.open(corrupted, "w") as dest_tar:
        for member in source_tar.getmembers():
            body = source_tar.extractfile(member).read()
            if member.name == "objects/raw/a.parquet":
                body = b"tampered!"
                member.size = len(body)
            dest_tar.addfile(member, io.BytesIO(body))
    restore_data_dir = tmp_path / "restored"
    restore_state_db = restore_data_dir / "state" / "cnesdata.sqlite3"

    with pytest.raises(RestoreRejected, match="hash_mismatch"):
        restore_backup(corrupted, restore_state_db, restore_data_dir)

    assert not restore_state_db.exists()
    assert not (restore_data_dir / "objects").exists()


def test_restore_backup_recusa_tenant_divergente(tmp_path: Path) -> None:
    state_db, data_dir = _prepared_dirs(tmp_path)
    target = tmp_path / "backup.tar"
    manifest = create_backup(state_db, data_dir, target, _NOW)
    tampered_manifest = manifest.model_copy(update={"tenant_id": "999999"})
    tampered = tmp_path / "tampered.tar"
    with tarfile.open(target, "r") as source_tar, tarfile.open(tampered, "w") as dest_tar:
        for member in source_tar.getmembers():
            body = source_tar.extractfile(member).read()
            if member.name == "manifest.json":
                body = tampered_manifest.model_dump_json().encode()
                member.size = len(body)
            dest_tar.addfile(member, io.BytesIO(body))
    restore_data_dir = tmp_path / "restored"
    restore_state_db = restore_data_dir / "state" / "cnesdata.sqlite3"

    with pytest.raises(RestoreRejected, match="tenant_mismatch"):
        restore_backup(tampered, restore_state_db, restore_data_dir)

    assert not restore_state_db.exists()


def test_restore_backup_falha_no_publish_sem_deixar_arvore_parcial(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state_db, data_dir = _prepared_dirs(tmp_path)
    archive = tmp_path / "backup.tar"
    create_backup(state_db, data_dir, archive, _NOW)
    restore_data_dir = tmp_path / "restored"
    restore_data_dir.mkdir()
    restore_state_db = restore_data_dir / "state" / "cnesdata.sqlite3"
    real_replace = os.replace
    calls = 0

    def fail_install(source: str, destination: str) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("disk_full")
        real_replace(source, destination)

    monkeypatch.setattr("scripts.local_backup.os.replace", fail_install)
    with pytest.raises(OSError, match="disk_full"):
        restore_backup(archive, restore_state_db, restore_data_dir)

    assert list(restore_data_dir.iterdir()) == []


def test_restore_backup_rejeita_state_db_fora_da_arvore(tmp_path: Path) -> None:
    state_db, data_dir = _prepared_dirs(tmp_path)
    archive = tmp_path / "backup.tar"
    create_backup(state_db, data_dir, archive, _NOW)
    restore_data_dir = tmp_path / "restored"
    restore_state_db = tmp_path / "custom" / "state.sqlite3"

    with pytest.raises(RestoreRejected, match="state_db_path_invalid"):
        restore_backup(archive, restore_state_db, restore_data_dir)

    assert not restore_data_dir.exists()
