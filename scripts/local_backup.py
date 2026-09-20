"""Backup online do estado local (SQLite WAL + objetos imutáveis) e restauração verificada."""
from __future__ import annotations

import os
import shutil
import sqlite3
import tarfile
from datetime import datetime  # noqa: TC003
from hashlib import sha256
from secrets import token_hex
from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict

if TYPE_CHECKING:
    from pathlib import Path

_BACKUP_VERSION = 1
_STATE_ARCNAME = "state/cnesdata.sqlite3"
_MANIFEST_NAME = "manifest.json"
_EXCLUDED_AUDIT_NAMES = frozenset({"index.sqlite3", ".sink.lock"})


class BackupFile(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    path: str
    size_bytes: int
    sha256: str


class BackupManifest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    backup_version: int
    tenant_id: str
    created_at: datetime
    files: tuple[BackupFile, ...]


class BackupRejected(Exception):
    pass


class RestoreRejected(Exception):
    pass


def _hash_file(path: Path) -> tuple[int, str]:
    digest = sha256()
    size = 0
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
            size += len(chunk)
    return size, digest.hexdigest()


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _fsync_file(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _backup_sqlite(state_db: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    source = sqlite3.connect(state_db)
    try:
        destination = sqlite3.connect(target)
        try:
            source.backup(destination)
        finally:
            destination.close()
    finally:
        source.close()


def _copy_tree(source: Path, destination: Path, *, skip_names: frozenset[str]) -> None:
    if not source.exists():
        return
    for item in sorted(source.rglob("*")):
        if item.is_dir() or item.name in skip_names:
            continue
        target = destination / item.relative_to(source)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(item, target)


def _read_single_tenant(database_path: Path) -> str:
    connection = sqlite3.connect(database_path)
    try:
        rows = connection.execute("SELECT tenant_id FROM tenants").fetchall()
    finally:
        connection.close()
    if len(rows) != 1:
        raise BackupRejected("tenant_count_invalid")
    return rows[0][0]


def _collect_manifest_files(staging: Path) -> tuple[BackupFile, ...]:
    files = []
    for item in sorted(staging.rglob("*")):
        if item.is_dir():
            continue
        size, digest = _hash_file(item)
        files.append(BackupFile(path=item.relative_to(staging).as_posix(),
                                 size_bytes=size, sha256=digest))
    return tuple(files)


def _finalize_archive(staging: Path, target: Path) -> None:
    temporary = target.with_name(f".{target.name}.{token_hex(8)}.tmp")
    with tarfile.open(temporary, "w") as archive:
        for item in sorted(staging.rglob("*")):
            if item.is_file():
                archive.add(item, arcname=item.relative_to(staging).as_posix())
    _fsync_file(temporary)
    os.replace(temporary, target)
    _fsync_directory(target.parent)


def create_backup(state_db: Path, data_dir: Path, target: Path, now: datetime) -> BackupManifest:
    """Args: state_db, data_dir, target, now.
    Returns: Manifesto do backup (também gravado dentro do arquivo).
    Raises: BackupRejected: tenant ausente ou duplicado no state DB copiado.
    """
    staging = target.parent / f".{target.name}.{token_hex(8)}.staging"
    staging.mkdir(parents=True)
    try:
        _backup_sqlite(state_db, staging / _STATE_ARCNAME)
        _copy_tree(data_dir / "objects", staging / "objects", skip_names=frozenset())
        _copy_tree(data_dir / "audit", staging / "audit", skip_names=_EXCLUDED_AUDIT_NAMES)
        tenant_id = _read_single_tenant(staging / _STATE_ARCNAME)
        manifest = BackupManifest(
            backup_version=_BACKUP_VERSION, tenant_id=tenant_id, created_at=now,
            files=_collect_manifest_files(staging),
        )
        (staging / _MANIFEST_NAME).write_bytes(manifest.model_dump_json().encode())
        _finalize_archive(staging, target)
    finally:
        shutil.rmtree(staging, ignore_errors=True)
    return manifest


def _reject_if_nonempty(state_db: Path, data_dir: Path) -> None:
    if state_db.exists() or (data_dir.exists() and not data_dir.is_dir()):
        raise RestoreRejected("target_not_empty")
    if data_dir.exists() and any(data_dir.iterdir()):
        raise RestoreRejected("target_not_empty")


def _load_manifest(staging: Path) -> BackupManifest:
    payload = (staging / _MANIFEST_NAME).read_bytes()
    return BackupManifest.model_validate_json(payload)


def _verify_files(staging: Path, manifest: BackupManifest) -> None:
    found = {item.relative_to(staging).as_posix()
             for item in staging.rglob("*") if item.is_file()}
    found.discard(_MANIFEST_NAME)
    expected = {file.path for file in manifest.files}
    if found != expected:
        raise RestoreRejected("manifest_files_mismatch")
    for file in manifest.files:
        size, digest = _hash_file(staging / file.path)
        if size != file.size_bytes or digest != file.sha256:
            raise RestoreRejected("hash_mismatch")


def _verify_tenant(staging: Path, manifest: BackupManifest, expected_tenant_id: str) -> None:
    tenant_id = _read_single_tenant(staging / _STATE_ARCNAME)
    if tenant_id != manifest.tenant_id or tenant_id != expected_tenant_id:
        raise RestoreRejected("tenant_mismatch")


_RESTORED_FILE_MODE = 0o666
_RESTORED_DIR_MODE = 0o777


def _widen_permissions(root: Path) -> None:
    """Host operator e o processo do container podem ter UIDs sem GID em comum;
    permissao 'other' e a unica garantia robusta de acesso cross-UID no profile local."""
    if not root.exists():
        return
    for item in (root, *root.rglob("*")):
        item.chmod(_RESTORED_DIR_MODE if item.is_dir() else _RESTORED_FILE_MODE)


def _validate_state_db_path(state_db: Path, data_dir: Path) -> None:
    if state_db != data_dir / _STATE_ARCNAME:
        raise RestoreRejected("state_db_path_invalid")


def _publish_restore_tree(staging: Path, data_dir: Path) -> None:
    displaced = data_dir.parent / f".{data_dir.name}.{token_hex(8)}.old"
    had_target = data_dir.exists()
    if had_target:
        os.replace(data_dir, displaced)
    try:
        os.replace(staging, data_dir)
    except Exception:
        if had_target:
            os.replace(displaced, data_dir)
        raise
    if had_target:
        displaced.rmdir()
    _fsync_directory(data_dir.parent)


def restore_backup(
    archive: Path, state_db: Path, data_dir: Path, expected_tenant_id: str
) -> None:
    """Args: archive, state_db, data_dir, expected_tenant_id.
    Raises: RestoreRejected: alvo não vazio, hash divergente ou tenant divergente.
    """
    _validate_state_db_path(state_db, data_dir)
    _reject_if_nonempty(state_db, data_dir)
    data_dir.parent.mkdir(parents=True, exist_ok=True)
    staging = data_dir.parent / f".{data_dir.name}.{token_hex(8)}.restore"
    staging.mkdir(parents=True)
    try:
        with tarfile.open(archive, "r") as tar:
            tar.extractall(staging, filter="data")
        manifest = _load_manifest(staging)
        _verify_files(staging, manifest)
        _verify_tenant(staging, manifest, expected_tenant_id)
        (staging / _MANIFEST_NAME).unlink()
        _widen_permissions(staging)
        _publish_restore_tree(staging, data_dir)
    finally:
        shutil.rmtree(staging, ignore_errors=True)


__all__ = [
    "BackupFile",
    "BackupManifest",
    "BackupRejected",
    "RestoreRejected",
    "create_backup",
    "restore_backup",
]
