"""Backup online do estado local (SQLite WAL + objetos imutáveis) e restauração verificada.

CLI: `python -m central_api.local_backup {create,restore}` — roda dentro do container
`central-api-local`, como o usuário `app`, contra o `data_dir` montado em `/data`.
"""
from __future__ import annotations

import argparse
import contextlib
import logging
import os
import shutil
import sqlite3
import sys
import tarfile
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from secrets import token_hex
from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict

from cnes_domain.profiles import local_state_db, local_state_db_arcname, parse_profile

if TYPE_CHECKING:
    from collections.abc import Mapping

logger = logging.getLogger(__name__)

_BACKUP_VERSION = 1
_STATE_ARCNAME = local_state_db_arcname()
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


def create_backup(data_dir: Path, target: Path, now: datetime) -> BackupManifest:
    """Args: data_dir, target, now.
    Returns: Manifesto do backup (também gravado dentro do arquivo).
    Raises: BackupRejected: tenant ausente ou duplicado no state DB copiado.
    """
    state_db = local_state_db(data_dir)
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
    manifest = BackupManifest.model_validate_json(payload)
    if manifest.backup_version != _BACKUP_VERSION:
        raise RestoreRejected("backup_version_unsupported")
    return manifest


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
    """Defensivo: se o restore for executado por um principal com UID diferente
    do `app` do container (o fluxo documentado roda dentro do container, como
    `app`, e não precisa disto), permissao 'other' e a unica garantia robusta
    de acesso cross-UID no profile local."""
    if not root.exists():
        return
    for item in (root, *root.rglob("*")):
        item.chmod(_RESTORED_DIR_MODE if item.is_dir() else _RESTORED_FILE_MODE)


def _publish_restore_tree(staging: Path, data_dir: Path) -> None:
    """Move cada entrada de topo de `staging` para dentro de `data_dir`.

    `data_dir` (ex.: `/data`) pode ser o mountpoint de um volume nomeado, num
    filesystem diferente do seu próprio pai — `os.replace(staging, data_dir)`
    cruzaria device e falharia (EXDEV), então `staging` fica dentro de
    `data_dir` e cada filho é movido individualmente, mesmo filesystem
    garantido. Falha no meio do caminho desfaz o que já moveu.
    """
    published: list[Path] = []
    try:
        for child in sorted(staging.iterdir()):
            target = data_dir / child.name
            os.replace(child, target)
            published.append(target)
    except Exception:
        for target in reversed(published):
            with contextlib.suppress(Exception):
                os.replace(target, staging / target.name)
        raise
    _fsync_directory(data_dir)


def restore_backup(archive: Path, data_dir: Path, expected_tenant_id: str) -> None:
    """Args: archive, data_dir, expected_tenant_id.
    Raises: RestoreRejected: alvo não vazio, hash divergente ou tenant divergente.
    """
    state_db = local_state_db(data_dir)
    _reject_if_nonempty(state_db, data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    staging = data_dir / f".{token_hex(8)}.restore"
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


def _cli_create(data_dir: Path, target: Path) -> int:
    manifest = create_backup(data_dir, target, datetime.now(UTC))
    logger.info("backup_created target=%s tenant_id=%s files=%d",
                target, manifest.tenant_id, len(manifest.files))
    return 0


def _cli_restore(data_dir: Path, archive: Path, tenant_id: str) -> int:
    restore_backup(archive, data_dir, tenant_id)
    logger.info("backup_restored archive=%s data_dir=%s tenant_id=%s",
                archive, data_dir, tenant_id)
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m central_api.local_backup")
    subparsers = parser.add_subparsers(dest="command", required=True)
    create_parser = subparsers.add_parser("create", help="cria backup do data_dir atual")
    create_parser.add_argument("--target", required=True, help="caminho do arquivo .tar de saída")
    restore_parser = subparsers.add_parser("restore", help="restaura backup para o data_dir atual")
    restore_parser.add_argument(
        "--archive", required=True, help="caminho do arquivo .tar de entrada"
    )
    return parser


def main(argv: list[str] | None = None, env: Mapping[str, str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    settings = parse_profile(dict(env if env is not None else os.environ))
    logging.basicConfig(level=logging.INFO)
    if args.command == "create":
        return _cli_create(settings.data_dir, Path(args.target))
    return _cli_restore(settings.data_dir, Path(args.archive), settings.tenant_id)


__all__ = [
    "BackupFile",
    "BackupManifest",
    "BackupRejected",
    "RestoreRejected",
    "create_backup",
    "main",
    "restore_backup",
]


if __name__ == "__main__":
    sys.exit(main())
