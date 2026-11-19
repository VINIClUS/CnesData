"""Registro de certificados no plano de controle SQLite."""

from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING

from cnes_domain.auth.certificates import (
    CertificateConflict,
    CertificateRecord,
    CertificateStoreUnavailable,
    validate_certificate_instant_utc,
)

if TYPE_CHECKING:
    from datetime import datetime

    from cnes_domain.auth.certificates import CertificateKey
    from cnes_infra.control_plane.sqlite_adapter import SQLiteControlPlane

_COLUMNS = (
    "tenant_id",
    "agent_id",
    "serial",
    "certificate_fingerprint_sha256",
    "subject_cn",
    "issued_at",
    "expires_at",
    "revoked_at",
)
_STORAGE_ERRORS = (sqlite3.Error, OSError, RuntimeError)


class SQLiteCertificateRegister:
    def __init__(self, control_plane: SQLiteControlPlane) -> None:
        self._control_plane = control_plane

    def record(self, certificate: CertificateRecord) -> None:
        values = certificate.model_dump(mode="json")
        try:
            with self._control_plane.write_transaction() as connection:
                connection.execute(
                    "INSERT INTO provisioned_certificates "
                    "(tenant_id, agent_id, serial, certificate_fingerprint_sha256, "
                    "subject_cn, issued_at, expires_at, revoked_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    tuple(values[name] for name in _COLUMNS),
                )
        except sqlite3.IntegrityError as error:
            raise CertificateConflict from error
        except _STORAGE_ERRORS as error:
            raise CertificateStoreUnavailable from error

    def get(self, key: CertificateKey) -> CertificateRecord | None:
        try:
            with self._control_plane.read_connection() as connection:
                row = connection.execute(
                    "SELECT tenant_id, agent_id, serial, certificate_fingerprint_sha256, "
                    "subject_cn, issued_at, expires_at, revoked_at FROM provisioned_certificates "
                    "WHERE tenant_id = ? AND agent_id = ? AND serial = ?",
                    (key.tenant_id, key.agent_id, key.serial),
                ).fetchone()
        except _STORAGE_ERRORS as error:
            raise CertificateStoreUnavailable from error
        if row is None:
            return None
        return CertificateRecord.model_validate(dict(zip(_COLUMNS, row, strict=True)))

    def is_active(
        self, key: CertificateKey, certificate_fingerprint_sha256: str, now: datetime
    ) -> bool:
        record = self.get(key)
        return (
            record is not None
            and record.certificate_fingerprint_sha256 == certificate_fingerprint_sha256
            and record.revoked_at is None
            and record.issued_at <= now < record.expires_at
        )

    def revoke(self, key: CertificateKey, revoked_at: datetime) -> bool:
        validate_certificate_instant_utc(revoked_at)
        try:
            with self._control_plane.write_transaction() as connection:
                result = connection.execute(
                    "UPDATE provisioned_certificates SET revoked_at = ? "
                    "WHERE tenant_id = ? AND agent_id = ? AND serial = ? AND revoked_at IS NULL",
                    (revoked_at.isoformat(), key.tenant_id, key.agent_id, key.serial),
                )
                return result.rowcount == 1
        except _STORAGE_ERRORS as error:
            raise CertificateStoreUnavailable from error
