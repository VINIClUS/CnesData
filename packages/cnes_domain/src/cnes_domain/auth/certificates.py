"""Tipos e porta do registro de certificados."""

from datetime import datetime, timedelta
from typing import Annotated, ClassVar, Protocol, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

Nonempty = Annotated[str, Field(min_length=1)]


def validate_certificate_instant_utc(value: datetime) -> None:
    """Args:
        value: instante a validar.
    Raises:
        ValueError: instante sem UTC.
    """
    if value.utcoffset() != timedelta(0):
        raise ValueError("code=certificate_instant_not_utc")


class CertificateRecord(BaseModel):
    model_config = ConfigDict(frozen=True)

    tenant_id: Nonempty
    agent_id: Nonempty
    serial: Nonempty
    certificate_fingerprint_sha256: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
    subject_cn: Nonempty
    issued_at: datetime
    expires_at: datetime
    revoked_at: datetime | None = None


    @field_validator("issued_at", "expires_at", "revoked_at")
    @classmethod
    def _require_utc(cls, value: datetime | None) -> datetime | None:
        if value is not None:
            validate_certificate_instant_utc(value)
        return value

    @model_validator(mode="after")
    def _require_valid_interval(self) -> Self:
        if self.expires_at <= self.issued_at:
            raise ValueError("code=certificate_invalid_interval")
        return self


class CertificateKey(BaseModel):
    model_config = ConfigDict(frozen=True)

    tenant_id: Nonempty
    agent_id: Nonempty
    serial: Nonempty


class CertificateConflict(RuntimeError):
    code: ClassVar[str] = "certificate_conflict"

    def __init__(self) -> None:
        super().__init__("code=certificate_conflict")


class CertificateStoreUnavailable(RuntimeError):
    code: ClassVar[str] = "certificate_store_unavailable"

    def __init__(self) -> None:
        super().__init__("code=certificate_store_unavailable")


class CertificateRegisterPort(Protocol):
    def record(  # pragma: no cover - Protocol stub
        self, certificate: CertificateRecord
    ) -> None: ...

    def get(  # pragma: no cover - Protocol stub
        self, key: CertificateKey
    ) -> CertificateRecord | None: ...

    def is_active(  # pragma: no cover - Protocol stub
        self, key: CertificateKey, certificate_fingerprint_sha256: str, now: datetime
    ) -> bool: ...

    def revoke(  # pragma: no cover - Protocol stub
        self, key: CertificateKey, revoked_at: datetime
    ) -> bool: ...
