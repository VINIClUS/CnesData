"""Adapter raw para vínculos PF do CNES nacional."""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from io import BytesIO
from typing import TYPE_CHECKING, Never

import polars as pl

from cnes_contracts import RawManifest, SnapshotMode, SourceType
from cnes_infra.ingestion.datasus_cnes_transport import (
    DatasusCnesError,
    DatasusCnesRequest,
    DatasusCnesTransportPort,
    _validate_request,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from datetime import datetime

    from cnes_domain.ports.object_store import ObjectStorePort

_SCHEMA = {
    "CPF": pl.String,
    "CNS": pl.String,
    "NOME_PROFISSIONAL": pl.String,
    "NOME_SOCIAL": pl.String,
    "SEXO": pl.String,
    "CBO": pl.String,
    "CNES": pl.String,
    "TIPO_VINCULO": pl.String,
    "SUS": pl.String,
    "CH_TOTAL": pl.Int64,
    "CH_AMBULATORIAL": pl.Int64,
    "CH_OUTRAS": pl.Int64,
    "CH_HOSPITALAR": pl.Int64,
    "FONTE": pl.String,
}
_COLUMNS = tuple(_SCHEMA)


@dataclass(frozen=True, slots=True)
class _RawObject:
    key: str
    payload: bytes
    digest: str
    row_count: int


def _raise(code: str, retryable: bool = False) -> Never:
    raise DatasusCnesError(code, retryable) from None


class DatasusCnesRawAdapter:
    def __init__(
        self,
        transport: DatasusCnesTransportPort,
        store: ObjectStorePort,
        clock: Callable[[], datetime],
    ) -> None:
        self._transport = transport
        self._store = store
        self._clock = clock

    def extract(self, request: DatasusCnesRequest) -> RawManifest:
        _validate_request(request)
        projected = self._project_rows(request)
        if not projected:
            _raise("source_not_published", True)
        frame = pl.DataFrame(projected, schema=_SCHEMA).sort(
            _COLUMNS, nulls_last=True
        )
        payload = self._serialize(frame)
        digest = sha256(payload).hexdigest()
        key = self._object_key(request)
        raw_object = _RawObject(key, payload, digest, frame.height)
        manifest = self._manifest(request, raw_object)
        self._store.put(key, BytesIO(payload), digest)
        return manifest

    def _project_rows(self, request: DatasusCnesRequest) -> list[dict[str, object]]:
        iterator = iter(self._transport.fetch(request))
        failed = True
        try:
            projected = [self._project(row, request) for row in iterator]
            failed = False
            return projected
        finally:
            close = getattr(iterator, "close", None)
            if callable(close):
                try:
                    close()
                except Exception:
                    if not failed:
                        raise

    @staticmethod
    def _project(
        row: Mapping[str, object], request: DatasusCnesRequest
    ) -> dict[str, object]:
        _validate_boundary(row, request)
        outpatient = _hours(row, "HORA_AMB")
        other = _hours(row, "HORAOUTR")
        hospital = _hours(row, "HORAHOSP")
        return {
            "CPF": _document(row, "CPF_PROF", 11),
            "CNS": _document(row, "CNS_PROF", 15),
            "NOME_PROFISSIONAL": _text(row, "NOMEPROF"),
            "NOME_SOCIAL": None,
            "SEXO": None,
            "CBO": _padded(row, "CBO", 6),
            "CNES": _padded(row, "CNES", 7),
            "TIPO_VINCULO": _padded(row, "VINCULAC", 6),
            "SUS": _sus(row),
            "CH_TOTAL": outpatient + other + hospital,
            "CH_AMBULATORIAL": outpatient,
            "CH_OUTRAS": other,
            "CH_HOSPITALAR": hospital,
            "FONTE": "NACIONAL",
        }

    @staticmethod
    def _serialize(frame: pl.DataFrame) -> bytes:
        output = BytesIO()
        frame.write_parquet(
            output,
            compression="zstd",
            compression_level=3,
            statistics=True,
            row_group_size=64_000,
        )
        return output.getvalue()

    @staticmethod
    def _object_key(request: DatasusCnesRequest) -> str:
        return (
            f"raw/{request.tenant_id}/CNES_NACIONAL/{request.competencia}/"
            f"{request.snapshot_id}/data.parquet"
        )

    def _manifest(
        self,
        request: DatasusCnesRequest,
        raw_object: _RawObject,
    ) -> RawManifest:
        return RawManifest(
            manifest_version=1,
            manifest_id=request.snapshot_id,
            tenant_id=request.tenant_id,
            source_type=SourceType.CNES_NACIONAL,
            file_subtype=request.file_subtype,
            competencia=request.competencia,
            agent_id=request.agent_id,
            agent_version=request.agent_version,
            schema_version="cnes-profissional-v1",
            snapshot_mode=SnapshotMode.FULL,
            snapshot_id=request.snapshot_id,
            base_snapshot_id=None,
            sequence=1,
            previous_manifest_sha256=None,
            object_sha256=raw_object.digest,
            row_count=raw_object.row_count,
            size_bytes=len(raw_object.payload),
            object_key=raw_object.key,
            created_at=self._clock(),
        )


def _text(row: Mapping[str, object], field: str) -> str:
    try:
        value = row[field]
    except (KeyError, TypeError):
        _raise("field_invalid")
    if not isinstance(value, str):
        _raise("field_invalid")
    return value.strip()


def _validate_boundary(row: Mapping[str, object], request: DatasusCnesRequest) -> None:
    try:
        competencia = _text(row, "COMPETEN")
        municipio = _text(row, "CODUFMUN")
    except DatasusCnesError:
        _raise("transport_contract_invalid")
    if competencia != request.competencia.replace("-", ""):
        _raise("transport_contract_invalid")
    if municipio != request.tenant_id:
        _raise("transport_contract_invalid")


def _document(row: Mapping[str, object], field: str, width: int) -> str | None:
    value = _text(row, field)
    if value == "" or value == "9" * width:
        return None
    if len(value) != width or not _is_ascii_digits(value):
        _raise("field_invalid")
    return value


def _padded(row: Mapping[str, object], field: str, width: int) -> str:
    value = _text(row, field)
    if not _is_ascii_digits(value) or len(value) > width:
        _raise("field_invalid")
    return value.zfill(width)


def _sus(row: Mapping[str, object]) -> str:
    value = _text(row, "PROF_SUS")
    if value == "1":
        return "S"
    if value == "0":
        return "N"
    _raise("field_invalid")


def _hours(row: Mapping[str, object], field: str) -> int:
    try:
        value = row[field]
    except (KeyError, TypeError):
        _raise("field_invalid")
    if value is None or value == "":
        return 0
    if isinstance(value, bool):
        _raise("field_invalid")
    if isinstance(value, int) and value >= 0:
        return value
    if isinstance(value, float) and value >= 0 and value.is_integer():
        return int(value)
    if isinstance(value, str) and _is_ascii_digits(value.strip()):
        return int(value.strip())
    _raise("field_invalid")


def _is_ascii_digits(value: str) -> bool:
    return value.isascii() and value.isdigit()
