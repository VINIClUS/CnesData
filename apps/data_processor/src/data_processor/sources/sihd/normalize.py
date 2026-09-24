"""Stage function: normaliza um subtipo SIHD em Parquet de dados + Parquet de qualidade."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal
from hashlib import sha256
from io import BytesIO
from typing import TYPE_CHECKING

import polars as pl

from cnes_contracts.manifests.outputs import OutputManifest
from cnes_contracts.manifests.processing import NormalizeResult
from cnes_contracts.manifests.raw import SourceType
from data_processor.adapters.sihd_local_adapter import _MAP_INTERNACAO_RAW, _MAP_PROC_AIH_RAW
from data_processor.pipeline.delta_reconstruction import reconstruct_from_deltas
from data_processor.sources.sihd.contract import (
    DATE_FORMAT,
    INTERNACAO_DOMAINS,
    INTERNACAO_SCHEMA,
    INTERNACAO_SCHEMA_VERSION,
    INTERNACAO_SOURCE_SCHEMA,
    PROC_AIH_SCHEMA,
    PROC_AIH_SCHEMA_VERSION,
    PROC_AIH_SOURCE_SCHEMA,
    PROCEDURE_PATTERN,
    QUALITY_SCHEMA,
    QUALITY_SCHEMA_VERSION,
    SIHD_DEPENDENCIES,
    SUBTYPE_FILES,
    SUBTYPE_INTERNACAO,
    SUBTYPE_PROC_AIH,
)

if TYPE_CHECKING:
    from cnes_contracts.manifests.processing import NormalizeRequest
    from cnes_contracts.manifests.raw import RawManifest
    from cnes_domain.ports.object_store import ObjectStat, ObjectStorePort

type Check = tuple[str, pl.Expr, str]

_KEY = "SIHD_KEY"
_CENT = Decimal("0.01")
_CDC_OPS = ("I", "U", "D")


@dataclass(frozen=True, slots=True)
class _SubtypeSpec:
    raw_map: dict[str, str]
    source_schema: dict[str, type[pl.DataType]]
    output_schema: dict[str, type[pl.DataType]]
    schema_version: str
    delta_key: tuple[str, ...]
    key_parts: tuple[pl.Expr, ...]
    required: tuple[str, ...]
    domains: dict[str, tuple[str, ...]]
    procedure_fields: tuple[str, ...]
    date_fields: tuple[str, ...]


_SPECS: dict[str, _SubtypeSpec] = {
    SUBTYPE_INTERNACAO: _SubtypeSpec(
        raw_map=_MAP_INTERNACAO_RAW,
        source_schema=INTERNACAO_SOURCE_SCHEMA,
        output_schema=INTERNACAO_SCHEMA,
        schema_version=INTERNACAO_SCHEMA_VERSION,
        delta_key=("COMPETENCIA", "OE_GESTOR", "SEQ"),
        key_parts=(
            pl.col("NUM_AIH"), pl.col("PROC_REALIZADO"), pl.col("COMPETENCIA"),
            pl.concat_str(
                [pl.col("OE_GESTOR").fill_null(""), pl.col("SEQ").cast(pl.String)],
                separator=".",
            ),
        ),
        required=("NUM_AIH", "OE_GESTOR", "CNES"),
        domains=INTERNACAO_DOMAINS,
        procedure_fields=("PROC_SOLICITADO", "PROC_REALIZADO"),
        date_fields=("DT_INTERNACAO", "DT_SAIDA"),
    ),
    SUBTYPE_PROC_AIH: _SubtypeSpec(
        raw_map=_MAP_PROC_AIH_RAW,
        source_schema=PROC_AIH_SOURCE_SCHEMA,
        output_schema=PROC_AIH_SCHEMA,
        schema_version=PROC_AIH_SCHEMA_VERSION,
        delta_key=("COMPETENCIA", "OE_GESTOR", "SEQ_PRINC", "INDX"),
        key_parts=(
            pl.col("NUM_AIH"), pl.col("PROCEDIMENTO"), pl.col("COMPETENCIA"),
            pl.concat_str(
                [
                    pl.col("OE_GESTOR").fill_null(""),
                    pl.col("SEQ_PRINC").cast(pl.String),
                    pl.col("INDX").cast(pl.String),
                ],
                separator=".",
            ),
        ),
        required=("NUM_AIH", "OE_GESTOR", "CNES", "PROCEDIMENTO", "VALOR"),
        domains={},
        procedure_fields=("PROCEDIMENTO",),
        date_fields=(),
    ),
}


def normalize_sihd(request: NormalizeRequest, store: ObjectStorePort) -> NormalizeResult:
    """Normaliza a cadeia raw de um unico subtipo SIHD.

    Args:
        request: cadeia FULL+DELTA de um subtipo e as duas target_keys do layout.
        store: porta de objetos generica.

    Returns:
        NormalizeResult com manifests de dados e de qualidade, ordenados por object_key.

    Raises:
        ValueError: source/subtipo/target fora do contrato, competencia divergente,
            chave duplicada ou saida nao verificada apos put.
    """
    subtype = _validate_request(request)
    spec = _SPECS[subtype]
    manifests = request.raw_manifests
    base = manifests[0]
    frames = [_canonicalize(_read_frame(store, item), spec) for item in manifests]
    _check_ops(frames[1:])
    current = reconstruct_from_deltas(frames[0], frames[1:], spec.delta_key)
    _check_competencia(current, base.competencia)
    keyed = _with_key(current, spec)
    quality = _quality(keyed, spec, base.manifest_id)
    data = _finalize(keyed, spec, request, base)
    outputs = (
        (request.target_keys[0], data, spec.schema_version),
        (request.target_keys[1], quality, QUALITY_SCHEMA_VERSION),
    )
    return NormalizeResult(
        manifests=tuple(_write(request, store, output, base) for output in outputs)
    )


def serialize_parquet(frame: pl.DataFrame) -> bytes:
    """Serializa o frame com as opcoes de Parquet do data plane.

    Args:
        frame: DataFrame a serializar.

    Returns:
        Bytes do Parquet.
    """
    output = BytesIO()
    frame.write_parquet(
        output, compression="zstd", compression_level=3, statistics=True, row_group_size=64_000,
    )
    return output.getvalue()


def persist_verified(store: ObjectStorePort, key: str, payload: bytes) -> ObjectStat:
    """Grava o payload e confirma via stat que o SHA-256 persistido bate.

    Args:
        store: porta de objetos.
        key: chave de destino.
        payload: bytes a gravar.

    Returns:
        ObjectStat relido do store.

    Raises:
        ValueError: objeto ausente ou SHA-256 divergente apos put.
    """
    digest = sha256(payload).hexdigest()
    store.put(key, BytesIO(payload), digest)
    stat = store.stat(key)
    if stat is None:
        raise ValueError(f"output_not_found key={key}")
    if stat.sha256 != digest:
        raise ValueError(f"output_sha256_mismatch key={key}")
    return stat


def _validate_request(request: NormalizeRequest) -> str:
    if request.source_type is not SourceType.SIHD:
        raise ValueError(f"unexpected_source_type source_type={request.source_type.value}")
    subtypes = {item.file_subtype for item in request.raw_manifests}
    if len(subtypes) != 1:
        raise ValueError(f"single_subtype_required subtypes={sorted(subtypes)}")
    subtype = subtypes.pop()
    if subtype not in {dependency.file_subtype for dependency in SIHD_DEPENDENCIES}:
        raise ValueError(f"unexpected_file_subtype file_subtype={subtype}")
    leaves = tuple(key.rsplit("/", 1)[-1] for key in request.target_keys)
    if leaves != SUBTYPE_FILES[subtype]:
        raise ValueError(f"unexpected_target_keys file_subtype={subtype}")
    return subtype


def _read_frame(store: ObjectStorePort, manifest: RawManifest) -> pl.DataFrame:
    with store.open(manifest.object_key) as handle:
        return pl.read_parquet(handle)


def _blank_to_null(name: str) -> pl.Expr:
    column = pl.col(name).str.strip_chars()
    return pl.when(column.str.len_chars() == 0).then(None).otherwise(column).alias(name)


def _canonicalize(frame: pl.DataFrame, spec: _SubtypeSpec) -> pl.DataFrame:
    if frame.width == 0:
        frame = pl.DataFrame(schema=spec.source_schema)
    frame = frame.rename({k: v for k, v in spec.raw_map.items() if k in frame.columns})
    frame = frame.with_columns(
        [pl.lit(None).alias(name) for name in spec.source_schema if name not in frame.columns]
    )
    columns = [pl.col(name).cast(dtype) for name, dtype in spec.source_schema.items()]
    op = [pl.col("_op")] if "_op" in frame.columns else []
    frame = frame.select([*columns, *op])
    frame = frame.with_columns(
        [_blank_to_null(name) for name, dtype in spec.source_schema.items() if dtype is pl.String]
    )
    competencia = pl.col("COMPETENCIA")
    return frame.with_columns(
        pl.col("CNES").str.pad_start(7, "0"),
        pl.concat_str([competencia.str.slice(0, 4), competencia.str.slice(4, 2)], separator="-")
        .alias("COMPETENCIA"),
    )


def _check_ops(deltas: list[pl.DataFrame]) -> None:
    for delta in deltas:
        if "_op" not in delta.columns:
            raise ValueError("invalid_cdc_op op=missing")
        invalid = delta.filter(~pl.col("_op").is_in(_CDC_OPS).fill_null(False))["_op"]
        if invalid.len():
            raise ValueError(f"invalid_cdc_op op={invalid[0]}")


def _check_competencia(frame: pl.DataFrame, competencia: str) -> None:
    found = frame.filter(pl.col("COMPETENCIA").ne_missing(competencia))["COMPETENCIA"]
    if found.len():
        raise ValueError(f"competencia_divergente expected={competencia} found={found[0]}")


def _with_key(frame: pl.DataFrame, spec: _SubtypeSpec) -> pl.DataFrame:
    parts = [part.fill_null("") for part in spec.key_parts]
    keyed = frame.with_columns(pl.concat_str(parts, separator="|").alias(_KEY))
    duplicated = keyed.filter(pl.col(_KEY).is_duplicated())[_KEY]
    if duplicated.len():
        raise ValueError(f"duplicate_sihd_key key={duplicated[0]}")
    return keyed


def _checks(spec: _SubtypeSpec) -> list[Check]:
    checks: list[Check] = [
        (name, pl.col(name).is_null(), "campo_obrigatorio_ausente") for name in spec.required
    ]
    checks += [
        (name, pl.col(name).is_not_null() & ~pl.col(name).is_in(allowed), "codigo_desconhecido")
        for name, allowed in spec.domains.items()
    ]
    checks += [
        (name, ~pl.col(name).str.contains(PROCEDURE_PATTERN), "codigo_desconhecido")
        for name in spec.procedure_fields
    ]
    checks += [
        (name, pl.col(name).is_not_null() & _parse_date(name).is_null(), "data_invalida")
        for name in spec.date_fields
    ]
    return checks


def _quality(frame: pl.DataFrame, spec: _SubtypeSpec, manifest_id: str) -> pl.DataFrame:
    issues = [
        frame.filter(condition.fill_null(False)).select(
            pl.col(_KEY),
            pl.lit(field).alias("field"),
            pl.col(field).cast(pl.String).alias("value"),
            pl.lit(code).alias("issue_code"),
            pl.lit(manifest_id).alias("_source_manifest_id"),
        )
        for field, condition, code in _checks(spec)
    ]
    empty = pl.DataFrame(schema=QUALITY_SCHEMA)
    return pl.concat([empty, *issues]).sort([_KEY, "field", "issue_code"])


def _parse_date(name: str) -> pl.Expr:
    return pl.col(name).str.strptime(pl.Date, DATE_FORMAT, strict=False)


def _centavos(value: str) -> int:
    return int(Decimal(value).quantize(_CENT, rounding=ROUND_HALF_UP) * 100)


def _finalize(
    frame: pl.DataFrame, spec: _SubtypeSpec, request: NormalizeRequest, base: RawManifest
) -> pl.DataFrame:
    derived = [_parse_date(name).alias(name) for name in spec.date_fields]
    if "VALOR_CENTAVOS" in spec.output_schema:
        values = [None if item is None else _centavos(item) for item in frame["VALOR"]]
        derived.append(pl.Series("VALOR_CENTAVOS", values, dtype=pl.Int64))
    frame = frame.with_columns(
        *derived,
        pl.lit(base.manifest_id).alias("_source_manifest_id"),
        pl.lit(base.snapshot_id).alias("_source_snapshot_id"),
        pl.lit(request.source_type.value).alias("_source_type"),
        pl.lit(request.normalized_at.isoformat()).alias("_normalized_at"),
    )
    columns = [pl.col(name).cast(dtype) for name, dtype in spec.output_schema.items()]
    return frame.select(columns).sort(_KEY)


def _write(
    request: NormalizeRequest,
    store: ObjectStorePort,
    output: tuple[str, pl.DataFrame, str],
    base: RawManifest,
) -> OutputManifest:
    target_key, frame, schema_version = output
    stat = persist_verified(store, target_key, serialize_parquet(frame))
    stem = target_key.rsplit("/", 1)[-1].removesuffix(".parquet")
    return OutputManifest(
        manifest_version=1,
        manifest_id=f"normalized-{request.run_id}-{request.unit_id}-{request.attempt}-{stem}",
        tenant_id=request.tenant_id,
        layer="normalized",
        source_type=request.source_type,
        competencia=base.competencia,
        run_id=request.run_id,
        unit_id=request.unit_id,
        attempt=request.attempt,
        schema_version=schema_version,
        object_key=target_key,
        object_sha256=stat.sha256,
        row_count=frame.height,
        created_at=request.normalized_at,
    )
