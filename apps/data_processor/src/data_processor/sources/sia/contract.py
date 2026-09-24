"""Contrato do plugin SIA: layout do catálogo, alvos por folha, qualidade e I/O verificado."""

from __future__ import annotations

import gzip
import json
from dataclasses import dataclass
from hashlib import sha256
from io import BytesIO
from typing import TYPE_CHECKING

import polars as pl

from cnes_contracts.manifests.raw import SourceType
from cnes_domain.orchestration.source_definitions.sia import (
    SIA_DEFINITION,
    SIA_DEPENDENCIES,
    SIA_LAYOUT,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from cnes_contracts.manifests.outputs import OutputManifest
    from cnes_contracts.manifests.processing import (
        MaterializeRequest,
        NormalizeRequest,
        ReconcileRequest,
    )
    from cnes_contracts.manifests.raw import RawManifest
    from cnes_domain.ports.object_store import ObjectStat, ObjectStorePort

__all__ = [
    "PROVENANCE_METADATA_KEY",
    "SIA_DEFINITION",
    "SIA_DEPENDENCIES",
    "SIA_LAYOUT",
    "NormalizeTargets",
    "QualityCheck",
    "SiaContractError",
    "provenance_metadata",
    "read_output",
    "read_raw",
    "resolve_normalize_targets",
    "resolve_reconcile_inputs",
    "resolve_serving_targets",
    "serialize_parquet",
    "split_quality",
    "with_source_row",
    "write_verified",
]

PROVENANCE_METADATA_KEY = "sia_raw_manifest_sha256s"
REJEITADA = "rejeitada"
DUPLICATA = "duplicata"
_LAYOUT_BY_SUBTYPE = {item.file_subtype: item for item in SIA_LAYOUT.normalized}
_NORMALIZED_LEAVES = frozenset(
    name for item in SIA_LAYOUT.normalized for name in item.normalized_filenames
)
_QUALITY_COLUMNS = ("_source_row", "issue_code", "disposicao", "detalhe")
_GZIP_MAGIC = b"\x1f\x8b"


class SiaContractError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class NormalizeTargets:
    subtype: str
    data_key: str
    quality_key: str


@dataclass(frozen=True, slots=True)
class QualityCheck:
    issue_code: str
    predicate: pl.Expr
    detalhe: pl.Expr


def _leaf(key: str) -> str:
    return key.rsplit("/", 1)[-1]


def resolve_normalize_targets(request: NormalizeRequest) -> NormalizeTargets:
    """Valida a unidade SIA (um subtipo FULL) e separa as chaves de dado e qualidade.

    Raises:
        SiaContractError: fonte, subtipo, cadeia ou target_keys fora do layout.
    """
    if request.source_type is not SourceType.SIA_LOCAL:
        raise SiaContractError(f"sia_source_type_invalid source_type={request.source_type.value}")
    subtypes = sorted({manifest.file_subtype for manifest in request.raw_manifests})
    if len(subtypes) != 1:
        raise SiaContractError(f"sia_subtype_mixed subtypes={','.join(subtypes)}")
    layout = _LAYOUT_BY_SUBTYPE.get(subtypes[0])
    if layout is None:
        raise SiaContractError(f"sia_subtype_unknown subtype={subtypes[0]}")
    if len(request.raw_manifests) != 1:
        raise SiaContractError(f"sia_delta_unsupported subtype={layout.file_subtype}")
    data_leaf, quality_leaf = layout.normalized_filenames
    by_leaf = {_leaf(key): key for key in request.target_keys}
    if len(request.target_keys) != 2 or set(by_leaf) != {data_leaf, quality_leaf}:
        raise SiaContractError(f"sia_target_keys_invalid subtype={layout.file_subtype}")
    return NormalizeTargets(layout.file_subtype, by_leaf[data_leaf], by_leaf[quality_leaf])


def resolve_reconcile_inputs(request: ReconcileRequest) -> dict[str, OutputManifest]:
    """Indexa os dez manifests normalizados SIA pela folha do object_key.

    Raises:
        SiaContractError: destino fora do layout, fonte errada ou conjunto incompleto.
    """
    expected_targets = (SIA_LAYOUT.reconciliation_filename, SIA_LAYOUT.divergence_filename)
    if (_leaf(request.reconciliation_key), _leaf(request.divergence_key)) != expected_targets:
        raise SiaContractError("sia_reconcile_targets_invalid")
    foreign = [m.object_key for m in request.normalized_manifests
               if m.source_type is not SourceType.SIA_LOCAL]
    if foreign:
        raise SiaContractError(f"sia_normalized_source_invalid key={foreign[0]}")
    by_leaf = {_leaf(manifest.object_key): manifest for manifest in request.normalized_manifests}
    missing = sorted(_NORMALIZED_LEAVES - set(by_leaf))
    unexpected = sorted(set(by_leaf) - _NORMALIZED_LEAVES)
    if missing or unexpected:
        raise SiaContractError(
            f"sia_normalized_set_invalid missing={','.join(missing)} "
            f"unexpected={','.join(unexpected)}"
        )
    return by_leaf


def resolve_serving_targets(request: MaterializeRequest) -> dict[str, str]:
    """Mapeia cada documento de serving SIA para sua chave.

    Raises:
        SiaContractError: target_keys diferentes dos documentos do layout.
    """
    by_name = {_leaf(key).removesuffix(".json"): key for key in request.target_keys}
    if len(request.target_keys) != len(SIA_LAYOUT.serving_documents) or set(by_name) != set(
        SIA_LAYOUT.serving_documents
    ):
        raise SiaContractError("sia_serving_targets_invalid")
    return by_name


def with_source_row(frame: pl.DataFrame) -> pl.DataFrame:
    """Anexa `_source_row` (posição da linha no raw) como discriminador estável."""
    return frame.with_row_index("_source_row").with_columns(pl.col("_source_row").cast(pl.Int64))


def _first_matching(checks: tuple[QualityCheck, ...], field: str) -> pl.Expr:
    expression = pl.lit(None, dtype=pl.String)
    for check in reversed(checks):
        value = pl.lit(check.issue_code) if field == "issue_code" else check.detalhe
        expression = pl.when(check.predicate).then(value).otherwise(expression)
    return expression


def split_quality(
    frame: pl.DataFrame, checks: tuple[QualityCheck, ...], dedup_keys: tuple[str, ...]
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Separa linhas mantidas das retiradas (rejeitadas pela 1a regra ou duplicatas).

    Returns:
        (linhas mantidas, linhas de qualidade com `_source_row, issue_code, disposicao, detalhe`).
    """
    flagged = frame.with_columns(
        _first_matching(checks, "issue_code").alias("issue_code"),
        _first_matching(checks, "detalhe").cast(pl.String).alias("detalhe"),
    )
    rejected = flagged.filter(pl.col("issue_code").is_not_null()).with_columns(
        pl.lit(REJEITADA).alias("disposicao")
    )
    remaining = flagged.filter(pl.col("issue_code").is_null()).with_columns(
        pl.col("_source_row").min().over(list(dedup_keys)).alias("_first_row")
    )
    is_duplicate = pl.col("_source_row") != pl.col("_first_row")
    duplicates = remaining.filter(is_duplicate).with_columns(
        pl.lit("linha_duplicada").alias("issue_code"),
        pl.lit(DUPLICATA).alias("disposicao"),
        pl.format("primeira_linha={}", "_first_row").alias("detalhe"),
    )
    kept = remaining.filter(~is_duplicate).drop("issue_code", "detalhe", "_first_row")
    quality = pl.concat(
        [rejected.select(_QUALITY_COLUMNS), duplicates.select(_QUALITY_COLUMNS)]
    ).sort("_source_row")
    return kept, quality


def serialize_parquet(frame: pl.DataFrame, metadata: Mapping[str, str]) -> bytes:
    """Serializa o frame com as opções fixas do data plane e metadata key-value."""
    output = BytesIO()
    frame.write_parquet(
        output, compression="zstd", compression_level=3, statistics=True,
        row_group_size=64_000, metadata=dict(metadata),
    )
    return output.getvalue()


def write_verified(store: ObjectStorePort, key: str, payload: bytes) -> ObjectStat:
    """Grava o objeto e confirma por `stat` que o SHA-256 persistido é o esperado.

    Raises:
        SiaContractError: objeto ausente ou com hash divergente após put.
    """
    digest = sha256(payload).hexdigest()
    store.put(key, BytesIO(payload), digest)
    stat = store.stat(key)
    if stat is None:
        raise SiaContractError(f"output_not_found key={key}")
    if stat.sha256 != digest:
        raise SiaContractError(f"output_sha256_mismatch key={key}")
    return stat


def _verified_bytes(store: ObjectStorePort, key: str, expected_sha256: str) -> bytes:
    with store.open(key) as handle:
        payload = handle.read()
    if sha256(payload).hexdigest() != expected_sha256:
        raise SiaContractError(f"input_sha256_mismatch key={key}")
    return payload


def read_raw(store: ObjectStorePort, manifest: RawManifest) -> pl.DataFrame:
    """Lê o Parquet raw declarado no manifest após conferir o SHA-256 do objeto armazenado."""
    payload = _verified_bytes(store, manifest.object_key, manifest.object_sha256)
    if payload[:2] == _GZIP_MAGIC:
        payload = gzip.decompress(payload)
    return pl.read_parquet(BytesIO(payload))


def read_output(
    store: ObjectStorePort, manifest: OutputManifest
) -> tuple[pl.DataFrame, dict[str, str]]:
    """Lê um Parquet de saída verificado e sua metadata key-value."""
    payload = _verified_bytes(store, manifest.object_key, manifest.object_sha256)
    return pl.read_parquet(BytesIO(payload)), pl.read_parquet_metadata(BytesIO(payload))


def provenance_metadata(schema_version: str, raw_hashes: set[str]) -> dict[str, str]:
    """Metadata de proveniência: schema e hashes dos raw manifests contribuintes."""
    return {
        "schema_version": schema_version,
        PROVENANCE_METADATA_KEY: json.dumps(sorted(raw_hashes)),
    }
