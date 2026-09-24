"""Contrato do plugin BPA: constantes do catálogo e I/O verificado de artefatos."""

from __future__ import annotations

from hashlib import sha256
from io import BytesIO
from typing import TYPE_CHECKING

import polars as pl

from cnes_domain.orchestration.source_definitions.bpa import (
    BPA_DEFINITION,
    BPA_DEPENDENCIES,
    BPA_LAYOUT,
)

if TYPE_CHECKING:
    from cnes_contracts.manifests.outputs import OutputManifest
    from cnes_contracts.manifests.raw import RawManifest
    from cnes_domain.ports.object_store import ObjectStat, ObjectStorePort

DATA_FILENAMES = tuple(item.normalized_filenames[0] for item in BPA_LAYOUT.normalized)
QUALITY_FILENAMES = tuple(item.normalized_filenames[1] for item in BPA_LAYOUT.normalized)
PROVENANCE_SCHEMA: dict[str, type[pl.DataType]] = {
    "_source_manifest_id": pl.String, "_source_snapshot_id": pl.String,
    "_source_type": pl.String, "_normalized_at": pl.String,
}


def leaf(key: str) -> str:
    return key.rsplit("/", 1)[-1]


def read_parquet(store: ObjectStorePort, manifest: RawManifest | OutputManifest) -> pl.DataFrame:
    """Lê o Parquet do manifest só depois de conferir o SHA-256 dos bytes.

    Args:
        store: porta de objetos.
        manifest: manifest com `object_key` e `object_sha256` esperados.

    Returns:
        DataFrame lido dos bytes verificados.

    Raises:
        ValueError: bytes divergentes do `object_sha256` do manifest.
    """
    with store.open(manifest.object_key) as handle:
        payload = handle.read()
    if sha256(payload).hexdigest() != manifest.object_sha256:
        raise ValueError(f"input_sha256_mismatch key={manifest.object_key}")
    return pl.read_parquet(BytesIO(payload))


def serialize_parquet(frame: pl.DataFrame) -> bytes:
    output = BytesIO()
    frame.write_parquet(
        output, compression="zstd", compression_level=3, statistics=True, row_group_size=64_000,
    )
    return output.getvalue()


def persist(store: ObjectStorePort, key: str, payload: bytes) -> ObjectStat:
    """Grava o artefato e só o aceita após reler stat com o mesmo SHA-256.

    Args:
        store: porta de objetos.
        key: chave de destino.
        payload: bytes do artefato.

    Returns:
        ObjectStat relido do store.

    Raises:
        ValueError: objeto ausente após put ou SHA-256 divergente.
    """
    digest = sha256(payload).hexdigest()
    store.put(key, BytesIO(payload), digest)
    stat = store.stat(key)
    if stat is None:
        raise ValueError(f"output_not_found key={key}")
    if stat.sha256 != digest:
        raise ValueError(f"output_sha256_mismatch key={key}")
    return stat


__all__ = [
    "BPA_DEFINITION",
    "BPA_DEPENDENCIES",
    "BPA_LAYOUT",
    "DATA_FILENAMES",
    "PROVENANCE_SCHEMA",
    "QUALITY_FILENAMES",
    "leaf",
    "persist",
    "read_parquet",
    "serialize_parquet",
]
