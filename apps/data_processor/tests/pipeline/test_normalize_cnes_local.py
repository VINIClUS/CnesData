"""TDD de normalize_cnes_local: replay da cadeia raw + schema cnes-normalized-v1."""

from __future__ import annotations

import hashlib
from contextlib import nullcontext
from dataclasses import dataclass, field
from datetime import UTC, datetime
from io import BytesIO
from typing import TYPE_CHECKING

import polars as pl
import pytest

from cnes_contracts.manifests.processing import NormalizeRequest
from cnes_contracts.manifests.raw import RawManifest, SnapshotMode, SourceType
from cnes_contracts.manifests.validation import manifest_sha256
from cnes_domain.ports.object_store import ObjectStat
from data_processor.pipeline.normalize_cnes_local import (
    _NORMALIZED_COLUMNS,
    _SCHEMA_VERSION,
    normalize_cnes_local,
)

if TYPE_CHECKING:
    from collections.abc import BinaryIO
    from contextlib import AbstractContextManager as ContextManager

_TENANT = "354130"
_COMPETENCIA = "2026-01"
_RUN_ID = "run-1"
_UNIT_ID = "unit-1"
_NOW = datetime(2026, 1, 15, 12, tzinfo=UTC)
_TARGET_KEY = f"normalized/{_TENANT}/CNES_LOCAL/{_COMPETENCIA}/{_RUN_ID}/cnes_local.parquet"


@dataclass
class _FakeObjectStore:
    objects: dict[str, bytes] = field(default_factory=dict)

    def put(self, key: str, body: BinaryIO, expected_sha256: str) -> ObjectStat:
        data = body.read()
        digest = hashlib.sha256(data).hexdigest()
        if digest != expected_sha256:
            raise ValueError(f"sha256_mismatch key={key}")
        self.objects[key] = data
        return ObjectStat(key=key, size_bytes=len(data), sha256=digest)

    def open(self, key: str) -> ContextManager[BinaryIO]:
        return nullcontext(BytesIO(self.objects[key]))

    def stat(self, key: str) -> ObjectStat | None:
        data = self.objects.get(key)
        if data is None:
            return None
        return ObjectStat(key=key, size_bytes=len(data), sha256=hashlib.sha256(data).hexdigest())

    def delete(self, key: str) -> None:
        self.objects.pop(key, None)

    def promote(self, source_key: str, destination_key: str, expected_sha256: str) -> ObjectStat:
        data = self.objects.pop(source_key)
        self.objects[destination_key] = data
        return ObjectStat(key=destination_key, size_bytes=len(data), sha256=expected_sha256)


@dataclass
class _BlindStatStore(_FakeObjectStore):
    def stat(self, key: str) -> ObjectStat | None:
        return None


def _row(cpf: str, cns: str | None, cnes: str = "0000001", cbo: str = "000001",
         nome: str = "Ana", **overrides: object) -> dict[str, object]:
    base = {
        "CPF": cpf, "CNS": cns, "NOME_PROFISSIONAL": nome, "NOME_SOCIAL": None,
        "SEXO": "F", "CBO": cbo, "CNES": cnes, "TIPO_VINCULO": "1", "SUS": "S",
        "CH_TOTAL": 40, "CH_AMBULATORIAL": 40, "CH_OUTRAS": 0, "CH_HOSPITALAR": 0,
        "FONTE": "LOCAL",
    }
    base.update(overrides)
    return base


def _serialize(rows: list[dict[str, object]]) -> bytes:
    output = BytesIO()
    pl.DataFrame(rows).write_parquet(output, compression="zstd", compression_level=3)
    return output.getvalue()


def _put_raw(store: _FakeObjectStore, key: str, rows: list[dict[str, object]]) -> str:
    payload = _serialize(rows)
    digest = hashlib.sha256(payload).hexdigest()
    store.put(key, BytesIO(payload), digest)
    return digest


def _raw_manifest(
    snapshot_id: str, sequence: int, object_key: str, digest: str,
    row_count: int, previous: RawManifest | None = None,
) -> RawManifest:
    mode = SnapshotMode.FULL if sequence == 1 else SnapshotMode.DELTA
    return RawManifest(
        manifest_version=1,
        manifest_id=f"cnes-local-{snapshot_id}",
        tenant_id=_TENANT,
        source_type=SourceType.CNES_LOCAL,
        file_subtype="ST",
        competencia=_COMPETENCIA,
        agent_id="agent-1",
        agent_version="1",
        schema_version="cnes-local-raw-v1",
        snapshot_mode=mode,
        snapshot_id=snapshot_id,
        base_snapshot_id=None if sequence == 1 else "full-1",
        sequence=sequence,
        previous_manifest_sha256=None if previous is None else manifest_sha256(previous),
        object_sha256=digest,
        row_count=row_count,
        size_bytes=1,
        object_key=object_key,
        created_at=_NOW,
    )


def _full_manifest(store: _FakeObjectStore, rows: list[dict[str, object]]) -> RawManifest:
    key = f"raw/{_TENANT}/CNES_LOCAL/{_COMPETENCIA}/full-1/data.parquet"
    digest = _put_raw(store, key, rows)
    return _raw_manifest("full-1", 1, key, digest, len(rows))


def _delta_manifest(
    store: _FakeObjectStore, snapshot_id: str, sequence: int,
    rows: list[dict[str, object]], previous: RawManifest,
) -> RawManifest:
    key = f"raw/{_TENANT}/CNES_LOCAL/{_COMPETENCIA}/{snapshot_id}/data.parquet"
    digest = _put_raw(store, key, rows)
    return _raw_manifest(snapshot_id, sequence, key, digest, len(rows), previous=previous)


def _request(raw_manifests: tuple[RawManifest, ...], attempt: int = 1) -> NormalizeRequest:
    return NormalizeRequest(
        tenant_id=_TENANT,
        run_id=_RUN_ID,
        unit_id=_UNIT_ID,
        attempt=attempt,
        source_type=SourceType.CNES_LOCAL,
        raw_manifests=raw_manifests,
        target_keys=(_TARGET_KEY,),
        normalized_at=_NOW,
    )


def test_rejeita_mais_de_um_target_key() -> None:
    store = _FakeObjectStore()
    full = _full_manifest(store, [_row("11111111111", "111111111111111")])
    request = NormalizeRequest(
        tenant_id=_TENANT, run_id=_RUN_ID, unit_id=_UNIT_ID, attempt=1,
        source_type=SourceType.CNES_LOCAL, raw_manifests=(full,),
        target_keys=(_TARGET_KEY, f"{_TARGET_KEY}.bak"), normalized_at=_NOW,
    )

    with pytest.raises(ValueError, match="target_keys_must_be_single"):
        normalize_cnes_local(request, store)


def test_deltas_fora_de_ordem_sao_aplicados_por_sequence() -> None:
    store = _FakeObjectStore()
    full = _full_manifest(
        store,
        [
            _row("11111111111", "111111111111111", nome="Ana"),
            _row("22222222222", "222222222222222", nome="Bruno"),
        ],
    )
    delta_1 = _delta_manifest(
        store, "delta-1", 2,
        [{**_row("22222222222", "222222222222222", nome="Bruno V1"), "_op": "U"}],
        previous=full,
    )
    # delta_2 omits NOME_SOCIAL entirely: exercises _materialize_missing_columns
    row_v2 = _row("22222222222", "222222222222222", nome="Bruno V2")
    del row_v2["NOME_SOCIAL"]
    delta_2 = _delta_manifest(
        store, "delta-2", 3, [{**row_v2, "_op": "U"}], previous=delta_1,
    )
    request = _request((full, delta_2, delta_1))  # shuffled on purpose

    result = normalize_cnes_local(request, store)

    frame = pl.read_parquet(BytesIO(store.objects[_TARGET_KEY]))
    updated = frame.filter(pl.col("CPF") == "22222222222").to_dicts()[0]
    assert updated["NOME_PROFISSIONAL"] == "Bruno V2"
    assert updated["NOME_SOCIAL"] is None
    assert result.manifests[0].row_count == 2


def test_colunas_de_provenance_nao_mutam_frame_de_entrada() -> None:
    store = _FakeObjectStore()
    rows = [_row("11111111111", "111111111111111")]
    full = _full_manifest(store, rows)
    raw_bytes_before = bytes(store.objects[full.object_key])
    request = _request((full,))

    normalize_cnes_local(request, store)

    assert store.objects[full.object_key] == raw_bytes_before


def test_gera_manifesto_normalizado_com_18_colunas_na_ordem_cpf_primeiro() -> None:
    store = _FakeObjectStore()
    full = _full_manifest(store, [_row("11111111111", "111111111111111")])
    request = _request((full,))

    result = normalize_cnes_local(request, store)

    assert len(result.manifests) == 1
    manifest = result.manifests[0]
    assert manifest.schema_version == _SCHEMA_VERSION
    assert manifest.layer == "normalized"
    frame = pl.read_parquet(BytesIO(store.objects[_TARGET_KEY]))
    assert tuple(frame.columns) == _NORMALIZED_COLUMNS
    assert set(frame["_source_type"].to_list()) == {"CNES_LOCAL"}
    assert set(frame["FONTE"].to_list()) == {"LOCAL"}


def test_saida_ordenada_por_chave_natural_com_nulls_last() -> None:
    store = _FakeObjectStore()
    full = _full_manifest(
        store,
        [
            _row("11111111111", None, cbo="000002", nome="SemCns"),
            _row("11111111111", "222222222222222", cbo="000001", nome="ComCns"),
        ],
    )
    request = _request((full,))

    normalize_cnes_local(request, store)

    frame = pl.read_parquet(BytesIO(store.objects[_TARGET_KEY]))
    assert frame["NOME_PROFISSIONAL"].to_list() == ["ComCns", "SemCns"]


def test_execucoes_repetidas_produzem_o_mesmo_sha256() -> None:
    store_a = _FakeObjectStore()
    store_b = _FakeObjectStore()
    rows = [_row("11111111111", "111111111111111"), _row("22222222222", "222222222222222")]
    full_a = _full_manifest(store_a, rows)
    full_b = _full_manifest(store_b, rows)

    result_a = normalize_cnes_local(_request((full_a,)), store_a)
    result_b = normalize_cnes_local(_request((full_b,)), store_b)

    assert result_a.manifests[0].object_sha256 == result_b.manifests[0].object_sha256


def test_falha_quando_objeto_escrito_nao_e_encontrado() -> None:
    store = _BlindStatStore()
    full = _full_manifest(store, [_row("11111111111", "111111111111111")])
    request = _request((full,))

    with pytest.raises(ValueError, match="output_not_found"):
        normalize_cnes_local(request, store)
