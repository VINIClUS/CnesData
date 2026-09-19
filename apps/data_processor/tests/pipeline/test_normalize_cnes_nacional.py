"""TDD de normalize_cnes_nacional: manifesto FULL unico -> schema cnes-normalized-v1."""

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
from data_processor.pipeline.normalize_cnes_local import normalize_cnes_local
from data_processor.pipeline.normalize_cnes_nacional import (
    _NORMALIZED_COLUMNS,
    _SCHEMA_VERSION,
    normalize_cnes_nacional,
)

if TYPE_CHECKING:
    from collections.abc import BinaryIO
    from contextlib import AbstractContextManager as ContextManager

_TENANT = "354130"
_COMPETENCIA = "2026-01"
_RUN_ID = "run-1"
_UNIT_ID = "unit-1"
_NOW = datetime(2026, 1, 15, 12, tzinfo=UTC)
_TARGET_KEY = f"normalized/{_TENANT}/CNES_NACIONAL/{_COMPETENCIA}/{_RUN_ID}/cnes_nacional.parquet"
_SCHEMA_FONTE: dict[str, type[pl.DataType]] = {
    "CPF": pl.String, "CNS": pl.String, "NOME_PROFISSIONAL": pl.String,
    "NOME_SOCIAL": pl.String, "SEXO": pl.String, "CBO": pl.String, "CNES": pl.String,
    "TIPO_VINCULO": pl.String, "SUS": pl.String, "CH_TOTAL": pl.Int64,
    "CH_AMBULATORIAL": pl.Int64, "CH_OUTRAS": pl.Int64, "CH_HOSPITALAR": pl.Int64,
    "FONTE": pl.String,
}


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
        "SEXO": None, "CBO": cbo, "CNES": cnes, "TIPO_VINCULO": "1", "SUS": "S",
        "CH_TOTAL": 40, "CH_AMBULATORIAL": 40, "CH_OUTRAS": 0, "CH_HOSPITALAR": 0,
        "FONTE": "NACIONAL",
    }
    base.update(overrides)
    return base


def _serialize(rows: list[dict[str, object]]) -> bytes:
    output = BytesIO()
    frame = pl.DataFrame(rows, schema=_SCHEMA_FONTE)
    frame.write_parquet(output, compression="zstd", compression_level=3)
    return output.getvalue()


def _put_raw(store: _FakeObjectStore, key: str, rows: list[dict[str, object]]) -> str:
    payload = _serialize(rows)
    digest = hashlib.sha256(payload).hexdigest()
    store.put(key, BytesIO(payload), digest)
    return digest


def _raw_manifest(
    snapshot_id: str, object_key: str, digest: str, row_count: int,
    source_type: SourceType = SourceType.CNES_NACIONAL,
    snapshot_mode: SnapshotMode = SnapshotMode.FULL,
    sequence: int = 1, competencia: str = _COMPETENCIA,
    base_snapshot_id: str | None = None, previous_manifest_sha256: str | None = None,
) -> RawManifest:
    return RawManifest(
        manifest_version=1,
        manifest_id=f"cnes-nacional-{snapshot_id}",
        tenant_id=_TENANT,
        source_type=source_type,
        file_subtype="PF",
        competencia=competencia,
        agent_id="agent-1",
        agent_version="1",
        schema_version="cnes-profissional-v1",
        snapshot_mode=snapshot_mode,
        snapshot_id=snapshot_id,
        base_snapshot_id=base_snapshot_id,
        sequence=sequence,
        previous_manifest_sha256=previous_manifest_sha256,
        object_sha256=digest,
        row_count=row_count,
        size_bytes=1,
        object_key=object_key,
        created_at=_NOW,
    )


def _full_manifest(store: _FakeObjectStore, rows: list[dict[str, object]]) -> RawManifest:
    key = f"raw/{_TENANT}/CNES_NACIONAL/{_COMPETENCIA}/full-1/data.parquet"
    digest = _put_raw(store, key, rows)
    return _raw_manifest("full-1", key, digest, len(rows))


def _request(raw_manifests: tuple[RawManifest, ...], attempt: int = 1) -> NormalizeRequest:
    return NormalizeRequest(
        tenant_id=_TENANT,
        run_id=_RUN_ID,
        unit_id=_UNIT_ID,
        attempt=attempt,
        source_type=SourceType.CNES_NACIONAL,
        raw_manifests=raw_manifests,
        target_keys=(_TARGET_KEY,),
        normalized_at=_NOW,
    )


def test_normaliza_manifesto_nacional_unico_para_schema_compartilhado() -> None:
    store = _FakeObjectStore()
    rows = [_row("11111111111", "111111111111111"), _row("22222222222", "222222222222222")]
    full = _full_manifest(store, rows)
    request = _request((full,))

    result = normalize_cnes_nacional(request, store)

    assert len(result.manifests) == 1
    manifest = result.manifests[0]
    assert manifest.schema_version == _SCHEMA_VERSION
    assert manifest.layer == "normalized"
    assert manifest.source_type == SourceType.CNES_NACIONAL
    assert manifest.competencia == full.competencia
    assert manifest.row_count == len(rows)
    frame = pl.read_parquet(BytesIO(store.objects[_TARGET_KEY]))
    assert tuple(frame.columns) == _NORMALIZED_COLUMNS
    assert set(frame["_source_type"].to_list()) == {"CNES_NACIONAL"}
    assert set(frame["FONTE"].to_list()) == {"NACIONAL"}
    payload = store.objects[_TARGET_KEY]
    assert manifest.object_sha256 == hashlib.sha256(payload).hexdigest()


def test_rejeita_cadeia_full_delta() -> None:
    store = _FakeObjectStore()
    full = _full_manifest(store, [_row("11111111111", "111111111111111")])
    delta_key = f"raw/{_TENANT}/CNES_NACIONAL/{_COMPETENCIA}/delta-1/data.parquet"
    digest = _put_raw(store, delta_key, [_row("22222222222", "222222222222222")])
    delta = _raw_manifest(
        "delta-1", delta_key, digest, 1, snapshot_mode=SnapshotMode.DELTA, sequence=2,
        base_snapshot_id=full.snapshot_id, previous_manifest_sha256=manifest_sha256(full),
    )
    request = _request((full, delta))

    with pytest.raises(ValueError, match="unexpected_raw_manifest_shape"):
        normalize_cnes_nacional(request, store)


def test_rejeita_manifesto_unico_delta() -> None:
    store = _FakeObjectStore()
    key = f"raw/{_TENANT}/CNES_NACIONAL/{_COMPETENCIA}/delta-1/data.parquet"
    digest = _put_raw(store, key, [_row("11111111111", "111111111111111")])
    delta = _raw_manifest(
        "delta-1", key, digest, 1, snapshot_mode=SnapshotMode.DELTA, sequence=2,
        base_snapshot_id="full-1", previous_manifest_sha256="a" * 64,
    )
    request = NormalizeRequest.model_construct(
        tenant_id=_TENANT, run_id=_RUN_ID, unit_id=_UNIT_ID, attempt=1,
        source_type=SourceType.CNES_NACIONAL, raw_manifests=(delta,),
        target_keys=(_TARGET_KEY,), normalized_at=_NOW,
    )

    with pytest.raises(ValueError, match="unexpected_raw_manifest_shape"):
        normalize_cnes_nacional(request, store)


def test_rejeita_source_type_diferente_de_nacional() -> None:
    store = _FakeObjectStore()
    key = f"raw/{_TENANT}/CNES_LOCAL/{_COMPETENCIA}/full-1/data.parquet"
    digest = _put_raw(store, key, [_row("11111111111", "111111111111111")])
    full = _raw_manifest("full-1", key, digest, 1, source_type=SourceType.CNES_LOCAL)
    request = NormalizeRequest.model_construct(
        tenant_id=_TENANT, run_id=_RUN_ID, unit_id=_UNIT_ID, attempt=1,
        source_type=SourceType.CNES_NACIONAL, raw_manifests=(full,),
        target_keys=(_TARGET_KEY,), normalized_at=_NOW,
    )

    with pytest.raises(ValueError, match="unexpected_raw_manifest_shape"):
        normalize_cnes_nacional(request, store)


def test_preserva_nulos_da_fonte_sem_inventar_valor() -> None:
    store = _FakeObjectStore()
    full = _full_manifest(store, [_row("11111111111", "111111111111111")])
    request = _request((full,))

    normalize_cnes_nacional(request, store)

    frame = pl.read_parquet(BytesIO(store.objects[_TARGET_KEY]))
    row = frame.to_dicts()[0]
    assert row["NOME_SOCIAL"] is None
    assert row["SEXO"] is None


def test_ordenacao_e_schema_identicos_a_normalizacao_local() -> None:
    nacional_store = _FakeObjectStore()
    full_nacional = _full_manifest(
        nacional_store,
        [
            _row("11111111111", None, cbo="000002", nome="SemCns"),
            _row("11111111111", "222222222222222", cbo="000001", nome="ComCns"),
        ],
    )
    nacional_request = _request((full_nacional,))
    normalize_cnes_nacional(nacional_request, nacional_store)
    nacional_frame = pl.read_parquet(BytesIO(nacional_store.objects[_TARGET_KEY]))

    local_store = _FakeObjectStore()
    local_key = f"raw/{_TENANT}/CNES_LOCAL/{_COMPETENCIA}/full-1/data.parquet"
    local_rows = [
        _row("11111111111", None, cbo="000002", nome="SemCns", FONTE="LOCAL"),
        _row("11111111111", "222222222222222", cbo="000001", nome="ComCns", FONTE="LOCAL"),
    ]
    local_digest = _put_raw(local_store, local_key, local_rows)
    local_manifest = RawManifest(
        manifest_version=1, manifest_id="cnes-local-full-1", tenant_id=_TENANT,
        source_type=SourceType.CNES_LOCAL, file_subtype="ST", competencia=_COMPETENCIA,
        agent_id="agent-1", agent_version="1", schema_version="cnes-local-raw-v1",
        snapshot_mode=SnapshotMode.FULL, snapshot_id="full-1", base_snapshot_id=None,
        sequence=1, previous_manifest_sha256=None, object_sha256=local_digest,
        row_count=len(local_rows), size_bytes=1, object_key=local_key, created_at=_NOW,
    )
    local_target_key = (
        f"normalized/{_TENANT}/CNES_LOCAL/{_COMPETENCIA}/{_RUN_ID}/cnes_local.parquet"
    )
    local_request = NormalizeRequest(
        tenant_id=_TENANT, run_id=_RUN_ID, unit_id=_UNIT_ID, attempt=1,
        source_type=SourceType.CNES_LOCAL, raw_manifests=(local_manifest,),
        target_keys=(local_target_key,), normalized_at=_NOW,
    )
    normalize_cnes_local(local_request, local_store)
    local_frame = pl.read_parquet(BytesIO(local_store.objects[local_target_key]))

    assert nacional_frame.columns == local_frame.columns
    assert nacional_frame.schema == local_frame.schema
    key_columns = ["CPF", "CNS", "CNES", "CBO"]
    nacional_keys = nacional_frame.select(key_columns).to_dicts()
    local_keys = local_frame.select(key_columns).to_dicts()
    assert nacional_keys == local_keys
    assert nacional_frame["NOME_PROFISSIONAL"].to_list() == ["ComCns", "SemCns"]


def test_rejeita_mais_de_um_target_key() -> None:
    store = _FakeObjectStore()
    full = _full_manifest(store, [_row("11111111111", "111111111111111")])
    request = NormalizeRequest(
        tenant_id=_TENANT, run_id=_RUN_ID, unit_id=_UNIT_ID, attempt=1,
        source_type=SourceType.CNES_NACIONAL, raw_manifests=(full,),
        target_keys=(_TARGET_KEY, f"{_TARGET_KEY}.bak"), normalized_at=_NOW,
    )

    with pytest.raises(ValueError, match="target_keys_must_be_single"):
        normalize_cnes_nacional(request, store)


def test_falha_quando_objeto_escrito_nao_e_encontrado() -> None:
    store = _BlindStatStore()
    full = _full_manifest(store, [_row("11111111111", "111111111111111")])
    request = _request((full,))

    with pytest.raises(ValueError, match="output_not_found"):
        normalize_cnes_nacional(request, store)


def test_execucoes_repetidas_produzem_o_mesmo_sha256() -> None:
    store_a = _FakeObjectStore()
    store_b = _FakeObjectStore()
    rows = [_row("11111111111", "111111111111111"), _row("22222222222", "222222222222222")]
    full_a = _full_manifest(store_a, rows)
    full_b = _full_manifest(store_b, rows)

    result_a = normalize_cnes_nacional(_request((full_a,)), store_a)
    result_b = normalize_cnes_nacional(_request((full_b,)), store_b)

    assert result_a.manifests[0].object_sha256 == result_b.manifests[0].object_sha256
