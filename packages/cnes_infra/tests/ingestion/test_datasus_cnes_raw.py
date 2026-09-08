"""Testes do adapter raw para CNES nacional."""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta, timezone
from hashlib import sha256
from io import BytesIO
from pathlib import Path
from traceback import format_exception
from typing import Any

import polars as pl
import pyarrow.parquet as pq
import pytest

from cnes_domain.ports.object_store import ObjectStat
from cnes_infra.ingestion import datasus_cnes_transport as transport_module
from cnes_infra.ingestion.datasus_cnes_raw import DatasusCnesRawAdapter
from cnes_infra.ingestion.datasus_cnes_transport import (
    DatasusCnesFtpTransport,
    DatasusCnesRequest,
)

_ROOT = Path(__file__).resolve().parents[4]
_GOLDEN = _ROOT / "docs" / "fixtures" / "data-plane" / "cnes-nacional-v1.parquet"
_DBC_FIXTURE = Path(__file__).parent / "fixtures" / "PFSP2601.dbc"
_CREATED_AT = datetime(2026, 2, 1, tzinfo=UTC)
_COLUMNS = (
    "CPF",
    "CNS",
    "NOME_PROFISSIONAL",
    "NOME_SOCIAL",
    "SEXO",
    "CBO",
    "CNES",
    "TIPO_VINCULO",
    "SUS",
    "CH_TOTAL",
    "CH_AMBULATORIAL",
    "CH_OUTRAS",
    "CH_HOSPITALAR",
    "FONTE",
)


class _Transport:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.rows = rows
        self.requests: list[DatasusCnesRequest] = []

    def fetch(self, request: DatasusCnesRequest):
        self.requests.append(request)
        yield from self.rows


class _Store:
    def __init__(self) -> None:
        self.calls: list[tuple[str, bytes, str]] = []

    def put(self, key: str, body: Any, expected_sha256: str) -> ObjectStat:
        payload = body.read()
        self.calls.append((key, payload, expected_sha256))
        return ObjectStat(key, len(payload), expected_sha256)


def _request(**updates: str) -> DatasusCnesRequest:
    values = {
        "tenant_id": "354130",
        "competencia": "2026-01",
        "file_subtype": "CNES_VINCULO",
        "snapshot_id": "snapshot-1",
        "agent_id": "system-datasus",
        "agent_version": "1.2.3",
    }
    return DatasusCnesRequest(**(values | updates))


def _row(**updates: object) -> dict[str, object]:
    values: dict[str, object] = {
        "CPF_PROF": "90000000001",
        "CNS_PROF": "700000000000001",
        "NOMEPROF": " Profissional Teste ",
        "CBO": "123",
        "CNES": "456",
        "VINCULAC": "12",
        "PROF_SUS": "1",
        "HORAOUTR": 8,
        "HORAHOSP": "20",
        "HORA_AMB": "12",
        "COMPETEN": "202601",
        "CODUFMUN": "354130",
    }
    return values | updates


def _extract(rows: list[dict[str, object]], request: DatasusCnesRequest | None = None):
    store = _Store()
    manifest = DatasusCnesRawAdapter(_Transport(rows), store, lambda: _CREATED_AT).extract(
        request or _request()
    )
    return manifest, store


def _read(store: _Store) -> pl.DataFrame:
    return pl.read_parquet(BytesIO(store.calls[0][1]))


def _error_code(rows: list[dict[str, object]]) -> tuple[str, bool]:
    with pytest.raises(Exception) as captured:
        _extract(rows)
    return captured.value.code, captured.value.retryable


def _replace_dbf_field(payload: bytearray, field_name: str, value: bytes) -> None:
    record_offset = 1
    descriptor_offset = 32
    while payload[descriptor_offset] != 0x0D:
        descriptor = payload[descriptor_offset : descriptor_offset + 32]
        name = bytes(descriptor[:11]).split(b"\x00", 1)[0].decode("ascii")
        length = descriptor[16]
        if name == field_name:
            header_size = int.from_bytes(payload[8:10], "little")
            payload[header_size + record_offset : header_size + record_offset + length] = value
            return
        record_offset += length
        descriptor_offset += 32
    raise AssertionError(field_name)


def test_projeta_schema_padding_documentos_nulos_e_horas():
    manifest, store = _extract(
        [_row(CPF_PROF="99999999999", CNS_PROF="999999999999999", HORAOUTR="")]
    )
    frame = _read(store)

    assert frame.columns == list(_COLUMNS)
    assert frame.schema == {
        **dict.fromkeys(_COLUMNS[:9], pl.String),
        **dict.fromkeys(_COLUMNS[9:13], pl.Int64),
        "FONTE": pl.String,
    }
    assert frame.to_dicts() == [
        {
            "CPF": None,
            "CNS": None,
            "NOME_PROFISSIONAL": "Profissional Teste",
            "NOME_SOCIAL": None,
            "SEXO": None,
            "CBO": "000123",
            "CNES": "0000456",
            "TIPO_VINCULO": "000012",
            "SUS": "S",
            "CH_TOTAL": 32,
            "CH_AMBULATORIAL": 12,
            "CH_OUTRAS": 0,
            "CH_HOSPITALAR": 20,
            "FONTE": "NACIONAL",
        }
    ]
    assert manifest.row_count == 1


def test_preserva_duplicatas_ordena_nulos_por_ultimo_e_mapeia_nao_sus():
    first = _row(CPF_PROF="", CNS_PROF="", NOMEPROF="Zulu", PROF_SUS="0")
    second = _row(CPF_PROF="10000000000", CNS_PROF="100000000000000", NOMEPROF="Alfa")
    _, store = _extract([first, second, second.copy()])

    rows = _read(store).to_dicts()

    assert [row["CPF"] for row in rows] == ["10000000000", "10000000000", None]
    assert rows[0] == rows[1]
    assert rows[2]["SUS"] == "N"


def test_bytes_hash_e_metadados_parquet_sao_deterministicos():
    rows = [_row(CPF_PROF="20000000000"), _row(CPF_PROF="10000000000")]
    first_manifest, first_store = _extract(rows)
    second_manifest, second_store = _extract(list(reversed(rows)))
    first_payload = first_store.calls[0][1]
    metadata = pq.ParquetFile(BytesIO(first_payload)).metadata

    assert first_payload == second_store.calls[0][1]
    assert first_manifest.object_sha256 == second_manifest.object_sha256
    assert first_manifest.object_sha256 == sha256(first_payload).hexdigest()
    assert metadata.created_by == "Polars"
    assert metadata.num_row_groups == 1
    assert all(
        metadata.row_group(0).column(index).compression == "ZSTD"
        and metadata.row_group(0).column(index).statistics is not None
        for index in range(metadata.num_columns)
    )


def test_schema_parquet_preserva_contrato_do_fixture_ratificado():
    _, store = _extract([_row()])
    generated = pq.ParquetFile(BytesIO(store.calls[0][1]))
    golden = pq.ParquetFile(_GOLDEN)

    assert generated.schema_arrow == golden.schema_arrow
    assert generated.metadata.created_by == golden.metadata.created_by == "Polars"


def test_row_groups_tem_no_maximo_64000_linhas():
    _, store = _extract([_row()] * 64_001)
    metadata = pq.ParquetFile(BytesIO(store.calls[0][1])).metadata

    assert metadata.num_row_groups == 2
    assert [metadata.row_group(index).num_rows for index in range(2)] == [64_000, 1]


def test_faz_put_unico_e_retorna_manifesto_full():
    request = _request()
    transport = _Transport([_row()])
    store = _Store()
    manifest = DatasusCnesRawAdapter(transport, store, lambda: _CREATED_AT).extract(request)
    key, payload, digest = store.calls[0]

    assert transport.requests == [request]
    assert len(store.calls) == 1
    assert key == "raw/354130/CNES_NACIONAL/2026-01/snapshot-1/data.parquet"
    assert digest == sha256(payload).hexdigest()
    assert manifest.model_dump() == {
        "manifest_version": 1,
        "manifest_id": "snapshot-1",
        "tenant_id": "354130",
        "source_type": "CNES_NACIONAL",
        "file_subtype": "CNES_VINCULO",
        "competencia": "2026-01",
        "agent_id": "system-datasus",
        "agent_version": "1.2.3",
        "schema_version": "cnes-profissional-v1",
        "snapshot_mode": "FULL",
        "snapshot_id": "snapshot-1",
        "base_snapshot_id": None,
        "sequence": 1,
        "previous_manifest_sha256": None,
        "object_sha256": digest,
        "row_count": 1,
        "size_bytes": len(payload),
        "object_key": key,
        "created_at": _CREATED_AT,
    }


@pytest.mark.parametrize(
    ("updates", "code"),
    [
        ({"COMPETEN": "202512", "CODUFMUN": "330455"}, "transport_contract_invalid"),
        ({"CODUFMUN": "330455"}, "transport_contract_invalid"),
    ],
)
def test_revalida_competencia_e_municipio_do_transport_fake(
    updates: dict[str, object], code: str
):
    assert _error_code([_row(**updates)]) == (code, False)


@pytest.mark.parametrize(
    "updates",
    [
        {"CPF_PROF": "123"},
        {"CPF_PROF": "²" * 11},
        {"CNS_PROF": "123"},
        {"CBO": "ABC"},
        {"CBO": "00000²"},
        {"CBO": "1234567"},
        {"CNES": "12345678"},
        {"VINCULAC": "1234567"},
        {"PROF_SUS": ""},
        {"PROF_SUS": "2"},
        {"HORAOUTR": "1.5"},
        {"HORAHOSP": -1},
        {"HORA_AMB": "ABC"},
        {"HORA_AMB": "²"},
    ],
)
def test_rejeita_campos_invalidos_com_falha_final(updates: dict[str, object]):
    assert _error_code([_row(**updates)]) == ("field_invalid", False)


def test_erro_e_logs_nao_expoem_dados_pessoais(caplog: pytest.LogCaptureFixture):
    cpf = "12345678901"
    cns = "123456789012345"
    nome = "NOME MUITO SENSIVEL"
    caplog.set_level(logging.DEBUG)

    with pytest.raises(Exception) as captured:
        _extract([_row(CPF_PROF=cpf, CNS_PROF=cns, NOMEPROF=nome, PROF_SUS="x")])

    exposed = "".join(format_exception(captured.value)) + caplog.text
    assert cpf not in exposed
    assert cns not in exposed
    assert nome not in exposed


def test_rejeita_transport_sem_linhas_como_nao_publicado():
    assert _error_code([]) == ("source_not_published", True)


@pytest.mark.parametrize(
    "updates",
    [
        {"NOMEPROF": None},
        {"HORAOUTR": True},
        {"HORAOUTR": 1.0},
    ],
)
def test_valida_tipos_de_campos_do_dbf(updates: dict[str, object]):
    if isinstance(updates.get("HORAOUTR"), float):
        _, store = _extract([_row(**updates)])
        assert _read(store)["CH_OUTRAS"].to_list() == [1]
        return
    assert _error_code([_row(**updates)]) == ("field_invalid", False)


def test_rejeita_campos_ausentes():
    missing_competencia = _row()
    missing_competencia.pop("COMPETEN")
    missing_hours = _row()
    missing_hours.pop("HORAOUTR")

    assert _error_code([missing_competencia]) == ("transport_contract_invalid", False)
    assert _error_code([missing_hours]) == ("field_invalid", False)


def test_rejeita_snapshot_inseguro_sem_put():
    store = _Store()
    adapter = DatasusCnesRawAdapter(_Transport([_row()]), store, lambda: _CREATED_AT)

    with pytest.raises(Exception) as captured:
        adapter.extract(_request(snapshot_id="bad/path"))

    assert (captured.value.code, captured.value.retryable) == ("request_invalid", False)
    assert store.calls == []


def test_valida_manifesto_antes_do_put():
    store = _Store()

    def invalid_clock() -> datetime:
        return datetime(2026, 2, 1, tzinfo=timezone(timedelta(hours=-3)))

    adapter = DatasusCnesRawAdapter(_Transport([_row()]), store, invalid_clock)

    with pytest.raises(Exception):
        adapter.extract(_request())

    assert store.calls == []


def test_fecha_iterador_do_transport_quando_projecao_falha():
    closed = False

    def rows():
        nonlocal closed
        try:
            yield _row(PROF_SUS="invalid")
        finally:
            closed = True

    retained = rows()
    transport = _Transport([])
    transport.fetch = lambda request: retained

    with pytest.raises(Exception):
        DatasusCnesRawAdapter(transport, _Store(), lambda: _CREATED_AT).extract(_request())

    assert closed is True


def test_aceita_iterador_do_transport_sem_close():
    transport = _Transport([])
    transport.fetch = lambda request: iter([_row()])

    manifest = DatasusCnesRawAdapter(transport, _Store(), lambda: _CREATED_AT).extract(_request())

    assert manifest.row_count == 1


@pytest.mark.parametrize("payload", [b"\x00" * 10, b"\x00" * 1313, None])
def test_rejeita_dbc_ausente_ou_com_cabecalho_malformado_sem_put(
    monkeypatch: pytest.MonkeyPatch, payload: bytes | None
):
    store = _Store()
    transport = DatasusCnesFtpTransport()

    def download(_remote: str, destination: Path) -> None:
        if payload is not None:
            destination.write_bytes(payload)

    monkeypatch.setattr(transport, "_download_protected", download)
    adapter = DatasusCnesRawAdapter(transport, store, lambda: _CREATED_AT)

    with pytest.raises(BaseException) as captured:
        adapter.extract(_request())

    assert isinstance(captured.value, Exception)
    assert (captured.value.code, captured.value.retryable) == ("dbc_invalid", False)
    assert store.calls == []


@pytest.mark.parametrize("mutation", ["truncated", "invalid_eof", "invalid_shape"])
def test_rejeita_dbf_fisicamente_invalido_sem_put(
    monkeypatch: pytest.MonkeyPatch, mutation: str
):
    store = _Store()
    transport = DatasusCnesFtpTransport()
    real_decompress = transport_module.decompress

    def download(_remote: str, destination: Path) -> None:
        destination.write_bytes(_DBC_FIXTURE.read_bytes())

    def truncate(source: str, destination: str) -> None:
        real_decompress(source, destination)
        payload = bytearray(Path(destination).read_bytes())
        if mutation == "truncated":
            payload[4:8] = (2).to_bytes(4, "little")
        elif mutation == "invalid_eof":
            payload[-1] = 0
        else:
            payload[10:12] = (1).to_bytes(2, "little")
        Path(destination).write_bytes(payload)

    monkeypatch.setattr(transport, "_download_protected", download)
    monkeypatch.setattr(transport_module, "decompress", truncate)
    adapter = DatasusCnesRawAdapter(transport, store, lambda: _CREATED_AT)

    with pytest.raises(Exception) as captured:
        adapter.extract(_request())

    assert (captured.value.code, captured.value.retryable) == ("dbf_invalid", False)
    assert store.calls == []


def test_preserva_cancelamento_do_descompressor(monkeypatch: pytest.MonkeyPatch):
    store = _Store()
    transport = DatasusCnesFtpTransport()

    def download(_remote: str, destination: Path) -> None:
        destination.write_bytes(_DBC_FIXTURE.read_bytes())

    def cancel(_source: str, _destination: str) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(transport, "_download_protected", download)
    monkeypatch.setattr(transport_module, "decompress", cancel)

    with pytest.raises(KeyboardInterrupt):
        DatasusCnesRawAdapter(transport, store, lambda: _CREATED_AT).extract(_request())

    assert store.calls == []


def test_rejeita_overflow_numerico_do_dbf_como_campo_invalido(
    monkeypatch: pytest.MonkeyPatch,
):
    store = _Store()
    transport = DatasusCnesFtpTransport()
    real_decompress = transport_module.decompress

    def download(_remote: str, destination: Path) -> None:
        destination.write_bytes(_DBC_FIXTURE.read_bytes())

    def inject_overflow(source: str, destination: str) -> None:
        real_decompress(source, destination)
        payload = bytearray(Path(destination).read_bytes())
        _replace_dbf_field(payload, "HORAOUTR", b"***")
        Path(destination).write_bytes(payload)

    monkeypatch.setattr(transport, "_download_protected", download)
    monkeypatch.setattr(transport_module, "decompress", inject_overflow)

    with pytest.raises(Exception) as captured:
        DatasusCnesRawAdapter(transport, store, lambda: _CREATED_AT).extract(_request())

    assert (captured.value.code, captured.value.retryable) == ("field_invalid", False)
    assert store.calls == []
