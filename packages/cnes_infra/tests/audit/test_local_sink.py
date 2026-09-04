"""Conformidade do sink de auditoria local."""

from __future__ import annotations

import json
import multiprocessing
import sqlite3
from hashlib import sha256
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow.parquet as pq
import pytest

from cnes_domain.control_plane.errors import Conflict
from cnes_infra.audit.local_sink import LocalAuditSink
from packages.cnes_infra.tests.contracts.audit_sink_contract import (
    AuditSinkCase,
    StoredAuditEvent,
    audit_event,
    audit_sink_cases,
    canonical_body,
)

if TYPE_CHECKING:
    from cnes_domain.control_plane.entities import OutboxEvent


class _LocalProbe:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.sink = _FaultSink(root)

    def expected_location(self, event: OutboxEvent) -> str:
        return f"audit/{event.tenant_id}/2026/07/15/events.jsonl"

    def stored(self) -> tuple[StoredAuditEvent, ...]:
        database = sqlite3.connect(self.root / "audit" / "index.sqlite3")
        rows = database.execute(
            "SELECT log_path, offset, length, sha256 FROM events ORDER BY log_path, offset"
        ).fetchall()
        database.close()
        result = []
        for log_path, offset, length, digest in rows:
            with (self.root / log_path).open("rb") as stream:
                stream.seek(offset)
                body = stream.read(length).removesuffix(b"\n")
            result.append(StoredAuditEvent(log_path, body, digest))
        return tuple(result)

    def fail_next(self) -> None:
        self.sink.boundary = "after_file_write"


class _FaultSink(LocalAuditSink):
    def __init__(self, root: Path, parquet_batch_size: int = 1000) -> None:
        self.boundary: str | None = None
        super().__init__(root, parquet_batch_size)

    def _fault(self, boundary: str) -> None:
        if boundary == self.boundary:
            self.boundary = None
            raise OSError("audit_write=denied")


@pytest.mark.parametrize("case", audit_sink_cases(), ids=lambda case: case.name)
def test_cumpre_contrato_compartilhado(tmp_path: Path, case: AuditSinkCase) -> None:
    case.run(_LocalProbe(tmp_path))


@pytest.mark.parametrize(
    "boundary",
    ["after_file_write", "after_file_fsync", "before_index_commit", "after_index_commit"],
)
def test_recupera_evento_completo_apos_falha(tmp_path: Path, boundary: str) -> None:
    event = audit_event()
    sink = _FaultSink(tmp_path)
    sink.boundary = boundary
    with pytest.raises(OSError, match="audit_write=denied"):
        sink.append(event)

    reopened = LocalAuditSink(tmp_path)
    reopened.append(event)

    log = tmp_path / "audit" / "tenant-a" / "2026" / "07" / "15" / "events.jsonl"
    assert log.read_bytes() == canonical_body(event) + b"\n"
    assert _index_count(tmp_path) == 1


def test_recupera_orfao_antes_do_append_de_sink_ja_instanciado(tmp_path: Path) -> None:
    first = _FaultSink(tmp_path)
    second = LocalAuditSink(tmp_path)
    first.boundary = "before_index_commit"
    with pytest.raises(OSError, match="audit_write=denied"):
        first.append(audit_event("event-001"))

    second.append(audit_event("event-002"))

    assert _index_count(tmp_path) == 2
    assert len(_log_path(tmp_path).read_bytes().splitlines()) == 2


def test_trunca_somente_cauda_parcial(tmp_path: Path) -> None:
    event = audit_event()
    LocalAuditSink(tmp_path).append(event)
    log = _log_path(tmp_path)
    with log.open("ab") as stream:
        stream.write(b'{"event_id":"incompleto"')

    LocalAuditSink(tmp_path)

    assert log.read_bytes() == canonical_body(event) + b"\n"


@pytest.mark.parametrize(
    "record",
    [b"{}\n", json.dumps(audit_event().model_dump(mode="json"), indent=2).encode() + b"\n"],
    ids=["invalido", "nao_canonico"],
)
def test_rejeita_registro_completo_invalido(tmp_path: Path, record: bytes) -> None:
    log = _log_path(tmp_path)
    log.parent.mkdir(parents=True)
    log.write_bytes(record)

    with pytest.raises(ValueError, match="audit_record=invalid"):
        LocalAuditSink(tmp_path)


def test_rejeita_evento_duplicado_na_cauda_nao_indexada(tmp_path: Path) -> None:
    event = audit_event()
    LocalAuditSink(tmp_path).append(event)
    with _log_path(tmp_path).open("ab") as stream:
        stream.write(canonical_body(event) + b"\n")

    with pytest.raises(Conflict, match="event_id=immutable"):
        LocalAuditSink(tmp_path)


def _append_in_process(root: str, event_id: str) -> None:
    LocalAuditSink(Path(root)).append(audit_event(event_id))


def test_serializa_append_entre_processos(tmp_path: Path) -> None:
    context = multiprocessing.get_context("spawn")
    processes = [
        context.Process(target=_append_in_process, args=(str(tmp_path), f"event-{index:03d}"))
        for index in range(12)
    ]
    for process in processes:
        process.start()
    for process in processes:
        process.join(timeout=10)

    assert all(process.exitcode == 0 for process in processes)
    assert _index_count(tmp_path) == 12
    assert len(_log_path(tmp_path).read_bytes().splitlines()) == 12


def test_materializa_lote_parquet_textual_e_deterministico(tmp_path: Path) -> None:
    events = (audit_event("event-001"), audit_event("event-002", payload={"n": 2}))
    sink = LocalAuditSink(tmp_path, parquet_batch_size=2)
    for event in events:
        sink.append(event)

    jsonl = b"".join(canonical_body(event) + b"\n" for event in events)
    digest = sha256(jsonl).hexdigest()
    parquet = _log_path(tmp_path).parent / f"batch-{digest}.parquet"
    table = pq.read_table(parquet)

    assert table.schema.names == [
        "tenant_id",
        "event_id",
        "event_type",
        "aggregate_id",
        "payload_json",
        "created_at",
        "delivered_at",
    ]
    assert all(str(field.type) == "string" for field in table.schema)
    assert table.column("event_id").to_pylist() == ["event-001", "event-002"]
    assert table.column("payload_json").to_pylist()[1] == '{"n":2}'
    assert _log_path(tmp_path).read_bytes() == jsonl

    _clear_batch_associations(tmp_path)
    original_digest = sha256(parquet.read_bytes()).hexdigest()
    LocalAuditSink(tmp_path, parquet_batch_size=2).append(events[0])
    assert list(parquet.parent.glob("batch-*.parquet")) == [parquet]
    assert sha256(parquet.read_bytes()).hexdigest() == original_digest


def test_rejeita_lote_preexistente_com_conteudo_divergente(tmp_path: Path) -> None:
    events = (audit_event("event-001"), audit_event("event-002"))
    sink = LocalAuditSink(tmp_path, parquet_batch_size=3)
    for event in events:
        sink.append(event)
    jsonl = b"".join(canonical_body(event) + b"\n" for event in events)
    parquet = _log_path(tmp_path).parent / f"batch-{sha256(jsonl).hexdigest()}.parquet"
    parquet.write_bytes(b"divergente")

    with pytest.raises(Conflict, match="batch=immutable"):
        LocalAuditSink(tmp_path, parquet_batch_size=2)


def test_rejeita_lote_quando_registro_indexado_foi_truncado(tmp_path: Path) -> None:
    sink = LocalAuditSink(tmp_path, parquet_batch_size=3)
    sink.append(audit_event("event-001"))
    sink.append(audit_event("event-002"))
    log = _log_path(tmp_path)
    log.write_bytes(log.read_bytes()[:-1])

    with pytest.raises(ValueError, match="audit_record=missing"):
        LocalAuditSink(tmp_path, parquet_batch_size=2)


def test_rejeita_parametros_e_componentes_inseguros(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="parquet_batch_size=invalid"):
        LocalAuditSink(tmp_path, parquet_batch_size=0)
    unsafe = audit_event().model_copy()
    object.__setattr__(unsafe, "tenant_id", "../tenant")
    with pytest.raises(ValueError, match="audit_path=invalid"):
        LocalAuditSink(tmp_path).append(unsafe)


def _log_path(root: Path) -> Path:
    return root / "audit" / "tenant-a" / "2026" / "07" / "15" / "events.jsonl"


def _index_count(root: Path) -> int:
    database = sqlite3.connect(root / "audit" / "index.sqlite3")
    count = database.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    database.close()
    return int(count)


def _clear_batch_associations(root: Path) -> None:
    database = sqlite3.connect(root / "audit" / "index.sqlite3")
    try:
        database.execute("UPDATE events SET batch_path = NULL")
        database.commit()
    finally:
        database.close()
