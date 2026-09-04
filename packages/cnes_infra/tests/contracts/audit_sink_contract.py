"""Casos reutilizáveis de conformidade dos sinks de auditoria."""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from hashlib import sha256
from typing import Any, Protocol

from cnes_domain.control_plane.entities import OutboxEvent
from cnes_domain.control_plane.errors import Conflict
from cnes_domain.ports.audit import AuditSinkPort


@dataclass(frozen=True, slots=True)
class StoredAuditEvent:
    location: str
    body: bytes
    sha256: str


class AuditSinkProbe(Protocol):
    sink: Any

    def stored(self) -> tuple[StoredAuditEvent, ...]: ...

    def expected_location(self, event: OutboxEvent) -> str: ...

    def fail_next(self) -> None: ...


_Runner = Callable[[AuditSinkProbe], None]


@dataclass(frozen=True, slots=True)
class AuditSinkCase:
    """Executa uma invariável contra um sink e seu probe de backend."""

    name: str
    _runner: _Runner = field(repr=False, compare=False)

    def run(self, probe: AuditSinkProbe) -> None:
        try:
            assert isinstance(probe.sink, AuditSinkPort)
            self._runner(probe)
        except Exception as error:
            raise AssertionError(f"case={self.name}") from error


def audit_event(
    event_id: str = "event-001",
    *,
    payload: dict[str, Any] | None = None,
    created_at: datetime | None = None,
) -> OutboxEvent:
    return OutboxEvent(
        tenant_id="tenant-a",
        event_id=event_id,
        event_type="job.created",
        aggregate_id="job-001",
        payload=payload if payload is not None else {"acao": "criar", "ordem": [2, 1]},
        created_at=created_at or datetime(2026, 7, 15, 10, 30, tzinfo=UTC),
        delivered_at=None,
    )


def canonical_body(event: OutboxEvent) -> bytes:
    return json.dumps(
        event.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
    ).encode()


def _case_serializacao_canonica(probe: AuditSinkProbe) -> None:
    event = audit_event(payload={"z": 1, "nested": {"b": 2, "a": "ação"}})
    probe.sink.append(event)
    assert probe.stored()[0].body == canonical_body(event)


def _case_identidade_e_metadados(probe: AuditSinkProbe) -> None:
    event = audit_event()
    probe.sink.append(event)
    stored = probe.stored()[0]
    assert stored.location == probe.expected_location(event)
    assert stored.sha256 == sha256(canonical_body(event)).hexdigest()


def _case_ordem_deterministica(probe: AuditSinkProbe) -> None:
    events = (audit_event("event-002"), audit_event("event-001"))
    for event in events:
        probe.sink.append(event)
    stored = probe.stored()
    assert tuple(json.loads(item.body)["event_id"] for item in stored) == (
        "event-002",
        "event-001",
    )
    assert tuple(item.location for item in stored) == tuple(
        probe.expected_location(event) for event in events
    )


def _case_replay_idempotente(probe: AuditSinkProbe) -> None:
    event = audit_event()
    probe.sink.append(event)
    probe.sink.append(event)
    assert len(probe.stored()) == 1


def _case_rejeita_event_id_divergente(probe: AuditSinkProbe) -> None:
    event = audit_event()
    probe.sink.append(event)
    changed = audit_event(payload={"acao": "alterar"})
    try:
        probe.sink.append(changed)
    except Conflict:
        return
    raise AssertionError("conflict_required")


def _case_propaga_erro_permanente(probe: AuditSinkProbe) -> None:
    probe.fail_next()
    try:
        probe.sink.append(audit_event())
    except OSError:
        return
    raise AssertionError("permanent_error_required")


def audit_sink_cases() -> tuple[AuditSinkCase, ...]:
    """Retorna o catálogo estável de invariáveis dos sinks de auditoria."""
    return (
        AuditSinkCase("serializacao_canonica", _case_serializacao_canonica),
        AuditSinkCase("identidade_e_metadados", _case_identidade_e_metadados),
        AuditSinkCase("ordem_deterministica", _case_ordem_deterministica),
        AuditSinkCase("replay_idempotente", _case_replay_idempotente),
        AuditSinkCase("event_id_divergente", _case_rejeita_event_id_divergente),
        AuditSinkCase("erro_permanente", _case_propaga_erro_permanente),
    )
