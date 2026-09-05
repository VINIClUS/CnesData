from __future__ import annotations

from datetime import UTC, datetime

import pytest

from cnes_domain.control_plane.entities import OutboxEvent
from cnes_domain.outbox_dispatcher import DispatchResult, dispatch_once

NOW = datetime(2026, 9, 5, 12, tzinfo=UTC)


def make_event(event_id: str, tenant_id: str = "tenant-a") -> OutboxEvent:
    return OutboxEvent(
        tenant_id=tenant_id,
        event_id=event_id,
        event_type="run.published",
        aggregate_id=f"run-{event_id}",
        payload={"event_id": event_id},
        created_at=NOW,
        delivered_at=None,
    )


class FakeControlPlane:
    def __init__(
        self,
        events: tuple[OutboxEvent, ...] = (),
        timeline: list[str] | None = None,
    ) -> None:
        self.events = events
        self.timeline = timeline if timeline is not None else []
        self.pending_limits: list[int] = []
        self.delivered: dict[str, datetime] = {}
        self.mark_failures: dict[str, int] = {}
        self.pending_error: Exception | None = None

    def pending_outbox(self, limit: int) -> tuple[OutboxEvent, ...]:
        self.pending_limits.append(limit)
        self.timeline.append(f"pending:{limit}")
        if self.pending_error is not None:
            raise self.pending_error
        pending = (event for event in self.events if event.event_id not in self.delivered)
        return tuple(pending)[:limit]

    def mark_outbox_delivered(self, event_id: str, delivered_at: datetime) -> None:
        self.timeline.append(f"mark:{event_id}")
        failures = self.mark_failures.get(event_id, 0)
        if failures:
            self.mark_failures[event_id] = failures - 1
            raise RuntimeError("mark=failed")
        self.delivered[event_id] = delivered_at


class FakeAuditSink:
    def __init__(self, timeline: list[str] | None = None) -> None:
        self.timeline = timeline if timeline is not None else []
        self.appended: list[OutboxEvent] = []
        self.append_failures: dict[str, int] = {}

    def append(self, event: OutboxEvent) -> None:
        self.timeline.append(f"append:{event.event_id}")
        self.appended.append(event)
        failures = self.append_failures.get(event.event_id, 0)
        if failures:
            self.append_failures[event.event_id] = failures - 1
            raise RuntimeError("append=failed")


@pytest.mark.parametrize("limit", [0, -1])
def test_rejeita_limite_invalido_sem_consultar_outbox(limit: int) -> None:
    control_plane = FakeControlPlane()

    with pytest.raises(ValueError, match=r"^limit=invalid$"):
        dispatch_once(control_plane, FakeAuditSink(), NOW, limit)

    assert control_plane.pending_limits == []


def test_fila_vazia_consulta_limite_padrao() -> None:
    control_plane = FakeControlPlane()

    result = dispatch_once(control_plane, FakeAuditSink(), NOW)

    assert result == DispatchResult(delivered=0, failed=0)
    assert control_plane.pending_limits == [100]


def test_respeita_limite_e_ordem_do_lote() -> None:
    timeline: list[str] = []
    control_plane = FakeControlPlane(
        (make_event("event-2"), make_event("event-1"), make_event("event-3")),
        timeline,
    )

    result = dispatch_once(control_plane, FakeAuditSink(timeline), NOW, limit=2)

    assert result == DispatchResult(delivered=2, failed=0)
    assert timeline == [
        "pending:2",
        "append:event-2",
        "mark:event-2",
        "append:event-1",
        "mark:event-1",
    ]


def test_falha_do_sink_nao_marca_e_tenta_eventos_seguintes() -> None:
    timeline: list[str] = []
    control_plane = FakeControlPlane(
        (make_event("event-1"), make_event("event-2")),
        timeline,
    )
    sink = FakeAuditSink(timeline)
    sink.append_failures["event-1"] = 1

    result = dispatch_once(control_plane, sink, NOW)

    assert result == DispatchResult(delivered=1, failed=1)
    assert timeline == [
        "pending:100",
        "append:event-1",
        "append:event-2",
        "mark:event-2",
    ]
    assert control_plane.delivered == {"event-2": NOW}


def test_falha_da_marcacao_mantem_evento_pendente() -> None:
    control_plane = FakeControlPlane((make_event("event-1"),))
    control_plane.mark_failures["event-1"] = 1

    result = dispatch_once(control_plane, FakeAuditSink(), NOW)

    assert result == DispatchResult(delivered=0, failed=1)
    assert control_plane.delivered == {}
    assert control_plane.pending_outbox(100) == (make_event("event-1"),)


def test_novo_ciclo_repete_append_com_mesmo_event_id() -> None:
    control_plane = FakeControlPlane((make_event("event-1"),))
    control_plane.mark_failures["event-1"] = 2
    sink = FakeAuditSink()

    first = dispatch_once(control_plane, sink, NOW)
    second = dispatch_once(control_plane, sink, NOW)

    assert first == second == DispatchResult(delivered=0, failed=1)
    assert [event.event_id for event in sink.appended] == ["event-1", "event-1"]


def test_reinicio_posterior_conclui_entrega() -> None:
    control_plane = FakeControlPlane((make_event("event-1"),))
    failing_sink = FakeAuditSink()
    failing_sink.append_failures["event-1"] = 1
    first = dispatch_once(control_plane, failing_sink, NOW)

    restarted_sink = FakeAuditSink()
    second = dispatch_once(control_plane, restarted_sink, NOW)

    assert first == DispatchResult(delivered=0, failed=1)
    assert second == DispatchResult(delivered=1, failed=0)
    assert [event.event_id for event in restarted_sink.appended] == ["event-1"]
    assert control_plane.delivered == {"event-1": NOW}


def test_isola_falha_entre_eventos_de_tenants_diferentes() -> None:
    event_a = make_event("event-a", tenant_id="tenant-a")
    event_b = make_event("event-b", tenant_id="tenant-b")
    control_plane = FakeControlPlane((event_a, event_b))
    sink = FakeAuditSink()
    sink.append_failures["event-a"] = 1

    result = dispatch_once(control_plane, sink, NOW)

    assert result == DispatchResult(delivered=1, failed=1)
    assert [(event.tenant_id, event.event_id) for event in sink.appended] == [
        ("tenant-a", "event-a"),
        ("tenant-b", "event-b"),
    ]
    assert control_plane.delivered == {"event-b": NOW}


def test_propaga_falha_ao_consultar_outbox() -> None:
    control_plane = FakeControlPlane()
    error = RuntimeError("pending=failed")
    control_plane.pending_error = error

    with pytest.raises(RuntimeError, match=r"^pending=failed$") as raised:
        dispatch_once(control_plane, FakeAuditSink(), NOW)

    assert raised.value is error
