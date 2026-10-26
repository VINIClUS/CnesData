"""Regressões compartilhadas de cursor/CAS com SQLite e DynamoDB reais."""

from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from pathlib import Path
from threading import Barrier
from typing import Any

import pytest
from moto import mock_aws

from cnes_domain.billing.models import BillingAuditEvent
from cnes_domain.control_plane.errors import Conflict
from cnes_domain.outbox_dispatcher import DispatchResult, dispatch_once
from cnes_infra.billing.audit_outbox import DynamoBillingAudit
from cnes_infra.billing.dynamodb_items import audit_outbox_event
from packages.cnes_infra.tests.contracts.clock import _NOW
from packages.cnes_infra.tests.control_plane.outbox_cursor_support import (
    OutboxHarness,
    evento,
)


@pytest.fixture(params=["sqlite", "dynamodb"])
def harness(
    request: pytest.FixtureRequest, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[OutboxHarness]:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    with mock_aws():
        yield OutboxHarness(request.param, tmp_path)


def _billing(harness: OutboxHarness, index: int) -> None:
    audit = BillingAuditEvent(
        event_id=f"billing:{index:03}", event_type="entitlement.changed",
        aggregate_id="ba_01", actor_id="stripe", reason_code="webhook_projection",
        occurred_at=_NOW + timedelta(seconds=index), attributes={"entitlement_version": 1},
    )
    if harness.backend == "dynamodb":
        DynamoBillingAudit(harness.client, "cnesdata-control-plane").append(audit)
    else:
        harness.put(audit_outbox_event(audit))


def test_cem_falhas_transitorias_de_billing_nao_impedem_segunda_pagina(
    harness: OutboxHarness,
) -> None:
    for index in range(100):
        _billing(harness, index)
    harness.put(evento("good", 101))
    sink = harness.sink()

    def indisponivel(event: Any) -> None:
        if event.tenant_id == "_billing":
            raise OSError("sink=temporarily_unavailable")

    sink.before = indisponivel
    assert dispatch_once(harness.adapter, sink, _NOW) == DispatchResult(0, 100)
    assert harness.stored("good").delivered_at is None
    assert dispatch_once(harness.adapter, sink, _NOW) == DispatchResult(1, 0)
    assert harness.stored("good").delivered_at == _NOW
    assert all(harness.stored(f"billing:{i:03}").delivered_at is None for i in range(100))


def test_billing_valido_e_entregue_pelo_sink_real(harness: OutboxHarness) -> None:
    _billing(harness, 0)
    result = dispatch_once(harness.adapter, harness.sink(), _NOW)
    assert result == DispatchResult(1, 0)
    [record] = harness.local_records()
    assert (record.tenant_id, record.event_id, record.event_type) == (
        "_billing", "billing:000", "entitlement.changed"
    )
    assert record.payload == {
        "actor_id": "stripe", "reason_code": "webhook_projection",
        "attributes": {"entitlement_version": 1},
    }


@pytest.mark.parametrize("separator", ["#", "/", "\\"])
def test_cem_ids_invalidos_no_sink_s3_nao_impedem_segunda_pagina(
    harness: OutboxHarness, separator: str,
) -> None:
    for index in range(100):
        harness.put(evento(f"poison{separator}{index:03}", index))
    harness.put(evento("good", 101))
    sink = harness.sink("s3")
    with pytest.raises(ValueError, match="audit_path=invalid"):
        sink.sink.append(evento(f"poison{separator}000"))

    assert dispatch_once(harness.adapter, sink, _NOW) == DispatchResult(0, 100)
    assert dispatch_once(harness.adapter, sink, _NOW) == DispatchResult(1, 0)
    assert harness.stored("good").delivered_at == _NOW
    assert harness.stored(f"poison{separator}000").delivered_at is None


def test_cursor_da_volta_repete_poison_e_entrega_novo_evento_atras(
    harness: OutboxHarness,
) -> None:
    harness.put(evento("poison"))
    harness.put(evento("good", 1))
    sink = harness.sink()

    def indisponivel(event: Any) -> None:
        if event.event_id == "poison":
            raise OSError("sink=temporarily_unavailable")

    sink.before = indisponivel
    assert dispatch_once(harness.adapter, sink, _NOW, 1) == DispatchResult(0, 1)
    harness.put(evento("new-behind", -1))
    for _ in range(6):
        dispatch_once(harness.adapter, sink, _NOW, 1)
    assert harness.stored("good").delivered_at == _NOW
    assert harness.stored("new-behind").delivered_at == _NOW
    assert sum(event.event_id == "poison" for event in sink.attempts) >= 2
    assert harness.stored("poison").delivered_at is None
    sink.before = None
    for _ in range(4):
        dispatch_once(harness.adapter, sink, _NOW, 1)
    assert harness.stored("poison").delivered_at == _NOW
    assert sorted(record.event_id for record in harness.local_records()) == [
        "good", "new-behind", "poison"
    ]


def test_reinicio_retoma_proxima_pagina_duravel(harness: OutboxHarness) -> None:
    harness.put(evento("poison"))
    harness.put(evento("good", 1))
    sink = harness.sink("s3")

    def indisponivel(event: Any) -> None:
        if event.event_id == "poison":
            raise OSError("sink=temporarily_unavailable")

    sink.before = indisponivel
    assert dispatch_once(harness.adapter, sink, _NOW, 1) == DispatchResult(0, 1)
    restarted = harness.restart()
    assert dispatch_once(restarted, sink, _NOW, 1) == DispatchResult(1, 0)
    assert harness.stored("good").delivered_at == _NOW


@pytest.mark.parametrize("established", [False, True])
def test_avanco_concorrente_do_cursor_tem_um_vencedor(
    harness: OutboxHarness, established: bool,
) -> None:
    harness.put(evento("first"))
    harness.put(evento("second", 1))
    harness.put(evento("third", 2))
    if established:
        initial = harness.adapter.read_outbox_page(None, 1)
        assert harness.adapter.advance_outbox_cursor(initial.expected, initial.next_cursor)
    adapters = [harness.adapter, harness.restart()]
    pages = [adapter.read_outbox_page(None, 1) for adapter in adapters]
    expected_event = evento("second", 1) if established else evento("first")
    assert pages[0].events == pages[1].events == (expected_event,)
    assert (pages[0].expected is not None) is established
    barrier = Barrier(2)

    def competir(index: int) -> bool:
        barrier.wait(timeout=5)
        page = pages[index]
        return adapters[index].advance_outbox_cursor(page.expected, page.next_cursor)

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(competir, range(2)))
    assert sorted(results) == [False, True]
    following = evento("third", 2) if established else evento("second", 1)
    assert harness.restart().read_outbox_page(None, 1).events == (following,)


def test_token_antigo_perde_cas_apos_volta_sem_aba(harness: OutboxHarness) -> None:
    harness.put(evento("first"))
    harness.put(evento("second", 1))
    initial = harness.adapter.read_outbox_page(None, 1)
    assert harness.adapter.advance_outbox_cursor(initial.expected, initial.next_cursor)
    saved = harness.adapter.read_outbox_page(None, 1)
    page = saved
    wrapped = False
    for _ in range(6):
        assert harness.adapter.advance_outbox_cursor(page.expected, page.next_cursor)
        if page.next_cursor.position is None:
            wrapped = True
        elif wrapped and page.next_cursor.position == saved.expected.position:
            break
        page = harness.adapter.read_outbox_page(None, 1)
    else:
        pytest.fail("cursor=did_not_repeat_position_after_wrap")
    assert page.next_cursor.position == saved.expected.position
    assert page.next_cursor.version > saved.expected.version
    assert not harness.adapter.advance_outbox_cursor(initial.expected, initial.next_cursor)
    assert not harness.adapter.advance_outbox_cursor(saved.expected, saved.next_cursor)
    current = harness.restart().read_outbox_page(None, 1)
    assert current.expected == page.next_cursor
    assert current.events == (evento("second", 1),)


@pytest.mark.parametrize("delta", [0, 2, -1])
def test_cas_rejeita_transicao_de_versao_invalida_sem_mudar_posicao(
    harness: OutboxHarness, delta: int,
) -> None:
    harness.put(evento("first"))
    harness.put(evento("second", 1))
    first = harness.adapter.read_outbox_page(None, 1)
    assert harness.adapter.advance_outbox_cursor(first.expected, first.next_cursor)
    page = harness.adapter.read_outbox_page(None, 1)
    invalid = type(page.next_cursor)(
        version=page.expected.version + delta, position=page.next_cursor.position
    )
    with pytest.raises(ValueError):
        harness.adapter.advance_outbox_cursor(page.expected, invalid)
    assert harness.restart().read_outbox_page(None, 1).expected == page.expected


def test_pagina_vazia_avanca_token_e_rejeita_replay_antigo(harness: OutboxHarness) -> None:
    page = harness.adapter.read_outbox_page(None, 1)
    assert page.events == ()
    assert harness.adapter.advance_outbox_cursor(page.expected, page.next_cursor)
    after = harness.restart().read_outbox_page(None, 1)
    assert not harness.adapter.advance_outbox_cursor(page.expected, page.next_cursor)
    harness.put(evento("new"))
    assert harness.adapter.read_outbox_page(None, 1).events == (evento("new"),)
    assert after.expected == page.next_cursor


@pytest.mark.parametrize("mutation", ["delivered", "missing", "position", "identity"])
def test_revalida_segundo_candidato_imediatamente_antes_do_append(
    harness: OutboxHarness, mutation: str,
) -> None:
    first, second = evento("first"), evento("second", 1)
    harness.put(first)
    harness.put(second)
    sink = harness.sink()

    def alterar_segundo(event: Any) -> None:
        if event.event_id != "first":
            return
        changes = {
            "delivered": {"delivered_at": _NOW},
            "position": {"created_at": _NOW + timedelta(days=1)},
            "identity": {"event_id": "substituted"},
        }
        changed = None if mutation == "missing" else second.model_copy(update=changes[mutation])
        harness.mutate(changed, "second")

    sink.before = alterar_segundo
    dispatch_once(harness.adapter, sink, _NOW, 2)
    assert [record.event_id for record in harness.local_records()] == ["first"]
    assert [event.event_id for event in sink.attempts] == ["first"]


def test_marcacao_repetida_preserva_timestamp_e_rejeita_redecisao(harness: OutboxHarness) -> None:
    harness.put(evento("event"))
    harness.adapter.mark_outbox_delivered("event", _NOW)
    harness.adapter.mark_outbox_delivered("event", _NOW)
    with pytest.raises(Conflict):
        harness.adapter.mark_outbox_delivered("event", _NOW + timedelta(seconds=1))
    assert harness.stored("event").delivered_at == _NOW


class _ProcessCrash(BaseException):
    pass


def test_crash_apos_append_reinicia_com_replay_canonico_idempotente(
    harness: OutboxHarness, monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = evento("first")
    harness.put(original)
    sink = harness.sink()
    harness.fail_marker_once(monkeypatch, _ProcessCrash("process=terminated"))
    with pytest.raises(_ProcessCrash):
        dispatch_once(harness.adapter, sink, _NOW, 1)
    assert harness.stored("first") == original
    assert harness.local_records() == [original]
    restarted = harness.restart()
    for _ in range(3):
        dispatch_once(restarted, sink, _NOW, 1)
    assert harness.stored("first").delivered_at == _NOW
    assert harness.local_records() == [original]
    assert sink.attempts == [original, original]


def test_falha_no_marcador_avanca_pagina_e_repete_append_apos_volta(
    harness: OutboxHarness, monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness.put(evento("first"))
    harness.put(evento("second", 1))
    sink = harness.sink()
    harness.fail_marker_once(monkeypatch, OSError("marker=unavailable"))
    assert dispatch_once(harness.adapter, sink, _NOW, 1) == DispatchResult(0, 1)
    assert harness.stored("first").delivered_at is None
    assert dispatch_once(harness.adapter, sink, _NOW, 1) == DispatchResult(1, 0)
    assert harness.stored("second").delivered_at == _NOW
    for _ in range(4):
        dispatch_once(harness.adapter, sink, _NOW, 1)
    assert harness.stored("first").delivered_at == _NOW
    assert sorted(record.event_id for record in harness.local_records()) == ["first", "second"]


def test_dispatch_perde_cas_e_retoma_cursor_vencedor_na_proxima_chamada(
    harness: OutboxHarness,
) -> None:
    harness.put(evento("first"))
    harness.put(evento("second", 1))
    contender = harness.restart()
    page = contender.read_outbox_page(None, 1)
    sink = harness.sink()

    def vencer_durante_append(event: Any) -> None:
        if event.event_id == "first":
            assert contender.advance_outbox_cursor(page.expected, page.next_cursor)

    sink.before = vencer_durante_append
    assert dispatch_once(harness.adapter, sink, _NOW, 1) == DispatchResult(1, 0)
    assert contender.read_outbox_page(None, 1).expected == page.next_cursor
    assert harness.stored("second").delivered_at is None
    assert dispatch_once(harness.adapter, sink, _NOW, 1) == DispatchResult(1, 0)
    assert [record.event_id for record in harness.local_records()] == ["first", "second"]
