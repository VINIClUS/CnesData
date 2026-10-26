"""Contrato compartilhado de leitura sem escrita e gerações de cursor."""

from typing import Any

import pytest

from packages.cnes_infra.tests.control_plane import test_outbox_cursor as shared_cursor_tests
from packages.cnes_infra.tests.control_plane.outbox_cursor_support import OutboxHarness, evento

harness = shared_cursor_tests.harness


def _snapshot_sqlite(harness: OutboxHarness) -> tuple[str, ...]:
    with harness.adapter.read_connection() as connection:
        return tuple(connection.iterdump())


def test_duas_leituras_iniciais_nao_criam_nem_alteram_cursor(harness: OutboxHarness) -> None:
    harness.put(evento("first"))
    before = _snapshot_sqlite(harness) if harness.backend == "sqlite" else None
    if harness.backend == "dynamodb":
        harness.sdk.calls.clear()
    first = harness.adapter.read_outbox_page(None, 1)
    second = harness.restart().read_outbox_page(None, 1)
    assert first.expected is second.expected is None
    assert first.events == second.events == (evento("first"),)
    assert first.next_cursor == second.next_cursor
    if harness.backend == "sqlite":
        assert _snapshot_sqlite(harness) == before
    else:
        assert {name for name, _ in harness.sdk.calls} <= {"get_item", "query"}
    assert harness.adapter.advance_outbox_cursor(first.expected, first.next_cursor)
    assert not harness.adapter.advance_outbox_cursor(second.expected, second.next_cursor)


def test_leitura_explicita_usa_posicao_anterior_sem_mover_duravel(harness: OutboxHarness) -> None:
    for index, event_id in enumerate(("first", "second", "third", "fourth")):
        harness.put(evento(event_id, index))
    first = harness.adapter.read_outbox_page(None, 1)
    assert harness.adapter.advance_outbox_cursor(first.expected, first.next_cursor)
    saved = harness.adapter.read_outbox_page(None, 1)
    assert saved.events == (evento("second", 1),)
    assert harness.adapter.advance_outbox_cursor(saved.expected, saved.next_cursor)
    ahead = harness.adapter.read_outbox_page(None, 1)
    assert ahead.events == (evento("third", 2),)
    explicit = harness.adapter.read_outbox_page(saved.expected, 1)
    assert explicit.events == (evento("second", 1),)
    durable = harness.restart().read_outbox_page(None, 1)
    assert durable.expected == ahead.expected
    assert durable.events == (evento("third", 2),)


def test_duas_voltas_vazias_incrementam_geracao_sem_apagar_cursor(harness: OutboxHarness) -> None:
    first = harness.adapter.read_outbox_page(None, 1)
    assert first.events == ()
    assert first.expected is None
    assert first.next_cursor.version == 1
    assert first.next_cursor.position is None
    assert harness.adapter.advance_outbox_cursor(None, first.next_cursor)
    second = harness.restart().read_outbox_page(None, 1)
    assert second.expected == first.next_cursor
    assert second.events == ()
    assert second.next_cursor.version == second.expected.version + 1
    assert second.next_cursor.position is None
    assert harness.adapter.advance_outbox_cursor(second.expected, second.next_cursor)
    third = harness.restart().read_outbox_page(None, 1)
    assert third.expected == second.next_cursor
    assert third.expected.position is None
    assert not harness.adapter.advance_outbox_cursor(None, first.next_cursor)
    assert not harness.adapter.advance_outbox_cursor(second.expected, second.next_cursor)
    harness.put(evento("new"))
    assert harness.adapter.read_outbox_page(None, 1).events == (evento("new"),)


def test_next_none_da_volta_incrementa_versao_e_mantem_token(harness: OutboxHarness) -> None:
    harness.put(evento("first"))
    harness.put(evento("second", 1))
    first = harness.adapter.read_outbox_page(None, 1)
    assert harness.adapter.advance_outbox_cursor(first.expected, first.next_cursor)
    page = harness.adapter.read_outbox_page(None, 1)
    assert page.expected.position is not None
    assert harness.adapter.advance_outbox_cursor(page.expected, None)
    wrapped = harness.restart().read_outbox_page(None, 1)
    assert wrapped.expected.version == page.expected.version + 1
    assert wrapped.expected.position is None
    assert wrapped.events == (evento("first"),)
    assert not harness.adapter.advance_outbox_cursor(page.expected, page.next_cursor)
    assert not harness.adapter.advance_outbox_cursor(None, first.next_cursor)


@pytest.mark.parametrize("invalid_version", [-1, True, "1"])
def test_cursor_rejeita_versao_invalida_sem_afetar_avanco(
    harness: OutboxHarness, invalid_version: Any,
) -> None:
    harness.put(evento("first"))
    page = harness.adapter.read_outbox_page(None, 1)
    with pytest.raises((ValueError, TypeError)):
        type(page.next_cursor)(version=invalid_version, position=None)
    zero = type(page.next_cursor)(version=0, position=None)
    assert harness.adapter.read_outbox_page(zero, 1).events == (evento("first"),)
    assert harness.adapter.advance_outbox_cursor(page.expected, page.next_cursor)
    assert harness.restart().read_outbox_page(None, 1).expected == page.next_cursor


def test_cursor_e_pagina_imutaveis_preservam_token_do_cas(harness: OutboxHarness) -> None:
    harness.put(evento("first"))
    page = harness.adapter.read_outbox_page(None, 1)
    original = page.next_cursor
    original_version = original.version
    for field, value in (("version", -1), ("position", "tampered")):
        with pytest.raises((AttributeError, TypeError, ValueError)):
            setattr(original, field, value)
    for field, value in (("events", ()), ("expected", original), ("next_cursor", None)):
        with pytest.raises((AttributeError, TypeError, ValueError)):
            setattr(page, field, value)
    assert original.version == original_version
    assert page.events == (evento("first"),)
    assert harness.adapter.advance_outbox_cursor(page.expected, original)
    assert harness.restart().read_outbox_page(None, 1).expected == original


@pytest.mark.parametrize("invalid_position", [1, True, ("position",)])
def test_cursor_rejeita_posicao_fora_do_contrato(
    harness: OutboxHarness, invalid_position: Any,
) -> None:
    page = harness.adapter.read_outbox_page(None, 1)
    with pytest.raises((ValueError, TypeError)):
        type(page.next_cursor)(version=0, position=invalid_position)
    assert harness.adapter.advance_outbox_cursor(page.expected, page.next_cursor)
    assert harness.restart().read_outbox_page(None, 1).expected == page.next_cursor


@pytest.mark.parametrize("limit", [0, -1])
def test_read_rejeita_limite_nao_positivo_sem_escrever(
    harness: OutboxHarness, limit: int, monkeypatch: pytest.MonkeyPatch,
) -> None:
    before = _snapshot_sqlite(harness) if harness.backend == "sqlite" else None
    if harness.backend == "dynamodb":
        harness.sdk.calls.clear()
    connections: list[str] = []
    connect = harness.adapter._connect if harness.backend == "sqlite" else (lambda: None)

    def observe_connection() -> Any:
        connections.append("connect")
        return connect()

    with monkeypatch.context() as patch:
        if harness.backend == "sqlite":
            patch.setattr(harness.adapter, "_connect", observe_connection)
        with pytest.raises(ValueError):
            harness.adapter.read_outbox_page(None, limit)
        assert connections == []
    if harness.backend == "sqlite":
        assert _snapshot_sqlite(harness) == before
    else:
        assert harness.sdk.calls == []
    assert harness.adapter.read_outbox_page(None, 1).expected is None
