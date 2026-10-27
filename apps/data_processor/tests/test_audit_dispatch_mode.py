"""Contrato público do modo dedicado e entrega real SQLite/local."""

import importlib
import logging
import os
import socket
import sys
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch

import pytest

from cnes_domain.billing.models import BillingAuditEvent
from cnes_infra.billing.dynamodb_items import audit_outbox_event
from packages.cnes_infra.tests.contracts.clock import _NOW
from packages.cnes_infra.tests.control_plane.outbox_cursor_support import OutboxHarness, evento

_MINIMUM = {
    "PROFILE": "aws",
    "AWS_REGION": "us-east-2",
    "AWS_CONTROL_PLANE_TABLE": "audit-control-configurado",
    "AWS_AUDIT_BUCKET": "audit-bucket-configurado",
    "AWS_AUDIT_RETENTION_DAYS": "365",
}
_ENVELOPE = (
    "TENANT_ID",
    "RUN_ID",
    "WAVE_ID",
    "DISPATCH_ID",
    "UNIT_ID",
    "EXECUTION_OWNER",
    "LEASE_SECONDS",
)


@pytest.fixture(autouse=True)
def _isolar_rede(monkeypatch: pytest.MonkeyPatch) -> None:
    def negar(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("network=forbidden")

    for name in ("create_connection", "getaddrinfo", "gethostbyname", "gethostbyname_ex"):
        monkeypatch.setattr(socket, name, negar)
    for name in ("connect", "connect_ex", "sendto"):
        monkeypatch.setattr(socket.socket, name, negar)


def _invalidos() -> list[Any]:
    required = (
        "AWS_REGION",
        "AWS_CONTROL_PLANE_TABLE",
        "AWS_AUDIT_BUCKET",
        "AWS_AUDIT_RETENTION_DAYS",
    )
    cases = [
        pytest.param({key: None}, ["dispatch-outbox-once"], id=f"sem-{key}") for key in required
    ]
    cases += [
        pytest.param({key: " "}, ["dispatch-outbox-once"], id=f"vazio-{key}") for key in required
    ]
    cases += [
        pytest.param(
            {"AWS_AUDIT_RETENTION_DAYS": value}, ["dispatch-outbox-once"], id=f"retencao-{value}"
        )
        for value in ("0", "-1", "1.5", "abc")
    ]
    cases += [
        pytest.param(
            {"AWS_PRODUCTION_POLICY": value}, ["dispatch-outbox-once"], id=f"policy-{value}"
        )
        for value in ("", "TRUE", "yes")
    ]
    cases += [
        pytest.param(
            {"AWS_PRODUCTION_POLICY": "true", key: value},
            ["dispatch-outbox-once"],
            id=f"producao-{key}",
        )
        for key, value in (("AWS_REGION", "us-east-1"), ("AWS_AUDIT_RETENTION_DAYS", "364"))
    ]
    cases += [
        pytest.param({key: value}, ["dispatch-outbox-once"], id=f"envelope-{key}-{value}")
        for key in _ENVELOPE
        for value in ("", "presente")
    ]
    cases += [
        pytest.param({}, args, id=f"argv-{index}")
        for index, args in enumerate(
            (
                ["desconhecido"],
                ["dispatch-outbox-once", "extra"],
                ["dispatch-outbox-once", "--limit=101"],
                ["dispatch-outbox-once", "--deadline=61"],
            )
        )
    ]
    return cases


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("changes", "argv"),
    [
        *_invalidos(),
        pytest.param(
            {"AWS_AUDIT_BUCKET": "invalid/name"}, ["dispatch-outbox-once"], id="bucket-barra"
        ),
        pytest.param(
            {"AWS_AUDIT_BUCKET": r"invalid\name"}, ["dispatch-outbox-once"], id="bucket-contrabarra"
        ),
    ],
)
async def test_configuracao_invalida_falha_antes_de_observabilidade_e_sessao(
    monkeypatch: pytest.MonkeyPatch,
    changes: dict[str, Any],
    argv: list[str],
) -> None:
    app = importlib.import_module("data_processor.main")
    values = dict(_MINIMUM)
    for key, value in changes.items():
        if value is None:
            values.pop(key, None)
        else:
            values[key] = value
    spies = [Mock(side_effect=AssertionError("preflight=io_forbidden")) for _ in range(4)]
    for name, spy in zip(
        ("configure_json_stdout", "init_telemetry", "Session", "_setup_logging"), spies, strict=True
    ):
        monkeypatch.setattr(app, name, spy)
    logger = Mock(side_effect=AssertionError("preflight=logging_forbidden"))
    with (
        patch.dict(os.environ, values, clear=True),
        patch.object(sys, "argv", ["data_processor", *argv]),
        patch.object(logging, "getLogger", logger),
    ):
        assert await app.main() == 2
    for spy in [*spies, logger]:
        spy.assert_not_called()


@pytest.fixture
def local(tmp_path: Path) -> OutboxHarness:
    return OutboxHarness("sqlite", tmp_path)


def _executar(control_plane: Any, sink: Any) -> int:
    composition = importlib.import_module("data_processor.aws_composition")
    dispatch = importlib.import_module("data_processor.audit_dispatch")
    runtime = composition.AwsAuditRuntime(control_plane, sink, lambda: _NOW)
    return dispatch.run_audit_dispatch_once(runtime)


def _confirmar(local: OutboxHarness, events: list[Any]) -> None:
    for event in events:
        assert local.stored(event.event_id) == event.model_copy(update={"delivered_at": _NOW})
    assert sorted(local.local_records(), key=lambda event: event.event_id) == sorted(
        events,
        key=lambda event: event.event_id,
    )


def _sink_poison(local: OutboxHarness) -> Any:
    sink = local.sink()

    def indisponivel(event: Any) -> None:
        if event.event_id.startswith("poison"):
            raise OSError("sink=temporarily_unavailable")

    sink.before = indisponivel
    return sink


def _pagina_cheia(local: OutboxHarness) -> list[Any]:
    events = [evento("poison"), *[evento(f"entrega{index:03}", index) for index in range(1, 100)]]
    for event in [*events, evento("bom", 101)]:
        local.put(event)
    return events


def _cursor(local: OutboxHarness, version: int, continuation: bool) -> None:
    page = local.restart().read_outbox_page(None, 100)
    assert page.expected is not None
    assert page.expected.version == version
    assert (page.expected.position is not None) is continuation


def test_passada_vazia_retorna_zero_e_persiste_cursor(local: OutboxHarness) -> None:
    assert _executar(local.adapter, local.sink()) == 0

    page = local.restart().read_outbox_page(None, 100)
    assert page.events == ()
    assert page.expected is not None
    assert (page.expected.version, page.expected.position) == (1, None)
    assert local.local_records() == []


def test_entrega_evento_real_e_reinicio_nao_duplica(local: OutboxHarness) -> None:
    event = evento("entregue")
    local.put(event)

    assert _executar(local.adapter, local.sink()) == 0
    _confirmar(local, [event])
    assert _executar(local.restart(), local.sink()) == 0
    _confirmar(local, [event])


def test_seletor_e_entrypoint_publicos_entregam_evento_sem_envelope(local: OutboxHarness) -> None:
    entrypoint = importlib.import_module("data_processor.aws_entrypoint")
    composition = importlib.import_module("data_processor.aws_composition")
    event = evento("publico")
    local.put(event)
    invocation = None
    try:
        invocation = entrypoint.select_aws_invocation(_MINIMUM, ["dispatch-outbox-once"])
    except entrypoint.EntrypointConfigurationError:
        pass

    assert invocation is not None, "audit_mode=must_be_selected"
    assert (invocation.mode.value, invocation.envelope) == ("dispatch-outbox-once", None)
    runtime = composition.AwsAuditRuntime(local.adapter, local.sink(), lambda: _NOW)
    assert entrypoint.run_aws_entrypoint(invocation, runtime) == 0
    _confirmar(local, [event])


def test_billing_valido_com_tenant_canonico_chega_ao_sink(local: OutboxHarness) -> None:
    billing = BillingAuditEvent(
        event_id="billing:000",
        event_type="entitlement.changed",
        aggregate_id="ba_01",
        actor_id="stripe",
        reason_code="webhook_projection",
        occurred_at=_NOW,
        attributes={"entitlement_version": 1},
    )
    event = audit_outbox_event(billing)
    local.put(event)

    assert _executar(local.adapter, local.sink()) == 0

    _confirmar(local, [event])
    [record] = local.local_records()
    assert (record.tenant_id, record.event_type, record.payload) == (
        "_billing",
        "entitlement.changed",
        {
            "actor_id": "stripe",
            "reason_code": "webhook_projection",
            "attributes": {"entitlement_version": 1},
        },
    )


def test_falha_real_de_fsync_retorna_um_e_preserva_pending(
    local: OutboxHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    event = evento("fsync")
    local.put(event)
    sink = local.sink()
    real_fsync = os.fsync
    reached: list[int] = []

    def falhar(descriptor: int) -> None:
        reached.append(descriptor)
        raise OSError("disk=unavailable")

    monkeypatch.setattr(os, "fsync", falhar)
    assert _executar(local.adapter, sink) == 1
    assert reached
    assert local.stored(event.event_id) == event
    monkeypatch.setattr(os, "fsync", real_fsync)
    assert _executar(local.restart(), local.sink()) == 0
    assert _executar(local.restart(), local.sink()) == 0
    _confirmar(local, [event])


def test_falha_de_persistencia_apos_append_e_retry_sao_idempotentes(
    local: OutboxHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    event = evento("first")
    local.put(event)
    local.fail_marker_once(monkeypatch, OSError("sqlite=unavailable"))

    assert _executar(local.adapter, local.sink()) == 1
    assert local.stored("first") == event
    assert local.local_records() == [event]
    assert _executar(local.restart(), local.sink()) == 0
    assert _executar(local.restart(), local.sink()) == 0
    _confirmar(local, [event])


def test_cem_poison_limitam_uma_passada_e_proxima_invocacao_entrega_bom(
    local: OutboxHarness,
) -> None:
    poison = [evento(f"poison/{index:03}", index) for index in range(100)]
    for event in poison:
        local.put(event)
    good = evento("bom", 101)
    local.put(good)
    sink = _sink_poison(local)

    assert _executar(local.adapter, sink) == 1
    assert local.stored("bom") == good
    assert local.local_records() == []
    _cursor(local, 1, True)
    assert local.restart().read_outbox_page(None, 100).events == (good,)
    assert _executar(local.restart(), sink) == 0
    _confirmar(local, [good])
    assert all(local.stored(event.event_id) == event for event in poison)
    _cursor(local, 2, False)
    assert local.restart().read_outbox_page(None, 100).events == tuple(poison)


def test_wrap_retry_e_novo_runtime_preservam_eventos_e_idempotencia(local: OutboxHarness) -> None:
    first_page = _pagina_cheia(local)
    poison, good, behind = first_page[0], evento("bom", 101), evento("atras", -1)
    sink = _sink_poison(local)
    assert _executar(local.adapter, sink) == 1
    _cursor(local, 1, True)
    _confirmar(local, first_page[1:])
    assert local.stored("poison") == poison
    assert local.restart().read_outbox_page(None, 100).events == (good,)
    local.put(behind)
    assert _executar(local.restart(), sink) == 0
    _cursor(local, 2, False)
    assert local.stored("atras") == behind
    _confirmar(local, [*first_page[1:], good])
    assert _executar(local.restart(), sink) == 1
    _cursor(local, 3, False)
    assert local.stored("poison") == poison
    _confirmar(local, [*first_page[1:], good, behind])
    assert sum(event.event_id == "poison" for event in sink.attempts) == 2
    sink.before = None
    assert _executar(local.restart(), sink) == 0
    _cursor(local, 4, False)
    assert _executar(local.restart(), local.sink()) == 0
    _cursor(local, 5, False)
    _confirmar(local, [*first_page, good, behind])


def test_sink_local_aceita_barra_no_id_sem_duplicar_no_reinicio(local: OutboxHarness) -> None:
    event = evento("invalido/id")
    local.put(event)

    assert _executar(local.adapter, local.sink()) == 0
    assert _executar(local.restart(), local.sink()) == 0

    _confirmar(local, [event])
