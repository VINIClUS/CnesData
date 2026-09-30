"""Testes dos modelos de inbox de webhooks Stripe."""

from dataclasses import fields, replace
from datetime import UTC, datetime, timedelta, timezone

import pytest

from cnes_domain.billing.inbox import (
    STRIPE_EVENT_PAGE_LIMIT,
    InboxAcceptResult,
    InboxClaim,
    InboxDisposition,
    InboxProcessingState,
    InboxRecoveryRecord,
    ProjectionResult,
    ReconciliationRequest,
    ReconciliationResult,
    RecoveryRequest,
    RecoveryResult,
    ReservationRecoveryRequest,
    ReservationRecoveryResult,
    StripeEvent,
    StripeEventListRequest,
    StripeEventPage,
    StripeRecoveryCursor,
    require_cursor_successor,
)

_NOW = datetime(2026, 9, 1, 12, tzinfo=UTC)
_NAIVE = _NOW.replace(tzinfo=None)
_OFFSET = datetime(2026, 9, 1, 12, tzinfo=timezone(timedelta(hours=-3)))
_SHA = "a" * 64
_ACTIVE = [
    InboxProcessingState.PENDING,
    InboxProcessingState.PROCESSING,
    InboxProcessingState.FAILED_RETRYABLE,
]
_TERMINAL = [
    InboxProcessingState.PROCESSED,
    InboxProcessingState.FAILED_FINAL,
    InboxProcessingState.IGNORED,
]


def _claim(**overrides: object) -> InboxClaim:
    values = {
        "event_id": "evt_1",
        "event_type": "invoice.paid",
        "customer_id": "cus_1",
        "subscription_id": "sub_1",
        "attempt": 1,
        "acquired": True,
    }
    return InboxClaim(**{**values, **overrides})  # type: ignore[arg-type]


def _record(**overrides: object) -> InboxRecoveryRecord:
    values = {
        "state": InboxProcessingState.PENDING,
        "attempt": 0,
        "due_at": _NOW,
        "due_index_key": "due#1",
    }
    return InboxRecoveryRecord(**{**values, **overrides})  # type: ignore[arg-type]


def _event(**overrides: object) -> StripeEvent:
    values = {
        "event_id": "evt_1",
        "event_type": "invoice.paid",
        "created_at": _NOW,
        "stripe_customer_id": "cus_1",
        "stripe_subscription_id": "sub_1",
        "payload_sha256": _SHA,
    }
    return StripeEvent(**{**values, **overrides})  # type: ignore[arg-type]


def _list_request(**overrides: object) -> StripeEventListRequest:
    values = {"created_gte": _NOW, "starting_after": "evt_0", "limit": 100}
    return StripeEventListRequest(**{**values, **overrides})  # type: ignore[arg-type]


def _cursor(**overrides: object) -> StripeRecoveryCursor:
    values = {
        "cycle_id": "cycle_1",
        "created_gte": _NOW,
        "starting_after": "evt_0",
        "version": 1,
    }
    return StripeRecoveryCursor(**{**values, **overrides})  # type: ignore[arg-type]


def test_enums_expoem_valores_do_contrato() -> None:
    assert [d.value for d in InboxDisposition] == ["accepted", "duplicate", "ignored"]
    assert [s.value for s in InboxProcessingState] == [
        "pending",
        "processing",
        "processed",
        "failed_retryable",
        "failed_final",
        "ignored",
    ]
    assert STRIPE_EVENT_PAGE_LIMIT == 100


def test_instancia_inbox_accept_result_com_todos_os_campos() -> None:
    result = InboxAcceptResult(event_id="evt_1", disposition=InboxDisposition.ACCEPTED)
    assert result.event_id == "evt_1"
    assert result.disposition is InboxDisposition.ACCEPTED


def test_rejeita_inbox_accept_result_com_evento_vazio() -> None:
    with pytest.raises(ValueError, match="blank_value field=event_id"):
        InboxAcceptResult(event_id=" ", disposition=InboxDisposition.DUPLICATE)


def test_instancia_inbox_claim_com_todos_os_campos() -> None:
    claim = InboxClaim(
        event_id="evt_1",
        event_type="invoice.paid",
        customer_id="cus_1",
        subscription_id="sub_1",
        attempt=2,
        acquired=True,
    )
    assert (claim.attempt, claim.acquired, claim.subscription_id) == (2, True, "sub_1")


def test_inbox_claim_exige_attempt_positivo_quando_adquirido() -> None:
    assert _claim(acquired=True, attempt=1).attempt == 1
    assert _claim(acquired=False, attempt=None).attempt is None
    for attempt in (None, 0):
        with pytest.raises(ValueError, match="reason=claim_attempt_mismatch"):
            _claim(acquired=True, attempt=attempt)
    for attempt in (0, 1):
        with pytest.raises(ValueError, match="reason=claim_attempt_mismatch"):
            _claim(acquired=False, attempt=attempt)


def test_rejeita_claim_com_acquired_nao_booleano() -> None:
    with pytest.raises(ValueError, match="reason=claim_acquired_not_bool"):
        _claim(acquired=1)


def test_aceita_claim_sem_subscription_e_rejeita_subscription_vazia() -> None:
    assert _claim(subscription_id=None).subscription_id is None
    with pytest.raises(ValueError, match="blank_value field=subscription_id"):
        _claim(subscription_id=" ")


@pytest.mark.parametrize("field", ["event_id", "event_type", "customer_id"])
def test_rejeita_claim_com_identificador_vazio(field: str) -> None:
    with pytest.raises(ValueError, match=f"blank_value field={field}"):
        _claim(**{field: ""})


def test_instancia_recovery_record_com_todos_os_campos() -> None:
    record = InboxRecoveryRecord(
        state=InboxProcessingState.FAILED_RETRYABLE,
        attempt=3,
        due_at=_NOW,
        due_index_key="due#9",
    )
    assert record.state is InboxProcessingState.FAILED_RETRYABLE
    assert (record.attempt, record.due_at, record.due_index_key) == (3, _NOW, "due#9")


def test_recovery_record_tem_exatamente_quatro_campos() -> None:
    assert [f.name for f in fields(InboxRecoveryRecord)] == [
        "state",
        "attempt",
        "due_at",
        "due_index_key",
    ]


@pytest.mark.parametrize("state", _ACTIVE)
def test_estado_ativo_exige_due_at_e_chave(state: InboxProcessingState) -> None:
    assert _record(state=state).due_index_key == "due#1"
    for overrides in ({"due_at": None}, {"due_index_key": None}):
        with pytest.raises(ValueError, match="reason=active_state_requires_due"):
            _record(state=state, **overrides)
    with pytest.raises(ValueError, match="blank_value field=due_index_key"):
        _record(state=state, due_index_key=" ")
    with pytest.raises(ValueError, match="datetime_not_utc field=due_at"):
        _record(state=state, due_at=_NAIVE)


@pytest.mark.parametrize("state", _TERMINAL)
def test_estado_terminal_proibe_due_at_e_chave(state: InboxProcessingState) -> None:
    assert _record(state=state, due_at=None, due_index_key=None).due_at is None
    for overrides in (
        {"due_at": _NOW},
        {"due_index_key": "due#1"},
        {"due_at": _NOW, "due_index_key": "due#1"},
    ):
        with pytest.raises(ValueError, match="reason=terminal_state_forbids_due"):
            _record(state=state, **{"due_at": None, "due_index_key": None, **overrides})


def test_rejeita_recovery_record_com_attempt_negativo() -> None:
    with pytest.raises(ValueError, match="negative_value field=attempt"):
        _record(attempt=-1)


def test_instancia_projection_result_com_todos_os_campos() -> None:
    result = ProjectionResult(event_id="evt_1", applied=True, entitlement_version=4)
    assert (result.event_id, result.applied, result.entitlement_version) == ("evt_1", True, 4)


def test_projection_result_aceita_nao_aplicado_sem_versao() -> None:
    assert ProjectionResult("evt_1", False, None).entitlement_version is None


def test_projection_result_exige_versao_quando_aplicado() -> None:
    with pytest.raises(ValueError, match="reason=applied_requires_version"):
        ProjectionResult("evt_1", True, None)


@pytest.mark.parametrize("version", [0, -1, True])
def test_rejeita_projection_result_com_versao_invalida(version: object) -> None:
    with pytest.raises(ValueError, match="positive_value_required field=entitlement_version"):
        ProjectionResult("evt_1", False, version)  # type: ignore[arg-type]


def test_rejeita_projection_result_com_applied_nao_booleano() -> None:
    with pytest.raises(ValueError, match="reason=applied_not_bool"):
        ProjectionResult("evt_1", 1, 2)  # type: ignore[arg-type]


def test_rejeita_projection_result_com_evento_vazio() -> None:
    with pytest.raises(ValueError, match="blank_value field=event_id"):
        ProjectionResult("", False, None)


def test_instancia_recovery_request_com_todos_os_campos() -> None:
    request = RecoveryRequest(lookback_hours=24, batch_size=100)
    assert (request.lookback_hours, request.batch_size) == (24, 100)


def test_recovery_request_aceita_batch_minimo() -> None:
    assert RecoveryRequest(lookback_hours=1, batch_size=1).batch_size == 1


@pytest.mark.parametrize("batch_size", [0, 101, -5])
def test_rejeita_recovery_request_com_batch_fora_da_faixa(batch_size: int) -> None:
    with pytest.raises(ValueError, match="reason=batch_size_out_of_range"):
        RecoveryRequest(lookback_hours=1, batch_size=batch_size)


@pytest.mark.parametrize("batch_size", [True, 1.5, "10"])
def test_rejeita_recovery_request_com_batch_nao_inteiro(batch_size: object) -> None:
    with pytest.raises(ValueError, match="reason=batch_size_out_of_range"):
        RecoveryRequest(lookback_hours=1, batch_size=batch_size)  # type: ignore[arg-type]


def test_rejeita_recovery_request_com_lookback_invalido() -> None:
    with pytest.raises(ValueError, match="positive_value_required field=lookback_hours"):
        RecoveryRequest(lookback_hours=0, batch_size=10)


def test_instancia_recovery_result_com_todos_os_campos() -> None:
    result = RecoveryResult(scanned=5, imported=2, reprocessed=1, failed=0, next_cursor="c1")
    assert (result.scanned, result.imported, result.reprocessed) == (5, 2, 1)
    assert (result.failed, result.next_cursor) == (0, "c1")


@pytest.mark.parametrize("field", ["scanned", "imported", "reprocessed", "failed"])
def test_rejeita_recovery_result_com_contador_negativo(field: str) -> None:
    values = {"scanned": 1, "imported": 1, "reprocessed": 1, "failed": 1, "next_cursor": None}
    with pytest.raises(ValueError, match=f"negative_value field={field}"):
        RecoveryResult(**{**values, field: -1})  # type: ignore[arg-type]


def test_rejeita_recovery_result_com_cursor_vazio() -> None:
    with pytest.raises(ValueError, match="blank_value field=next_cursor"):
        RecoveryResult(0, 0, 0, 0, " ")


def test_instancia_reservation_recovery_request_com_todos_os_campos() -> None:
    request = ReservationRecoveryRequest(now=_NOW, limit=10, cursor="c1")
    assert (request.now, request.limit, request.cursor) == (_NOW, 10, "c1")


@pytest.mark.parametrize("now", [_NAIVE, _OFFSET])
def test_rejeita_reservation_recovery_request_com_now_nao_utc(now: datetime) -> None:
    with pytest.raises(ValueError, match="datetime_not_utc field=now"):
        ReservationRecoveryRequest(now=now, limit=10, cursor=None)


def test_rejeita_reservation_recovery_request_com_limite_ou_cursor_invalido() -> None:
    with pytest.raises(ValueError, match="positive_value_required field=limit"):
        ReservationRecoveryRequest(now=_NOW, limit=0, cursor=None)
    with pytest.raises(ValueError, match="blank_value field=cursor"):
        ReservationRecoveryRequest(now=_NOW, limit=1, cursor="")


def test_instancia_reservation_recovery_result_com_todos_os_campos() -> None:
    result = ReservationRecoveryResult(examined=5, released=5, next_cursor="c1")
    assert (result.examined, result.released, result.next_cursor) == (5, 5, "c1")


def test_rejeita_reservation_recovery_result_com_liberadas_acima_de_examinadas() -> None:
    with pytest.raises(ValueError, match="reason=released_exceeds_examined"):
        ReservationRecoveryResult(examined=1, released=2, next_cursor=None)


def test_rejeita_reservation_recovery_result_com_contador_negativo_ou_cursor_vazio() -> None:
    with pytest.raises(ValueError, match="negative_value field=examined"):
        ReservationRecoveryResult(examined=-1, released=0, next_cursor=None)
    with pytest.raises(ValueError, match="negative_value field=released"):
        ReservationRecoveryResult(examined=1, released=-1, next_cursor=None)
    with pytest.raises(ValueError, match="blank_value field=next_cursor"):
        ReservationRecoveryResult(examined=1, released=1, next_cursor=" ")


def test_instancia_reconciliation_request_com_todos_os_campos() -> None:
    request = ReconciliationRequest(limit=10, cursor="c1")
    assert (request.limit, request.cursor) == (10, "c1")


def test_rejeita_reconciliation_request_com_limite_ou_cursor_invalido() -> None:
    with pytest.raises(ValueError, match="positive_value_required field=limit"):
        ReconciliationRequest(limit=0, cursor=None)
    with pytest.raises(ValueError, match="blank_value field=cursor"):
        ReconciliationRequest(limit=1, cursor=" ")


def test_instancia_reconciliation_result_com_todos_os_campos() -> None:
    result = ReconciliationResult(
        examined=9, drift_found=3, corrected=3, failed=1, next_cursor="c1",
    )
    assert (result.examined, result.drift_found, result.corrected) == (9, 3, 3)
    assert (result.failed, result.next_cursor) == (1, "c1")


def test_rejeita_reconciliation_result_com_corrigidos_acima_de_drift() -> None:
    with pytest.raises(ValueError, match="reason=corrected_exceeds_drift"):
        ReconciliationResult(examined=5, drift_found=1, corrected=2, failed=0, next_cursor=None)


@pytest.mark.parametrize("field", ["examined", "drift_found", "corrected", "failed"])
def test_rejeita_reconciliation_result_com_contador_negativo(field: str) -> None:
    values = {"examined": 1, "drift_found": 1, "corrected": 0, "failed": 0, "next_cursor": None}
    with pytest.raises(ValueError, match=f"negative_value field={field}"):
        ReconciliationResult(**{**values, field: -1})  # type: ignore[arg-type]


def test_rejeita_reconciliation_result_com_cursor_vazio() -> None:
    with pytest.raises(ValueError, match="blank_value field=next_cursor"):
        ReconciliationResult(0, 0, 0, 0, "")


def test_instancia_stripe_event_com_todos_os_campos() -> None:
    event = StripeEvent(
        event_id="evt_1",
        event_type="invoice.paid",
        created_at=_NOW,
        stripe_customer_id="cus_1",
        stripe_subscription_id="sub_1",
        payload_sha256=_SHA,
    )
    assert (event.event_id, event.event_type, event.created_at) == ("evt_1", "invoice.paid", _NOW)
    assert (event.stripe_customer_id, event.stripe_subscription_id) == ("cus_1", "sub_1")
    assert event.payload_sha256 == _SHA


def test_stripe_event_aceita_cliente_e_assinatura_ausentes() -> None:
    event = _event(stripe_customer_id=None, stripe_subscription_id=None)
    assert event.stripe_customer_id is None
    assert event.stripe_subscription_id is None


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("event_id", "", "blank_value field=event_id"),
        ("event_type", " ", "blank_value field=event_type"),
        ("created_at", _NAIVE, "datetime_not_utc field=created_at"),
        ("stripe_customer_id", " ", "blank_value field=stripe_customer_id"),
        ("stripe_subscription_id", " ", "blank_value field=stripe_subscription_id"),
        ("payload_sha256", "ABC", "invalid_sha256 field=payload_sha256"),
    ],
)
def test_rejeita_stripe_event_invalido(field: str, value: object, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        _event(**{field: value})


def test_instancia_stripe_event_list_request_com_todos_os_campos() -> None:
    request = StripeEventListRequest(created_gte=_NOW, starting_after="evt_0", limit=50)
    assert (request.created_gte, request.starting_after, request.limit) == (_NOW, "evt_0", 50)


def test_lista_eventos_aceita_limite_entre_1_e_100() -> None:
    assert _list_request(limit=1).limit == 1
    assert _list_request(limit=100).limit == 100
    for limit in (0, 101, True):
        with pytest.raises(ValueError, match="reason=limit_out_of_range"):
            _list_request(limit=limit)


def test_rejeita_lista_eventos_com_limite_nao_inteiro() -> None:
    with pytest.raises(ValueError, match="reason=limit_out_of_range"):
        _list_request(limit="10")


@pytest.mark.parametrize("created_gte", [_NAIVE, _OFFSET])
def test_rejeita_lista_eventos_com_created_gte_nao_utc(created_gte: datetime) -> None:
    with pytest.raises(ValueError, match="datetime_not_utc field=created_gte"):
        _list_request(created_gte=created_gte)


def test_lista_eventos_valida_starting_after() -> None:
    assert _list_request(starting_after=None).starting_after is None
    with pytest.raises(ValueError, match="blank_value field=starting_after"):
        _list_request(starting_after=" ")


def test_instancia_stripe_event_page_com_todos_os_campos() -> None:
    page = StripeEventPage(events=(_event(),), has_more=True)
    assert page.events == (_event(),)
    assert page.has_more is True


def test_stripe_event_page_aceita_pagina_vazia() -> None:
    assert StripeEventPage(events=(), has_more=False).events == ()


@pytest.mark.parametrize("has_more", [1, "true", None])
def test_rejeita_stripe_event_page_com_has_more_nao_booleano(has_more: object) -> None:
    with pytest.raises(ValueError, match="reason=has_more_not_bool"):
        StripeEventPage(events=(), has_more=has_more)  # type: ignore[arg-type]


def test_instancia_stripe_recovery_cursor_com_todos_os_campos() -> None:
    cursor = StripeRecoveryCursor(
        cycle_id="cycle_1", created_gte=_NOW, starting_after="evt_0", version=3,
    )
    assert (cursor.cycle_id, cursor.created_gte) == ("cycle_1", _NOW)
    assert (cursor.starting_after, cursor.version) == ("evt_0", 3)


def test_stripe_recovery_cursor_tem_exatamente_quatro_campos() -> None:
    assert [f.name for f in fields(StripeRecoveryCursor)] == [
        "cycle_id",
        "created_gte",
        "starting_after",
        "version",
    ]


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"cycle_id": " "}, "blank_value field=cycle_id"),
        ({"created_gte": _NAIVE}, "datetime_not_utc field=created_gte"),
        ({"starting_after": " "}, "blank_value field=starting_after"),
        ({"version": 0}, "positive_value_required field=version"),
    ],
)
def test_rejeita_stripe_recovery_cursor_invalido(overrides: dict, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        _cursor(**overrides)


def test_cursor_aceita_starting_after_ausente() -> None:
    assert _cursor(starting_after=None).starting_after is None


def test_advance_preserva_ciclo_e_incrementa_versao_em_um() -> None:
    original = _cursor(version=4)
    advanced = original.advance("evt_9")
    assert advanced == replace(original, starting_after="evt_9", version=5)
    assert original.version == 4
    assert original.advance(None).starting_after is None


def test_require_cursor_successor_aceita_resultado_de_advance() -> None:
    expected = _cursor()
    require_cursor_successor(expected, expected.advance("evt_9"))


@pytest.mark.parametrize(
    "overrides",
    [
        {"cycle_id": "cycle_2", "version": 2},
        {"created_gte": _NOW + timedelta(hours=1), "version": 2},
        {"version": 1},
        {"version": 3},
    ],
)
def test_require_cursor_successor_rejeita_substituto_invalido(overrides: dict) -> None:
    with pytest.raises(ValueError, match="reason=cursor_not_successor"):
        require_cursor_successor(_cursor(), replace(_cursor(), **overrides))
