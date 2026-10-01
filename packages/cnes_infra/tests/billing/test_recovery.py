"""Testes do WebhookRecovery (BIL-021) sobre moto com inbox, cursor e projetor reais."""

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Any

import boto3
import pytest
from moto import mock_aws

from cnes_domain.billing.errors import PermanentBillingError, RetryableBillingError
from cnes_domain.billing.inbox import (
    InboxProcessingState,
    RecoveryRequest,
    StripeEvent,
    StripeEventListRequest,
    StripeEventPage,
    StripeRecoveryCursor,
)
from cnes_domain.billing.models import ReadConsistency
from cnes_infra.billing.recovery import RecoveryDependencies, WebhookRecovery
from cnes_infra.billing.recovery_cursor import DynamoRecoveryCursor
from packages.cnes_infra.tests.billing.billing_factories import NOW, TABLE_NAME, create_table
from packages.cnes_infra.tests.billing.test_projector import (
    CUSTOMER,
    SHA,
    SUBSCRIPTION,
    UPDATED,
    ProjectorEnv,
    make_state,
)

STRONG = ReadConsistency.STRONG
REQUEST = RecoveryRequest(72, 100)
PROCESSED = InboxProcessingState.PROCESSED
FAILED_RETRYABLE = InboxProcessingState.FAILED_RETRYABLE
ALL_IDS = [f"evt_{number:03d}" for number in range(205, 0, -1)]


def stripe_event(
    event_id: str,
    created_at: datetime = NOW,
    event_type: str = UPDATED,
    customer: str | None = CUSTOMER,
) -> StripeEvent:
    return StripeEvent(event_id, event_type, created_at, customer, SUBSCRIPTION, SHA)


def page(*event_ids: str, has_more: bool = False) -> StripeEventPage:
    return StripeEventPage(tuple(stripe_event(event_id) for event_id in event_ids), has_more)


def serve_pages(ids: list[str]) -> Callable[[StripeEventListRequest], StripeEventPage]:
    def serve(request: StripeEventListRequest) -> StripeEventPage:
        start = 0 if request.starting_after is None else ids.index(request.starting_after) + 1
        chunk = ids[start : start + request.limit]
        return page(*chunk, has_more=start + request.limit < len(ids))

    return serve


class RecoveryEnv(ProjectorEnv):
    def __init__(self, client: Any) -> None:
        super().__init__(client)
        self.cursor = DynamoRecoveryCursor(client, TABLE_NAME, self.clock.now)
        self.stripe.list_events.return_value = page()

    def recovery(self, cursor: Any = None) -> WebhookRecovery:
        return WebhookRecovery(
            RecoveryDependencies(
                inbox=self.inbox,
                projector=self.projector(),
                stripe=self.stripe,
                cursor=cursor or self.cursor,
                clock=self.clock.now,
            )
        )

    def requested_starting_after(self) -> list[str | None]:
        calls = self.stripe.list_events.call_args_list
        return [call.args[0].starting_after for call in calls]

    def inbox_ids(self) -> set[str]:
        items = self.client.scan(TableName=TABLE_NAME, ConsistentRead=True)["Items"]
        return {i["event_id"]["S"] for i in items if i["entity"]["S"] == "STRIPEEVENTINBOX"}

    def drain_cycle(self, recovery: WebhookRecovery, max_runs: int = 10) -> None:
        for _ in range(max_runs):
            recovery.run(REQUEST)
            if self.cursor.load(STRONG) is None:
                return
        raise AssertionError("cycle_not_drained")


@contextmanager
def recovery_env() -> Iterator[RecoveryEnv]:
    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        create_table(client)
        yield RecoveryEnv(client)


class FlakyCursor:
    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self.armed = True

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    def advance(self, expected: Any, replacement: Any) -> bool:
        self._crash()
        return self._inner.advance(expected, replacement)

    def complete(self, expected: Any, completed_at: datetime) -> bool:
        self._crash()
        return self._inner.complete(expected, completed_at)

    def _crash(self) -> None:
        if self.armed:
            self.armed = False
            raise RuntimeError("worker_crash")


def test_recovery_reclama_processing_vencido():
    with recovery_env() as env:
        env.accept()
        env.inbox.claim("evt_01", NOW)
        env.clock.advance(timedelta(seconds=301))
        result = env.recovery().drain_inbox(100)
        state = env.inbox_state()
    assert result.reprocessed == 1
    assert (result.scanned, result.imported, result.failed) == (1, 0, 0)
    assert result.next_cursor is None
    assert state is PROCESSED


def test_failed_retryable_duravel_nao_bloqueia_sweep():
    with recovery_env() as env:
        env.stripe.get_current_state.side_effect = RetryableBillingError("stripe_unavailable")
        env.accept("evt_failed")
        result = env.recovery().run(REQUEST)
        state = env.inbox_state("evt_failed")
        cursor = env.cursor.load(STRONG)
    assert result.failed == 1
    assert state is FAILED_RETRYABLE
    assert env.stripe.list_events.call_count == 1
    assert cursor is None


def test_recovery_importa_evento_nao_entregue():
    with recovery_env() as env:
        env.stripe.list_events.return_value = page("evt_missing")
        result = env.recovery().run(REQUEST)
        state = env.inbox_state("evt_missing")
    assert result.imported == 1
    assert result.reprocessed == 1
    assert state is PROCESSED


def test_crash_apos_claim_espera_lease_antes_de_reclaim():
    with recovery_env() as env:
        env.stripe.get_current_state.side_effect = [RuntimeError("worker_crash"), make_state()]
        env.accept("evt_01")
        with pytest.raises(RuntimeError):
            env.projector().process("evt_01")
        crashed = env.inbox_state()
        due_now = env.inbox.list_recoverable(env.clock.now(), 100)
        env.clock.advance(timedelta(seconds=301))
        result = env.recovery().run(REQUEST)
        state = env.inbox_state()
    assert crashed is InboxProcessingState.PROCESSING
    assert due_now == ()
    assert result.reprocessed == 1
    assert state is PROCESSED


def test_mais_de_um_limit_percorre_paginas_mais_antigas():
    pages = [
        page(*ALL_IDS[:100], has_more=True),
        page(*ALL_IDS[100:200], has_more=True),
        page(*ALL_IDS[200:]),
        page(),
    ]
    with recovery_env() as env:
        env.stripe.list_events.side_effect = pages
        recovery = env.recovery()
        results = [recovery.run(REQUEST) for _ in range(3)]
        after_third = env.cursor.load(STRONG)
        recovery.run(REQUEST)
        states = {env.inbox_state(event_id) for event_id in ALL_IDS}
    assert env.requested_starting_after() == [None, "evt_106", "evt_006", None]
    assert sum(result.imported for result in results) == 205
    assert [result.next_cursor for result in results] == ["evt_106", "evt_006", None]
    assert after_third is None
    assert states == {PROCESSED}


def test_crash_antes_do_cas_repete_pagina_sem_perder_eventos():
    with recovery_env() as env:
        env.stripe.list_events.side_effect = serve_pages(ALL_IDS)
        flaky = FlakyCursor(env.cursor)
        recovery = env.recovery(cursor=flaky)
        with pytest.raises(RuntimeError, match="worker_crash"):
            recovery.run(REQUEST)
        stuck = env.cursor.load(STRONG)
        env.drain_cycle(recovery)
        states = {env.inbox_state(event_id) for event_id in ALL_IDS}
        snapshot = env.snapshot()
        accepted = env.inbox_ids()
    assert stuck.starting_after is None
    assert accepted == set(ALL_IDS)
    assert states == {PROCESSED}
    assert snapshot.entitlement_version == 1


def test_failed_da_pagina_conclui_e_fila_reprocessa_fora_do_lookback():
    with recovery_env() as env:
        env.stripe.get_current_state.side_effect = [
            RetryableBillingError("stripe_unavailable"),
            make_state(),
        ]
        env.stripe.list_events.side_effect = [page("evt_failed"), page()]
        recovery = env.recovery()
        first = recovery.run(REQUEST)
        failed_state = env.inbox_state("evt_failed")
        cursor_after_first = env.cursor.load(STRONG)
        env.clock.advance(timedelta(hours=73))
        second = recovery.run(REQUEST)
        final_state = env.inbox_state("evt_failed")
        second_request = env.stripe.list_events.call_args_list[1].args[0]
    assert first.failed == 1
    assert failed_state is FAILED_RETRYABLE
    assert cursor_after_first is None
    assert second.reprocessed == 1
    assert final_state is PROCESSED
    assert second_request.created_gte > NOW


def test_has_more_sem_progresso_preserva_cursor():
    with recovery_env() as env:
        env.accept("evt_106")
        claim = env.inbox.claim("evt_106", NOW)
        env.inbox.mark_processed(claim, 2)
        current = StripeRecoveryCursor("cycle-01", NOW - timedelta(hours=72), "evt_106", 1)
        env.cursor.start(current)
        env.stripe.list_events.return_value = page("evt_106", has_more=True)
        with pytest.raises(RetryableBillingError, match="stripe_cursor_not_progressing"):
            env.recovery().run(REQUEST)
        stored = env.cursor.load(STRONG)
    assert stored == current


def test_pagina_com_evento_em_processing_vivo_nao_avanca_cursor():
    with recovery_env() as env:
        env.accept("evt_busy")
        env.inbox.claim("evt_busy", NOW)
        env.stripe.list_events.return_value = page("evt_busy", has_more=True)
        with pytest.raises(RetryableBillingError, match="stripe_recovery_page_unsettled"):
            env.recovery().run(REQUEST)
        stored = env.cursor.load(STRONG)
    assert stored.starting_after is None
    assert stored.version == 1


def test_pagina_vazia_com_has_more_nao_avanca_cursor():
    with recovery_env() as env:
        env.stripe.list_events.return_value = page(has_more=True)
        with pytest.raises(RetryableBillingError, match="stripe_cursor_not_progressing"):
            env.recovery().run(REQUEST)
        stored = env.cursor.load(STRONG)
    assert stored.version == 1


def test_eventos_ignorados_da_pagina_nao_sao_processados():
    ignored = (
        stripe_event("evt_type", event_type="charge.refunded"),
        stripe_event("evt_nocustomer", customer=None),
    )
    with recovery_env() as env:
        env.stripe.list_events.return_value = StripeEventPage(ignored, False)
        result = env.recovery().run(REQUEST)
        states = {env.inbox_state(event.event_id) for event in ignored}
    assert result.imported == 0
    assert result.reprocessed == 0
    assert states == {InboxProcessingState.IGNORED}
    env.stripe.get_current_state.assert_not_called()


def test_duplicata_ja_processada_nao_e_reprocessada():
    with recovery_env() as env:
        env.accept("evt_01")
        env.projector().process("evt_01")
        env.stripe.list_events.return_value = page("evt_01")
        result = env.recovery().run(REQUEST)
    assert (result.imported, result.reprocessed, result.failed) == (0, 0, 0)
    assert env.stripe.get_current_state.call_count == 1


def test_retryable_do_list_events_propaga_sem_mutar_cursor():
    with recovery_env() as env:
        env.stripe.list_events.side_effect = RetryableBillingError("stripe_unavailable")
        with pytest.raises(RetryableBillingError, match="stripe_unavailable"):
            env.recovery().run(REQUEST)
        stored = env.cursor.load(STRONG)
    assert stored.version == 1
    assert stored.starting_after is None


def test_excecao_desconhecida_do_projetor_propaga():
    with recovery_env() as env:
        env.stripe.get_current_state.side_effect = RuntimeError("boom")
        env.accept("evt_01")
        with pytest.raises(RuntimeError, match="boom"):
            env.recovery().run(REQUEST)
    env.stripe.list_events.assert_not_called()


def test_falha_final_conta_como_failed():
    with recovery_env() as env:
        env.stripe.get_current_state.side_effect = PermanentBillingError("stripe_gone")
        env.accept("evt_01")
        result = env.recovery().drain_inbox(100)
        state = env.inbox_state()
    assert result.failed == 1
    assert state is InboxProcessingState.FAILED_FINAL


def test_pagina_liquida_cada_evento_logo_apos_a_tentativa():
    with recovery_env() as env:
        states = {"evt_b": RetryableBillingError("stripe_unavailable")}

        def current_state(request: Any) -> Any:
            outcome = states.pop("evt_b", None)
            if outcome is not None:
                raise outcome
            env.clock.advance(timedelta(seconds=31))
            return make_state()

        env.stripe.get_current_state.side_effect = current_state
        env.stripe.list_events.return_value = page("evt_b", "evt_a")
        result = env.recovery().run(REQUEST)
        cursor = env.cursor.load(STRONG)
        failed_state = env.inbox_state("evt_b")
        processed_state = env.inbox_state("evt_a")
    assert cursor is None
    assert result.failed == 1
    assert failed_state is FAILED_RETRYABLE
    assert processed_state is PROCESSED
