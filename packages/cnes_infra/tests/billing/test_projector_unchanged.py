"""Testes do projetor sem mudança de entitlement: sem bump de versão nem auditoria."""

import logging
from datetime import timedelta

import pytest

from cnes_domain.billing.errors import RetryableBillingError
from cnes_domain.billing.inbox import InboxProcessingState
from packages.cnes_infra.tests.billing.test_projector import (
    PRICE_V2,
    STRONG,
    ScriptedProjection,
    make_state,
    projector_env,
)

PROCESSED = InboxProcessingState.PROCESSED


def _process_two(env, projection=None):
    env.accept("evt_01")
    env.accept("evt_02")
    projector = env.projector(projection=projection)
    return projector.process("evt_01"), projector.process("evt_02")


def test_evento_repetido_sem_mudanca_mantem_versao_e_conclui_inbox():
    with projector_env() as env:
        first, second = _process_two(env)
        snapshot = env.snapshot()
        audits = len(env.all_audit_payloads())
        state = env.inbox_state("evt_02")
        record = env.inbox.get_recovery_record("evt_02", STRONG)
        skipped = env.audit_rows("entitlement.changed", "evt_02", 2)
    assert (first.entitlement_version, second.entitlement_version) == (1, 1)
    assert second.applied is True
    assert snapshot.entitlement_version == 1
    assert snapshot.source_event_id == "evt_01"
    assert state is PROCESSED
    assert record.state is PROCESSED
    assert audits == 2
    assert skipped == []


def test_evento_sem_mudanca_registra_log_de_projecao_inalterada(caplog):
    with projector_env() as env:
        with caplog.at_level(logging.INFO, logger="cnes_infra.billing.projector"):
            _process_two(env)
    assert "stripe_projection_unchanged event_id=evt_02 version=1" in caplog.text


def test_campo_alterado_incrementa_a_versao():
    with projector_env() as env:
        env.stripe.get_current_state.side_effect = [
            make_state(),
            make_state(stripe_price_id=PRICE_V2),
        ]
        _, second = _process_two(env)
        snapshot = env.snapshot()
    assert second.entitlement_version == 2
    assert snapshot.plan_version_id == "plan_v2"
    assert snapshot.source_event_id == "evt_02"


def test_validade_maior_sem_mudanca_de_campos_incrementa_a_versao():
    with projector_env() as env:
        env.accept("evt_01")
        env.accept("evt_02")
        env.projector().process("evt_01")
        env.clock.advance(timedelta(days=40))
        second = env.projector().process("evt_02")
        snapshot = env.snapshot()
    assert second.entitlement_version == 2
    assert snapshot.valid_until > make_state().period_end + timedelta(hours=72)


def test_conclusao_perdida_repete_ate_gravar_na_proxima_tentativa():
    with projector_env() as env:
        env.accept("evt_01")
        env.accept("evt_02")
        env.projector().process("evt_01")
        projection = ScriptedProjection(env.projection, [], [False])
        result = env.projector(projection=projection).process("evt_02")
    assert result.applied is True
    assert result.entitlement_version == 1
    assert projection.completions == 2
    assert env.stripe.get_current_state.call_count == 3


def test_conclusao_perdida_esgota_orcamento_de_cas():
    with projector_env() as env:
        env.accept("evt_01")
        env.accept("evt_02")
        env.projector().process("evt_01")
        projection = ScriptedProjection(env.projection, [], [False, False, False])
        with pytest.raises(RetryableBillingError) as raised:
            env.projector(projection=projection).process("evt_02")
        state = env.inbox_state("evt_02")
    assert raised.value.code == "snapshot_cas_exhausted"
    assert state is InboxProcessingState.FAILED_RETRYABLE
