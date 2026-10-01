"""Revogação com finalização concorrente do Run e quedas antes da auditoria."""

from typing import Any

import pytest

from cnes_domain.billing.revocation import RevocationPhase
from cnes_domain.control_plane.commands import FinalizeRunCancellation
from cnes_domain.control_plane.entities import OutboxEvent
from cnes_domain.control_plane.enums import RunState
from cnes_infra.billing.audit_outbox import DynamoBillingAudit
from packages.cnes_infra.tests.billing.billing_factories import NOW
from packages.cnes_infra.tests.billing.quota_support import ACCOUNT, TENANT
from packages.cnes_infra.tests.billing.revocation_support import (
    RUN_ID,
    RevEnv,
    open_env,
    stored_run,
    transaction_entities,
)
from packages.cnes_infra.tests.billing.test_dynamodb_revocation_service import (
    COMMAND,
    RecordingExecutor,
    ServiceOptions,
    assert_run_canceled,
    build_service,
    outbox_events,
    seed_simple_run,
)


@pytest.fixture
def env() -> Any:
    with open_env() as opened:
        yield opened


class CoordinatorExecutor(RecordingExecutor):
    def __init__(self, env: RevEnv) -> None:
        super().__init__()
        self.env = env

    def cancel(self, request: Any) -> None:
        super().cancel(request)
        event = OutboxEvent(
            tenant_id=TENANT,
            event_id=f"run.canceled:{TENANT}:{request.run_id}",
            event_type="run.canceled",
            aggregate_id=request.run_id,
            payload={"dataset_name": "cnes"},
            created_at=NOW,
            delivered_at=None,
        )
        finalize = FinalizeRunCancellation(
            tenant_id=TENANT, run_id=request.run_id,
            expected_state=RunState.CANCEL_REQUESTED, canceled_at=self.env.clock.now(),
        )
        self.env.plane.finalize_run_cancellation(finalize, event)


class FlakyAudit:
    def __init__(self, inner: Any) -> None:
        self.inner = inner
        self.failures = 1

    def append(self, event: Any) -> None:
        if event.event_type == "run.canceled" and self.failures > 0:
            self.failures -= 1
            raise RuntimeError("audit_down")
        self.inner.append(event)


def audited_run_ids(env: RevEnv) -> list[str]:
    return [
        event.aggregate_id
        for event in outbox_events(env, "run.canceled")
        if event.event_id.startswith(f"run.canceled:{ACCOUNT}") or event.tenant_id != TENANT
    ]


def test_run_finalizado_pelo_coordenador_durante_a_revogacao_e_liquidado(env: RevEnv) -> None:
    dispatch = seed_simple_run(env, RUN_ID)

    result = build_service(env, CoordinatorExecutor(env)).revoke(COMMAND)

    assert result.fenced_run_ids == (RUN_ID,)
    assert_run_canceled(env, RUN_ID, dispatch)
    assert audited_run_ids(env) == [RUN_ID]
    assert env.store.get_revocation_progress(ACCOUNT).phase is RevocationPhase.COMPLETE


def test_queda_na_auditoria_apos_cancelar_run_e_reauditado_na_retentativa(env: RevEnv) -> None:
    seed_simple_run(env, RUN_ID)
    audit = FlakyAudit(DynamoBillingAudit(env.spy, env.table))
    service = build_service(env, RecordingExecutor(), ServiceOptions(audit=audit))

    with pytest.raises(RuntimeError, match="audit_down"):
        service.revoke(COMMAND)
    assert stored_run(env).state is RunState.CANCELED
    assert audited_run_ids(env) == []
    service.revoke(COMMAND)

    assert audited_run_ids(env) == [RUN_ID]
    assert env.store.get_revocation_progress(ACCOUNT).phase is RevocationPhase.COMPLETE


def test_queda_ao_salvar_progresso_apos_cancelar_run_e_reauditado_na_retentativa(
    env: RevEnv,
) -> None:
    seed_simple_run(env, RUN_ID)
    service = build_service(env, RecordingExecutor())

    def crash() -> None:
        entities = transaction_entities(env.spy.transactions[-1])
        if "BILLINGREVOCATIONPROGRESS" in entities and stored_run(env).state is RunState.CANCELED:
            env.spy.before_transact = None
            raise RuntimeError("crash_injected")

    env.spy.before_transact = crash

    with pytest.raises(RuntimeError, match="crash_injected"):
        service.revoke(COMMAND)
    assert audited_run_ids(env) == [RUN_ID]
    service.revoke(COMMAND)

    assert audited_run_ids(env) == [RUN_ID]
    assert env.store.get_revocation_progress(ACCOUNT).phase is RevocationPhase.COMPLETE
