"""Testes da varredura de revogações pendentes (revoke-pending)."""

from typing import Any, cast

import boto3
import pytest
from moto import mock_aws

from cnes_domain.billing.errors import (
    BillingDependencyError,
    PermanentBillingError,
    RetryableBillingError,
)
from cnes_domain.billing.inbox import ReconciliationRequest
from cnes_domain.billing.revocation import RevocationResult
from cnes_infra.billing.keys import revocation_sweep_cursor_key, stripe_reconciliation_cursor_key
from cnes_infra.billing.reconciliation_cursor import DynamoReconciliationCursor
from cnes_infra.billing.revocation_sweep import (
    REVOCATION_SWEEP_ACTOR_ID,
    PendingRevocationPort,
    RevocationSweep,
    RevocationSweepDependencies,
    RevocationSweepResult,
    SweepCursorPort,
)
from cnes_infra.control_plane.dynamodb_keys import item_key
from packages.cnes_infra.tests.billing.billing_factories import NOW, TABLE_NAME, create_table
from packages.cnes_infra.tests.billing.reconciliation_support import (
    FakeCatalog,
    FakeCursor,
    FakeMetrics,
    stripe_account,
)


class _Enforcer:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []
        self.results: dict[str, RevocationResult | Exception | None] = {}

    def resume_pending(self, billing_account_id: str, actor_id: str) -> RevocationResult | None:
        self.calls.append((billing_account_id, actor_id))
        outcome = self.results.get(billing_account_id)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def _sweep(*account_ids: str) -> tuple[RevocationSweep, dict[str, Any]]:
    parts: dict[str, Any] = {
        "catalog": FakeCatalog([stripe_account(a) for a in account_ids], None),
        "cursor": FakeCursor(),
        "enforcer": _Enforcer(),
        "metrics": FakeMetrics(),
    }
    deps = RevocationSweepDependencies(
        parts["catalog"], parts["cursor"], parts["enforcer"], parts["metrics"], lambda: NOW
    )
    return RevocationSweep(deps), parts


def test_retoma_cada_conta_da_pagina_e_avanca_o_cursor() -> None:
    sweep, parts = _sweep("ba_01", "ba_02")
    parts["enforcer"].results["ba_02"] = RevocationResult(4, ("run-1", "run-2"), (), ("run-3",))
    result = sweep.run(ReconciliationRequest(10, None))
    assert result == RevocationSweepResult(2, 1, 2, 1, 0, None)
    assert parts["enforcer"].calls == [
        ("ba_01", REVOCATION_SWEEP_ACTOR_ID),
        ("ba_02", REVOCATION_SWEEP_ACTOR_ID),
    ]
    assert parts["cursor"].saves == ["ba_01", "ba_02", None]


def test_runs_fenceadas_emitem_metrica_de_cancelamento_por_revogacao() -> None:
    sweep, parts = _sweep("ba_01")
    parts["enforcer"].results["ba_01"] = RevocationResult(4, ("run-1",), ())
    sweep.run(ReconciliationRequest(10, None))
    [metric] = parts["metrics"].named("RunsCanceledByRevocation")
    assert (metric.value, dict(metric.dimensions)) == (1, {"Reason": "revocation_resumed"})


def test_sem_runs_fenceadas_nao_emite_metrica() -> None:
    sweep, parts = _sweep("ba_01")
    sweep.run(ReconciliationRequest(10, None))
    assert parts["metrics"].emitted == []


def test_cursor_da_requisicao_prevalece_e_proxima_pagina_e_devolvida() -> None:
    sweep, parts = _sweep("ba_01", "ba_02", "ba_03")
    parts["catalog"].next_cursor = "ba_02"
    result = sweep.run(ReconciliationRequest(1, "ba_01"))
    assert parts["catalog"].calls == [(1, "ba_01")]
    assert result.next_cursor == "ba_02"
    assert parts["cursor"].saves == ["ba_02", "ba_02"]


def test_cursor_persistido_e_usado_sem_cursor_na_requisicao() -> None:
    sweep, parts = _sweep("ba_01", "ba_02")
    parts["cursor"].save(parts["cursor"].stored, "ba_01")
    sweep.run(ReconciliationRequest(10, None))
    assert parts["catalog"].calls == [(10, "ba_01")]


def test_falha_da_conta_conta_em_failed_e_o_cursor_avanca(caplog) -> None:
    sweep, parts = _sweep("ba_01", "ba_02")
    parts["enforcer"].results["ba_01"] = PermanentBillingError("revocation_progress_contended")
    result = sweep.run(ReconciliationRequest(10, None))
    assert (result.examined, result.failed) == (2, 1)
    assert parts["cursor"].saves == ["ba_01", "ba_02", None]
    assert "billing_revoke_pending_failed billing_account_id=ba_01" in caplog.text


def test_indisponibilidade_do_dynamodb_para_o_lote_sem_avancar() -> None:
    sweep, parts = _sweep("ba_01", "ba_02")
    parts["enforcer"].results["ba_02"] = BillingDependencyError("dynamodb_unavailable")
    result = sweep.run(ReconciliationRequest(10, None))
    assert (result.examined, result.failed, result.next_cursor) == (2, 1, "ba_01")
    assert parts["cursor"].saves == ["ba_01"]


def test_cursor_disputado_levanta_retryable() -> None:
    sweep, parts = _sweep("ba_01")
    parts["cursor"].contended_after = 0
    with pytest.raises(RetryableBillingError) as error:
        sweep.run(ReconciliationRequest(10, None))
    assert error.value.code == "revocation_sweep_cursor_contended"


def test_defeito_inesperado_propaga_sem_avancar() -> None:
    sweep, parts = _sweep("ba_01")
    parts["enforcer"].results["ba_01"] = RuntimeError("bug")
    with pytest.raises(RuntimeError):
        sweep.run(ReconciliationRequest(10, None))
    assert parts["cursor"].saves == []


def test_resultado_rejeita_contador_negativo() -> None:
    with pytest.raises(ValueError, match="failed"):
        RevocationSweepResult(0, 0, 0, 0, -1, None)


def test_fakes_cumprem_os_protocolos() -> None:
    assert isinstance(_Enforcer(), PendingRevocationPort)
    assert isinstance(FakeCursor(), SweepCursorPort)


def test_cursor_da_varredura_usa_chave_propria_sem_tocar_a_reconciliacao() -> None:
    assert revocation_sweep_cursor_key() == ("BILLING#SYSTEM", "REVOCATION#PENDING")
    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        create_table(client)
        cursor = DynamoReconciliationCursor(
            client, TABLE_NAME, lambda: NOW, revocation_sweep_cursor_key()
        )
        saved = cursor.save(cursor.load(), "ba_07")
        assert cursor.load() == saved
        sweep_item = client.get_item(
            TableName=TABLE_NAME, Key=item_key(*revocation_sweep_cursor_key()),
        )
        reconcile_item = client.get_item(
            TableName=TABLE_NAME, Key=item_key(*stripe_reconciliation_cursor_key()),
        )
    assert cast("Any", sweep_item)["Item"]["position"] == {"S": "ba_07"}
    assert "Item" not in reconcile_item
