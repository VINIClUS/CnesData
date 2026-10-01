"""Testes da auditoria durável de run_execution.bind_failed."""

import logging

import pytest

from cnes_domain.billing.errors import BillingDependencyError, PermanentBillingError
from cnes_domain.billing.execution_policy import (
    BillingExecutionDependencies,
    BillingExecutionStarted,
)
from cnes_domain.profiles import BillingMode

from .test_execution_policy import (
    DISPATCH,
    NOW,
    _bindable,
    _permit,
    _request,
    _run,
)


class _SpyAudit:
    def __init__(self, error: Exception | None = None) -> None:
        self.events: list = []
        self.error = error

    def append(self, event) -> None:
        if self.error is not None:
            raise self.error
        self.events.append(event)


def _started(fake, audit) -> BillingExecutionStarted:
    return BillingExecutionStarted(
        BillingExecutionDependencies(fake, lambda: NOW, BillingMode.STRIPE, audit)
    )


def test_falha_de_vinculacao_grava_audit_duravel_e_repropaga() -> None:
    error = PermanentBillingError("run_execution_stale")
    fake = _bindable()
    fake.bind_error = error
    audit = _SpyAudit()

    with pytest.raises(PermanentBillingError) as raised:
        _started(fake, audit)(_run(), _request(), "exec-1", _permit())

    assert raised.value is error
    [event] = audit.events
    assert event.event_id == f"run_execution.bind_failed:tenant-1:run-1:{DISPATCH}"
    assert event.event_type == "run_execution.bind_failed"
    assert event.aggregate_id == "run-1"
    assert event.actor_id == "system:billing_execution"
    assert event.reason_code == "bind_failed"
    assert event.occurred_at == NOW
    assert dict(event.attributes) == {
        "tenant_id": "tenant-1",
        "dispatch_id": DISPATCH,
        "error_code": "run_execution_stale",
    }


def test_erro_sem_code_usa_nome_do_tipo_no_audit() -> None:
    fake = _bindable()
    fake.bind_error = RuntimeError("boom")
    audit = _SpyAudit()

    with pytest.raises(RuntimeError):
        _started(fake, audit)(_run(), _request(), "exec-1", _permit())

    assert audit.events[0].attributes["error_code"] == "RuntimeError"


def test_falha_do_audit_nao_mascara_erro_original(caplog: pytest.LogCaptureFixture) -> None:
    error = PermanentBillingError("run_execution_stale")
    fake = _bindable()
    fake.bind_error = error
    audit = _SpyAudit(BillingDependencyError("dynamodb_unavailable"))

    with caplog.at_level(logging.WARNING), pytest.raises(PermanentBillingError) as raised:
        _started(fake, audit)(_run(), _request(), "exec-1", _permit())

    assert raised.value is error
    assert (
        "billing_audit_append_failed event_type=run_execution.bind_failed "
        "code=dynamodb_unavailable"
    ) in [r.getMessage() for r in caplog.records]


def test_defeito_do_audit_tambem_nao_mascara_erro_original(
    caplog: pytest.LogCaptureFixture,
) -> None:
    error = PermanentBillingError("run_execution_stale")
    fake = _bindable()
    fake.bind_error = error
    audit = _SpyAudit(RuntimeError("audit_bug"))

    with caplog.at_level(logging.WARNING), pytest.raises(PermanentBillingError) as raised:
        _started(fake, audit)(_run(), _request(), "exec-1", _permit())

    assert raised.value is error
    assert (
        "billing_audit_append_failed event_type=run_execution.bind_failed code=RuntimeError"
    ) in [r.getMessage() for r in caplog.records]


def test_sem_audit_configurado_apenas_repropaga() -> None:
    fake = _bindable()
    fake.bind_error = PermanentBillingError("run_execution_stale")

    with pytest.raises(PermanentBillingError):
        _started(fake, None)(_run(), _request(), "exec-1", _permit())
