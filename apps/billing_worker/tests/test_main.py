"""Testes do CLI do worker de billing: parser, exit codes e logs."""

import logging
from unittest.mock import Mock, patch

import pytest

from apps.billing_worker.tests.support import (
    REGION,
    SECRET_KEY,
    STRIPE_ENV,
    WEBHOOK_SECRET,
    make_worker,
)
from billing_worker.main import main
from cnes_domain.billing.errors import (
    BillingDependencyError,
    PermanentBillingError,
    RetryableBillingError,
)
from cnes_domain.billing.inbox import ReconciliationRequest, RecoveryRequest
from cnes_infra.billing.settings import BillingConfigurationError


def _injetar(**overrides: object):
    worker, jobs = make_worker(**overrides)
    return patch("billing_worker.main.build_worker", return_value=worker), jobs


def test_inbox_delega_ao_dreno_exatamente_uma_vez() -> None:
    injection, jobs = _injetar()
    with injection:
        assert main(["inbox", "--limit", "37"]) == 0
    jobs.recovery.drain_inbox.assert_called_once_with(37)
    jobs.recovery.run.assert_not_called()


@pytest.mark.parametrize(
    "command", ["inbox", "reconcile", "revoke-pending", "release-expired-reservations"],
)
def test_comandos_limitados_usam_limite_padrao_de_100(command) -> None:
    injection, jobs = _injetar()
    with injection:
        assert main([command]) == 0
    calls = (
        jobs.recovery.drain_inbox.call_args_list
        + jobs.reconciler.run.call_args_list
        + jobs.revocations.run.call_args_list
        + jobs.reservations.reconcile_expired_reservations.call_args_list
    )
    [call] = calls
    argument = call.args[0]
    assert getattr(argument, "limit", argument) == 100


def test_recover_registra_contadores(caplog) -> None:
    injection, jobs = _injetar()
    with injection, caplog.at_level(logging.INFO):
        assert main(["recover"]) == 0
    jobs.recovery.run.assert_called_once_with(RecoveryRequest(72, 100))
    assert "billing_worker_cycle_done command=recover scanned=3" in caplog.text
    assert "next_cursor=evt_9" in caplog.text


def test_reconcile_registra_contadores(caplog) -> None:
    injection, jobs = _injetar()
    with injection, caplog.at_level(logging.INFO):
        assert main(["reconcile", "--limit", "7"]) == 0
    jobs.reconciler.run.assert_called_once_with(ReconciliationRequest(7, None))
    assert "billing_worker_cycle_done command=reconcile examined=4 drift_found=1" in caplog.text


def test_revoke_pending_registra_contadores(caplog) -> None:
    injection, _jobs = _injetar()
    with injection, caplog.at_level(logging.INFO):
        assert main(["revoke-pending", "--limit", "3"]) == 0
    assert "command=revoke-pending examined=2 resumed=1 fenced=3" in caplog.text


def test_revoke_pending_sem_enforcement_sai_com_sucesso(caplog) -> None:
    injection, _jobs = _injetar(revocations=None)
    with injection, caplog.at_level(logging.INFO):
        assert main(["revoke-pending"]) == 0
    assert "billing_worker_skipped command=revoke-pending reason=enforcement_off" in caplog.text


def test_release_expired_registra_contadores(caplog) -> None:
    injection, _jobs = _injetar()
    with injection, caplog.at_level(logging.INFO):
        assert main(["release-expired-reservations", "--limit", "9"]) == 0
    assert "command=release-expired-reservations examined=5 released=2" in caplog.text


def test_modo_desabilitado_e_noop_com_sucesso(caplog) -> None:
    with (
        patch("billing_worker.main.build_worker", return_value=None),
        caplog.at_level(logging.INFO),
    ):
        assert main(["reconcile"]) == 0
    assert "billing_worker_skipped mode=disabled" in caplog.text


@pytest.mark.parametrize(
    "error",
    [
        RetryableBillingError("reconciliation_cursor_contended"),
        BillingDependencyError("dynamodb_unavailable"),
        PermanentBillingError("invalid_recovery_cursor"),
    ],
)
@pytest.mark.parametrize(
    "command",
    ["inbox", "recover", "reconcile", "revoke-pending", "release-expired-reservations"],
)
def test_falha_de_billing_sem_registro_do_retry_retorna_1(command, error, caplog) -> None:
    injection, jobs = _injetar()
    for runner in (jobs.recovery.drain_inbox, jobs.recovery.run, jobs.reconciler.run,
                   jobs.revocations.run, jobs.reservations.reconcile_expired_reservations):
        runner.side_effect = error
    with injection, caplog.at_level(logging.INFO):
        assert main([command]) == 1
    assert f"billing_worker_cycle_failed command={command} code={error.code}" in caplog.text


@pytest.mark.parametrize(
    "argv",
    [[], ["nope"], ["inbox", "--limit", "0"], ["reconcile", "--limit", "101"],
     ["revoke-pending", "--limit", "x"], ["recover", "--limit", "5"]],
)
def test_entrada_invalida_retorna_2(argv) -> None:
    injection, _jobs = _injetar()
    with injection as build:
        assert main(argv) == 2
    build.assert_not_called()


def test_help_retorna_0(capsys) -> None:
    assert main(["--help"]) == 0
    out = capsys.readouterr().out
    assert "revoke-pending" in out
    assert "release-expired-reservations" in out


def test_configuracao_invalida_retorna_2(caplog) -> None:
    error = BillingConfigurationError("aws_region_required")
    with (
        patch("billing_worker.main.build_worker", side_effect=error),
        caplog.at_level(logging.INFO),
    ):
        assert main(["inbox"]) == 2
    assert "billing_worker_config_invalid code=aws_region_required" in caplog.text


@pytest.mark.parametrize(("retryable", "expected"), [(True, 1), (False, 2)])
def test_falha_de_segredo_mapeia_exit_pelo_retryable(retryable, expected) -> None:
    from cnes_infra.billing.secrets_manager import SecretProviderError

    error = SecretProviderError("ThrottlingException", retryable)
    with patch("billing_worker.main.build_worker", side_effect=error):
        assert main(["recover"]) == expected


def test_usa_sessao_boto3_com_a_regiao_pedida() -> None:
    from billing_worker.main import _session

    assert _session(REGION).region_name == REGION


class _FakeSecrets:
    def get_secret_value(self, *, SecretId: str) -> dict[str, str]:  # noqa: N803
        return {"SecretString": SECRET_KEY if SecretId.endswith(":sk") else WEBHOOK_SECRET}


def test_ciclo_stripe_completo_nao_loga_segredos(monkeypatch, caplog) -> None:
    dynamodb = Mock()
    dynamodb.query.return_value = {"Items": []}
    session = Mock()
    session.client.side_effect = lambda name, **_: (
        _FakeSecrets() if name == "secretsmanager" else dynamodb
    )
    for key, value in STRIPE_ENV.items():
        monkeypatch.setenv(key, value)
    with (
        patch("billing_worker.main._session", return_value=session),
        caplog.at_level(logging.DEBUG),
    ):
        assert main(["inbox", "--limit", "5"]) == 0
    assert "billing_worker_cycle_done command=inbox" in caplog.text
    assert SECRET_KEY not in caplog.text
    assert WEBHOOK_SECRET not in caplog.text
