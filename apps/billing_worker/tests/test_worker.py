"""Testes do worker de billing e do CLI."""

import logging
from unittest.mock import Mock, patch

import pytest

from billing_worker.main import main
from billing_worker.worker import BillingWorker, build_worker
from cnes_domain.billing.errors import (
    BillingDependencyError,
    PermanentBillingError,
    RetryableBillingError,
)
from cnes_domain.billing.inbox import RecoveryRequest, RecoveryResult
from cnes_infra.billing.settings import BillingConfigurationError

REGION = "sa-east-1"
TABLE = "control-plane"
SECRET_KEY = "sk_test_SEGREDO"  # noqa: S105
WEBHOOK_SECRET = "whsec_SEGREDO"  # noqa: S105
ORIGIN = "https://app.example.com"
STRIPE_ENV = {
    "PROFILE": "aws",
    "BILLING_MODE": "stripe",
    "AWS_REGION": REGION,
    "AWS_CONTROL_PLANE_TABLE": TABLE,
    "DYNAMODB_ENDPOINT_URL": "  ",
    "STRIPE_SECRET_KEY_SECRET_ARN": "arn:aws:secretsmanager:sa-east-1:1:secret:sk",
    "STRIPE_WEBHOOK_SECRET_SECRET_ARN": "arn:aws:secretsmanager:sa-east-1:1:secret:wh",
    "BILLING_RETURN_ORIGINS": ORIGIN,
    "BILLING_SUCCESS_URL": f"{ORIGIN}/ok",
    "BILLING_CANCEL_URL": f"{ORIGIN}/cancel",
    "BILLING_PORTAL_RETURN_URL": f"{ORIGIN}/portal",
}
RESULT = RecoveryResult(3, 1, 2, 0, "evt_9")


@pytest.fixture
def recovery() -> Mock:
    mock = Mock()
    mock.drain_inbox.return_value = RESULT
    mock.run.return_value = RESULT
    return mock


@pytest.fixture
def worker(recovery) -> BillingWorker:
    return BillingWorker(recovery, RecoveryRequest(72, 100))


def _env(**overrides: str) -> dict[str, str]:
    return {**STRIPE_ENV, **overrides}


def _without(*keys: str) -> dict[str, str]:
    return {k: v for k, v in STRIPE_ENV.items() if k not in keys}


def test_worker_inbox_delega_ao_dreno_bounded(worker, recovery) -> None:
    assert worker.run_inbox(limit=37) == recovery.drain_inbox.return_value
    recovery.drain_inbox.assert_called_once_with(37)


def test_worker_recover_delega_uma_vez_com_a_requisicao(worker, recovery) -> None:
    assert worker.run_recover() == RESULT
    recovery.run.assert_called_once_with(RecoveryRequest(72, 100))


@pytest.mark.parametrize(
    "values",
    [
        {"PROFILE": "aws", "BILLING_MODE": "disabled"},
        {"PROFILE": "local", "TENANT_ID": "354130", "BILLING_MODE": "disabled"},
    ],
)
def test_build_worker_desabilitado_nao_cria_sessao(values) -> None:
    factory = Mock()
    assert build_worker(values, factory) is None
    factory.assert_not_called()


def test_build_worker_stripe_compoe_provider_e_storage_da_mesma_sessao() -> None:
    session = Mock()
    factory = Mock(return_value=session)
    with patch("billing_worker.worker.build_stripe_billing") as build:
        worker = build_worker(STRIPE_ENV, factory)
    factory.assert_called_once_with(REGION)
    assert [c.args[0] for c in session.client.call_args_list] == ["secretsmanager", "dynamodb"]
    session.client.assert_any_call("dynamodb", region_name=REGION, endpoint_url=None)
    settings, provider, storage, _clock = build.call_args.args
    assert provider.__class__.__name__ == "SecretsManagerSecretProvider"
    assert (storage.client, storage.table_name) == (session.client.return_value, TABLE)
    assert worker._request == settings.recovery
    assert worker._recovery is build.return_value.recovery


def test_build_worker_repassa_endpoint_dynamodb_local() -> None:
    session = Mock()
    values = _env(DYNAMODB_ENDPOINT_URL=" http://localhost:8000 ")
    with patch("billing_worker.worker.build_stripe_billing"):
        build_worker(values, Mock(return_value=session))
    session.client.assert_any_call(
        "dynamodb", region_name=REGION, endpoint_url="http://localhost:8000",
    )


def test_build_worker_relogio_retorna_utc() -> None:
    with patch("billing_worker.worker.build_stripe_billing") as build:
        build_worker(STRIPE_ENV, Mock())
    assert build.call_args.args[3]().utcoffset().total_seconds() == 0


@pytest.mark.parametrize(
    ("values", "code"),
    [
        (_without("AWS_REGION"), "aws_region_required"),
        (_env(AWS_REGION="  "), "aws_region_required"),
        (_without("AWS_CONTROL_PLANE_TABLE"), "billing_table_required"),
        (_without("STRIPE_SECRET_KEY_SECRET_ARN"), "stripe_secret_key_arn_required"),
        (_env(BILLING_SUCCESS_URL="https://evil.example/x"), "billing_return_urls_invalid"),
    ],
)
def test_build_worker_rejeita_config_invalida_antes_de_criar_sessao(values, code) -> None:
    factory = Mock()
    with pytest.raises(BillingConfigurationError) as error:
        build_worker(values, factory)
    assert error.value.code == code
    factory.assert_not_called()


def test_build_worker_exige_provider_em_modo_stripe() -> None:
    with (
        patch("billing_worker.worker.build_secret_provider", return_value=None),
        pytest.raises(BillingConfigurationError) as error,
    ):
        build_worker(STRIPE_ENV, Mock())
    assert error.value.code == "secret_provider_required"


def _injetar(recovery: Mock):
    worker = BillingWorker(recovery, RecoveryRequest(72, 100))
    return patch("billing_worker.main.build_worker", return_value=worker)


def test_main_inbox_delega_ao_dreno_exatamente_uma_vez(recovery) -> None:
    with _injetar(recovery):
        assert main(["inbox", "--limit", "37"]) == 0
    recovery.drain_inbox.assert_called_once_with(37)
    recovery.run.assert_not_called()


def test_main_inbox_usa_limite_padrao_de_100(recovery) -> None:
    with _injetar(recovery):
        assert main(["inbox"]) == 0
    recovery.drain_inbox.assert_called_once_with(100)


def test_main_recover_sucesso_registra_contadores(recovery, caplog) -> None:
    with _injetar(recovery), caplog.at_level(logging.INFO):
        assert main(["recover"]) == 0
    recovery.run.assert_called_once_with(RecoveryRequest(72, 100))
    assert "billing_worker_cycle_done command=recover scanned=3" in caplog.text
    assert "next_cursor=evt_9" in caplog.text


def test_main_modo_desabilitado_e_noop_com_sucesso(caplog) -> None:
    with (
        patch("billing_worker.main.build_worker", return_value=None),
        caplog.at_level(logging.INFO),
    ):
        assert main(["inbox"]) == 0
    assert "billing_worker_skipped mode=disabled" in caplog.text


@pytest.mark.parametrize(
    "error",
    [
        RetryableBillingError("stripe_unavailable"),
        BillingDependencyError("dynamodb_unavailable"),
        PermanentBillingError("projection_invalid"),
    ],
)
@pytest.mark.parametrize("command", [["inbox"], ["recover"]])
def test_main_falha_de_billing_no_ciclo_retorna_1(recovery, command, error, caplog) -> None:
    recovery.drain_inbox.side_effect = error
    recovery.run.side_effect = error
    with _injetar(recovery), caplog.at_level(logging.INFO):
        assert main(command) == 1
    assert f"billing_worker_cycle_failed command={command[0]}" in caplog.text


@pytest.mark.parametrize(
    "argv",
    [[], ["nope"], ["inbox", "--limit", "0"], ["inbox", "--limit", "101"],
     ["inbox", "--limit", "x"]],
)
def test_main_entrada_invalida_retorna_2(argv, recovery) -> None:
    with _injetar(recovery) as build:
        assert main(argv) == 2
    build.assert_not_called()


def test_main_help_retorna_0(capsys) -> None:
    assert main(["--help"]) == 0
    assert "billing-worker" in capsys.readouterr().out


def test_main_configuracao_invalida_retorna_2(caplog) -> None:
    error = BillingConfigurationError("aws_region_required")
    with (
        patch("billing_worker.main.build_worker", side_effect=error),
        caplog.at_level(logging.INFO),
    ):
        assert main(["inbox"]) == 2
    assert "billing_worker_config_invalid code=aws_region_required" in caplog.text


@pytest.mark.parametrize(("retryable", "expected"), [(True, 1), (False, 2)])
def test_main_falha_de_segredo_mapeia_exit_pelo_retryable(retryable, expected) -> None:
    from cnes_infra.billing.secrets_manager import SecretProviderError

    error = SecretProviderError("ThrottlingException", retryable)
    with patch("billing_worker.main.build_worker", side_effect=error):
        assert main(["recover"]) == expected


def test_main_usa_sessao_boto3_com_a_regiao_pedida() -> None:
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
