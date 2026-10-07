"""Contrato das settings imutáveis do profile aws."""

from dataclasses import FrozenInstanceError

import pytest

from cnes_infra.aws.settings import AwsRuntimeConfigurationError, AwsRuntimeSettings


def _valid_values() -> dict[str, str]:
    return {
        "PROFILE": "aws",
        "AUTH_MODE": "oidc",
        "AWS_REGION": "us-east-1",
        "AWS_CONTROL_PLANE_TABLE": "cnesdata-test-control-plane",
        "AWS_DATA_BUCKET": "cnesdata-test-data",
        "AWS_AUDIT_BUCKET": "cnesdata-test-audit",
        "AWS_STATE_MACHINE_ARN": (
            "arn:aws:states:us-east-1:000000000000:stateMachine:cnesdata-test"
        ),
        "AWS_PROCESSOR_CONTAINER_NAME": "processor",
        "AWS_PROCESSOR_MAX_CONCURRENCY": "8",
        "AWS_PROCESSOR_LEASE_SECONDS": "300",
        "AWS_PROCESSOR_RECOVERY_BATCH_SIZE": "100",
        "AWS_SERVING_URL_TTL_SECONDS": "300",
        "AWS_AUDIT_RETENTION_DAYS": "365",
        "OIDC_ISSUER": "https://id.example.test",
        "OIDC_AUDIENCE": "cnesdata-dashboard",
    }


def test_aceita_configuracao_aws_sem_credenciais_estaticas() -> None:
    settings = AwsRuntimeSettings.from_mapping(_valid_values())

    assert settings.region == "us-east-1"
    assert settings.control_plane_table == "cnesdata-test-control-plane"
    assert settings.data_bucket == "cnesdata-test-data"
    assert settings.audit_bucket == "cnesdata-test-audit"
    assert settings.state_machine_arn.endswith(":stateMachine:cnesdata-test")
    assert settings.processor_container_name == "processor"
    assert settings.oidc_issuer == "https://id.example.test"
    assert settings.oidc_audience == "cnesdata-dashboard"
    assert settings.serving_url_ttl_seconds == 300
    assert settings.audit_retention_days == 365
    assert settings.processor_max_concurrency == 8
    assert settings.processor_lease_seconds == 300
    assert settings.processor_recovery_batch_size == 100
    assert settings.dynamodb_endpoint_url is None
    assert settings.service_endpoint_url is None
    assert not hasattr(settings, "access_key_id")
    assert not hasattr(settings, "secret_access_key")


def test_aplica_defaults_operacionais() -> None:
    values = _valid_values()
    for key in (
        "AWS_SERVING_URL_TTL_SECONDS",
        "AWS_PROCESSOR_MAX_CONCURRENCY",
        "AWS_PROCESSOR_LEASE_SECONDS",
        "AWS_PROCESSOR_RECOVERY_BATCH_SIZE",
    ):
        del values[key]

    settings = AwsRuntimeSettings.from_mapping(values)

    assert settings.serving_url_ttl_seconds == 300
    assert settings.processor_max_concurrency == 8
    assert settings.processor_lease_seconds == 300
    assert settings.processor_recovery_batch_size == 100


def test_le_endpoints_de_emulador() -> None:
    values = _valid_values() | {
        "DYNAMODB_ENDPOINT_URL": "http://127.0.0.1:18000",
        "AWS_ENDPOINT_URL": "http://127.0.0.1:4566",
    }

    settings = AwsRuntimeSettings.from_mapping(values)

    assert settings.dynamodb_endpoint_url == "http://127.0.0.1:18000"
    assert settings.service_endpoint_url == "http://127.0.0.1:4566"


def test_trata_endpoint_vazio_como_ausente() -> None:
    values = _valid_values() | {"DYNAMODB_ENDPOINT_URL": "", "AWS_ENDPOINT_URL": ""}

    settings = AwsRuntimeSettings.from_mapping(values)

    assert settings.dynamodb_endpoint_url is None
    assert settings.service_endpoint_url is None


def test_impede_mutacao() -> None:
    settings = AwsRuntimeSettings.from_mapping(_valid_values())

    with pytest.raises(FrozenInstanceError):
        settings.region = "sa-east-1"  # type: ignore[misc]


@pytest.mark.parametrize(
    ("key", "value", "error"),
    [
        ("PROFILE", "local", "profile_must_be_aws"),
        ("AUTH_MODE", "local", "auth_mode_must_be_oidc"),
        ("AWS_SERVING_URL_TTL_SECONDS", "901", "serving_ttl_out_of_range"),
        ("AWS_AUDIT_RETENTION_DAYS", "0", "audit_retention_days_invalid"),
        ("AWS_PROCESSOR_MAX_CONCURRENCY", "0", "processor_concurrency_invalid"),
        ("AWS_PROCESSOR_LEASE_SECONDS", "0", "processor_lease_invalid"),
        ("AWS_PROCESSOR_RECOVERY_BATCH_SIZE", "1001", "recovery_batch_invalid"),
    ],
)
def test_rejeita_configuracao_insegura(key: str, value: str, error: str) -> None:
    values = _valid_values() | {key: value}

    with pytest.raises(AwsRuntimeConfigurationError, match=error):
        AwsRuntimeSettings.from_mapping(values)


@pytest.mark.parametrize(
    ("key", "value", "error"),
    [
        ("AWS_SERVING_URL_TTL_SECONDS", "29", "serving_ttl_out_of_range"),
        ("AWS_PROCESSOR_MAX_CONCURRENCY", "41", "processor_concurrency_invalid"),
        ("AWS_PROCESSOR_LEASE_SECONDS", "3601", "processor_lease_invalid"),
        ("AWS_PROCESSOR_RECOVERY_BATCH_SIZE", "0", "recovery_batch_invalid"),
    ],
)
def test_rejeita_limites_opostos(key: str, value: str, error: str) -> None:
    values = _valid_values() | {key: value}

    with pytest.raises(AwsRuntimeConfigurationError, match=error):
        AwsRuntimeSettings.from_mapping(values)


def test_rejeita_profile_ausente() -> None:
    values = _valid_values()
    del values["PROFILE"]

    with pytest.raises(AwsRuntimeConfigurationError, match="profile_must_be_aws"):
        AwsRuntimeSettings.from_mapping(values)


@pytest.mark.parametrize(
    ("key", "value"),
    [("AUTH_MODE", "jwt"), ("OIDC_ISSUER", ""), ("OIDC_ISSUER", "   ")],
)
def test_traduz_erro_de_profile_settings(key: str, value: str) -> None:
    values = _valid_values() | {key: value}

    with pytest.raises(AwsRuntimeConfigurationError, match="profile_settings_invalid"):
        AwsRuntimeSettings.from_mapping(values)


def test_rejeita_issuer_ausente() -> None:
    values = _valid_values()
    del values["OIDC_ISSUER"]

    with pytest.raises(AwsRuntimeConfigurationError, match="profile_settings_invalid"):
        AwsRuntimeSettings.from_mapping(values)


def test_ignora_campos_de_profile_fora_do_escopo_aws() -> None:
    values = _valid_values() | {
        "TENANT_ID": "tenant-a",
        "BILLING_MODE": "manual",
        "DATA_DIR": "/var/lib/cnes",
    }

    settings = AwsRuntimeSettings.from_mapping(values)

    assert settings.region == "us-east-1"


def test_exige_auth_mode_explicito() -> None:
    values = _valid_values()
    del values["AUTH_MODE"]

    with pytest.raises(AwsRuntimeConfigurationError, match="auth_mode_must_be_oidc"):
        AwsRuntimeSettings.from_mapping(values)


@pytest.mark.parametrize(
    "key",
    [
        "AWS_REGION",
        "AWS_CONTROL_PLANE_TABLE",
        "AWS_DATA_BUCKET",
        "AWS_AUDIT_BUCKET",
        "AWS_STATE_MACHINE_ARN",
        "AWS_PROCESSOR_CONTAINER_NAME",
        "OIDC_AUDIENCE",
        "AWS_AUDIT_RETENTION_DAYS",
    ],
)
def test_rejeita_obrigatorio_ausente(key: str) -> None:
    values = _valid_values()
    del values[key]

    with pytest.raises(AwsRuntimeConfigurationError, match=f"missing={key}"):
        AwsRuntimeSettings.from_mapping(values)


def test_rejeita_obrigatorio_em_branco() -> None:
    values = _valid_values() | {"AWS_REGION": "   "}

    with pytest.raises(AwsRuntimeConfigurationError, match="missing=AWS_REGION"):
        AwsRuntimeSettings.from_mapping(values)


@pytest.mark.parametrize("value", ["abc", "8.5", ""])
def test_rejeita_inteiro_malformado(value: str) -> None:
    values = _valid_values() | {"AWS_PROCESSOR_MAX_CONCURRENCY": value}

    with pytest.raises(
        AwsRuntimeConfigurationError, match="integer_invalid=AWS_PROCESSOR_MAX_CONCURRENCY"
    ):
        AwsRuntimeSettings.from_mapping(values)


def test_remove_uma_barra_final_do_issuer() -> None:
    values = _valid_values() | {"OIDC_ISSUER": "https://id.example.test/realms/cnes/"}

    settings = AwsRuntimeSettings.from_mapping(values)

    assert settings.oidc_issuer == "https://id.example.test/realms/cnes"


@pytest.mark.parametrize("issuer", [" https://id.example.test", "https://id.example.test/ "])
def test_remove_espacos_do_issuer(issuer: str) -> None:
    values = _valid_values() | {"OIDC_ISSUER": issuer}

    settings = AwsRuntimeSettings.from_mapping(values)

    assert settings.oidc_issuer == "https://id.example.test"


def test_aceita_issuer_http_com_endpoint_de_emulador() -> None:
    values = _valid_values() | {
        "OIDC_ISSUER": "http://keycloak:8080/realms/cnes",
        "AWS_ENDPOINT_URL": "http://aws-emulator:4566",
    }

    settings = AwsRuntimeSettings.from_mapping(values)

    assert settings.oidc_issuer == "http://keycloak:8080/realms/cnes"


@pytest.mark.parametrize(
    "issuer",
    [
        "http://id.example.test",
        "ftp://id.example.test",
        "https://id.example.test?tenant=1",
        "https://id.example.test#frag",
        "https://",
        "id.example.test",
    ],
)
def test_rejeita_issuer_invalido(issuer: str) -> None:
    values = _valid_values() | {"OIDC_ISSUER": issuer}

    with pytest.raises(AwsRuntimeConfigurationError, match="oidc_issuer_invalid"):
        AwsRuntimeSettings.from_mapping(values)


def test_rejeita_issuer_ftp_mesmo_com_emulador() -> None:
    values = _valid_values() | {
        "OIDC_ISSUER": "ftp://id.example.test",
        "AWS_ENDPOINT_URL": "http://aws-emulator:4566",
    }

    with pytest.raises(AwsRuntimeConfigurationError, match="oidc_issuer_invalid"):
        AwsRuntimeSettings.from_mapping(values)


def test_erro_de_configuracao_e_value_error() -> None:
    assert issubclass(AwsRuntimeConfigurationError, ValueError)
