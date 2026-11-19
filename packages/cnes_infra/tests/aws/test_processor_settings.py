"""Contrato público de configuração dos modos de unidade e recuperação AWS."""

import pytest

from cnes_infra.aws.processor_settings import AwsRecoverySettings, AwsUnitSettings
from cnes_infra.aws.settings import AwsRuntimeConfigurationError

_MINIMUM = {
    "AWS_REGION": "us-east-1",
    "AWS_CONTROL_PLANE_TABLE": "processor-test-control-plane",
    "AWS_DATA_BUCKET": "processor-test-data",
    "AWS_STATE_MACHINE_ARN": "arn:aws:states:us-east-1:000000000000:stateMachine:processor-test",
    "AWS_PROCESSOR_CONTAINER_NAME": "processor",
}
_PRODUCTION = _MINIMUM | {
    "AWS_PRODUCTION_POLICY": "true",
    "AWS_ENVIRONMENT": "prod",
    "AWS_REGION": "us-east-2",
    "AWS_PROCESSOR_MAX_CONCURRENCY": "1",
    "AWS_PROCESSOR_LEASE_SECONDS": "3600",
    "AWS_STATE_MACHINE_ARN": "arn:aws:states:us-east-2:000000000000:stateMachine:processor-test",
    "AWS_PUBLIC_SUBNET_IDS": "subnet-00000000000000001,subnet-00000000000000002",
    "AWS_SECURITY_GROUP_ID": "sg-00000000000000001",
}
_SETTINGS = (AwsUnitSettings, AwsRecoverySettings)


@pytest.mark.parametrize(("kind", "lease"), [(AwsUnitSettings, 300), (AwsRecoverySettings, 3600)])
def test_configuracao_minima_aplica_politica_do_modo(
    kind: type[AwsUnitSettings], lease: int,
) -> None:
    settings = kind.from_mapping(_MINIMUM)

    assert (settings.processor_lease_seconds, settings.processor_max_concurrency) == (lease, 8)
    assert (settings.environment, settings.production_policy) == ("default", False)
    assert settings.control_plane_table == "processor-test-control-plane"
    assert settings.data_bucket == "processor-test-data"
    assert settings.processor_container_name == "processor"
    assert settings.state_machine_arn == (
        "arn:aws:states:us-east-1:000000000000:stateMachine:processor-test"
    )
    assert (settings.dynamodb_endpoint_url, settings.service_endpoint_url) == (None, None)
    assert (settings.public_subnet_ids, settings.security_group_id) == ((), None)


@pytest.mark.parametrize("kind", _SETTINGS)
def test_producao_aceita_somente_recursos_e_politicas_do_worker(
    kind: type[AwsUnitSettings],
) -> None:
    settings = kind.from_mapping(_PRODUCTION | {
        "AUTH_MODE": "invalid", "OIDC_ISSUER": "invalid", "OIDC_AUDIENCE": "",
        "AWS_AUDIT_BUCKET": "", "AWS_AUDIT_RETENTION_DAYS": "invalid",
    })

    assert (settings.region, settings.environment, settings.production_policy) == (
        "us-east-2", "prod", True,
    )
    assert (settings.processor_max_concurrency, settings.processor_lease_seconds) == (1, 3600)
    assert settings.public_subnet_ids == (
        "subnet-00000000000000001", "subnet-00000000000000002",
    )
    assert settings.security_group_id == "sg-00000000000000001"


@pytest.mark.parametrize("kind", _SETTINGS)
@pytest.mark.parametrize("value", [None, "", "   "], ids=["ausente", "vazio", "branco"])
@pytest.mark.parametrize("key", tuple(_MINIMUM))
def test_rejeita_recurso_obrigatorio_ausente_ou_branco(
    kind: type[AwsUnitSettings], key: str, value: str | None,
) -> None:
    values = dict(_MINIMUM)
    if value is None:
        del values[key]
    else:
        values[key] = value

    with pytest.raises(AwsRuntimeConfigurationError, match=f"^missing={key}$"):
        kind.from_mapping(values)


@pytest.mark.parametrize("kind", _SETTINGS)
@pytest.mark.parametrize(
    ("arn", "error"),
    [
        ("invalid", "state_machine_arn=invalid"),
        (
            "arn:aws:states:us-east-1:000000000000:stateMachine:worker:1",
            "state_machine_arn=qualified",
        ),
        (
            "arn:aws:ecs:us-east-1:000000000000:stateMachine:worker",
            "state_machine_arn=invalid_service",
        ),
    ],
    ids=["malformado", "versao", "servico_errado"],
)
def test_rejeita_arn_incapaz_de_identificar_state_machine(
    kind: type[AwsUnitSettings], arn: str, error: str,
) -> None:
    with pytest.raises(AwsRuntimeConfigurationError, match=f"^{error}$"):
        kind.from_mapping(_MINIMUM | {"AWS_STATE_MACHINE_ARN": arn})


@pytest.mark.parametrize(
    ("key", "value", "error"),
    [
        ("AWS_PROCESSOR_MAX_CONCURRENCY", "0", "processor_max_concurrency=invalid"),
        ("AWS_PROCESSOR_MAX_CONCURRENCY", "41", "processor_max_concurrency=invalid"),
        ("AWS_PROCESSOR_LEASE_SECONDS", "29", "processor_lease_seconds=invalid"),
        ("AWS_PROCESSOR_LEASE_SECONDS", "3601", "processor_lease_seconds=invalid"),
        ("AWS_PROCESSOR_LEASE_SECONDS", "", "integer_invalid=AWS_PROCESSOR_LEASE_SECONDS"),
    ],
)
def test_unidade_rejeita_limites_inseguros(key: str, value: str, error: str) -> None:
    with pytest.raises(AwsRuntimeConfigurationError, match=f"^{error}$"):
        AwsUnitSettings.from_mapping(_MINIMUM | {key: value})


@pytest.mark.parametrize("lease", [30, 3600])
def test_unidade_aceita_extremos_do_lease(lease: int) -> None:
    settings = AwsUnitSettings.from_mapping(_MINIMUM | {"AWS_PROCESSOR_LEASE_SECONDS": str(lease)})

    assert settings.processor_lease_seconds == lease


@pytest.mark.parametrize(
    ("key", "value", "expected"),
    [
        ("AWS_PROCESSOR_RECOVERY_BATCH_SIZE", "99", 100),
        ("AWS_PROCESSOR_RECOVERY_BATCH_SIZE", "101", 100),
        ("AWS_PROCESSOR_LEASE_SECONDS", "3599", 3600),
        ("AWS_PROCESSOR_LEASE_SECONDS", "3601", 3600),
    ],
)
def test_recovery_rejeita_divergencia_de_batch_ou_lease(
    key: str, value: str, expected: int,
) -> None:
    with pytest.raises(AwsRuntimeConfigurationError, match=f"^fixed={key} expected={expected}$"):
        AwsRecoverySettings.from_mapping(_MINIMUM | {key: value})


@pytest.mark.parametrize(
    "key", ["AWS_PROCESSOR_RECOVERY_BATCH_SIZE", "AWS_PROCESSOR_LEASE_SECONDS"],
)
@pytest.mark.parametrize("value", ["", "invalid"])
def test_recovery_rejeita_knob_malformado(key: str, value: str) -> None:
    with pytest.raises(AwsRuntimeConfigurationError, match=f"^integer_invalid={key}$"):
        AwsRecoverySettings.from_mapping(_MINIMUM | {key: value})


def test_recovery_aceita_batch_e_lease_canonicos_explicitos() -> None:
    settings = AwsRecoverySettings.from_mapping(_MINIMUM | {
        "AWS_PROCESSOR_RECOVERY_BATCH_SIZE": "100", "AWS_PROCESSOR_LEASE_SECONDS": "3600",
    })

    assert settings.processor_lease_seconds == 3600


@pytest.mark.parametrize("kind", _SETTINGS)
@pytest.mark.parametrize(
    ("key", "value", "error"),
    [
        ("AWS_REGION", "us-east-1", "production_region=invalid"),
        ("AWS_PROCESSOR_MAX_CONCURRENCY", "2", "production_processor_max_concurrency=invalid"),
        ("AWS_PUBLIC_SUBNET_IDS", "", "production_public_subnet_ids=required"),
        ("AWS_SECURITY_GROUP_ID", "", "production_security_group_id=required"),
    ],
)
def test_producao_rejeita_recursos_fora_da_politica(
    kind: type[AwsUnitSettings], key: str, value: str, error: str,
) -> None:
    with pytest.raises(AwsRuntimeConfigurationError, match=f"^{error}$"):
        kind.from_mapping(_PRODUCTION | {key: value})


def test_unidade_em_producao_rejeita_lease_generico() -> None:
    with pytest.raises(
        AwsRuntimeConfigurationError, match=r"^production_processor_lease_seconds=invalid$",
    ):
        AwsUnitSettings.from_mapping(_PRODUCTION | {"AWS_PROCESSOR_LEASE_SECONDS": "300"})


@pytest.mark.parametrize("kind", _SETTINGS)
def test_endpoints_explicitos_sao_encaminhados_sem_credenciais(kind: type[AwsUnitSettings]) -> None:
    settings = kind.from_mapping(_MINIMUM | {
        "DYNAMODB_ENDPOINT_URL": "http://127.0.0.1:18000",
        "AWS_ENDPOINT_URL": "http://127.0.0.1:4566",
    })

    assert settings.dynamodb_endpoint_url == "http://127.0.0.1:18000"
    assert settings.service_endpoint_url == "http://127.0.0.1:4566"
