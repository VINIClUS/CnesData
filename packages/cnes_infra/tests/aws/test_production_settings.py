"""Invariantes das settings aws sob a política de produção."""

from collections.abc import Mapping

import pytest

from cnes_infra.aws.settings import (
    AwsRuntimeConfigurationError,
    AwsRuntimeSettings,
    production_policy_enabled,
)
from packages.cnes_infra.tests.aws.production_values import (
    PRODUCTION_SECURITY_GROUP_ID,
    PRODUCTION_SUBNET_IDS,
    PRODUCTION_VALUES,
)


def _without(key: str) -> dict[str, str]:
    values = dict(PRODUCTION_VALUES)
    del values[key]
    return values


def _with(key: str, value: str | None) -> dict[str, str]:
    return _without(key) if value is None else PRODUCTION_VALUES | {key: value}


def _assert_rejected(values: Mapping[str, str], error: str) -> None:
    with pytest.raises(AwsRuntimeConfigurationError, match=f"^{error}$"):
        AwsRuntimeSettings.from_mapping(values)


def test_producao_aceita_valores_canonicos() -> None:
    settings = AwsRuntimeSettings.from_mapping(PRODUCTION_VALUES)

    assert production_policy_enabled(PRODUCTION_VALUES) is True
    assert settings.production_policy is True
    assert settings.environment == "prod"
    assert settings.region == "us-east-2"
    assert settings.public_subnet_ids == PRODUCTION_SUBNET_IDS
    assert settings.security_group_id == PRODUCTION_SECURITY_GROUP_ID
    assert (settings.processor_max_concurrency, settings.processor_lease_seconds) == (1, 3600)
    assert settings.serving_url_ttl_seconds == 300


def test_producao_aceita_ttl_padrao_de_300() -> None:
    settings = AwsRuntimeSettings.from_mapping(_without("AWS_SERVING_URL_TTL_SECONDS"))

    assert settings.serving_url_ttl_seconds == 300


@pytest.mark.parametrize(
    ("key", "value", "error"),
    [
        ("AWS_PUBLIC_SUBNET_IDS", None, "production_subnets_required"),
        ("AWS_PUBLIC_SUBNET_IDS", "", "production_subnets_required"),
        ("AWS_SECURITY_GROUP_ID", None, "production_security_group_required"),
        ("AWS_SECURITY_GROUP_ID", "", "production_security_group_required"),
    ],
    ids=["sem_subnets", "subnets_vazias", "sem_security_group", "security_group_vazio"],
)
def test_producao_exige_subnets_e_security_group(key: str, value: str | None, error: str) -> None:
    _assert_rejected(_with(key, value), error)


@pytest.mark.parametrize("value", [None, "8"], ids=["padrao_oito", "oito"])
def test_producao_exige_concorrencia_um(value: str | None) -> None:
    values = _with("AWS_PROCESSOR_MAX_CONCURRENCY", value)

    _assert_rejected(values, "production_concurrency_must_be_one")


@pytest.mark.parametrize("value", [None, "300", "3599"], ids=["padrao_300", "generico", "abaixo"])
def test_producao_exige_lease_3600(value: str | None) -> None:
    values = _with("AWS_PROCESSOR_LEASE_SECONDS", value)

    _assert_rejected(values, "production_lease_must_be_3600")


@pytest.mark.parametrize(
    ("key", "value", "error"),
    [
        ("AWS_SERVING_URL_TTL_SECONDS", "299", "production_serving_ttl_must_be_300"),
        ("AWS_SERVING_URL_TTL_SECONDS", "900", "production_serving_ttl_must_be_300"),
        ("AWS_REGION", "us-east-1", "production_region_must_be_us_east_2"),
        ("AWS_REGION", "sa-east-1", "production_region_must_be_us_east_2"),
    ],
    ids=["ttl_menor", "ttl_maior", "us_east_1", "sa_east_1"],
)
def test_producao_exige_ttl_300_e_regiao_us_east_2(key: str, value: str, error: str) -> None:
    _assert_rejected(PRODUCTION_VALUES | {key: value}, error)


def test_producao_exige_environment_explicito() -> None:
    _assert_rejected(_without("AWS_ENVIRONMENT"), "environment_required")


@pytest.mark.parametrize("environment", ["", "Prod", "prod_1"])
def test_producao_rejeita_environment_invalido(environment: str) -> None:
    _assert_rejected(PRODUCTION_VALUES | {"AWS_ENVIRONMENT": environment}, "environment_invalid")


@pytest.mark.parametrize(
    "subnets",
    [
        "subnet-00000000000000002,subnet-00000000000000001",
        "subnet-00000000000000001,subnet-00000000000000001",
        "subnet-00000000000000001,subnet-00000000000000002,subnet-00000000000000001",
    ],
    ids=["fora_de_ordem", "duplicada", "duplicada_nao_adjacente"],
)
def test_subnets_devem_vir_normalizadas(subnets: str) -> None:
    values = PRODUCTION_VALUES | {"AWS_PUBLIC_SUBNET_IDS": subnets}

    _assert_rejected(values, "public_subnets_not_normalized")


@pytest.mark.parametrize(
    "subnets",
    [
        " ",
        "subnet-0000000",
        "subnet-000000000000000001",
        "subnet-0000000g",
        "subnet-0000000A",
        "SUBNET-00000001",
        "sg-00000001",
        "subnet-00000001,",
        ",subnet-00000001",
        "subnet-00000001, subnet-00000002",
        "subnet-00000001\n",
    ],
    ids=[
        "em_branco", "hex_curto", "hex_longo", "nao_hex", "hex_maiusculo", "prefixo_maiusculo",
        "security_group", "virgula_final", "virgula_inicial", "espaco_apos_virgula",
        "quebra_de_linha",
    ],
)
def test_producao_rejeita_subnet_invalida(subnets: str) -> None:
    values = PRODUCTION_VALUES | {"AWS_PUBLIC_SUBNET_IDS": subnets}

    _assert_rejected(values, "public_subnet_invalid")


@pytest.mark.parametrize(
    "security_group",
    [
        " ",
        "sg-0000000",
        "sg-000000000000000001",
        "sg-0000000G",
        "SG-00000001",
        "subnet-00000001",
        " sg-00000001",
        "sg-00000001\n",
        "sg-00000001,sg-00000002",
    ],
    ids=[
        "em_branco", "hex_curto", "hex_longo", "hex_maiusculo", "prefixo_maiusculo", "subnet",
        "espaco", "quebra_de_linha", "lista",
    ],
)
def test_producao_rejeita_security_group_invalido(security_group: str) -> None:
    values = PRODUCTION_VALUES | {"AWS_SECURITY_GROUP_ID": security_group}

    _assert_rejected(values, "security_group_invalid")


def test_producao_aceita_ids_de_rede_curtos_e_longos() -> None:
    values = PRODUCTION_VALUES | {
        "AWS_PUBLIC_SUBNET_IDS": "subnet-0123456789abcdef0,subnet-89abcdef",
        "AWS_SECURITY_GROUP_ID": "sg-89abcdef",
    }

    settings = AwsRuntimeSettings.from_mapping(values)

    assert settings.public_subnet_ids == ("subnet-0123456789abcdef0", "subnet-89abcdef")
    assert settings.security_group_id == "sg-89abcdef"
