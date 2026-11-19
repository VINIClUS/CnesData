"""Invariantes das settings aws sob a política de produção."""

import re
from collections.abc import Mapping
from dataclasses import fields

import pytest

from cnes_infra.aws.settings import (
    AwsRuntimeConfigurationError,
    AwsRuntimeSettings,
    production_policy_enabled,
)
from packages.cnes_infra.tests.aws.production_values import (
    PRODUCTION_OIDC_DOMAIN,
    PRODUCTION_SECURITY_GROUP_ID,
    PRODUCTION_SUBNET_IDS,
    PRODUCTION_VALUES,
)

_ISSUER_ERROR = "production_oidc_issuer_must_be_cognito_us_east_2"
_STATIC_KEY_FIELD = re.compile(r"key|secret|token|credential")


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
    assert settings.audit_retention_days == 365


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


@pytest.mark.parametrize("days", [1, 30, 364, 366, 36500])
def test_producao_rejeita_retencao_de_audit_diferente_de_365(days: int) -> None:
    values = PRODUCTION_VALUES | {"AWS_AUDIT_RETENTION_DAYS": str(days)}

    _assert_rejected(values, "production_audit_retention_must_be_365_days")


@pytest.mark.parametrize("days", [1, 30, 730])
def test_fora_da_producao_aceita_retencao_de_audit_diferente_de_365(days: int) -> None:
    production_only = {"AWS_PUBLIC_SUBNET_IDS", "AWS_SECURITY_GROUP_ID", "OIDC_DOMAIN"}
    generic = {k: v for k, v in PRODUCTION_VALUES.items() if k not in production_only}
    values = generic | {"AWS_PRODUCTION_POLICY": "false", "AWS_AUDIT_RETENTION_DAYS": str(days)}

    assert AwsRuntimeSettings.from_mapping(values).audit_retention_days == days


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


def test_producao_expoe_dominio_e_endpoints_padrao_do_cognito() -> None:
    settings = AwsRuntimeSettings.from_mapping(PRODUCTION_VALUES)

    assert settings.oidc_audience == "https://api.cnesdata.com.br"
    assert settings.oidc_issuer == PRODUCTION_VALUES["OIDC_ISSUER"]
    assert settings.oidc_domain == PRODUCTION_OIDC_DOMAIN
    assert settings.oidc_authorize_url == f"{PRODUCTION_OIDC_DOMAIN}/oauth2/authorize"
    assert settings.oidc_token_url == f"{PRODUCTION_OIDC_DOMAIN}/oauth2/token"
    assert settings.oidc_logout_url == f"{PRODUCTION_OIDC_DOMAIN}/logout"


def test_producao_nao_expoe_campo_de_chave_estatica() -> None:
    values = PRODUCTION_VALUES | {
        "AWS_ACCESS_KEY_ID": "test-access",
        "AWS_SECRET_ACCESS_KEY": "test-secret",
        "AWS_SESSION_TOKEN": "test-token",
    }

    settings = AwsRuntimeSettings.from_mapping(values)

    assert [field.name for field in fields(settings) if _STATIC_KEY_FIELD.search(field.name)] == []


@pytest.mark.parametrize(
    "audience",
    [
        "cnesdata-dashboard",
        "https://api.cnesdata.com.br/",
        "https://api.cnesdata.com.br/api/v1",
        "http://api.cnesdata.com.br",
        "https://api.dev.cnesdata.com.br",
        "https://API.cnesdata.com.br",
    ],
    ids=["client_id", "barra_final", "base_da_api", "http", "dev", "maiusculas"],
)
def test_producao_exige_audience_igual_a_origem_da_api(audience: str) -> None:
    values = PRODUCTION_VALUES | {"OIDC_AUDIENCE": audience}

    _assert_rejected(values, "production_oidc_audience_must_be_api_origin")


@pytest.mark.parametrize(
    ("key", "value", "error"),
    [
        (
            "OIDC_ISSUER",
            "https://cognito-idp.us-east-1.amazonaws.com/us-east-1_Example1",
            _ISSUER_ERROR,
        ),
        (
            "OIDC_ISSUER",
            "https://cognito-idp.us-east-2.amazonaws.com/us-east-1_Example1",
            _ISSUER_ERROR,
        ),
        (
            "OIDC_ISSUER",
            "https://cognito-idp.sa-east-1.amazonaws.com/sa-east-1_Example1",
            _ISSUER_ERROR,
        ),
        (
            "OIDC_DOMAIN",
            "https://cnesdata-prod-example.auth.us-east-1.amazoncognito.com",
            "oidc_domain_invalid",
        ),
    ],
    ids=["pool_us_east_1", "pool_de_outra_regiao", "issuer_sa_east_1", "dominio_us_east_1"],
)
def test_producao_exige_regiao_us_east_2(key: str, value: str, error: str) -> None:
    _assert_rejected(PRODUCTION_VALUES | {key: value}, error)


@pytest.mark.parametrize(
    "issuer",
    [
        "https://id.example.test/realms/cnes",
        "https://cognito-idp.us-east-2.amazonaws.com",
        "https://cognito-idp.us-east-2.amazonaws.com/us-east-2_",
        "https://cognito-idp.us-east-2.amazonaws.com/us-east-2_Example-1",
        "https://cognito-idp.us-east-2.amazonaws.com/us-east-2_Example1/extra",
        "https://cognito-idp.us-east-2.amazonaws.com:443/us-east-2_Example1",
        "https://cnesdata-prod-example.auth.us-east-2.amazoncognito.com",
    ],
    ids=[
        "keycloak", "sem_pool", "pool_vazio", "pool_com_hifen", "caminho_extra", "porta",
        "dominio_no_lugar_do_issuer",
    ],
)
def test_producao_exige_issuer_de_user_pool_cognito(issuer: str) -> None:
    _assert_rejected(PRODUCTION_VALUES | {"OIDC_ISSUER": issuer}, _ISSUER_ERROR)


@pytest.mark.parametrize("value", [None, ""], ids=["ausente", "vazio"])
def test_producao_exige_oidc_domain(value: str | None) -> None:
    _assert_rejected(_with("OIDC_DOMAIN", value), "production_oidc_domain_required")


@pytest.mark.parametrize(
    "domain",
    [
        " ",
        "cnesdata-prod-example.auth.us-east-2.amazoncognito.com",
        "https://auth.cnesdata.com.br",
        "http://cnesdata-prod-example.auth.us-east-2.amazoncognito.com",
        "https://cnesdata-prod-example.auth.us-east-2.amazoncognito.com/",
        "https://cnesdata-prod-example.auth.us-east-2.amazoncognito.com/oauth2/authorize",
        "https://cnesdata-prod-example.auth.us-east-2.amazoncognito.com:443",
        "https://cnesdata-prod-example.auth-fips.us-east-2.amazoncognito.com",
        "https://CNESDATA.auth.us-east-2.amazoncognito.com",
        "https://-cnesdata.auth.us-east-2.amazoncognito.com",
        "https://cnesdata-.auth.us-east-2.amazoncognito.com",
        "https://cnes_data.auth.us-east-2.amazoncognito.com",
        "https://cnes.data.auth.us-east-2.amazoncognito.com",
        "https://.auth.us-east-2.amazoncognito.com",
        f"https://{'a' * 64}.auth.us-east-2.amazoncognito.com",
        "https://cnesdata-prod-example.auth.us-east-2.amazoncognito.com\n",
    ],
    ids=[
        "em_branco", "sem_esquema", "dominio_customizado", "http", "barra_final", "caminho",
        "porta", "fips", "maiusculas", "inicia_com_hifen", "termina_com_hifen", "underscore",
        "subdominio", "prefixo_vazio", "prefixo_longo", "quebra_de_linha",
    ],
)
def test_producao_rejeita_oidc_domain_invalido(domain: str) -> None:
    _assert_rejected(PRODUCTION_VALUES | {"OIDC_DOMAIN": domain}, "oidc_domain_invalid")


@pytest.mark.parametrize(
    "prefix",
    ["a", "0", "cnesdata-prod-1a2b", "a" + "b" * 61 + "c"],
    ids=["um_caractere", "digito", "com_hifen", "sessenta_e_tres_caracteres"],
)
def test_producao_aceita_prefixo_de_dominio_cognito(prefix: str) -> None:
    domain = f"https://{prefix}.auth.us-east-2.amazoncognito.com"

    settings = AwsRuntimeSettings.from_mapping(PRODUCTION_VALUES | {"OIDC_DOMAIN": domain})

    assert settings.oidc_domain == domain
    assert settings.oidc_logout_url == f"{domain}/logout"
