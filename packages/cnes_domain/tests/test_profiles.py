from pathlib import Path

import pytest
from pydantic import ValidationError

from cnes_domain.profiles import (
    AuthMode,
    BillingMode,
    ProfileNotImplemented,
    ProfileSettings,
    RuntimeProfile,
    local_objects_dir,
    local_state_db,
    local_state_db_arcname,
    parse_profile,
)


def test_aplica_defaults_e_converte_diretorio() -> None:
    settings = parse_profile({"TENANT_ID": "354130", "DATA_DIR": "tenant-data"})

    assert settings == ProfileSettings(
        profile=RuntimeProfile.LOCAL,
        tenant_id="354130",
        data_dir=Path("tenant-data"),
        auth_mode=AuthMode.LOCAL,
        billing_mode=BillingMode.DISABLED,
        oidc_issuer=None,
    )


def test_aplica_diretorio_default() -> None:
    settings = parse_profile({"TENANT_ID": "354130"})

    assert settings.data_dir == Path("data")


@pytest.mark.parametrize("tenant_id", [None, "", "35413", "3541308", "35413A"])
def test_rejeita_tenant_ausente_ou_invalido(tenant_id: str | None) -> None:
    env = {} if tenant_id is None else {"TENANT_ID": tenant_id}

    with pytest.raises(ValidationError):
        parse_profile(env)


def test_impede_mutacao() -> None:
    settings = parse_profile({"TENANT_ID": "354130"})

    with pytest.raises(ValidationError, match="frozen_instance"):
        settings.tenant_id = "123456"


def test_rejeita_campo_extra() -> None:
    with pytest.raises(ValidationError, match="extra_forbidden"):
        ProfileSettings(tenant_id="354130", unexpected=True)


def test_rejeita_stripe_no_profile_local() -> None:
    with pytest.raises(ValidationError, match="local_billing_disabled"):
        parse_profile({"TENANT_ID": "354130", "BILLING_MODE": "stripe"})


@pytest.mark.parametrize("issuer", [None, "", "   "])
def test_rejeita_oidc_sem_issuer(issuer: str | None) -> None:
    env = {"TENANT_ID": "354130", "AUTH_MODE": "oidc"}
    if issuer is not None:
        env["OIDC_ISSUER"] = issuer

    with pytest.raises(ValidationError, match="oidc_issuer_required"):
        parse_profile(env)


def test_aceita_profile_aws_com_oidc_e_stripe() -> None:
    settings = parse_profile(
        {
            "PROFILE": "aws",
            "TENANT_ID": "354130",
            "DATA_DIR": "/var/lib/cnes",
            "AUTH_MODE": "oidc",
            "BILLING_MODE": "stripe",
            "OIDC_ISSUER": "https://issuer.example",
        }
    )

    assert settings.profile is RuntimeProfile.AWS
    assert settings.auth_mode is AuthMode.OIDC
    assert settings.billing_mode is BillingMode.STRIPE
    assert settings.oidc_issuer == "https://issuer.example"


@pytest.mark.parametrize(
    ("name", "value"),
    [("PROFILE", "desktop"), ("AUTH_MODE", "jwt"), ("BILLING_MODE", "manual")],
)
def test_rejeita_enum_desconhecido(name: str, value: str) -> None:
    with pytest.raises(ValidationError, match="enum"):
        parse_profile({"TENANT_ID": "354130", name: value})


def test_ignora_variaveis_ambientais_nao_relacionadas() -> None:
    settings = parse_profile(
        {
            "TENANT_ID": "354130",
            "AWS_SECRET_ACCESS_KEY": "secret",
            "DATABASE_URL": "postgresql://ignored",
        }
    )

    assert settings == ProfileSettings(tenant_id="354130")


def test_profile_not_implemented_e_not_implemented_error() -> None:
    with pytest.raises(ProfileNotImplemented, match="aws_runtime_plan_required"):
        raise ProfileNotImplemented("aws_runtime_plan_required")


def test_state_db_deriva_de_data_dir() -> None:
    settings = parse_profile({"TENANT_ID": "354130", "DATA_DIR": "tenant-data"})

    assert settings.state_db == Path("tenant-data/state/cnesdata.sqlite3")
    assert settings.state_db == local_state_db(Path("tenant-data"))


def test_objects_dir_deriva_de_data_dir() -> None:
    settings = parse_profile({"TENANT_ID": "354130", "DATA_DIR": "tenant-data"})

    assert settings.objects_dir == Path("tenant-data/objects")
    assert settings.objects_dir == local_objects_dir(Path("tenant-data"))


def test_state_db_arcname_e_relativo_e_consistente_com_state_db() -> None:
    data_dir = Path("tenant-data")

    assert local_state_db_arcname() == "state/cnesdata.sqlite3"
    assert local_state_db(data_dir) == data_dir / local_state_db_arcname()
