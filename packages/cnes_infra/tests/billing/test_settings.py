"""Testes das settings de billing."""

import pytest

from cnes_domain.billing.models import BillingEnforcementMode
from cnes_domain.profiles import BillingMode
from cnes_infra.billing.settings import (
    LOCAL_BILLING_SETTINGS,
    BillingConfigurationError,
    BillingSettings,
)

_LOCAL = {"PROFILE": "local", "TENANT_ID": "354130"}
_AWS = {"PROFILE": "aws"}


def test_local_defaulta_billing_disabled():
    settings = BillingSettings.from_mapping(_LOCAL)

    assert settings.mode is BillingMode.DISABLED
    assert settings.enforcement_mode is BillingEnforcementMode.OFF
    assert settings.cache_ttl_seconds == 60
    assert settings.enforced is False


@pytest.mark.parametrize("mode", list(BillingMode))
def test_aws_aceita_disabled_e_stripe(mode):
    settings = BillingSettings.from_mapping({**_AWS, "BILLING_MODE": mode.value})

    assert settings.mode is mode


def test_aws_sem_billing_mode_defaulta_disabled():
    assert BillingSettings.from_mapping(_AWS).mode is BillingMode.DISABLED


def test_local_com_stripe_rejeita_configuracao():
    with pytest.raises(BillingConfigurationError) as caught:
        BillingSettings.from_mapping({**_LOCAL, "BILLING_MODE": "stripe"})

    assert caught.value.code == "local_billing_disabled"
    assert str(caught.value) == "code=local_billing_disabled"


def test_billing_mode_invalido_rejeita_configuracao():
    with pytest.raises(BillingConfigurationError) as caught:
        BillingSettings.from_mapping({**_AWS, "BILLING_MODE": "bogus"})

    assert caught.value.code == "billing_profile_invalid"


def test_profile_invalido_rejeita_configuracao():
    with pytest.raises(BillingConfigurationError) as caught:
        BillingSettings.from_mapping({"PROFILE": "bogus"})

    assert caught.value.code == "billing_profile_invalid"


@pytest.mark.parametrize("tenant", ["tenant-a", "", "12345"])
def test_aws_ignora_tenant_id_fora_do_formato_local(tenant):
    settings = BillingSettings.from_mapping({**_AWS, "TENANT_ID": tenant, "BILLING_MODE": "stripe"})

    assert settings.mode is BillingMode.STRIPE


def test_local_continua_validando_tenant_id():
    with pytest.raises(BillingConfigurationError) as caught:
        BillingSettings.from_mapping({"PROFILE": "local", "TENANT_ID": "tenant-a"})

    assert caught.value.code == "billing_profile_invalid"


def test_local_sem_tenant_rejeita_configuracao():
    with pytest.raises(BillingConfigurationError) as caught:
        BillingSettings.from_mapping({"PROFILE": "local"})

    assert caught.value.code == "tenant_id_required"


@pytest.mark.parametrize("raw", ["0", "60"])
def test_ttl_aceita_limites(raw):
    settings = BillingSettings.from_mapping({**_LOCAL, "BILLING_CACHE_TTL_SECONDS": raw})

    assert settings.cache_ttl_seconds == int(raw)


@pytest.mark.parametrize("raw", ["-1", "61"])
def test_ttl_fora_da_faixa_rejeita_configuracao(raw):
    with pytest.raises(BillingConfigurationError) as caught:
        BillingSettings.from_mapping({**_LOCAL, "BILLING_CACHE_TTL_SECONDS": raw})

    assert caught.value.code == "billing_cache_ttl_out_of_range"


def test_ttl_nao_inteiro_rejeita_configuracao():
    with pytest.raises(BillingConfigurationError) as caught:
        BillingSettings.from_mapping({**_LOCAL, "BILLING_CACHE_TTL_SECONDS": "abc"})

    assert caught.value.code == "billing_cache_ttl_invalid"


@pytest.mark.parametrize("mode", list(BillingEnforcementMode))
def test_enforcement_mode_valido_e_interpretado(mode):
    settings = BillingSettings.from_mapping({**_LOCAL, "BILLING_ENFORCEMENT_MODE": mode.value})

    assert settings.enforcement_mode is mode


@pytest.mark.parametrize("raw", ["ENFORCE", "strict"])
def test_enforcement_mode_invalido_rejeita_configuracao(raw):
    with pytest.raises(BillingConfigurationError) as caught:
        BillingSettings.from_mapping({**_LOCAL, "BILLING_ENFORCEMENT_MODE": raw})

    assert caught.value.code == "billing_enforcement_mode_invalid"


@pytest.mark.parametrize("mode", list(BillingMode))
@pytest.mark.parametrize("enforcement", list(BillingEnforcementMode))
def test_enforced_somente_para_stripe_com_enforce(mode, enforcement):
    settings = BillingSettings(mode, enforcement, 60)

    expected = mode is BillingMode.STRIPE and enforcement is BillingEnforcementMode.ENFORCE
    assert settings.enforced is expected


def test_local_billing_settings_desabilita_billing():
    assert LOCAL_BILLING_SETTINGS.mode is BillingMode.DISABLED
    assert LOCAL_BILLING_SETTINGS.enforcement_mode is BillingEnforcementMode.OFF
    assert LOCAL_BILLING_SETTINGS.cache_ttl_seconds == 60


def test_erro_de_configuracao_e_value_error():
    assert issubclass(BillingConfigurationError, ValueError)


def test_chaves_nao_relacionadas_sao_ignoradas():
    settings = BillingSettings.from_mapping({**_LOCAL, "AUTH_MODE": "oidc"})

    assert settings.mode is BillingMode.DISABLED


@pytest.mark.parametrize(
    ("mode", "enforcement", "expected"),
    [
        (BillingMode.DISABLED, BillingEnforcementMode.ENFORCE, BillingMode.DISABLED),
        (BillingMode.STRIPE, BillingEnforcementMode.OFF, BillingMode.DISABLED),
        (BillingMode.STRIPE, BillingEnforcementMode.SHADOW, BillingMode.DISABLED),
        (BillingMode.STRIPE, BillingEnforcementMode.ENFORCE, BillingMode.STRIPE),
    ],
)
def test_modo_de_execucao_so_exige_companion_com_stripe_em_enforce(
    mode: BillingMode, enforcement: BillingEnforcementMode, expected: BillingMode,
) -> None:
    assert BillingSettings(mode, enforcement, 60).execution_mode is expected
