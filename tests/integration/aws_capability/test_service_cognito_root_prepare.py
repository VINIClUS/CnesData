"""Readback de identidade, ownership e settings precede o terminal deliberado."""

from dataclasses import replace

import pytest

from tests.integration.aws_capability._profiles import PROFILES
from tests.integration.aws_capability._service_cognito_root_testing import (
    ROOT,
    RootLab,
    assert_private_channels,
    assert_trace,
    queue_trace,
    root_lab,
)
from tests.integration.aws_capability._service_read_assertions import assert_rejected
from tests.integration.aws_capability.contracts import ServiceFailure

__all__ = ["root_lab"]


@pytest.mark.parametrize(
    "profile",
    ["cognito-login", "cognito-pkce-negatives", "cognito-sdk"],
    ids=["login", "pkce", "sdk"],
)
def test_confere_root_antes_terminal(root_lab: RootLab, profile: str) -> None:
    root_lab.services = replace(
        root_lab.services, spec=replace(root_lab.services.spec, probe=profile)
    )
    root_lab.context.budget.limits = PROFILES[profile].limits
    steps = [["D", "pool_active"], ["T", "tags_owner"]]
    queue_trace(root_lab, steps)
    with pytest.raises(ServiceFailure) as error:
        root_lab.services.probe([ROOT], root_lab.services.spec, root_lab.context)
    assert_trace(root_lab, steps, ["work", "work"])
    assert error.value.args == ("probe=not_implemented",)
    assert PROFILES[profile].snapshot(profile)["limits"] == {
        "calls": 256 if profile == "cognito-sdk" else 512,
        "pages": 8,
        "attempts": 2,
        "cleanup_seconds": 90,
        "cleanup_reserve": 128 if profile == "cognito-sdk" else 256,
    }
    assert list(root_lab.directory.iterdir()) == []


@pytest.mark.parametrize(
    "reply",
    ["pool_lite", "pool_plus", "pool_inactive", "pool_no_protection", "pool_bool_protection"],
    ids=["lite", "plus", "inactive", "protecao_ausente", "protecao_bool"],
)
def test_rejeita_tier_e_protecao(root_lab: RootLab, reply: str) -> None:
    steps = [["D", reply], ["T", "tags_owner"]]
    queue_trace(root_lab, steps)
    with pytest.raises(ServiceFailure) as error:
        root_lab.services.probe([ROOT], root_lab.services.spec, root_lab.context)
    assert_trace(root_lab, steps, ["work", "work"])
    assert_private_channels(root_lab, error.value)
    assert_rejected(error.value)
    assert error.value.args != ("probe=not_implemented",)


@pytest.mark.parametrize(
    "reply",
    [
        "pool_mfa_on",
        "pool_username_phone",
        "pool_case_true",
        "pool_case_int",
        "pool_admin_false",
        "pool_admin_int",
        "pool_recovery_phone",
        "pool_minimum13",
        "pool_lower_false",
        "pool_upper_false",
        "pool_numbers_false",
        "pool_symbols_false",
        "pool_trigger",
        "pool_sms",
        "pool_issuer_updated",
    ],
    ids=[
        "mfa_on",
        "username_phone",
        "case_true",
        "case_int",
        "admin_false",
        "admin_int",
        "recovery_phone",
        "minimo13",
        "lower_false",
        "upper_false",
        "numbers_false",
        "symbols_false",
        "trigger",
        "sms",
        "issuer_updated",
    ],
)
def test_rejeita_settings(root_lab: RootLab, reply: str) -> None:
    steps = [["D", reply], ["T", "tags_owner"]]
    queue_trace(root_lab, steps)
    with pytest.raises(ServiceFailure) as error:
        root_lab.services.probe([ROOT], root_lab.services.spec, root_lab.context)
    assert_trace(root_lab, steps, ["work", "work"])
    assert_private_channels(root_lab, error.value)
    assert_rejected(error.value)
    assert error.value.args != ("probe=not_implemented",)


@pytest.mark.parametrize(
    ("describe", "tags"),
    [
        ("pool_active", "tags_project"),
        ("pool_active", "tags_purpose"),
        ("pool_active", "tags_run"),
        ("pool_active", "tags_expires"),
        ("pool_bad_arn_account", None),
        ("pool_bad_arn_region", None),
        ("pool_bad_name", None),
        ("pool_bad_id", None),
    ],
    ids=[
        "tag_project",
        "tag_purpose",
        "tag_run",
        "tag_expires",
        "arn_conta",
        "arn_regiao",
        "nome_errado",
        "id_errado",
    ],
)
def test_rejeita_owner(root_lab: RootLab, describe: str, tags: str | None) -> None:
    steps = [["D", describe]] if tags is None else [["D", describe], ["T", tags]]
    queue_trace(root_lab, steps)
    with pytest.raises(ServiceFailure) as error:
        root_lab.services.probe([ROOT], root_lab.services.spec, root_lab.context)
    assert_trace(root_lab, steps, ["work"] if tags is None else ["work", "work"])
    assert_private_channels(root_lab, error.value)
    assert_rejected(error.value)
    assert error.value.args != ("probe=not_implemented",)


@pytest.mark.parametrize(
    "reply",
    ["error_rnf", "error_denied", "error_local", "error_timeout", "error_invalid_json"],
    ids=["rnf", "denied", "local", "timeout", "json_invalido"],
)
def test_nao_promove_erro_a_readback(root_lab: RootLab, reply: str) -> None:
    steps = [["D", reply]]
    queue_trace(root_lab, steps)
    with pytest.raises(ServiceFailure) as error:
        root_lab.services.probe([ROOT], root_lab.services.spec, root_lab.context)
    assert_trace(root_lab, steps, ["work"])
    assert_private_channels(root_lab, error.value)
    assert_rejected(error.value)
    assert error.value.args != ("probe=not_implemented",)


@pytest.mark.parametrize(
    "case",
    ["spec_divergente", "sem_root", "root_outro_run"],
    ids=["spec_divergente", "sem_root", "root_outro_run"],
)
def test_rejeita_entrada_de_probe(root_lab: RootLab, case: str) -> None:
    spec = root_lab.services.spec
    resources = [ROOT]
    if case == "spec_divergente":
        spec = replace(spec, account="222222222222")
    elif case == "sem_root":
        resources = []
    else:
        resources = [replace(ROOT, name="cnesdata-cap-11223344")]
    with pytest.raises(ServiceFailure) as error:
        root_lab.services.probe(resources, spec, root_lab.context)
    assert_trace(root_lab, [], [])
    assert_private_channels(root_lab, error.value)
    assert_rejected(error.value)
    assert error.value.args != ("probe=not_implemented",)
