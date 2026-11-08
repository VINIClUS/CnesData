"""Ownership Cognito exige describe regional e tags frescas."""

from pathlib import Path

import pytest

from tests.integration.aws_capability._safety import owned
from tests.integration.aws_capability._service_cognito_testing import (
    ROOT,
    ReadLab,
    fixture_data,
    make_read_lab,
    respond,
)
from tests.integration.aws_capability._service_read_assertions import (
    assert_private,
    assert_process,
)
from tests.integration.aws_capability.contracts import Resource, ServiceFailure

POOL_ARN = "arn:aws:cognito-idp:us-east-2:111111111111:userpool/us-east-2_a1b2c3d4"
DESCRIBE = (
    "cognito-idp",
    "describe-user-pool",
    "us-east-2",
    {"UserPoolId": "us-east-2_a1b2c3d4"},
)
TAGS = (
    "cognito-idp",
    "list-tags-for-resource",
    "us-east-2",
    {"ResourceArn": POOL_ARN},
)


@pytest.fixture
def read_lab(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> ReadLab:
    return make_read_lab(monkeypatch, tmp_path)


def test_inspect_confere_identidade_e_tags_do_pool(read_lab: ReadLab) -> None:
    respond(read_lab, fixture_data("cognito")["describe"])
    respond(read_lab, fixture_data("cognito")["tags"])
    owner = read_lab.services.inspect(ROOT, read_lab.context)
    assert (owner.account, owner.region, dict(owner.tags)) == (
        "111111111111",
        "us-east-2",
        {
            "Project": "CnesData",
            "Purpose": "aws-capability",
            "Run": "a1b2c3d4",
            "ExpiresAt": "1060",
        },
    )
    assert_process(read_lab, [DESCRIBE, TAGS])


def test_inspect_le_id_regional_atribuido_pela_aws(read_lab: ReadLab) -> None:
    resource = Resource("cognito", "us-east-2_7f9x2m4q", "aws-assigned-pool")
    describe = (
        "cognito-idp",
        "describe-user-pool",
        "us-east-2",
        {"UserPoolId": resource.resource_id},
    )
    tags = (
        "cognito-idp",
        "list-tags-for-resource",
        "us-east-2",
        {
            "ResourceArn": (
                "arn:aws:cognito-idp:us-east-2:111111111111:userpool/"
                "us-east-2_7f9x2m4q"
            )
        },
    )
    respond(read_lab, fixture_data("cognito")["describe_aws_assigned"])
    respond(read_lab, fixture_data("cognito")["tags_aws_assigned"])
    owner = read_lab.services.inspect(resource, read_lab.context)
    assert (owner.account, owner.region, dict(owner.tags)) == (
        "111111111111",
        "us-east-2",
        {"Project": "CnesData", "Purpose": "aws-capability", "Run": "a1b2c3d4"},
    )
    assert not owned(resource, owner, read_lab.services.spec)
    assert_process(read_lab, [describe, tags])


def test_inspect_rele_pool_legacy_com_tags_nao_owned(read_lab: ReadLab) -> None:
    resource = Resource("cognito", "us-east-2_legacy99", "legacy-pool")
    describe = (
        "cognito-idp",
        "describe-user-pool",
        "us-east-2",
        {"UserPoolId": resource.resource_id},
    )
    tags = (
        "cognito-idp",
        "list-tags-for-resource",
        "us-east-2",
        {
            "ResourceArn": (
                "arn:aws:cognito-idp:us-east-2:111111111111:userpool/"
                "us-east-2_legacy99"
            )
        },
    )
    respond(read_lab, fixture_data("cognito")["describe_legacy"])
    respond(read_lab, fixture_data("cognito")["tags_legacy"])
    owner = read_lab.services.inspect(resource, read_lab.context)
    assert (owner.account, owner.region, dict(owner.tags)) == (
        "111111111111",
        "us-east-2",
        {"Project": "legacy-system", "Purpose": "imported"},
    )
    assert not owned(resource, owner, read_lab.services.spec)
    assert_process(read_lab, [describe, tags])


def test_campos_aws_extras_do_describe_sao_aceitos_e_ignorados(read_lab: ReadLab) -> None:
    respond(read_lab, fixture_data("cognito")["describe"])
    respond(read_lab, fixture_data("cognito")["tags_empty"])
    owner = read_lab.services.lookup(ROOT, read_lab.context)
    assert owner is not None
    assert dict(owner.tags) == {"Optional": ""}
    assert_process(read_lab, [DESCRIBE, TAGS])


@pytest.mark.parametrize(
    "case",
    [
        "wrong_account",
        "wrong_region",
        "wrong_service",
        "wrong_id",
        "wrong_name",
        "wrong_arn",
        "missing_id",
        "missing_name",
        "missing_arn",
        "empty",
        "bad_pool_type",
    ],
)
def test_describe_incoerente_nao_autoriza_tags(read_lab: ReadLab, case: str) -> None:
    respond(read_lab, fixture_data("cognito")[case])
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(ROOT, read_lab.context)
    assert error.value.args == ("service=describe_invalid",)
    assert_process(read_lab, [DESCRIBE])


def test_describe_cross_region_falha_fechado_sem_ownership_estrangeiro(read_lab: ReadLab) -> None:
    respond(read_lab, fixture_data("cognito")["wrong_region"])
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.inspect(ROOT, read_lab.context)
    assert error.value.args == ("service=describe_invalid",)
    assert_process(read_lab, [DESCRIBE])


@pytest.mark.parametrize(
    "resource",
    [
        Resource("dynamodb", "us-east-2_a1b2c3d4", "cnesdata-cap-a1b2c3d4"),
        Resource("cognito", "foreign", "cnesdata-cap-a1b2c3d4"),
        Resource("cognito", "us-east-2_a1b2c3d4", "foreign"),
    ],
    ids=["servico_errado", "id_errado", "nome_errado"],
)
def test_recurso_invalido_recusa_antes_de_qualquer_processo(
    read_lab: ReadLab, resource: Resource
) -> None:
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.inspect(resource, read_lab.context)
    assert error.value.args == ("service=resource_invalid",)
    assert_process(read_lab, [])


@pytest.mark.parametrize(
    "case",
    [
        "tags_missing",
        "tags_object",
        "tags_null",
        "tags_string",
        "tags_key_number",
        "tags_value_object",
        "tags_value_null",
    ],
)
def test_tags_com_formato_invalido_nao_produzem_ownership(
    read_lab: ReadLab,
    case: str,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    respond(read_lab, fixture_data("cognito")["describe"])
    respond(read_lab, fixture_data("cognito")[case])
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.inspect(ROOT, read_lab.context)
    assert error.value.args == ("service=tags_invalid",)
    captured = capsys.readouterr()
    assert_private(error.value, read_lab, caplog.text + captured.out + captured.err)
    assert_process(read_lab, [DESCRIBE, TAGS])


def test_segunda_inspecao_rele_tags_removidas_sem_cache(read_lab: ReadLab) -> None:
    respond(read_lab, fixture_data("cognito")["describe"])
    respond(read_lab, fixture_data("cognito")["tags"])
    first = read_lab.services.inspect(ROOT, read_lab.context)
    assert first.tags.get("Run") == "a1b2c3d4"
    respond(read_lab, fixture_data("cognito")["describe"])
    respond(read_lab, fixture_data("cognito")["tags_removed"])
    second = read_lab.services.inspect(ROOT, read_lab.context)
    assert dict(second.tags) == {"Project": "CnesData"}
    assert_process(read_lab, [DESCRIBE, TAGS, DESCRIBE, TAGS])


def test_arn_validado_nao_e_reutilizado_em_nova_inspecao(read_lab: ReadLab) -> None:
    respond(read_lab, fixture_data("cognito")["describe"])
    respond(read_lab, fixture_data("cognito")["tags"])
    assert read_lab.services.inspect(ROOT, read_lab.context).account == "111111111111"
    respond(read_lab, fixture_data("cognito")["wrong_account"])
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.inspect(ROOT, read_lab.context)
    assert error.value.args == ("service=describe_invalid",)
    assert_process(read_lab, [DESCRIBE, TAGS, DESCRIBE])
