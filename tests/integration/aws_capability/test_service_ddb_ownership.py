"""Ownership DynamoDB fresco com identidade e tags completas."""

from pathlib import Path

import pytest

from tests.integration.aws_capability._service_read_assertions import (
    assert_private,
    assert_process,
    assert_rejected,
)
from tests.integration.aws_capability._service_read_testing import (
    ROOT,
    ReadLab,
    fixture_data,
    make_read_lab,
    respond,
)
from tests.integration.aws_capability.contracts import Page, ServiceFailure

TABLE_ARN = "arn:aws:dynamodb:us-east-2:111111111111:table/cnesdata-cap-a1b2c3d4"
DESCRIBE = ("dynamodb", "describe-table", "us-east-2", {"TableName": "cnesdata-cap-a1b2c3d4"})
TAGS = ("dynamodb", "list-tags-of-resource", "us-east-2", {"ResourceArn": TABLE_ARN})


@pytest.fixture
def read_lab(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> ReadLab:
    return make_read_lab(monkeypatch, tmp_path)


def test_inspect_confere_identidade_e_todas_as_paginas_de_tags(read_lab: ReadLab) -> None:
    data = fixture_data("ownership")
    respond(read_lab, data["describe"])
    respond(read_lab, data["tags_first"])
    respond(read_lab, data["tags_last"])
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
    assert_process(
        read_lab,
        [
            DESCRIBE,
            TAGS,
            (
                "dynamodb",
                "list-tags-of-resource",
                "us-east-2",
                {
                    "ResourceArn": TABLE_ARN,
                    "NextToken": "tags-next",
                },
            ),
        ],
    )


@pytest.mark.parametrize(
    "case",
    [
        "wrong_account",
        "wrong_region",
        "wrong_name",
        "wrong_arn_name",
        "wrong_service",
        "missing_region",
        "missing_arn",
        "empty",
        "bad_table_type",
    ],
)
def test_describe_incoerente_nao_autoriza_leitura_de_tags(read_lab: ReadLab, case: str) -> None:
    respond(read_lab, fixture_data("ownership")[case])
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(ROOT, read_lab.context)
    assert_rejected(error.value)
    assert_process(read_lab, [DESCRIBE])


@pytest.mark.parametrize(
    "case", ["tags_duplicate", "tags_conflict", "tags_empty_token", "tags_missing"]
)
def test_tags_ambiguas_ou_incompletas_nao_produzem_ownership(read_lab: ReadLab, case: str) -> None:
    respond(read_lab, fixture_data("ownership")["describe"])
    respond(read_lab, fixture_data("ownership")[case])
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(ROOT, read_lab.context)
    assert_rejected(error.value)
    assert_process(read_lab, [DESCRIBE, TAGS])


def test_token_repetido_de_tags_nao_e_pagina_final(read_lab: ReadLab) -> None:
    respond(read_lab, fixture_data("ownership")["describe"])
    respond(read_lab, fixture_data("ownership")["tags_first"])
    respond(read_lab, fixture_data("ownership")["tags_repeat"])
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.inspect(ROOT, read_lab.context)
    assert_rejected(error.value)
    assert_process(
        read_lab,
        [
            DESCRIBE,
            TAGS,
            (
                "dynamodb",
                "list-tags-of-resource",
                "us-east-2",
                {
                    "ResourceArn": TABLE_ARN,
                    "NextToken": "tags-next",
                },
            ),
        ],
    )


def test_segunda_inspecao_rele_tags_removidas_sem_cache(read_lab: ReadLab) -> None:
    data = fixture_data("ownership")
    respond(read_lab, data["describe"])
    respond(read_lab, data["tags_complete"])
    first = read_lab.services.inspect(ROOT, read_lab.context)
    assert first.tags.get("Run") == "a1b2c3d4"
    respond(read_lab, data["describe"])
    respond(read_lab, data["tags_removed"])
    second = read_lab.services.inspect(ROOT, read_lab.context)
    assert (second.account, second.region, dict(second.tags)) == (
        "111111111111",
        "us-east-2",
        {"Project": "CnesData"},
    )
    assert_process(read_lab, [DESCRIBE, TAGS, DESCRIBE, TAGS])


def test_dynamodb_e_folha_apos_ownership_fresco_sem_filhos_artificiais(read_lab: ReadLab) -> None:
    respond(read_lab, fixture_data("ownership")["describe"])
    respond(read_lab, fixture_data("ownership")["tags_complete"])
    assert read_lab.services.dependents(ROOT, None, read_lab.context) == Page((), None)
    assert_process(read_lab, [DESCRIBE, TAGS])


def test_folha_nao_pula_validacao_fresca_das_tags_do_root(read_lab: ReadLab) -> None:
    respond(read_lab, fixture_data("ownership")["describe"])
    respond(read_lab, fixture_data("ownership")["tags_removed"])
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.dependents(ROOT, None, read_lab.context)
    assert_rejected(error.value)
    assert_process(read_lab, [DESCRIBE, TAGS])


def test_segunda_inspecao_nao_reutiliza_arn_validado_antes(read_lab: ReadLab) -> None:
    respond(read_lab, fixture_data("ownership")["describe"])
    respond(read_lab, fixture_data("ownership")["tags_complete"])
    assert read_lab.services.inspect(ROOT, read_lab.context).account == "111111111111"
    respond(read_lab, fixture_data("ownership")["wrong_account"])
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.inspect(ROOT, read_lab.context)
    assert_rejected(error.value)
    assert_process(read_lab, [DESCRIBE, TAGS, DESCRIBE])


@pytest.mark.parametrize(
    "second",
    [
        {"Tags": [{"Key": "Project", "Value": "CnesData"}]},
        {"Tags": [{"Key": "Project", "Value": "Other"}]},
    ],
    ids=["duplicada", "conflitante"],
)
def test_tag_repetida_entre_paginas_nao_e_sobrescrita_silenciosamente(
    read_lab: ReadLab, second: dict[str, object]
) -> None:
    respond(read_lab, fixture_data("ownership")["describe"])
    respond(read_lab, fixture_data("ownership")["tags_first"])
    respond(read_lab, second)
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.inspect(ROOT, read_lab.context)
    assert_rejected(error.value)
    assert_process(
        read_lab,
        [
            DESCRIBE,
            TAGS,
            (
                "dynamodb",
                "list-tags-of-resource",
                "us-east-2",
                {
                    "ResourceArn": TABLE_ARN,
                    "NextToken": "tags-next",
                },
            ),
        ],
    )


@pytest.mark.parametrize(
    "case",
    [
        "tags_shape_object",
        "tags_shape_null",
        "tags_shape_string",
        "tag_missing_key",
        "tag_missing_value",
        "tag_not_object",
        "tag_key_number",
        "tag_key_bool",
        "tag_key_null",
        "tag_value_number",
        "tag_value_object",
        "tag_value_null",
        "tags_token_number",
        "tags_token_null",
        "tags_token_list",
    ],
)
def test_shape_invalido_de_tags_nao_produz_ownership(
    read_lab: ReadLab,
    case: str,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    respond(read_lab, fixture_data("ownership")["describe"])
    respond(read_lab, fixture_data("ownership")[case])
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.inspect(ROOT, read_lab.context)
    assert_rejected(error.value)
    captured = capsys.readouterr()
    assert_private(error.value, read_lab, caplog.text + captured.out + captured.err)
    assert_process(read_lab, [DESCRIBE, TAGS])


def test_value_vazio_de_tag_e_string_valida_sem_rejeitar_campos_aws(read_lab: ReadLab) -> None:
    respond(read_lab, fixture_data("ownership")["describe"])
    respond(read_lab, fixture_data("ownership")["tags_empty_value"])
    owner = read_lab.services.inspect(ROOT, read_lab.context)
    assert (owner.account, owner.region, dict(owner.tags)) == (
        "111111111111",
        "us-east-2",
        {
            "Project": "CnesData",
            "Purpose": "aws-capability",
            "Run": "a1b2c3d4",
            "ExpiresAt": "1060",
            "Optional": "",
        },
    )
    assert_process(read_lab, [DESCRIBE, TAGS])
