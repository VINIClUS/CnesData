"""Lookup por ID DynamoDB distingue ausência remota de leitura inconclusiva."""

import subprocess
from pathlib import Path

import pytest

from tests.integration.aws_capability._service_read_assertions import (
    assert_process,
    assert_rejected,
)
from tests.integration.aws_capability._service_read_testing import (
    ROOT,
    ReadLab,
    fixture_data,
    make_read_lab,
    respond,
    respond_error,
)
from tests.integration.aws_capability.contracts import ServiceFailure

TABLE_ARN = "arn:aws:dynamodb:us-east-2:111111111111:table/cnesdata-cap-a1b2c3d4"
DESCRIBE = ("dynamodb", "describe-table", "us-east-2", {"TableName": "cnesdata-cap-a1b2c3d4"})
TAGS = ("dynamodb", "list-tags-of-resource", "us-east-2", {"ResourceArn": TABLE_ARN})


@pytest.fixture
def read_lab(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> ReadLab:
    return make_read_lab(monkeypatch, tmp_path)


def test_lookup_ausente_exige_not_found_remoto_no_describe_exato(read_lab: ReadLab) -> None:
    respond_error(read_lab, "remote_not_found")
    assert read_lab.services.lookup(ROOT, read_lab.context) is None
    assert_process(read_lab, [DESCRIBE])


@pytest.mark.parametrize(
    "failure",
    [
        "local_not_found",
        "remote_denied",
        "remote_403",
        "remote_404",
        "no_tags",
        "bad_json",
        "empty_stdout",
        "empty_object",
        "bad_stderr",
    ],
)
def test_erro_ambiguo_no_describe_nao_e_ausencia(read_lab: ReadLab, failure: str) -> None:
    respond_error(read_lab, failure)
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(ROOT, read_lab.context)
    assert_rejected(error.value)
    assert_process(read_lab, [DESCRIBE])


@pytest.mark.parametrize("failure", ["remote_not_found", "remote_denied", "no_tags", "bad_json"])
def test_erro_de_tags_depois_do_describe_nao_e_ausencia(read_lab: ReadLab, failure: str) -> None:
    respond(read_lab, fixture_data("ownership")["describe"])
    respond_error(read_lab, failure)
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(ROOT, read_lab.context)
    assert_rejected(error.value)
    assert_process(read_lab, [DESCRIBE, TAGS])


def test_timeout_de_transporte_nao_e_ausencia(read_lab: ReadLab) -> None:
    read_lab.process.outcomes.append(subprocess.TimeoutExpired("aws", 7))
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(ROOT, read_lab.context)
    assert_rejected(error.value)
    assert_process(read_lab, [DESCRIBE])


def test_inspect_exige_presenca_mesmo_com_not_found_conclusivo(read_lab: ReadLab) -> None:
    respond_error(read_lab, "remote_not_found")
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.inspect(ROOT, read_lab.context)
    assert_rejected(error.value)
    assert_process(read_lab, [DESCRIBE])


def test_lista_completa_vazia_nao_substitui_lookup_do_id(read_lab: ReadLab) -> None:
    respond(read_lab, {"TableNames": []})
    page = read_lab.services.inventory("dynamodb", None, read_lab.context)
    assert page.resources == ()
    assert page.cursor is None
    respond(read_lab, fixture_data("ownership")["describe"])
    respond(read_lab, fixture_data("ownership")["tags_complete"])
    owner = read_lab.services.lookup(ROOT, read_lab.context)
    assert owner is not None
    assert (owner.account, owner.region, owner.tags.get("Run")) == (
        "111111111111",
        "us-east-2",
        "a1b2c3d4",
    )
    assert_process(
        read_lab,
        [
            ("dynamodb", "list-tables", "us-east-2", {"Limit": 100}),
            DESCRIBE,
            TAGS,
        ],
    )


def test_lookup_rele_presenca_depois_de_ausencia_sem_cache(read_lab: ReadLab) -> None:
    respond_error(read_lab, "remote_not_found")
    assert read_lab.services.lookup(ROOT, read_lab.context) is None
    respond(read_lab, fixture_data("ownership")["describe"])
    respond(read_lab, fixture_data("ownership")["tags_complete"])
    owner = read_lab.services.lookup(ROOT, read_lab.context)
    assert owner is not None
    assert (owner.account, owner.region) == ("111111111111", "us-east-2")
    assert_process(read_lab, [DESCRIBE, DESCRIBE, TAGS])


def test_lookup_rele_ausencia_depois_de_presenca_sem_cache(read_lab: ReadLab) -> None:
    respond(read_lab, fixture_data("ownership")["describe"])
    respond(read_lab, fixture_data("ownership")["tags_complete"])
    assert read_lab.services.lookup(ROOT, read_lab.context) is not None
    respond_error(read_lab, "remote_not_found")
    assert read_lab.services.lookup(ROOT, read_lab.context) is None
    assert_process(read_lab, [DESCRIBE, TAGS, DESCRIBE])


def test_erro_na_segunda_pagina_de_tags_nao_confirma_ownership_parcial(read_lab: ReadLab) -> None:
    respond(read_lab, fixture_data("ownership")["describe"])
    respond(read_lab, fixture_data("ownership")["tags_first"])
    respond_error(read_lab, "remote_denied")
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(ROOT, read_lab.context)
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
