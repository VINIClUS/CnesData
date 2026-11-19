"""Ownership IAM exige Role autoritativo e todas as páginas frescas de tags."""

import json
from collections.abc import Iterator
from pathlib import Path

import pytest

from tests.integration.aws_capability._service_read_assertions import assert_process
from tests.integration.aws_capability._service_roots_testing import (
    ReadLab,
    load_fixture,
    make_read_lab,
)
from tests.integration.aws_capability.contracts import Ownership, Resource, ServiceFailure

FIXTURES = Path(__file__).parent / "fixtures" / "cli" / "r4"
ROLE = Resource("iam", "CapRole", "CapRole")
GET = ("iam", "get-role", "us-east-1", {"RoleName": "CapRole"})
TAGS = ("iam", "list-role-tags", "us-east-1", {"RoleName": "CapRole", "MaxItems": 100})
NEXT = (
    "iam",
    "list-role-tags",
    "us-east-1",
    {"RoleName": "CapRole", "MaxItems": 100, "Marker": "tags-next"},
)


def data(name: str) -> dict:
    return load_fixture(FIXTURES / f"{name}.json")


def reply(lab: ReadLab, value: object) -> None:
    lab.process.outcomes.append((0, json.dumps(value), ""))


@pytest.fixture
def read_lab(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> Iterator[ReadLab]:
    lab = make_read_lab(monkeypatch, tmp_path, caplog, capsys)
    yield lab
    lab.check_private()


def test_inspect_iam_acumula_tags_paginadas_e_retorna_regiao_global(read_lab: ReadLab) -> None:
    fixture = data("iam")
    for key in ("role", "tags_first", "tags_second"):
        reply(read_lab, fixture[key])
    owner = read_lab.services.inspect(ROLE, read_lab.context)
    assert owner == Ownership(
        "111111111111",
        {
            "Project": "CnesData",
            "Purpose": "aws-capability",
            "Run": "a1b2c3d4",
            "ExpiresAt": "1060",
        },
        "aws-global",
    )
    assert_process(read_lab, [GET, TAGS, NEXT])


def test_role_iam_sem_nome_sintetico_continua_legivel(read_lab: ReadLab) -> None:
    fixture = data("iam")
    reply(read_lab, fixture["role"])
    reply(read_lab, fixture["tags_empty"])
    assert read_lab.services.lookup(ROLE, read_lab.context) == Ownership(
        "111111111111", {}, "aws-global"
    )
    assert_process(read_lab, [GET, TAGS])


def test_lookup_iam_rele_tags_removidas_sem_cache(read_lab: ReadLab) -> None:
    fixture = data("iam")
    for key in ("role", "tags_first", "tags_second", "role", "tags_removed"):
        reply(read_lab, fixture[key])
    first = read_lab.services.lookup(ROLE, read_lab.context)
    second = read_lab.services.lookup(ROLE, read_lab.context)
    assert first is not None
    assert first.tags["Run"] == "a1b2c3d4"
    assert second == Ownership("111111111111", {"Project": "CnesData"}, "aws-global")
    assert_process(read_lab, [GET, TAGS, NEXT, GET, TAGS])


@pytest.mark.parametrize(
    "case",
    [
        "role_wrong_name",
        "role_wrong_path",
        "role_wrong_account",
        "role_wrong_arn_path",
        "role_missing",
        "role_null",
    ],
)
def test_readback_iam_divergente_falha_antes_das_tags(read_lab: ReadLab, case: str) -> None:
    reply(read_lab, data("iam")[case])
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(ROLE, read_lab.context)
    assert error.value.args == ("service=describe_invalid",)
    read_lab.check_private(error.value)
    assert_process(read_lab, [GET])


@pytest.mark.parametrize(
    "case",
    [
        "tags_missing",
        "tags_map",
        "tags_bad_entry",
        "tags_missing_value",
        "tags_number_key",
        "tags_truncated_missing_marker",
        "tags_terminal_marker",
    ],
)
def test_shape_de_tags_iam_invalido_falha_fechado(read_lab: ReadLab, case: str) -> None:
    reply(read_lab, data("iam")["role"])
    reply(read_lab, data("iam")[case])
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(ROLE, read_lab.context)
    assert error.value.args == ("service=tags_invalid",)
    read_lab.check_private(error.value)
    assert_process(read_lab, [GET, TAGS])


def test_chave_de_tag_duplicada_entre_paginas_e_ambigua(read_lab: ReadLab) -> None:
    fixture = data("iam")
    for key in ("role", "tags_first", "tags_duplicate"):
        reply(read_lab, fixture[key])
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(ROLE, read_lab.context)
    assert error.value.args == ("service=tags_ambiguous",)
    assert_process(read_lab, [GET, TAGS, NEXT])


@pytest.mark.parametrize(
    "resource",
    [
        Resource("iam", "", ""),
        Resource("iam", "CapRole", ""),
        Resource("iam", "", "CapRole"),
        Resource("iam", "CapRole", "OtherRole"),
    ],
)
def test_recurso_iam_invalido_falha_sem_processo(read_lab: ReadLab, resource: Resource) -> None:
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(resource, read_lab.context)
    assert error.value.args == ("service=resource_invalid",)
    assert_process(read_lab, [])
