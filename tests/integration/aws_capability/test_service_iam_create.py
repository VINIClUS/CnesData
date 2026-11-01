"""Criação do role raiz com boundary, trust exato do operador e readback fechado."""

import json
import logging
from copy import deepcopy
from dataclasses import replace
from pathlib import Path

import pytest

from tests.integration.aws_capability._b1_testing import (
    ACCOUNT,
    BOUNDARY_ARN,
    NAME,
    OPERATOR_ARN,
    ROLE_ARN,
    TAG_SET,
    TAGS,
    TRUST,
    b1_lab,
    creation_for,
    fail,
    ok,
)
from tests.integration.aws_capability._service_read_assertions import (
    assert_private,
    assert_process,
    assert_rejected,
)
from tests.integration.aws_capability._service_read_testing import ReadLab, make_read_lab
from tests.integration.aws_capability.contracts import RequestContext, Resource, ServiceFailure

ROOT = Resource("iam", NAME, NAME)
OPERATOR_NAME = "cnesdata-cap-operator-0a1b2c3d"
IDENTITY = {
    "Account": ACCOUNT,
    "Arn": f"arn:aws:sts::{ACCOUNT}:assumed-role/{OPERATOR_NAME}/session",
    "UserId": "synthetic",
}
CALLER = ("sts", "get-caller-identity", "us-east-1", {})
CREATE = (
    "iam",
    "create-role",
    "us-east-1",
    {
        "RoleName": NAME,
        "Path": "/cnesdata-cap/",
        "AssumeRolePolicyDocument": json.dumps(TRUST, sort_keys=True, separators=(",", ":")),
        "PermissionsBoundary": BOUNDARY_ARN,
        "MaxSessionDuration": 3600,
        "Tags": TAG_SET,
    },
)
GET = ("iam", "get-role", "us-east-1", {"RoleName": NAME})
TAGS_CALL = ("iam", "list-role-tags", "us-east-1", {"RoleName": NAME, "MaxItems": 100})
ROLE = {"RoleName": NAME, "Path": "/cnesdata-cap/", "Arn": ROLE_ARN}
FULL_ROLE = {
    **ROLE,
    "AssumeRolePolicyDocument": TRUST,
    "PermissionsBoundary": {
        "PermissionsBoundaryType": "Policy",
        "PermissionsBoundaryArn": BOUNDARY_ARN,
    },
    "MaxSessionDuration": 3600,
}


@pytest.fixture
def lab(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> ReadLab:
    caplog.set_level(logging.DEBUG)
    return b1_lab(make_read_lab(monkeypatch, tmp_path), "s3-retention", [])


def script(lab: ReadLab, role: dict | None = None, tags: list | None = None) -> None:
    ok(lab, IDENTITY)
    ok(lab, {"Role": ROLE})
    ok(lab, {"Role": FULL_ROLE if role is None else role})
    ok(lab, {"Tags": TAG_SET if tags is None else tags, "IsTruncated": False})


def test_cria_role_com_payload_exato_e_readback(lab: ReadLab) -> None:
    script(lab)
    resource = lab.services.create("iam", creation_for(lab), lab.context)
    assert resource == ROOT
    assert_process(lab, [CALLER, CREATE, GET, TAGS_CALL])
    assert lab.context.budget.work_used == 4


def test_trust_e_somente_o_operador_com_assume_role(lab: ReadLab) -> None:
    script(lab)
    lab.services.create("iam", creation_for(lab), lab.context)
    payload = json.loads(lab.process.calls[1][1]["input"])
    trust = json.loads(payload["AssumeRolePolicyDocument"])
    assert trust["Statement"] == [
        {"Effect": "Allow", "Principal": {"AWS": OPERATOR_ARN}, "Action": "sts:AssumeRole"}
    ]
    assert len(trust["Statement"]) == 1


@pytest.mark.parametrize("probe", ["s3-hold", "all", "iam-trust", "iam-s3", "iam-leadingkeys"])
def test_outros_perfis_preservam_stub_de_iam_sem_io(lab: ReadLab, probe: str) -> None:
    lab = b1_lab(lab, probe)
    with pytest.raises(ServiceFailure) as error:
        lab.services.create("iam", creation_for(lab), lab.context)
    assert error.value.args == ("service=not_implemented",)
    assert_process(lab, [])


def test_exige_entradas_do_operador(lab: ReadLab) -> None:
    ports = replace(lab.services.ports, operator=None)
    lab = replace(lab, services=replace(lab.services, ports=ports))
    with pytest.raises(ServiceFailure) as error:
        lab.services.create("iam", creation_for(lab), lab.context)
    assert error.value.args == ("service=operator_inputs_required",)
    assert_process(lab, [])


@pytest.mark.parametrize("case", ["spec", "name", "tags", "lane"])
def test_recusa_criacao_divergente_sem_io(lab: ReadLab, case: str) -> None:
    creation = creation_for(lab)
    context = lab.context
    if case == "spec":
        creation = replace(creation, spec=replace(creation.spec, run_id="ffffffff"))
    elif case == "name":
        creation = replace(creation, name="cnesdata-cap-ffffffff")
    elif case == "tags":
        creation = replace(creation, tags={**TAGS, "Run": "ffffffff"})
    else:
        context = RequestContext(lab.context.budget, "cleanup")
    with pytest.raises(ServiceFailure) as error:
        lab.services.create("iam", creation, context)
    assert_rejected(error.value)
    assert_process(lab, [])


@pytest.mark.parametrize(
    "arn",
    [
        f"arn:aws:sts::{ACCOUNT}:assumed-role/outro-role/session",
        f"arn:aws:sts::{ACCOUNT}:assumed-role/{OPERATOR_NAME}-x/session",
        f"arn:aws:iam::{ACCOUNT}:user/{OPERATOR_NAME}",
        f"arn:aws:iam::{ACCOUNT}:root",
    ],
)
def test_recusa_chamador_que_nao_e_o_role_do_operador(lab: ReadLab, arn: str) -> None:
    ok(lab, {**IDENTITY, "Arn": arn})
    with pytest.raises(ServiceFailure) as error:
        lab.services.create("iam", creation_for(lab), lab.context)
    assert error.value.args == ("service=operator_mismatch",)
    assert len(lab.process.calls) == 1


def test_repete_somente_malformed_policy_document_de_forma_limitada(lab: ReadLab) -> None:
    pauses: list[float] = []
    lab = b1_lab(lab, "s3-retention", pauses)
    ok(lab, IDENTITY)
    fail(lab, "MalformedPolicyDocument")
    fail(lab, "MalformedPolicyDocument")
    ok(lab, {"Role": ROLE})
    ok(lab, {"Role": FULL_ROLE})
    ok(lab, {"Tags": TAG_SET, "IsTruncated": False})
    resource = lab.services.create("iam", creation_for(lab), lab.context)
    assert resource == ROOT
    assert pauses == [2.0, 2.0]
    assert [call[0][2] for call in lab.process.calls].count("create-role") == 3


def test_malformed_policy_document_persistente_esgota_tentativas(lab: ReadLab) -> None:
    pauses: list[float] = []
    lab = b1_lab(lab, "s3-retention", pauses)
    ok(lab, IDENTITY)
    for _ in range(10):
        fail(lab, "MalformedPolicyDocument")
    with pytest.raises(ServiceFailure) as error:
        lab.services.create("iam", creation_for(lab), lab.context)
    assert error.value.args == ("service=create_failed",)
    assert [call[0][2] for call in lab.process.calls].count("create-role") == 4
    assert pauses == [2.0, 2.0, 2.0]


@pytest.mark.parametrize(
    "outcome",
    [
        (254, "", json.dumps({"Code": "AccessDenied"})),
        (254, "", json.dumps({"Code": "EntityAlreadyExists"})),
        (1, "", json.dumps({"Code": "MalformedPolicyDocument"})),
    ],
    ids=["acesso_negado", "ja_existe", "origem_local"],
)
def test_nao_repete_outros_erros_nem_falhas_locais(lab: ReadLab, outcome: tuple) -> None:
    pauses: list[float] = []
    lab = b1_lab(lab, "s3-retention", pauses)
    ok(lab, IDENTITY)
    lab.process.outcomes.append(outcome)
    with pytest.raises(ServiceFailure) as error:
        lab.services.create("iam", creation_for(lab), lab.context)
    assert_rejected(error.value)
    assert [call[0][2] for call in lab.process.calls].count("create-role") == 1
    assert pauses == []


def mutated(path: list[str | int], value: object) -> dict:
    role = deepcopy(FULL_ROLE)
    target = role
    for key in path[:-1]:
        target = target[key]
    if value is None:
        del target[path[-1]]
    else:
        target[path[-1]] = value
    return role


@pytest.mark.parametrize(
    "role",
    [
        mutated(["Path"], "/outro/"),
        mutated(["MaxSessionDuration"], 43200),
        mutated(["MaxSessionDuration"], None),
        mutated(["PermissionsBoundary"], None),
        mutated(["PermissionsBoundary", "PermissionsBoundaryArn"], BOUNDARY_ARN + "x"),
        mutated(["PermissionsBoundary", "PermissionsBoundaryType"], "PermissionsBoundaryType"),
        mutated(["AssumeRolePolicyDocument"], None),
        mutated(["AssumeRolePolicyDocument", "Statement", 0, "Principal", "AWS"], "*"),
        mutated(["AssumeRolePolicyDocument", "Statement", 0, "Action"], "sts:*"),
        mutated(["AssumeRolePolicyDocument", "Statement", 0, "Effect"], "Deny"),
        mutated(["AssumeRolePolicyDocument", "Statement", 0, "Condition"], {"Bool": {}}),
        mutated(["AssumeRolePolicyDocument", "Version"], "2008-10-17"),
        mutated(["Arn"], ROLE_ARN + "x"),
    ],
)
def test_readback_divergente_falha(lab: ReadLab, role: dict) -> None:
    script(lab, role=role)
    with pytest.raises(ServiceFailure) as error:
        lab.services.create("iam", creation_for(lab), lab.context)
    assert error.value.args == ("service=create_invalid",)


def test_readback_com_statement_adicional_falha(lab: ReadLab) -> None:
    role = deepcopy(FULL_ROLE)
    role["AssumeRolePolicyDocument"]["Statement"].append(dict(TRUST["Statement"][0]))
    script(lab, role=role)
    with pytest.raises(ServiceFailure) as error:
        lab.services.create("iam", creation_for(lab), lab.context)
    assert error.value.args == ("service=create_invalid",)


@pytest.mark.parametrize(
    "tags",
    [TAG_SET[:3], [*TAG_SET, {"Key": "Extra", "Value": "x"}]],
    ids=["tag_ausente", "tag_extra"],
)
def test_tags_divergentes_no_readback_falham(lab: ReadLab, tags: list) -> None:
    script(lab, tags=tags)
    with pytest.raises(ServiceFailure) as error:
        lab.services.create("iam", creation_for(lab), lab.context)
    assert error.value.args == ("service=create_invalid",)


def test_resposta_do_create_role_com_identidade_diferente_falha(lab: ReadLab) -> None:
    ok(lab, IDENTITY)
    ok(lab, {"Role": {**ROLE, "RoleName": "cnesdata-cap-ffffffff"}})
    with pytest.raises(ServiceFailure) as error:
        lab.services.create("iam", creation_for(lab), lab.context)
    assert error.value.args == ("service=create_invalid",)
    assert len(lab.process.calls) == 2


def test_nao_vaza_canary_nem_cria_arquivos(lab: ReadLab, caplog: pytest.LogCaptureFixture) -> None:
    script(lab)
    lab.services.create("iam", creation_for(lab), lab.context)
    assert list(lab.directory.iterdir()) == []
    assert_private(ServiceFailure("service=read_ok"), lab, caplog.text)
