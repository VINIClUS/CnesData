"""Exclusão de inline policies e role com espera limitada pela ausência."""

import logging
from pathlib import Path
from typing import cast

import pytest

from tests.integration.aws_capability._b1_testing import (
    NAME,
    ROLE_ARN,
    TAG_SET,
    b1_lab,
    cleanup_context,
    fail,
    ok,
)
from tests.integration.aws_capability._resource_ids import iam_child
from tests.integration.aws_capability._service_read_assertions import assert_private, assert_process
from tests.integration.aws_capability._service_read_testing import ReadLab, make_read_lab
from tests.integration.aws_capability.contracts import Resource, ServiceFailure

ROOT = Resource("iam", NAME, NAME)
ROLE = {"RoleName": NAME, "Path": "/cnesdata-cap/", "Arn": ROLE_ARN}
GET_ROLE = ("iam", "get-role", "us-east-1", {"RoleName": NAME})
TAGS = ("iam", "list-role-tags", "us-east-1", {"RoleName": NAME, "MaxItems": 100})
OWNERSHIP = [GET_ROLE, TAGS]
POLICY = {"RoleName": NAME, "PolicyName": "cap-p0"}
DELETE_POLICY = ("iam", "delete-role-policy", "us-east-1", POLICY)
GET_POLICY = ("iam", "get-role-policy", "us-east-1", POLICY)
DELETE_ROLE = ("iam", "delete-role", "us-east-1", {"RoleName": NAME})


@pytest.fixture
def lab(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> ReadLab:
    caplog.set_level(logging.DEBUG)
    return b1_lab(make_read_lab(monkeypatch, tmp_path), "s3-retention", [])


def child(lab: ReadLab) -> Resource:
    return iam_child(ROOT, lab.services.spec, "cap-p0")


def owner_replies(lab: ReadLab, tags: list | None = None) -> None:
    ok(lab, {"Role": ROLE})
    ok(lab, {"Tags": TAG_SET if tags is None else tags, "IsTruncated": False})


def test_remove_inline_policy_e_confirma_ausencia(lab: ReadLab) -> None:
    owner_replies(lab)
    ok(lab)
    fail(lab, "NoSuchEntity")
    lab.services.delete(child(lab), True, cleanup_context(lab))
    assert_process(lab, [*OWNERSHIP, DELETE_POLICY, GET_POLICY])


def test_aguarda_ausencia_da_policy_de_forma_limitada(lab: ReadLab) -> None:
    pauses: list[float] = []
    lab = b1_lab(lab, "s3-retention", pauses)
    owner_replies(lab)
    ok(lab)
    ok(lab, {**POLICY, "PolicyDocument": {"Version": "2012-10-17"}})
    fail(lab, "NoSuchEntity")
    lab.services.delete(child(lab), True, cleanup_context(lab))
    assert pauses == [1.0]


def test_policy_que_continua_visivel_esgota_a_espera(lab: ReadLab) -> None:
    pauses: list[float] = []
    lab = b1_lab(lab, "s3-retention", pauses)
    owner_replies(lab)
    ok(lab)
    for _ in range(10):
        ok(lab, {**POLICY, "PolicyDocument": {"Version": "2012-10-17"}})
    with pytest.raises(ServiceFailure) as error:
        lab.services.delete(child(lab), True, cleanup_context(lab))
    assert error.value.args == ("service=delete_pending",)
    assert len(pauses) == 4


def test_remove_role_e_confirma_ausencia(lab: ReadLab) -> None:
    owner_replies(lab)
    ok(lab)
    fail(lab, "NoSuchEntity")
    lab.services.delete(ROOT, True, cleanup_context(lab))
    assert_process(lab, [*OWNERSHIP, DELETE_ROLE, GET_ROLE])


def test_delete_conflict_do_role_repete_de_forma_limitada(lab: ReadLab) -> None:
    pauses: list[float] = []
    lab = b1_lab(lab, "s3-retention", pauses)
    owner_replies(lab)
    fail(lab, "DeleteConflict")
    ok(lab)
    fail(lab, "NoSuchEntity")
    lab.services.delete(ROOT, True, cleanup_context(lab))
    assert pauses == [1.0]
    assert [call[0][2] for call in lab.process.calls].count("delete-role") == 2


def test_delete_conflict_persistente_falha(lab: ReadLab) -> None:
    pauses: list[float] = []
    lab = b1_lab(lab, "s3-retention", pauses)
    owner_replies(lab)
    for _ in range(10):
        fail(lab, "DeleteConflict")
    with pytest.raises(ServiceFailure) as error:
        lab.services.delete(ROOT, True, cleanup_context(lab))
    assert error.value.args == ("service=delete_failed",)
    assert [call[0][2] for call in lab.process.calls].count("delete-role") == 5


def test_outro_erro_no_delete_do_role_nao_repete(lab: ReadLab) -> None:
    owner_replies(lab)
    fail(lab, "AccessDenied")
    with pytest.raises(ServiceFailure):
        lab.services.delete(ROOT, True, cleanup_context(lab))
    assert [call[0][2] for call in lab.process.calls].count("delete-role") == 1


@pytest.mark.parametrize("target", ["root", "child"])
def test_nao_remove_role_de_outra_posse(lab: ReadLab, target: str) -> None:
    owner_replies(lab, tags=TAG_SET[:3])
    resource = ROOT if target == "root" else child(lab)
    with pytest.raises(ServiceFailure) as error:
        lab.services.delete(resource, True, cleanup_context(lab))
    assert error.value.args == ("service=not_owned",)
    assert_process(lab, OWNERSHIP)


@pytest.mark.parametrize("case", ["work", "bypass_int", "outro_nome", "policy_invalida"])
def test_recusa_entrada_invalida_sem_io(lab: ReadLab, case: str) -> None:
    resource, bypass, context = child(lab), cast("bool", True), cleanup_context(lab)
    if case == "work":
        context = lab.context
    elif case == "bypass_int":
        bypass = cast("bool", 1)
    elif case == "outro_nome":
        resource = Resource("iam", "outro-role", "outro-role")
    else:
        resource = Resource("iam", '{"v":1}', NAME)
    with pytest.raises(ServiceFailure) as error:
        lab.services.delete(resource, bypass, context)
    assert error.value.args != ("service=not_implemented",)
    assert_process(lab, [])


def test_nao_vaza_canary(lab: ReadLab, caplog: pytest.LogCaptureFixture) -> None:
    owner_replies(lab)
    ok(lab)
    fail(lab, "NoSuchEntity")
    lab.services.delete(ROOT, True, cleanup_context(lab))
    assert_private(ServiceFailure("service=read_ok"), lab, caplog.text)
