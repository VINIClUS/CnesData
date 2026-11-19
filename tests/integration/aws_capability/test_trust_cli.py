"""Fronteira CLI das operações do iam-trust: saída vazia, payloads e mensagem de erro."""

import json
import logging
from pathlib import Path

import pytest

from tests.integration.aws_capability._b1_testing import ROLE_ARN, fail, ok
from tests.integration.aws_capability._probe_attempt import attempt
from tests.integration.aws_capability._service_read_assertions import assert_process
from tests.integration.aws_capability._service_read_testing import ReadLab, make_read_lab
from tests.integration.aws_capability._trust_probe import duration_category
from tests.integration.aws_capability.cli import Request

CHAINING = (
    "The requested DurationSeconds exceeds the 1 hour session limit "
    "for roles assumed by role chaining."
)
UPDATE = {"RoleName": "cnesdata-cap-a1b2c3d4", "PolicyDocument": '{"Version":"2012-10-17"}'}
ASSUME = {
    "RoleArn": ROLE_ARN,
    "RoleSessionName": "cap-a1b2c3d4",
    "DurationSeconds": 900,
    "Tags": [{"Key": "cap-tag", "Value": "cap-tag-value"}],
    "SourceIdentity": "cap-source",
}


@pytest.fixture
def lab(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> ReadLab:
    caplog.set_level(logging.DEBUG)
    return make_read_lab(monkeypatch, tmp_path)


def test_update_assume_role_policy_com_saida_vazia_e_sucesso_remoto(lab: ReadLab) -> None:
    ok(lab)
    request = Request("iam", "update-assume-role-policy", UPDATE)
    result = attempt(lab.services.ports.commands, request, lab.context)
    assert (result.ok, result.code) == (True, "ok")
    assert_process(lab, [("iam", "update-assume-role-policy", "us-east-1", UPDATE)])


def test_assume_role_leva_tags_e_source_identity_sem_alterar_o_formato(lab: ReadLab) -> None:
    ok(lab, {"Credentials": {}})
    attempt(lab.services.ports.commands, Request("sts", "assume-role", ASSUME), lab.context)
    assert_process(lab, [("sts", "assume-role", "us-east-1", ASSUME)])
    assert json.loads(lab.process.calls[0][1]["input"])["Tags"] == ASSUME["Tags"]


def test_mensagem_de_erro_chega_ao_attempt_e_so_a_categoria_e_derivada(lab: ReadLab) -> None:
    lab.process.outcomes.append(
        (254, "", json.dumps({"Code": "ValidationError", "Message": CHAINING}))
    )
    result = attempt(
        lab.services.ports.commands,
        Request("sts", "assume-role", {**ASSUME, "DurationSeconds": 3601}),
        lab.context,
    )
    assert (result.ok, result.code, result.message) == (False, "ValidationError", CHAINING)
    assert duration_category(result.message) == "role_chaining"
    assert CHAINING not in repr(result)


def test_erro_sem_mensagem_resulta_em_texto_vazio(lab: ReadLab) -> None:
    fail(lab, "AccessDenied")
    lab.process.outcomes[0] = (254, "", json.dumps({"Code": "AccessDenied"}))
    result = attempt(
        lab.services.ports.commands, Request("sts", "assume-role", ASSUME), lab.context
    )
    assert (result.ok, result.code, result.message) == (False, "AccessDenied", "")
