"""Avaliador de policies da fronteira com estado: ação, recurso e condição no mesmo statement."""

import json
from pathlib import Path

import pytest

from tests.integration.aws_capability import _matrix_policies as policies
from tests.integration.aws_capability._aws_fake_types_testing import FakeRole
from tests.integration.aws_capability._b1_testing import NAME, TAGS, TRUST
from tests.integration.aws_capability._probe_testing import ProbeLab, make_probe_lab
from tests.integration.aws_capability.cli import Request

TABLE = "arn:aws:dynamodb:us-east-2:111111111111:table/" + NAME
BUCKET = "arn:aws:s3:::" + NAME
LK = "dynamodb:LeadingKeys"
AUDIT = f"{NAME}-audit"
API = f"{NAME}-api"


@pytest.fixture
def lab(tmp_path: Path) -> ProbeLab:
    made = make_probe_lab(tmp_path, "iam-leadingkeys", seeded=False)
    for name in (AUDIT, API):
        made.aws.roles[name] = FakeRole(dict(TAGS), TRUST)
    return made


def grant(lab: ProbeLab, role: str, statements: list) -> None:
    document = json.dumps({"Version": "2012-10-17", "Statement": statements})
    request = Request(
        "iam", "put-role-policy",
        {"RoleName": role, "PolicyName": "p", "PolicyDocument": document},
    )
    assert lab.aws.call(request, lab.context).ok


@pytest.mark.parametrize(
    ("keys", "expected"),
    [
        (["TENANT#_GLOBAL"], True),
        (["OUTBOX#CURSOR"], True),
        (["TENANT#_GLOBAL", "OUTBOX#CURSOR"], True),
        (["OTHER#1"], False),
        (["TENANT#_GLOBAL", "OTHER#1"], False),
        ([], False),
    ],
)
def test_for_all_values_string_equals_com_null_exige_chave_presente_e_toda_no_conjunto(
    lab: ProbeLab, keys: list[str], expected: bool
) -> None:
    grant(lab, AUDIT, policies.audit_dynamodb(TABLE))
    assert lab.aws.permits("role-audit", "dynamodb:GetItem", TABLE, {LK: keys}) is expected


def test_query_audit_so_no_arn_do_gsi6_com_chave_permitida(lab: ProbeLab) -> None:
    grant(lab, AUDIT, policies.audit_dynamodb(TABLE))
    pending = {LK: ["OUTBOX#PENDING"]}
    assert lab.aws.permits("role-audit", "dynamodb:Query", f"{TABLE}/index/gsi6", pending)
    assert not lab.aws.permits("role-audit", "dynamodb:Query", f"{TABLE}/index/gsi1", pending)
    assert not lab.aws.permits("role-audit", "dynamodb:Query", TABLE, pending)
    other = {LK: ["OUTBOX#OTHER"]}
    assert not lab.aws.permits("role-audit", "dynamodb:Query", f"{TABLE}/index/gsi6", other)


def test_condicao_de_uma_acao_nao_vaza_para_o_statement_de_outra(lab: ProbeLab) -> None:
    grant(lab, AUDIT, policies.audit_dynamodb(TABLE))
    query_keys = {LK: ["OUTBOX#PENDING"]}
    assert not lab.aws.permits("role-audit", "dynamodb:GetItem", TABLE, query_keys)
    assert not lab.aws.permits("role-audit", "dynamodb:PutItem", TABLE, {LK: ["TENANT#_GLOBAL"]})


@pytest.mark.parametrize(
    ("keys", "expected"), [(["TENANT#x"], True), (["TENANT#"], True), (["OTHER#1"], False)]
)
def test_for_all_values_string_like_casa_curinga(
    lab: ProbeLab, keys: list[str], expected: bool
) -> None:
    grant(lab, API, policies.api_dynamodb(TABLE))
    assert lab.aws.permits("role-api", "dynamodb:GetItem", TABLE, {LK: keys}) is expected


def test_get_item_do_api_nao_concede_update_nem_query_na_tabela(lab: ProbeLab) -> None:
    grant(lab, API, policies.api_dynamodb(TABLE))
    keys = {LK: ["TENANT#x"]}
    assert not lab.aws.permits("role-api", "dynamodb:UpdateItem", TABLE, keys)
    assert not lab.aws.permits("role-api", "dynamodb:Query", TABLE, {LK: ["USER#1"]})
    assert lab.aws.permits("role-api", "dynamodb:Query", f"{TABLE}/index/gsi1", {LK: ["USER#1"]})
    other_index = f"{TABLE}/index/gsi6"
    assert not lab.aws.permits("role-api", "dynamodb:Query", other_index, {LK: ["USER#1"]})


def test_s3_recurso_com_curinga_distingue_prefixos(lab: ProbeLab) -> None:
    grant(lab, AUDIT, policies.audit_s3(BUCKET))
    assert lab.aws.permits("role-audit", "s3:GetObject", f"{BUCKET}/audit/a")
    assert not lab.aws.permits("role-audit", "s3:GetObject", f"{BUCKET}/raw/x")
    assert not lab.aws.permits("role-audit", "s3:GetObject", f"{BUCKET}/audit")
    assert lab.aws.permits("role-audit", "s3:GetBucketObjectLockConfiguration", BUCKET)
    assert not lab.aws.permits("role-audit", "s3:ListBucket", BUCKET, {"s3:prefix": ["audit/"]})


@pytest.mark.parametrize(
    ("prefixes", "expected"),
    [(["serving/"], True), (["reconciliation/"], True), (["raw/"], False), ([], False)],
)
def test_string_like_em_s3_prefix_exige_chave_presente_e_casando(
    lab: ProbeLab, prefixes: list[str], expected: bool
) -> None:
    grant(lab, API, policies.api_s3(BUCKET))
    keys = {"s3:prefix": prefixes}
    assert lab.aws.permits("role-api", "s3:ListBucket", BUCKET, keys) is expected


def test_api_le_serving_mas_nao_escreve(lab: ProbeLab) -> None:
    grant(lab, API, policies.api_s3(BUCKET))
    assert lab.aws.permits("role-api", "s3:GetObject", f"{BUCKET}/serving/x")
    assert not lab.aws.permits("role-api", "s3:GetObject", f"{BUCKET}/raw/x")
    assert not lab.aws.permits("role-api", "s3:PutObject", f"{BUCKET}/serving/y")


def test_policy_so_vale_depois_da_propagacao_e_operador_sempre_pode(tmp_path: Path) -> None:
    lab = make_probe_lab(tmp_path, "iam-leadingkeys", seeded=False, policy_lag=5)
    lab.aws.roles[AUDIT] = FakeRole(dict(TAGS), TRUST)
    grant(lab, AUDIT, policies.audit_s3(BUCKET))
    assert not lab.aws.permits("role-audit", "s3:GetObject", f"{BUCKET}/audit/a")
    lab.aws.counter += 5
    assert lab.aws.permits("role-audit", "s3:GetObject", f"{BUCKET}/audit/a")
    assert lab.aws.permits("operator", "s3:GetObject", f"{BUCKET}/qualquer")


def test_allowed_substituido_continua_valendo_antes_do_avaliador(lab: ProbeLab) -> None:
    grant(lab, AUDIT, policies.audit_s3(BUCKET))
    lab.aws.allowed = lambda who, action: False
    assert not lab.aws.permits("role-audit", "s3:GetObject", f"{BUCKET}/audit/a")
    assert not lab.aws.permits("operator", "s3:GetObject", f"{BUCKET}/audit/a")


def test_operador_de_condicao_desconhecido_falha_alto(lab: ProbeLab) -> None:
    statement = {
        "Effect": "Allow", "Action": "s3:GetObject", "Resource": f"{BUCKET}/*",
        "Condition": {"IpAddress": {"aws:SourceIp": "10.0.0.0/8"}},
    }
    grant(lab, AUDIT, [statement])
    with pytest.raises(AssertionError):
        lab.aws.permits("role-audit", "s3:GetObject", f"{BUCKET}/a")


def test_put_role_policy_normaliza_action_em_string(lab: ProbeLab) -> None:
    grant(lab, AUDIT, policies.audit_dynamodb(TABLE))
    actions, _, _ = lab.aws.roles[AUDIT].policies["p"]
    assert actions == {"dynamodb:Query", "dynamodb:GetItem", "dynamodb:UpdateItem"}
