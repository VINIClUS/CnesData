"""S3 da fronteira com estado: recurso por prefixo, ListBucket, 404 versus 403 e bucket policy."""

import json
from pathlib import Path

import pytest

from tests.integration.aws_capability import _matrix_policies as policies
from tests.integration.aws_capability._aws_fake_testing import FakeSession
from tests.integration.aws_capability._aws_fake_types_testing import FakeVersion, Reply
from tests.integration.aws_capability._b1_testing import ACCOUNT, NAME
from tests.integration.aws_capability._probe_testing import ProbeLab, make_probe_lab
from tests.integration.aws_capability.cli import Request

BUCKET = "arn:aws:s3:::" + NAME
AUDIT_ARN = f"arn:aws:iam::{ACCOUNT}:role/cnesdata-cap/{NAME}-audit"
API_ARN = f"arn:aws:iam::{ACCOUNT}:role/cnesdata-cap/{NAME}-api"
BLOCKED = f"{BUCKET}/serving/blocked/*"


def deny(principal: object = None, resource: str = BLOCKED, action: str = "s3:GetObject") -> dict:
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Deny",
                "Action": action,
                "Principal": {"AWS": API_ARN} if principal is None else principal,
                "Resource": resource,
            }
        ],
    }


@pytest.fixture
def lab(tmp_path: Path) -> ProbeLab:
    made = make_probe_lab(tmp_path, "iam-s3", list_gates_missing=True)
    for role, statements in (
        (f"{NAME}-audit", policies.audit_s3(BUCKET)),
        (f"{NAME}-api", policies.api_s3(BUCKET)),
    ):
        document = json.dumps({"Version": "2012-10-17", "Statement": statements})
        payload = {"RoleName": role, "PolicyName": "p", "PolicyDocument": document}
        assert made.aws.call(Request("iam", "put-role-policy", payload), made.context).ok
    bucket = made.aws.buckets[NAME]
    for key in ("audit/a", "raw/x", "serving/x", "serving/blocked/x", "reconciliation/x"):
        bucket.versions.append(FakeVersion(key, f"V_{key}"))
    return made


def call(lab: ProbeLab, who: str, operation: str, payload: dict) -> Reply:
    request = Request("s3api", operation, {"Bucket": NAME, **payload})
    if who == "operator":
        return lab.aws.call(request, lab.context)
    return FakeSession(lab.aws, who).call(request, lab.context)


def code(reply: Reply) -> str:
    return "ok" if reply.ok else reply.error.code  # type: ignore[union-attr]


def head(lab: ProbeLab, who: str, key: str) -> str:
    return code(call(lab, who, "head-object", {"Key": key}))


def test_head_object_respeita_o_prefixo_do_recurso(lab: ProbeLab) -> None:
    assert head(lab, "role-audit", "audit/a") == "ok"
    assert head(lab, "role-audit", "raw/x") == "403"
    assert head(lab, "role-api", "serving/x") == "ok"
    assert head(lab, "role-api", "reconciliation/x") == "ok"
    assert head(lab, "role-api", "raw/x") == "403"
    assert head(lab, "role-api", "audit/a") == "403"


def test_ausente_e_404_so_com_list_bucket_no_prefixo_e_403_sem_ele(lab: ProbeLab) -> None:
    assert head(lab, "role-api", "serving/missing") == "404"
    assert head(lab, "role-api", "raw/missing") == "403"
    assert head(lab, "role-audit", "audit/missing") == "403"
    assert head(lab, "operator", "audit/missing") == "404"


def test_sem_a_chave_do_fake_o_ausente_continua_404(tmp_path: Path) -> None:
    lab = make_probe_lab(tmp_path, "iam-s3", list_gates_missing=False)
    grant = {
        "Version": "2012-10-17",
        "Statement": policies.audit_s3(BUCKET),
    }
    payload = {"RoleName": f"{NAME}-audit", "PolicyName": "p", "PolicyDocument": json.dumps(grant)}
    assert lab.aws.call(Request("iam", "put-role-policy", payload), lab.context).ok
    assert head(lab, "role-audit", "audit/missing") == "404"


def test_list_objects_exige_prefixo_permitido(lab: ProbeLab) -> None:
    ok = call(lab, "role-api", "list-objects-v2", {"Prefix": "serving/", "MaxKeys": 1})
    assert dict(ok.data)["KeyCount"] == 1
    assert code(call(lab, "role-api", "list-objects-v2", {"Prefix": "raw/"})) == "AccessDenied"
    assert code(call(lab, "role-api", "list-objects-v2", {})) == "AccessDenied"
    assert code(call(lab, "role-audit", "list-objects-v2", {"Prefix": "audit/"})) == "AccessDenied"
    assert call(lab, "operator", "list-objects-v2", {"Prefix": "raw/"}).ok


def test_put_object_so_no_prefixo_do_audit(lab: ProbeLab) -> None:
    assert code(call(lab, "role-audit", "put-object", {"Key": "audit/b"})) == "ok"
    assert code(call(lab, "role-audit", "put-object", {"Key": "raw/y"})) == "AccessDenied"
    assert code(call(lab, "role-api", "put-object", {"Key": "serving/y"})) == "AccessDenied"


def lock(key: str) -> dict:
    until = "2030-01-01T00:00:00Z"
    return {"Key": key, "ObjectLockMode": "GOVERNANCE", "ObjectLockRetainUntilDate": until}


def test_put_object_com_lock_exige_put_object_retention_no_mesmo_prefixo(lab: ProbeLab) -> None:
    assert code(call(lab, "role-audit", "put-object", lock("audit/b"))) == "ok"
    assert code(call(lab, "role-audit", "put-object", lock("raw/b"))) == "AccessDenied"


def test_retencao_do_audit_so_em_audit_e_leitura_so_em_audit(lab: ProbeLab) -> None:
    put = call(lab, "role-audit", "put-object", {"Key": "audit/b"})
    version = dict(put.data)["VersionId"]
    read = call(lab, "role-audit", "get-object-retention", {"Key": "audit/b", "VersionId": version})
    assert code(read) == "NoSuchObjectLockConfiguration"
    raw = call(lab, "role-audit", "get-object-retention", {"Key": "raw/x", "VersionId": "V_raw/x"})
    assert code(raw) == "AccessDenied"
    api = call(lab, "role-api", "get-object-retention", {"Key": "serving/x", "VersionId": "x"})
    assert code(api) == "AccessDenied"


def test_delete_object_version_nao_concedido_ao_audit_mesmo_em_audit(lab: ProbeLab) -> None:
    delete = call(lab, "role-audit", "delete-object", {"Key": "audit/a", "VersionId": "V_audit/a"})
    assert code(delete) == "AccessDenied"
    assert any(v.key == "audit/a" for v in lab.aws.buckets[NAME].versions)


def test_lock_configuration_so_para_quem_tem_a_acao_no_bucket(lab: ProbeLab) -> None:
    assert call(lab, "role-audit", "get-object-lock-configuration", {}).ok
    assert code(call(lab, "role-api", "get-object-lock-configuration", {})) == "AccessDenied"


def policy_request(document: dict) -> dict:
    return {"Policy": json.dumps(document), "ExpectedBucketOwner": ACCOUNT}


def test_bucket_policy_deny_vale_para_o_role_nomeado_e_nao_para_o_resto(lab: ProbeLab) -> None:
    assert head(lab, "role-api", "serving/blocked/x") == "ok"
    assert call(lab, "operator", "put-bucket-policy", policy_request(deny())).ok
    assert head(lab, "role-api", "serving/blocked/x") == "403"
    assert head(lab, "role-api", "serving/x") == "ok"
    assert head(lab, "operator", "serving/blocked/x") == "ok"


def test_bucket_policy_com_principal_curinga_tambem_nega_o_operador(lab: ProbeLab) -> None:
    assert call(lab, "operator", "put-bucket-policy", policy_request(deny("*"))).ok
    assert head(lab, "operator", "serving/blocked/x") == "403"
    assert head(lab, "role-api", "serving/blocked/x") == "403"


def test_bucket_policy_so_vale_depois_da_propagacao(tmp_path: Path) -> None:
    lab = make_probe_lab(tmp_path, "iam-s3", bucket_policy_lag=3)
    lab.aws.buckets[NAME].versions.append(FakeVersion("serving/blocked/x", "V"))
    assert call(lab, "operator", "put-bucket-policy", policy_request(deny("*"))).ok
    assert head(lab, "operator", "serving/blocked/x") == "ok"
    for _ in range(3):
        head(lab, "operator", "serving/blocked/x")
    assert head(lab, "operator", "serving/blocked/x") == "403"


def test_principal_invalido_e_rejeitado_como_malformed_policy(lab: ProbeLab) -> None:
    ghost = {"AWS": f"arn:aws:iam::{ACCOUNT}:role/cnesdata-cap/ghost"}
    reply = call(lab, "operator", "put-bucket-policy", policy_request(deny(ghost)))
    assert code(reply) == "MalformedPolicy"
    assert lab.aws.buckets[NAME].policy is None


def test_principal_recem_criado_e_rejeitado_n_vezes(tmp_path: Path) -> None:
    lab = make_probe_lab(tmp_path, "iam-s3", bucket_policy_malformed=2)
    codes = [
        code(call(lab, "operator", "put-bucket-policy", policy_request(deny()))) for _ in range(3)
    ]
    assert codes == ["MalformedPolicy", "MalformedPolicy", "ok"]


def test_delete_bucket_policy_remove_ou_informa_ausencia(lab: ProbeLab) -> None:
    assert code(call(lab, "operator", "delete-bucket-policy", {})) == "NoSuchBucketPolicy"
    call(lab, "operator", "put-bucket-policy", policy_request(deny()))
    assert call(lab, "operator", "delete-bucket-policy", {}).ok
    assert lab.aws.buckets[NAME].policy is None
    assert head(lab, "role-api", "serving/blocked/x") == "ok"


def test_deny_amplo_bloqueia_delete_bucket_do_operador_ate_a_policy_sair(tmp_path: Path) -> None:
    lab = make_probe_lab(tmp_path, "iam-s3")
    document = deny("*", f"{BUCKET}", "s3:DeleteBucket")
    assert call(lab, "operator", "put-bucket-policy", policy_request(document)).ok
    assert code(call(lab, "operator", "delete-bucket", {})) == "AccessDenied"
    assert call(lab, "operator", "delete-bucket-policy", {}).ok
    assert call(lab, "operator", "delete-bucket", {}).ok
