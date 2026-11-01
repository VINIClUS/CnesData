"""Bucket policy A/B: 200 antes, negação depois da propagação e o controle ainda verde."""

import json
from pathlib import Path

import pytest

from tests.integration.aws_capability import lifecycle
from tests.integration.aws_capability._aws_fake_types_testing import FakeBucket, FakeRole
from tests.integration.aws_capability._b1_testing import ACCOUNT, NAME, TRUST
from tests.integration.aws_capability._journal import Journal, prepare_directory
from tests.integration.aws_capability._matrix_testing import Call, dims, record
from tests.integration.aws_capability._probe_testing import (
    BUCKET,
    LATENCY,
    ROLE_API,
    ROLE_AUDIT,
    ProbeLab,
    cleanup_calls,
    janitor_ports,
    make_probe_lab,
)
from tests.integration.aws_capability._profiles import PROFILES
from tests.integration.aws_capability._safety import tags
from tests.integration.aws_capability.contracts import ServiceFailure

API_ARN = f"arn:aws:iam::{ACCOUNT}:role/cnesdata-cap/{NAME}-api"
BLOCKED = "serving/blocked/x"
EXPECTED_POLICY = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Deny",
            "Action": "s3:GetObject",
            "Principal": {"AWS": API_ARN},
            "Resource": f"arn:aws:s3:::{NAME}/serving/blocked/*",
        }
    ],
}


def run(lab: ProbeLab) -> dict:
    return dict(lab.services.probe(lab.resources, lab.spec, lab.context))


def ab_calls(calls: list[Call]) -> list[Call]:
    start = next(i for i, c in enumerate(calls) if dims(c).get("key") == "serving/y")
    return calls[start + 1 :]


def test_policy_tem_um_unico_deny_exato_no_role_api_e_no_prefixo_bloqueado(tmp_path: Path) -> None:
    lab = make_probe_lab(tmp_path, "iam-s3")
    calls = record(lab)
    run(lab)
    (put,) = [c for c in calls if c.operation == "put-bucket-policy"]
    assert put.who == "operator"
    assert set(put.payload) == {"Bucket", "Policy", "ExpectedBucketOwner"}
    assert put.payload["Bucket"] == NAME
    document = json.loads(str(put.payload["Policy"]))
    assert document == EXPECTED_POLICY
    statement = document["Statement"][0]
    assert "NotPrincipal" not in statement
    assert statement["Principal"] != "*"
    assert statement["Principal"]["AWS"].endswith(f"/cnesdata-cap/{NAME}-api")


def test_lado_a_vem_antes_da_policy_e_lado_b_depois_com_controle_final(tmp_path: Path) -> None:
    lab = make_probe_lab(tmp_path, "iam-s3")
    calls = record(lab)
    observed = run(lab)
    tail = ab_calls(calls)
    assert [(c.who, c.operation, dims(c).get("key"), c.ok) for c in tail] == [
        ("role-api", "head-object", BLOCKED, True),
        ("operator", "put-bucket-policy", None, True),
        ("role-api", "head-object", BLOCKED, False),
        ("role-api", "head-object", "serving/x", True),
    ]
    assert tail[0].request == tail[2].request
    assert observed["blocked_before"] == "ok"
    assert observed["blocked_after"] == "403"
    assert observed["serving_after_flip"] == "ok"


def test_lado_a_nao_verde_falha_sem_gravar_a_policy(tmp_path: Path) -> None:
    lab = make_probe_lab(tmp_path, "iam-s3")
    original = lab.aws.permits

    def blocked_unreadable(who: str, action: str, resource: str, keys: object = None) -> bool:
        if who != "operator" and resource.endswith(BLOCKED):
            return False
        return original(who, action, resource, keys)  # type: ignore[arg-type]

    lab.aws.permits = blocked_unreadable  # type: ignore[method-assign]
    calls = record(lab)
    with pytest.raises(ServiceFailure) as error:
        run(lab)
    assert error.value.args == ("probe=step_failed step=blocked_before",)
    assert not [c for c in calls if c.operation == "put-bucket-policy"]


@pytest.mark.parametrize("lag", [0, 4, 11])
def test_propagacao_e_observada_com_segundos_e_o_head_final_e_negado(
    tmp_path: Path, lag: int
) -> None:
    lab = make_probe_lab(tmp_path, "iam-s3", bucket_policy_lag=lag)
    calls = record(lab)
    observed = run(lab)
    heads = [c for c in ab_calls(calls) if dims(c).get("key") == BLOCKED and c.who == "role-api"]
    assert [c.ok for c in heads] == [True, *([True] * max(lag - 1, 0)), False]
    assert observed["blocked_after"] == "403"
    assert isinstance(observed["blocked_flip_seconds"], float)
    assert observed["blocked_flip_seconds"] >= 0
    assert len(lab.pauses) >= max(lag - 1, 0)


def test_nunca_virar_e_observacao_e_nao_queda(tmp_path: Path) -> None:
    lab = make_probe_lab(tmp_path, "iam-s3", bucket_policy_lag=10**6)
    calls = record(lab)
    observed = run(lab)
    heads = [c for c in ab_calls(calls) if dims(c).get("key") == BLOCKED and c.who == "role-api"]
    assert len(heads) == 1 + 12
    assert observed["blocked_after"] == "ok"
    assert observed["serving_after_flip"] == "ok"
    assert observed["blocked_flip_seconds"] > 0


@pytest.mark.parametrize("malformed", [1, 3])
def test_principal_recem_criado_e_repetido_de_forma_limitada(
    tmp_path: Path, malformed: int
) -> None:
    lab = make_probe_lab(tmp_path, "iam-s3", bucket_policy_malformed=malformed)
    calls = record(lab)
    observed = run(lab)
    puts = [c for c in calls if c.operation == "put-bucket-policy"]
    assert [c.code for c in puts] == ["MalformedPolicy"] * malformed + ["ok"]
    assert len({str(c.payload["Policy"]) for c in puts}) == 1
    assert observed["policy_put_attempts"] == malformed + 1


def test_malformed_persistente_falha_depois_de_quatro_tentativas(tmp_path: Path) -> None:
    lab = make_probe_lab(tmp_path, "iam-s3", bucket_policy_malformed=10**6)
    calls = record(lab)
    with pytest.raises(ServiceFailure) as error:
        run(lab)
    assert error.value.args == ("probe=step_failed step=bucket_policy",)
    assert len([c for c in calls if c.operation == "put-bucket-policy"]) == 4


def test_outro_erro_do_put_nao_e_repetido(tmp_path: Path) -> None:
    lab = make_probe_lab(tmp_path, "iam-s3")
    original = lab.aws.permits

    def no_policy(who: str, action: str, resource: str, keys: object = None) -> bool:
        return action != "s3:PutBucketPolicy" and original(who, action, resource, keys)  # type: ignore[arg-type]

    lab.aws.permits = no_policy  # type: ignore[method-assign]
    calls = record(lab)
    with pytest.raises(ServiceFailure):
        run(lab)
    assert len([c for c in calls if c.operation == "put-bucket-policy"]) == 1


def test_controle_depois_da_virada_negado_falha_o_probe(tmp_path: Path) -> None:
    lab = make_probe_lab(tmp_path, "iam-s3")
    original = lab.aws.permits

    def broken(who: str, action: str, resource: str, keys: object = None) -> bool:
        policy = lab.aws.buckets[NAME].policy
        if policy is not None and resource.endswith("/serving/x"):
            return False
        return original(who, action, resource, keys)  # type: ignore[arg-type]

    lab.aws.permits = broken  # type: ignore[method-assign]
    with pytest.raises(ServiceFailure) as error:
        run(lab)
    assert error.value.args == ("probe=step_failed step=serving_after_flip",)


def attached(lab: ProbeLab, document: dict) -> None:
    prepare_directory(lab.spec.journal_dir)
    bucket = FakeBucket(tags(lab.spec))
    bucket.policy = document
    lab.aws.buckets[NAME] = bucket
    for role in (ROLE_AUDIT, ROLE_API):
        lab.aws.roles[role.name] = FakeRole(tags(lab.spec), TRUST)
    journal = Journal(lab.spec)
    journal.created = [BUCKET, ROLE_AUDIT, ROLE_API]
    journal.phase = "cleanup_requested"
    journal.save()


def test_cleanup_remove_bucket_que_ainda_tem_a_policy_do_probe(tmp_path: Path) -> None:
    lab = make_probe_lab(tmp_path / "private", "iam-s3", seeded=False, latency=LATENCY)
    attached(lab, EXPECTED_POLICY)
    outcome = lifecycle.janitor(lab.spec, janitor_ports(lab))
    assert (outcome.status, outcome.reason) == ("succeeded", "complete")
    assert (lab.aws.buckets, lab.aws.roles) == ({}, {})
    operations = lab.aws.operations()
    assert operations.index("delete-bucket-policy") < operations.index("delete-bucket")
    policy = next(r for r in lab.aws.requests if r.operation == "delete-bucket-policy")
    delete = next(r for r in lab.aws.requests if r.operation == "delete-bucket")
    assert dict(policy.payload) == dict(delete.payload) == {
        "Bucket": NAME, "ExpectedBucketOwner": ACCOUNT,
    }
    assert cleanup_calls(lab) <= PROFILES["iam-s3"].limits.cleanup_reserve


def test_cleanup_remove_bucket_mesmo_com_deny_amplo_que_travaria_o_operador(
    tmp_path: Path,
) -> None:
    lab = make_probe_lab(tmp_path / "private", "iam-s3", seeded=False, latency=LATENCY)
    broad = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Deny", "Action": "s3:DeleteBucket", "Principal": "*",
                "Resource": f"arn:aws:s3:::{NAME}",
            }
        ],
    }
    attached(lab, broad)
    outcome = lifecycle.janitor(lab.spec, janitor_ports(lab))
    assert (outcome.status, outcome.reason) == ("succeeded", "complete")
    assert lab.aws.buckets == {}


def test_cleanup_de_bucket_sem_policy_aceita_no_such_bucket_policy(tmp_path: Path) -> None:
    lab = make_probe_lab(tmp_path / "private", "iam-s3", seeded=False, latency=LATENCY)
    attached(lab, EXPECTED_POLICY)
    lab.aws.buckets[NAME].policy = None
    outcome = lifecycle.janitor(lab.spec, janitor_ports(lab))
    assert (outcome.status, outcome.reason) == ("succeeded", "complete")
    codes = [r.operation for r in lab.aws.requests if r.operation == "delete-bucket-policy"]
    assert codes == ["delete-bucket-policy"]
