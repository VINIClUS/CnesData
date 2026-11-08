"""Remoção da bucket policy do iam-s3 antes do bucket; os demais perfis não ganham chamada."""

import logging
from pathlib import Path

import pytest

from tests.integration.aws_capability._b1_testing import (
    ACCOUNT,
    NAME,
    TAG_SET,
    b1_lab,
    cleanup_context,
    fail,
    ok,
)
from tests.integration.aws_capability._service_read_assertions import assert_process
from tests.integration.aws_capability._service_read_testing import ReadLab, make_read_lab
from tests.integration.aws_capability.contracts import Resource, ServiceFailure

ROOT = Resource("s3", NAME, NAME)
OWNER = {"Bucket": NAME, "ExpectedBucketOwner": ACCOUNT}
LOCATION = ("s3api", "get-bucket-location", "us-east-2", OWNER)
TAGGING = ("s3api", "get-bucket-tagging", "us-east-2", OWNER)
DELETE_POLICY = ("s3api", "delete-bucket-policy", "us-east-2", OWNER)
DELETE_BUCKET = ("s3api", "delete-bucket", "us-east-2", OWNER)


@pytest.fixture
def lab(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> ReadLab:
    caplog.set_level(logging.DEBUG)
    return make_read_lab(monkeypatch, tmp_path)


def owner_replies(lab: ReadLab) -> None:
    ok(lab, {"LocationConstraint": "us-east-2"})
    ok(lab, {"TagSet": TAG_SET})


def test_iam_s3_remove_a_policy_do_bucket_antes_do_bucket(lab: ReadLab) -> None:
    lab = b1_lab(lab, "iam-s3", [])
    owner_replies(lab)
    ok(lab)
    ok(lab)
    fail(lab, "NoSuchBucket")
    lab.services.delete(ROOT, True, cleanup_context(lab))
    assert_process(lab, [LOCATION, TAGGING, DELETE_POLICY, DELETE_BUCKET, LOCATION])


def test_iam_s3_aceita_bucket_sem_policy(lab: ReadLab) -> None:
    lab = b1_lab(lab, "iam-s3", [])
    owner_replies(lab)
    fail(lab, "NoSuchBucketPolicy")
    ok(lab)
    fail(lab, "NoSuchBucket")
    lab.services.delete(ROOT, True, cleanup_context(lab))
    assert_process(lab, [LOCATION, TAGGING, DELETE_POLICY, DELETE_BUCKET, LOCATION])


@pytest.mark.parametrize("code", ["AccessDenied", "NoSuchBucket", "InternalError"])
def test_iam_s3_outro_erro_ao_remover_a_policy_falha_sem_apagar_o_bucket(
    lab: ReadLab, code: str
) -> None:
    lab = b1_lab(lab, "iam-s3", [])
    owner_replies(lab)
    fail(lab, code)
    with pytest.raises(ServiceFailure):
        lab.services.delete(ROOT, True, cleanup_context(lab))
    assert_process(lab, [LOCATION, TAGGING, DELETE_POLICY])


@pytest.mark.parametrize("probe", ["s3-hold", "s3-retention"])
def test_outros_perfis_s3_nao_ganham_chamada_de_policy(lab: ReadLab, probe: str) -> None:
    lab = b1_lab(lab, probe, [])
    owner_replies(lab)
    ok(lab)
    fail(lab, "NoSuchBucket")
    lab.services.delete(ROOT, True, cleanup_context(lab))
    assert_process(lab, [LOCATION, TAGGING, DELETE_BUCKET, LOCATION])
