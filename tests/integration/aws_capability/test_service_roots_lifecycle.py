"""Janitor público preserva bucket cross-region com os quatro roots disponíveis."""

import json
from collections.abc import Iterator
from dataclasses import replace
from pathlib import Path

import pytest

from tests.integration.aws_capability import lifecycle
from tests.integration.aws_capability._journal import Journal, prepare_directory
from tests.integration.aws_capability._service_read_assertions import assert_process
from tests.integration.aws_capability._service_roots_testing import (
    ReadLab,
    load_fixture,
    make_read_lab,
)
from tests.integration.aws_capability._testing import ProcessBoundary
from tests.integration.aws_capability.contracts import Policy, Ports, Resource

FIXTURE = Path(__file__).parent / "fixtures" / "cli" / "roots" / "cross_region_lifecycle.json"
BUCKET = Resource("s3", "cnesdata-cap-a1b2c3d4-foreign", "cnesdata-cap-a1b2c3d4-foreign")
ROOT_CALLS = [
    ("cognito-idp", "list-user-pools", "us-east-2", {"MaxResults": 60}),
    ("dynamodb", "list-tables", "us-east-2", {"Limit": 100}),
    ("s3api", "list-buckets", "us-east-2", {"MaxBuckets": 1000}),
    (
        "s3api",
        "get-bucket-location",
        "us-east-2",
        {"Bucket": "cnesdata-cap-a1b2c3d4-foreign", "ExpectedBucketOwner": "111111111111"},
    ),
    (
        "s3api",
        "get-bucket-tagging",
        "us-east-2",
        {"Bucket": "cnesdata-cap-a1b2c3d4-foreign", "ExpectedBucketOwner": "111111111111"},
    ),
    ("iam", "list-roles", "us-east-1", {"MaxItems": 100, "PathPrefix": "/cnesdata-cap/"}),
]
EXPECTED = [("sts", "get-caller-identity", "us-east-1", {}), *ROOT_CALLS, *ROOT_CALLS]


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


def test_janitor_publico_preserva_bucket_cross_region_com_inventario_completo(
    read_lab: ReadLab,
) -> None:
    fixture = load_fixture(FIXTURE)
    spec = replace(
        read_lab.services.spec,
        journal_dir=read_lab.directory / "journal",
        allow_governance_bypass=True,
    )
    prepare_directory(spec.journal_dir)
    journal = Journal(spec, phase="cleanup_requested", created=[BUCKET])
    journal.save()
    services = replace(read_lab.services, spec=spec)
    ports = Ports(
        services,
        ProcessBoundary([]),
        lambda: 1000,
        Policy("111111111111", frozenset({"us-east-2"})),
        read_lab.context.budget,
    )
    for key in (
        "identity",
        "cognito_empty",
        "dynamodb_empty",
        "s3_bucket",
        "location",
        "tags",
        "iam_empty",
        "cognito_empty",
        "dynamodb_empty",
        "s3_bucket",
        "location",
        "tags",
        "iam_empty",
    ):
        read_lab.process.outcomes.append((0, json.dumps(fixture[key]), ""))
    outcome = lifecycle.janitor(spec, ports)
    assert outcome.status == "incomplete"
    assert outcome.reason == "ownership_mismatch"
    assert outcome.evidence["inventory_complete"] is True
    assert outcome.remaining == (BUCKET,)
    read_lab.check_private(outcome)
    assert json.loads(journal.path.read_text())["created"] == [
        {"service": "s3", "resource_id": BUCKET.resource_id, "name": BUCKET.name}
    ]
    assert len(read_lab.process.calls) == 13
    assert_process(read_lab, EXPECTED)


def test_fixture_publica_carrega_sem_chaves_duplicadas() -> None:
    assert isinstance(load_fixture(FIXTURE), dict)
