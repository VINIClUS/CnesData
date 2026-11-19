"""Fence regional observado pela preservação pública do janitor real."""

from typing import cast

import pytest

from tests.integration.aws_capability import lifecycle
from tests.integration.aws_capability._oracle import assert_complete
from tests.integration.aws_capability._service_region_testing import (
    REGION_TAGS,
    RegionalOwnership,
    RegionlessOwnership,
)
from tests.integration.aws_capability._testing import Lab, seed_journal
from tests.integration.aws_capability.contracts import Ownership, Resource


@pytest.mark.parametrize(
    ("service", "region"),
    [
        ("cognito", "us-west-2"),
        ("dynamodb", "us-west-2"),
        ("s3", "us-west-2"),
        ("iam", "us-east-2"),
        ("cognito", "aws-global"),
        ("dynamodb", "aws-global"),
        ("s3", "aws-global"),
    ],
)
def test_janitor_preserva_root_com_regiao_divergente(lab: Lab, service: str, region: str) -> None:
    root = Resource(service, "regional-root", "cnesdata-cap-a1b2c3d4")
    lab.remote.add(root, RegionalOwnership("111111111111", REGION_TAGS, region))
    seed_journal(lab, [root])
    outcome = lifecycle.janitor(lab.spec, lab.ports())
    deleted = [event for event in lab.events if event[0] == "delete"]
    observed = (outcome.status, outcome.reason, set(lab.remote.resources), deleted)
    assert observed == ("incomplete", "ownership_mismatch", {"regional-root"}, [])


@pytest.mark.parametrize("service", ["cognito", "dynamodb", "s3", "iam"])
def test_janitor_preserva_root_sem_metadata_regional(lab: Lab, service: str) -> None:
    root = Resource(service, "regionless-root", "cnesdata-cap-a1b2c3d4")
    owner = cast("Ownership", RegionlessOwnership("111111111111", REGION_TAGS))
    lab.remote.add(root, owner)
    seed_journal(lab, [root])
    outcome = lifecycle.janitor(lab.spec, lab.ports())
    deleted = [event for event in lab.events if event[0] == "delete"]
    observed = (outcome.status, outcome.reason, set(lab.remote.resources), deleted)
    assert observed == ("incomplete", "ownership_mismatch", {"regionless-root"}, [])


@pytest.mark.parametrize("service", ["cognito", "dynamodb", "s3", "iam"])
def test_janitor_preserva_root_com_regiao_vazia(lab: Lab, service: str) -> None:
    root = Resource(service, "empty-region-root", "cnesdata-cap-a1b2c3d4")
    lab.remote.add(root, RegionalOwnership("111111111111", REGION_TAGS, ""))
    seed_journal(lab, [root])
    outcome = lifecycle.janitor(lab.spec, lab.ports())
    deleted = [event for event in lab.events if event[0] == "delete"]
    observed = (outcome.status, outcome.reason, set(lab.remote.resources), deleted)
    assert observed == ("incomplete", "ownership_mismatch", {"empty-region-root"}, [])


@pytest.mark.parametrize(
    ("service", "region"),
    [
        ("cognito", "us-east-2"),
        ("dynamodb", "us-east-2"),
        ("s3", "us-east-2"),
        ("iam", "aws-global"),
    ],
)
def test_janitor_remove_root_com_metadata_regional_autorizada(
    lab: Lab, service: str, region: str
) -> None:
    root = Resource(service, "authorized-root", "cnesdata-cap-a1b2c3d4")
    lab.remote.add(root, RegionalOwnership("111111111111", REGION_TAGS, region))
    seed_journal(lab, [root])
    outcome = lifecycle.janitor(lab.spec, lab.ports())
    assert_complete(outcome, lab.events)
    assert lab.remote.resources == {}
    assert [event for event in lab.events if event[0] == "delete"] == [
        ("delete", "authorized-root", True)
    ]


@pytest.mark.parametrize(
    ("service", "before", "after"),
    [
        ("cognito", "us-east-2", "us-west-2"),
        ("dynamodb", "us-east-2", "us-west-2"),
        ("s3", "us-east-2", "us-west-2"),
        ("iam", "aws-global", "us-east-2"),
    ],
)
def test_segundo_janitor_rele_regiao_antes_de_tentar_novo_delete(
    lab: Lab, service: str, before: str, after: str
) -> None:
    root = Resource(service, "fresh-region-root", "cnesdata-cap-a1b2c3d4")
    lab.remote.add(root, RegionalOwnership("111111111111", REGION_TAGS, before))
    lab.remote.delete_noop.add("fresh-region-root")
    seed_journal(lab, [root])
    first = lifecycle.janitor(lab.spec, lab.ports())
    assert (first.status, first.reason) == ("incomplete", "cleanup_failed")
    assert [event for event in lab.events if event[0] == "delete"] == [
        ("delete", "fresh-region-root", True)
    ]
    lab.remote.ownership["fresh-region-root"] = RegionalOwnership(
        "111111111111", REGION_TAGS, after
    )
    second = lifecycle.janitor(lab.spec, lab.ports())
    deleted = [event for event in lab.events if event[0] == "delete"]
    observed = (second.status, second.reason, set(lab.remote.resources), deleted)
    assert observed == ("incomplete", "ownership_mismatch", {"fresh-region-root"}, [])
