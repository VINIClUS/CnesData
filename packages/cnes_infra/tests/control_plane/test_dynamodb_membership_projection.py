"""Projeção gsi1 de membership no DynamoDB e convivência com o job claim."""
from collections.abc import Iterator
from typing import Any

import boto3
import pytest
from moto import mock_aws

from cnes_domain.control_plane.entities import Membership
from cnes_infra.auth.dynamodb_memberships import DynamoDBMembershipCandidates
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.control_plane.dynamodb_keys import entity_key, item_key, key_component
from packages.cnes_infra.tests.contracts.clock import _NOW, _TENANT, _agent, _event, _job
from packages.cnes_infra.tests.control_plane.test_dynamodb_adapter import (
    _TABLE_NAME,
    ClientSpy,
    _create_table,
)


@pytest.fixture
def client() -> Iterator[Any]:
    with mock_aws():
        dynamodb = boto3.client("dynamodb", region_name="us-east-1")
        _create_table(dynamodb)
        yield dynamodb


def _adapter(client: Any) -> DynamoDBControlPlane:
    return DynamoDBControlPlane(client, _TABLE_NAME, lambda: _NOW)


def _membership(tenant_id: str = _TENANT, user_id: str = "user-1") -> Membership:
    return Membership(tenant_id=tenant_id, user_id=user_id, role="gestor", created_at=_NOW)


def test_put_membership_grava_projecao_gsi1(client) -> None:
    _adapter(client).put_membership(_membership())
    key = entity_key(_TENANT, "MEMBERSHIP", "user-1")
    item = client.get_item(TableName=_TABLE_NAME, Key=item_key(*key))["Item"]
    assert item["gsi1pk"] == {"S": f"USER#{key_component('user-1')}"}
    assert item["gsi1sk"] == {"S": f"TENANT#{key_component(_TENANT)}"}


def test_candidatos_refletem_membership_gravada_e_revogada(client) -> None:
    adapter = _adapter(client)
    adapter.put_membership(_membership("tenant-a"))
    adapter.put_membership(_membership("tenant-b"))
    adapter.put_membership(_membership("tenant-c", user_id="user-2"))
    candidates = DynamoDBMembershipCandidates(client, _TABLE_NAME)
    assert set(candidates.list_candidates("user-1")) == {"tenant-a", "tenant-b"}
    key = entity_key("tenant-a", "MEMBERSHIP", "user-1")
    client.delete_item(TableName=_TABLE_NAME, Key=item_key(*key))
    assert candidates.list_candidates("user-1") == ("tenant-b",)


def test_queries_do_gsi1_usam_igualdade_de_particao(client) -> None:
    spy = ClientSpy(client)
    adapter = _adapter(spy)
    adapter.put_agent(_agent("agent-a"))
    adapter.create_job(_job("job-1"), _event("event-1"))
    adapter.put_membership(_membership())
    jobs = adapter.list_claimable_jobs(_TENANT, "agent-a", 10)
    tenants = DynamoDBMembershipCandidates(spy, _TABLE_NAME).list_candidates("user-1")
    assert [job.job_id for job in jobs] == ["job-1"]
    assert tenants == (_TENANT,)
    gsi1 = [request for request in spy.query_requests if request.get("IndexName") == "gsi1"]
    assert len(gsi1) == 2
    for request in gsi1:
        assert "begins_with" not in request["KeyConditionExpression"]
        assert request["KeyConditionExpression"].startswith("gsi1pk = :")
