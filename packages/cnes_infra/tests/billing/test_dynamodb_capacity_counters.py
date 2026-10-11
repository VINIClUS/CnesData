"""Testes do leitor forte dos contadores de capacidade usado pelo observador de shadow."""

from collections.abc import Iterator
from typing import Any
from unittest.mock import Mock

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from cnes_domain.billing.errors import BillingDependencyError, PermanentBillingError
from cnes_domain.billing.models import CapacityKind
from cnes_infra.billing.dynamodb_capacity_counters import DynamoCapacityCounters
from cnes_infra.billing.keys import capacity_usage_key
from cnes_infra.control_plane.dynamodb_keys import item_key
from packages.cnes_infra.tests.billing.billing_factories import TABLE_NAME, create_table
from packages.cnes_infra.tests.billing.shadow_support import seed_capacity

ACCOUNT = "ba_01"


@pytest.fixture
def client() -> Iterator[Any]:
    with mock_aws():
        dynamo = boto3.client("dynamodb", region_name="us-east-1")
        create_table(dynamo)
        yield dynamo


def test_item_ausente_devolve_none(client: Any) -> None:
    counters = DynamoCapacityCounters(client, TABLE_NAME)

    assert counters.get_capacity_count(ACCOUNT, CapacityKind.AGENT) is None


def test_contador_ausente_no_item_devolve_none(client: Any) -> None:
    seed_capacity(client, ACCOUNT, tenant_count=1)
    counters = DynamoCapacityCounters(client, TABLE_NAME)

    assert counters.get_capacity_count(ACCOUNT, CapacityKind.AGENT) is None
    assert counters.get_capacity_count(ACCOUNT, CapacityKind.TENANT) == 1


def test_le_contador_de_agentes(client: Any) -> None:
    seed_capacity(client, ACCOUNT, agent_count=3, tenant_count=0)
    counters = DynamoCapacityCounters(client, TABLE_NAME)

    assert counters.get_capacity_count(ACCOUNT, CapacityKind.AGENT) == 3
    assert counters.get_capacity_count(ACCOUNT, CapacityKind.TENANT) == 0


def test_contador_corrompido_vira_erro_permanente(client: Any) -> None:
    item = item_key(*capacity_usage_key(ACCOUNT)) | {"agent_count": {"S": "x"}}
    client.put_item(TableName=TABLE_NAME, Item=item)

    with pytest.raises(PermanentBillingError):
        DynamoCapacityCounters(client, TABLE_NAME).get_capacity_count(ACCOUNT, CapacityKind.AGENT)


def test_leitura_e_forte_e_falha_vira_dependencia() -> None:
    client = Mock()
    client.get_item.side_effect = ClientError(
        {"Error": {"Code": "InternalServerError", "Message": "boom"}}, "GetItem",
    )

    with pytest.raises(BillingDependencyError):
        DynamoCapacityCounters(client, TABLE_NAME).get_capacity_count(ACCOUNT, CapacityKind.AGENT)

    assert client.get_item.call_args.kwargs["ConsistentRead"] is True
