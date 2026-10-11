"""Tabela canônica do control plane (pk/sk, gsi1..gsi6 ALL, TTL expires_at) para os cenários."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Mapping

INDEXES = tuple(f"gsi{number}" for number in range(1, 7))
TTL_ATTRIBUTE = "expires_at"
_WAITER = {"Delay": 1, "MaxAttempts": 60}


def _key_schema(partition: str, sort: str) -> list[dict[str, str]]:
    return [
        {"AttributeName": partition, "KeyType": "HASH"},
        {"AttributeName": sort, "KeyType": "RANGE"},
    ]


def _indexes() -> list[dict[str, Any]]:
    return [
        {
            "IndexName": index,
            "KeySchema": _key_schema(f"{index}pk", f"{index}sk"),
            "Projection": {"ProjectionType": "ALL"},
        }
        for index in INDEXES
    ]


def create_canonical_table(
    dynamodb: Any, name: str, options: Mapping[str, Any] | None = None,
) -> str:
    """Cria a tabela on-demand com o schema canônico, espera ACTIVE e liga o TTL.

    Returns: ARN da tabela.
    """
    attributes = ("pk", "sk", *(f"{index}{part}" for index in INDEXES for part in ("pk", "sk")))
    dynamodb.create_table(
        TableName=name,
        BillingMode="PAY_PER_REQUEST",
        KeySchema=_key_schema("pk", "sk"),
        AttributeDefinitions=[{"AttributeName": item, "AttributeType": "S"} for item in attributes],
        GlobalSecondaryIndexes=_indexes(),
        **(options or {}),
    )
    try:
        dynamodb.get_waiter("table_exists").wait(TableName=name, WaiterConfig=_WAITER)
        dynamodb.update_time_to_live(
            TableName=name,
            TimeToLiveSpecification={"Enabled": True, "AttributeName": TTL_ATTRIBUTE},
        )
        return dynamodb.describe_table(TableName=name)["Table"]["TableArn"]
    except BaseException:
        delete_table(dynamodb, name)
        raise


def delete_table(dynamodb: Any, name: str) -> None:
    """Remove a tabela, desligando antes a proteção contra exclusão quando ativa."""
    table = dynamodb.describe_table(TableName=name)["Table"]
    if table.get("DeletionProtectionEnabled"):
        dynamodb.update_table(TableName=name, DeletionProtectionEnabled=False)
    dynamodb.delete_table(TableName=name)
    dynamodb.get_waiter("table_not_exists").wait(TableName=name, WaiterConfig=_WAITER)
