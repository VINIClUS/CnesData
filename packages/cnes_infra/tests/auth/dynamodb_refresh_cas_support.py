"""Pares literais, leitura moto e observações públicas de CAS."""

import json
import logging
import os
from collections import Counter
from collections.abc import Iterator
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from re import IGNORECASE, findall, search
from traceback import format_exception
from typing import Any, cast
from uuid import UUID

import pytest
from botocore.exceptions import ClientError

from cnes_domain.refresh_tokens import RefreshTokenMutation, RefreshTokenUnavailable
from packages.cnes_infra.tests.auth.dynamodb_refresh_support import (
    CANARY,
    FINGERPRINT,
    FINGERPRINT_CANARY,
    RECORD_CANARY,
    SECRET_CANARY,
    TENANT,
    TOKEN,
    DynamoBoundary,
    assert_log_privacy,
    fresh_store,
    requests_for,
)

FIXTURE = Path(__file__).parents[1] / "fixtures/auth/refresh_dynamodb_cas_schema1.json"
RAW = "".join(json.loads(FIXTURE.read_text(encoding="utf-8")))
VALUES: dict[str, Any] = json.loads(RAW)
USE = datetime.fromisoformat("2026-01-02T06:00:00.100002+00:00")
MAX = 9223372036854775807
TABLE = "refresh-tokens"
OBSERVATIONS: list[dict[str, Any]] = []


def pair(name: str = "base7") -> dict[str, Any]:
    return deepcopy(VALUES["states"][name])


def keys(group: str = "target") -> list[dict[str, Any]]:
    return deepcopy(list(VALUES["keys"][group].values()))


def mutation(**changes: Any) -> RefreshTokenMutation:
    values = {"tenant_id": TENANT, "now": USE, "expected_version": 7}
    values.update(changes)
    return RefreshTokenMutation(**values)


def changed(items: dict[str, Any], **changes: Any) -> dict[str, Any]:
    result = deepcopy(items)
    for item in result.values():
        item.update(deepcopy(changes))
    return result


def seed(boundary: DynamoBoundary, items: dict[str, Any]) -> None:
    for item in items.values():
        boundary.client.put_item(TableName=TABLE, Item=item)
        key = {"pk": item["pk"], "sk": item["sk"]}
        assert (
            boundary.client.get_item(
                TableName=TABLE,
                Key=key,
                ConsistentRead=True,
            )["Item"]
            == item
        )


def native_pair(boundary: DynamoBoundary, group: str = "target") -> dict[str, Any]:
    return {
        projection: boundary.client.get_item(
            TableName=TABLE,
            Key=key,
            ConsistentRead=True,
        ).get("Item")
        for projection, key in zip(("digest", "agent"), keys(group), strict=True)
    }


def assert_pair(boundary: DynamoBoundary, expected: dict[str, Any], group: str = "target") -> None:
    assert native_pair(boundary, group) == expected


def failure(code: str = "TransactionCanceledException", reasons: Any = None) -> ClientError:
    text = ":".join((CANARY, SECRET_CANARY, RECORD_CANARY, FINGERPRINT_CANARY))
    response: dict[str, Any] = {"Error": {"Code": code, "Message": text, "Item": text}}
    if reasons is not None:
        response["CancellationReasons"] = reasons
    return ClientError(cast("Any", response), "TransactWriteItems")


def pure_failure() -> ClientError:
    return failure(reasons=[{"Code": "None"}, {"Code": "ConditionalCheckFailed"}])


def assert_sanitized(error: BaseException) -> None:
    rendered = str(error) + repr(error) + "".join(format_exception(error))
    for value in (TOKEN, FINGERPRINT, CANARY, SECRET_CANARY, RECORD_CANARY, FINGERPRINT_CANARY):
        assert value not in rendered
    assert str(error) == "operation=mutate_refresh"
    assert error.__cause__ is None
    assert error.__context__ is None or error.__suppress_context__


def assert_atomic_reads(boundary: DynamoBoundary, count: int = 1) -> None:
    reads = requests_for(boundary, "transact_get_items")
    assert len(reads) == count
    for read in reads:
        assert read == {"TransactItems": [{"Get": {"TableName": TABLE, "Key": k}} for k in keys()]}
    discovery = requests_for(boundary, "get_item")
    assert len(discovery) == 1
    assert discovery[0] == {"TableName": TABLE, "Key": keys()[0], "ConsistentRead": True}


def assert_update_guards(boundary: DynamoBoundary, before: dict[str, Any]) -> None:
    writes = requests_for(boundary, "transact_write_items")
    assert len(writes) == 1
    actions = writes[0]["TransactItems"]
    assert [action["Update"]["Key"] for action in actions] == keys()
    for action, original in zip(actions, before.values(), strict=True):
        update = action["Update"]
        names = update["ExpressionAttributeNames"]
        values = update["ExpressionAttributeValues"]
        condition = update["ConditionExpression"]
        for field, raw in original.items():
            if field in {"pk", "sk"} or field.startswith("extra"):
                continue
            operands = findall(r"(#[A-Za-z0-9_]+)\s*=\s*(:[A-Za-z0-9_]+)", condition)
            assert search(r"\bOR\b", condition, IGNORECASE) is None
            assert any(
                names.get(alias) == field and values.get(value) == raw for alias, value in operands
            ), field


class CasCase:
    def __init__(self, boundary: DynamoBoundary) -> None:
        self.boundary = boundary
        self.boundaries = [boundary]
        self.secret_calls = 0

    def secret(self) -> str:
        self.secret_calls += 1
        raise AssertionError("mutation_secret_factory")

    def store(self, boundary: DynamoBoundary | None = None) -> Any:
        return fresh_store(boundary or self.boundary, self.secret)

    def independent(self) -> DynamoBoundary:
        boundary = self.boundary.independent()
        self.boundaries.append(boundary)
        return boundary

    def unavailable(self, operation: str = "mark_used", context: Any = None) -> None:
        with pytest.raises(RefreshTokenUnavailable) as caught:
            getattr(self.store(), operation)(TOKEN, context or mutation())
        assert_sanitized(caught.value)

    def assert_common(self, caplog: pytest.LogCaptureFixture) -> None:
        assert self.secret_calls == 0
        all_tokens: list[str] = []
        for boundary in self.boundaries:
            for name in ("query", "scan", "update_item", "put_item", "delete_item"):
                assert requests_for(boundary, name) == []
            tokens = [
                write.get("ClientRequestToken")
                for write in requests_for(
                    boundary,
                    "transact_write_items",
                )
            ]
            for token in tokens:
                try:
                    canonical = type(token) is str and str(UUID(token)) == token
                except ValueError:
                    canonical = False
                if not canonical:
                    raise AssertionError("cas_request_token_invalid")
            payloads = [
                (
                    name,
                    {
                        key: value
                        for key, value in request.items()
                        if name != "transact_write_items" or key != "ClientRequestToken"
                    },
                )
                for name, request in boundary.requests
            ]
            if TOKEN in repr(payloads):
                raise AssertionError("cas_request_payload_plaintext")
            if TOKEN in repr(native_pair(boundary)):
                raise AssertionError("cas_persisted_pair_plaintext")
            all_tokens.extend(cast("list[str]", tokens))
            assert len(tokens) == len(set(tokens))
            assert all(type(token) is str and len(token) == 36 for token in tokens)
        assert len(all_tokens) == len(set(all_tokens))
        assert_log_privacy(caplog.records)


@pytest.fixture
def cas_case(
    dynamodb_case: tuple[Any, Any],
    caplog: pytest.LogCaptureFixture,
    request: pytest.FixtureRequest,
) -> Iterator[CasCase]:
    boundary, _ = dynamodb_case
    case = CasCase(boundary)
    caplog.set_level(logging.DEBUG)
    try:
        yield case
    finally:
        case.assert_common(caplog)
        OBSERVATIONS.append(
            {
                "nodeid": request.node.nodeid,
                "factory_calls": case.secret_calls,
                "boundaries": [
                    dict(Counter(name for name, _ in b.requests)) for b in case.boundaries
                ],
            }
        )


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    destination = os.environ.get("CNES_CAS_BOUNDARY_EVIDENCE")
    if destination:
        Path(destination).write_text(json.dumps(OBSERVATIONS, indent=2))
