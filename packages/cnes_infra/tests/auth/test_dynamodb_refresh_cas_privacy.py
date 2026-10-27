"""Privacidade de payload CAS com metadados UUID independentes."""

from collections.abc import Iterator
from copy import deepcopy
from logging import DEBUG
from re import findall
from traceback import extract_tb
from typing import Any
from uuid import UUID

import boto3
import botocore.session
import pytest

from cnes_domain.refresh_tokens import RefreshTokenUnavailable
from packages.cnes_infra.tests.auth.dynamodb_refresh_cas_support import (
    CasCase,
    mutation,
    native_pair,
    pair,
    seed,
)
from packages.cnes_infra.tests.auth.dynamodb_refresh_support import (
    TOKEN,
    DynamoBoundary,
    assert_log_privacy,
    requests_for,
)

pytest_plugins = ("packages.cnes_infra.tests.auth.dynamodb_refresh_support",)
TABLE = "refresh-tokens"
SAFE_UUID = "11111111-1111-4111-8111-111111111111"
COLLISION_UUID = "11111111-1111-4111-8111-111111111abc"
SECOND_UUID = "22222222-2222-4222-8222-222222222222"
OBSERVATIONS: list[dict[str, Any]] = []


class PrivacyBoundary(DynamoBoundary):
    def __init__(self, client: Any) -> None:
        super().__init__(client)
        self.request_token = SAFE_UUID
        self.leak_payload = False
        self.native_successes: list[Any] = []

    def transact_write_items(self, **kwargs: Any) -> Any:
        forwarded = deepcopy(kwargs)
        forwarded["ClientRequestToken"] = self.request_token
        if self.leak_payload:
            inject_plaintext_condition(forwarded)
        response = super().__getattr__("transact_write_items")(**forwarded)
        self.native_successes.append(deepcopy(response))
        return response


def inject_plaintext_condition(request: dict[str, Any]) -> None:
    for action in request["TransactItems"]:
        update = action["Update"]
        operands = findall(
            r"(#[A-Za-z0-9_]+)\s*=\s*(:[A-Za-z0-9_]+)", update["ConditionExpression"]
        )
        aliases = [
            value
            for alias, value in operands
            if update["ExpressionAttributeNames"][alias] == "machine_fingerprint"
        ]
        assert len(aliases) == 1
        update["ExpressionAttributeValues"][aliases[0]] = {"S": TOKEN}


class PrivacyCase:
    def __init__(self, boundary: PrivacyBoundary, observer: DynamoBoundary) -> None:
        self.boundary = boundary
        self.observer = observer
        self.original = CasCase(boundary)
        self.expected: list[Any] = []
        self.effects: list[str] = []
        self.helper_results: list[Any] = []

    def state(self) -> list[Any]:
        items = self.observer.client.scan(TableName=TABLE, ConsistentRead=True)["Items"]
        return sorted(items, key=lambda item: (item["pk"]["S"], item["sk"]["S"]))

    def blocked(self, *_: Any, **__: Any) -> None:
        self.effects.append("client_session_or_close")
        raise AssertionError("cas_privacy_borrowed_client_effect")

    def successful_use(self, metadata: str) -> None:
        seed(self.observer, pair())
        assert native_pair(self.observer) == pair()
        self.boundary.request_token = metadata
        assert self.original.store().mark_used(TOKEN, mutation()) is True
        assert native_pair(self.observer) == pair("used8")
        requests = requests_for(self.boundary, "transact_write_items")
        assert len(requests) == len(self.boundary.native_successes) == 1
        assert requests[0]["ClientRequestToken"] == metadata
        assert self.boundary.native_successes[0]["ResponseMetadata"]["HTTPStatusCode"] == 200
        self.expected = sorted(
            pair("used8").values(), key=lambda item: (item["pk"]["S"], item["sk"]["S"])
        )
        assert self.state() == self.expected

    def check_original_helper(self, caplog: pytest.LogCaptureFixture) -> dict[str, Any]:
        before = self.state()
        result: dict[str, Any] = {"before": before, "outcome": "accepted"}
        try:
            self.original.assert_common(caplog)
        except AssertionError as error:
            frames = extract_tb(error.__traceback__)
            frame = frames[-1]
            if (
                frame.name != "assert_common"
                or frame.filename != self.original.assert_common.__code__.co_filename
            ):
                raise
            result.update(
                outcome="rejected",
                reason=str(error),
                assertion={"file": frame.filename, "function": frame.name, "text": frame.line},
            )
        finally:
            result["after"] = self.state()
            self.helper_results.append(result)
            assert result["after"] == before == self.expected
        return result


@pytest.fixture
def privacy_case(
    dynamodb_case: tuple[Any, Any],
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    request: pytest.FixtureRequest,
) -> Iterator[PrivacyCase]:
    case = PrivacyCase(PrivacyBoundary(dynamodb_case[0].client), dynamodb_case[0].independent())
    caplog.set_level(DEBUG)
    monkeypatch.setattr(boto3, "client", case.blocked)
    monkeypatch.setattr(boto3.session.Session, "__init__", case.blocked)
    monkeypatch.setattr(botocore.session.Session, "__init__", case.blocked)
    monkeypatch.setattr(boto3.session.Session, "client", case.blocked)
    monkeypatch.setattr(botocore.session.Session, "create_client", case.blocked)
    for boundary in (case.boundary, case.observer):
        monkeypatch.setattr(boundary.client, "close", case.blocked)
    try:
        yield case
    finally:
        assert case.original.secret_calls == 0
        assert case.effects == []
        assert case.state() == case.expected
        assert_log_privacy(caplog.records)
        assert not [record for record in caplog.records if "/src/cnes_" in record.pathname]
        OBSERVATIONS.append(
            {
                "nodeid": request.node.nodeid,
                "requests": case.boundary.requests,
                "native_successes": case.boundary.native_successes,
                "native_errors": [
                    {"operation": name, "response": getattr(error, "response", None)}
                    for name, error in case.boundary.forwarded_errors
                ],
                "helper_results": case.helper_results,
                "state_after": case.state(),
                "expected": case.expected,
                "factory_calls": case.original.secret_calls,
                "effects": case.effects,
            }
        )


def test_uuid_sem_colisao(privacy_case: PrivacyCase, caplog: pytest.LogCaptureFixture) -> None:
    assert str(UUID(SAFE_UUID)) == SAFE_UUID
    assert TOKEN not in SAFE_UUID
    privacy_case.successful_use(SAFE_UUID)
    result = privacy_case.check_original_helper(caplog)
    assert result["outcome"] == "accepted"


def test_uuid_contem_abc(privacy_case: PrivacyCase, caplog: pytest.LogCaptureFixture) -> None:
    assert str(UUID(COLLISION_UUID)) == COLLISION_UUID
    assert TOKEN in COLLISION_UUID
    privacy_case.successful_use(COLLISION_UUID)
    result = privacy_case.check_original_helper(caplog)
    assert result["outcome"] == "accepted"


def test_metadata_nao_uuid(privacy_case: PrivacyCase, caplog: pytest.LogCaptureFixture) -> None:
    metadata = "z" * 36
    with pytest.raises(ValueError):
        UUID(metadata)
    privacy_case.successful_use(metadata)
    result = privacy_case.check_original_helper(caplog)
    assert result["outcome"] == "rejected"
    assert result["reason"] == "cas_request_token_invalid"
    assert result["assertion"]["file"] == CasCase.assert_common.__code__.co_filename
    assert result["assertion"]["function"] == "assert_common"


def test_metadata_nao_canonica(privacy_case: PrivacyCase, caplog: pytest.LogCaptureFixture) -> None:
    metadata = "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA"
    assert str(UUID(metadata)) != metadata
    privacy_case.successful_use(metadata)
    result = privacy_case.check_original_helper(caplog)
    assert result["outcome"] == "rejected"
    assert result["reason"] == "cas_request_token_invalid"
    assert result["assertion"]["file"] == CasCase.assert_common.__code__.co_filename
    assert result["assertion"]["function"] == "assert_common"


def test_metadata_contem_segredo(
    privacy_case: PrivacyCase, caplog: pytest.LogCaptureFixture
) -> None:
    privacy_case.successful_use(TOKEN)
    result = privacy_case.check_original_helper(caplog)
    assert result["outcome"] == "rejected"
    assert result["reason"] == "cas_request_token_invalid"
    assert result["assertion"]["file"] == CasCase.assert_common.__code__.co_filename
    assert result["assertion"]["function"] == "assert_common"


def test_plaintext_no_payload(privacy_case: PrivacyCase, caplog: pytest.LogCaptureFixture) -> None:
    privacy_case.successful_use(SAFE_UUID)
    boundary = privacy_case.boundary
    boundary.request_token = SECOND_UUID
    boundary.leak_payload = True
    with pytest.raises(RefreshTokenUnavailable, match=r"^operation=mutate_refresh$"):
        privacy_case.original.store().mark_used(TOKEN, mutation(expected_version=8))
    writes = requests_for(boundary, "transact_write_items")
    assert len(writes) == 2
    assert writes[0]["ClientRequestToken"] != writes[1]["ClientRequestToken"]
    assert len(boundary.forwarded_errors) == 1
    error = boundary.forwarded_errors[0][1]
    response = getattr(error, "response", {})
    assert response["Error"]["Code"] == "TransactionCanceledException"
    assert len(response["CancellationReasons"]) == 2
    assert all(
        reason["Code"] == "ConditionalCheckFailed" for reason in response["CancellationReasons"]
    )
    assert native_pair(privacy_case.observer) == pair("used8")
    assert privacy_case.state() == privacy_case.expected
    result = privacy_case.check_original_helper(caplog)
    assert result["outcome"] == "rejected"
    assert result["reason"] == "cas_request_payload_plaintext"
    assert result["assertion"]["file"] == CasCase.assert_common.__code__.co_filename
    assert result["assertion"]["function"] == "assert_common"


def test_plaintext_no_item(privacy_case: PrivacyCase, caplog: pytest.LogCaptureFixture) -> None:
    privacy_case.successful_use(SAFE_UUID)
    expected = pair("used8")
    for item in expected.values():
        item["extra_plaintext"] = {"S": TOKEN}
    seed(privacy_case.observer, expected)
    assert native_pair(privacy_case.observer) == expected
    privacy_case.expected = sorted(
        expected.values(), key=lambda item: (item["pk"]["S"], item["sk"]["S"])
    )
    assert privacy_case.state() == privacy_case.expected
    result = privacy_case.check_original_helper(caplog)
    assert result["outcome"] == "rejected"
    assert result["reason"] == "cas_persisted_pair_plaintext"
    assert result["assertion"]["file"] == CasCase.assert_common.__code__.co_filename
    assert result["assertion"]["function"] == "assert_common"
