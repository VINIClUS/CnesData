"""Interleavings finitos com transações e condições reais no moto instalado."""

from typing import Any

import pytest
from botocore.exceptions import ClientError

from packages.cnes_infra.tests.auth.dynamodb_refresh_cas_support import (
    TABLE,
    USE,
    CasCase,
    assert_atomic_reads,
    assert_pair,
    keys,
    mutation,
    pair,
    seed,
)
from packages.cnes_infra.tests.auth.dynamodb_refresh_support import TOKEN, requests_for

pytest_plugins = (
    "packages.cnes_infra.tests.auth.dynamodb_refresh_support",
    "packages.cnes_infra.tests.auth.dynamodb_refresh_cas_support",
)


@pytest.mark.parametrize(
    "operations",
    [("mark_used", "mark_used"), ("revoke", "revoke"), ("mark_used", "revoke")],
    ids=["mark-mark", "revoke-revoke", "mark-revoke"],
)
def test_perdedor_rele_par_atomico_sem_repetir_escrita(
    cas_case: CasCase,
    operations: tuple[str, str],
) -> None:
    boundary: Any = cas_case.boundary
    other = cas_case.independent()
    seed(boundary, pair())
    original = boundary.transact_write_items
    interleavings: list[bool] = []

    def paused(**kwargs: Any) -> Any:
        assert not interleavings
        assert_atomic_reads(boundary)
        assert_pair(boundary, pair())
        result = getattr(cas_case.store(other), operations[1])(TOKEN, mutation())
        assert type(result) is bool
        assert result is True
        interleavings.append(result)
        return original(**kwargs)

    boundary.transact_write_items = paused
    assert (
        getattr(cas_case.store(), operations[0])(
            TOKEN,
            mutation(now=USE.replace(microsecond=100001)),
        )
        is False
    )
    assert interleavings == [True]
    assert_atomic_reads(boundary, count=2)
    assert_atomic_reads(other)
    assert len(requests_for(boundary, "transact_write_items")) == 1
    assert len(requests_for(other, "transact_write_items")) == 1
    assert len(boundary.forwarded_errors) == 1
    error = boundary.forwarded_errors[0][1]
    assert isinstance(error, ClientError)
    assert error.response.get("Error", {}).get("Code") == "TransactionCanceledException"
    reasons = [reason.get("Code") for reason in error.response.get("CancellationReasons", [])]
    assert len(reasons) == 2
    assert "ConditionalCheckFailed" in reasons
    assert set(reasons) <= {"None", "ConditionalCheckFailed"}
    assert_pair(boundary, pair("used8" if operations[1] == "mark_used" else "revoked8"))
    boundary.transact_write_items = original
    _assert_later(cas_case, operations[1])


def _assert_later(case: CasCase, winner: str) -> None:
    if winner == "revoke":
        assert case.store().mark_used(TOKEN, mutation(expected_version=8)) is False
        assert case.store().revoke(TOKEN, mutation(expected_version=8)) is False
        assert_pair(case.boundary, pair("revoked8"))
    else:
        assert (
            case.store().mark_used(
                TOKEN,
                mutation(now=USE.replace(microsecond=100003), expected_version=8),
            )
            is True
        )
        assert_pair(case.boundary, pair("reused9"))
        assert (
            case.store().revoke(
                TOKEN,
                mutation(now=USE.replace(microsecond=100004), expected_version=9),
            )
            is True
        )
        assert_pair(case.boundary, pair("revoked10"))


def test_corrupcao_sem_incremento_cancela_os_dois_updates(cas_case: CasCase) -> None:
    boundary: Any = cas_case.boundary
    seed(boundary, pair())
    original = boundary.transact_write_items
    corrupted = pair()
    corrupted["agent"]["last_used_at"] = {"S": "2026-01-02T04:05:06.234568+00:00"}
    calls: list[Any] = []

    def corrupt(**kwargs: Any) -> Any:
        calls.append(kwargs)
        assert_pair(boundary, pair())
        boundary.client.put_item(TableName=TABLE, Item=corrupted["agent"])
        return original(**kwargs)

    boundary.transact_write_items = corrupt
    cas_case.unavailable()
    assert len(calls) == 1
    assert_atomic_reads(boundary, count=2)
    assert len(requests_for(boundary, "transact_write_items")) == 1
    assert_pair(boundary, corrupted)
    assert boundary.client.get_item(TableName=TABLE, Key=keys()[0])["Item"] == pair()["digest"]
    error = boundary.forwarded_errors[0][1]
    assert isinstance(error, ClientError)
    assert [reason.get("Code") for reason in error.response.get("CancellationReasons", [])] == [
        "None",
        "ConditionalCheckFailed",
    ]
