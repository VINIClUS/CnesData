"""Importação DynamoDB com DTOs finitos e requisição transacional literal."""

from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from cnes_domain.refresh_tokens import RefreshTokenInvalid
from packages.cnes_infra.tests.auth.dynamodb_refresh_support import (
    EXPIRES,
    ISSUED,
    OTHER_AGENT,
    OTHER_TENANT,
    TOKEN,
    DynamoBoundary,
    fresh_store,
    item_key,
    literal_item,
    requests_for,
    token_input,
    token_record,
    ttl,
)

pytest_plugins = ("packages.cnes_infra.tests.auth.dynamodb_refresh_support",)


def _expected_item(record: Any, projection: str) -> dict[str, dict[str, Any]]:
    item = literal_item(projection)
    item.update(item_key(record.tenant_id, record.token_sha256, projection, record.agent_id))
    item.update(
        token_sha256={"S": record.token_sha256},
        tenant_id={"S": record.tenant_id},
        agent_id={"S": record.agent_id},
        machine_fingerprint={"S": record.machine_fingerprint},
        issued_at={"S": record.issued_at.isoformat()},
        token_expires_at={"S": record.expires_at.isoformat()},
        expires_at={"N": str(ttl(record.expires_at))},
        version={"N": str(record.version)},
    )
    for name in ("last_used_at", "revoked_at"):
        value = getattr(record, name)
        item[name] = {"NULL": True} if value is None else {"S": value.isoformat()}
    return item


def _assert_import_request(
    boundary: DynamoBoundary, record: Any, index: int = 0, expected_count: int = 1
) -> None:
    writes = requests_for(boundary, "transact_write_items")
    assert len(writes) == expected_count
    assert all(name == "transact_write_items" for name, _ in boundary.requests)
    request = writes[index]
    assert set(request) == {"TransactItems", "ClientRequestToken"}
    client_token = request["ClientRequestToken"]
    assert isinstance(client_token, str)
    assert client_token
    actions = request["TransactItems"]
    assert len(actions) == 2
    assert [set(action) for action in actions] == [{"Put"}, {"Put"}]
    for action, projection in zip(actions, ("DIGEST", "AGENT"), strict=True):
        put = action["Put"]
        assert put["TableName"] == "refresh-tokens"
        assert set(put) == {"TableName", "Item", "ConditionExpression"}
        assert put["Item"] == _expected_item(record, projection)
        assert put["ConditionExpression"] == (
            "attribute_not_exists(pk) AND attribute_not_exists(sk)"
        )


def test_import_publico_emite_um_par_literal_sem_fabrica(
    dynamodb_case: tuple[Any, Any],
) -> None:
    boundary, _ = dynamodb_case
    calls = 0

    def forbidden() -> str:
        nonlocal calls
        calls += 1
        return TOKEN

    record = token_record()
    fresh_store(boundary, forbidden).import_hashed(record)
    assert calls == 0
    _assert_import_request(boundary, record)


def test_create_e_importacao_tem_tokens_de_cliente_distintos(
    dynamodb_case: tuple[Any, Any],
) -> None:
    boundary, _ = dynamodb_case
    record = token_record(tenant_id=OTHER_TENANT, agent_id=OTHER_AGENT)
    store = fresh_store(boundary)
    assert store.create(token_input()) == TOKEN
    assert len(requests_for(boundary, "transact_write_items")) == 1
    store.import_hashed(record)
    writes = requests_for(boundary, "transact_write_items")
    assert len(writes) == 2
    assert writes[0]["ClientRequestToken"] != writes[1]["ClientRequestToken"]
    assert all(isinstance(item["ClientRequestToken"], str) for item in writes)
    assert all(item["ClientRequestToken"] for item in writes)
    _assert_import_request(boundary, record, index=1, expected_count=2)


def test_duas_importacoes_distintas_tem_tokens_e_pares_completos(
    dynamodb_case: tuple[Any, Any],
) -> None:
    boundary, _ = dynamodb_case
    first = token_record()
    second = token_record(tenant_id=OTHER_TENANT, agent_id=OTHER_AGENT)
    calls = 0

    def forbidden() -> str:
        nonlocal calls
        calls += 1
        return TOKEN

    store = fresh_store(boundary, forbidden)
    store.import_hashed(first)
    _assert_import_request(boundary, first)
    store.import_hashed(second)
    writes = requests_for(boundary, "transact_write_items")
    assert len(writes) == 2
    assert writes[0]["ClientRequestToken"] != writes[1]["ClientRequestToken"]
    assert calls == 0
    for index, record in enumerate((first, second)):
        _assert_import_request(boundary, record, index=index, expected_count=2)


@pytest.mark.parametrize(
    "record",
    [
        token_record(token_sha256="G" * 64),
        token_record(token_sha256=1),
        token_record(agent_id="bad"),
        token_record(agent_id=1),
        token_record(tenant_id="bad"),
        token_record(tenant_id=1),
        token_record(machine_fingerprint=""),
        token_record(machine_fingerprint=1),
        token_record(issued_at="bad"),
        token_record(issued_at=datetime.fromisoformat("2026-01-02T03:04:05")),
        token_record(issued_at=datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone(timedelta(hours=1)))),
        token_record(expires_at="bad", last_used_at=None),
        token_record(expires_at=ISSUED, last_used_at=None),
        token_record(expires_at=ISSUED - timedelta(microseconds=1), last_used_at=None),
        token_record(
            expires_at=datetime(2026, 1, 3, 3, 4, 5, tzinfo=timezone(timedelta(hours=1))),
            last_used_at=None,
        ),
        token_record(last_used_at="bad"),
        token_record(last_used_at=ISSUED - timedelta(microseconds=1)),
        token_record(last_used_at=EXPIRES + timedelta(microseconds=1)),
        token_record(last_used_at=datetime.fromisoformat("2026-01-02T04:05:06")),
        token_record(revoked_at="bad"),
        token_record(revoked_at=ISSUED - timedelta(microseconds=1)),
        token_record(revoked_at=datetime.fromisoformat("2026-01-04T05:06:07")),
    ],
)
def test_import_dto_invalido_nao_faz_fabrica_nem_sdk(
    dynamodb_case: tuple[Any, Any],
    record: Any,
) -> None:
    boundary, _ = dynamodb_case
    calls = 0

    def forbidden() -> str:
        nonlocal calls
        calls += 1
        return TOKEN

    with pytest.raises(RefreshTokenInvalid):
        fresh_store(boundary, forbidden).import_hashed(record)
    assert calls == 0
    assert boundary.requests == []


@pytest.mark.parametrize(
    ("issued_at", "expires_at"),
    [
        (datetime.fromisoformat("2026-01-02T03:04:05"), EXPIRES),
        (ISSUED, datetime(2026, 1, 3, 3, 4, 5, tzinfo=timezone(timedelta(hours=1)))),
        (ISSUED, ISSUED),
        (ISSUED, ISSUED - timedelta(microseconds=1)),
    ],
)
def test_create_utc_e_cronologia_invalidas_precedem_fabrica(
    dynamodb_case: tuple[Any, Any],
    issued_at: datetime,
    expires_at: datetime,
) -> None:
    boundary, _ = dynamodb_case
    calls = 0

    def forbidden() -> str:
        nonlocal calls
        calls += 1
        return TOKEN

    with pytest.raises(RefreshTokenInvalid):
        fresh_store(boundary, forbidden).create(
            token_input(issued_at=issued_at, expires_at=expires_at)
        )
    assert calls == 0
    assert boundary.requests == []


def test_import_aceita_last_used_no_expiry_e_revogacao_tardia(
    dynamodb_case: tuple[Any, Any],
) -> None:
    boundary, _ = dynamodb_case
    record = token_record(last_used_at=EXPIRES, revoked_at=EXPIRES + timedelta(days=1))
    fresh_store(boundary).import_hashed(record)
    _assert_import_request(boundary, record)
