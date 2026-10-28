"""Snapshots atômicos e transições pareadas de validação DynamoDB."""

from typing import Any

import pytest

from cnes_domain.refresh_tokens import RefreshTokenContext, RefreshTokenUnavailable
from packages.cnes_infra.tests.auth.dynamodb_refresh_support import (
    DIGEST,
    NOW,
    TENANT,
    TOKEN,
    DynamoBoundary,
    active_item,
    fresh_store,
    item_key,
    literal_item,
    read_item,
    requests_for,
    seed_item,
)

pytest_plugins = ("packages.cnes_infra.tests.auth.dynamodb_refresh_support",)


def _discovery_item() -> dict[str, dict[str, str]]:
    return active_item("DIGEST")


def _agent_item() -> dict[str, dict[str, str]]:
    return active_item("AGENT")


def _response(*slots: object) -> dict[str, Any]:
    responses: list[object] = []
    for slot in slots:
        if slot is None or slot == {}:
            responses.append(slot)
        else:
            responses.append({"Item": slot})
    return {
        "Responses": responses,
        "ResponseMetadata": {"HTTPStatusCode": 200, "RequestId": "synthetic"},
    }


def _context() -> RefreshTokenContext:
    return RefreshTokenContext(TENANT, NOW)


def _revoke_pair(boundary: DynamoBoundary) -> None:
    boundary.transact_write_items(
        TransactItems=[
            {
                "Update": {
                    "TableName": "refresh-tokens",
                    "Key": item_key(TENANT, DIGEST, "DIGEST"),
                    "UpdateExpression": "SET revoked_at = :revoked, version = :version",
                    "ExpressionAttributeValues": {
                        ":revoked": {"S": "2026-01-02T05:00:00.000000+00:00"},
                        ":version": {"N": "1"},
                    },
                },
            },
            {
                "Update": {
                    "TableName": "refresh-tokens",
                    "Key": item_key(TENANT, DIGEST, "AGENT"),
                    "UpdateExpression": "SET revoked_at = :revoked, version = :version",
                    "ExpressionAttributeValues": {
                        ":revoked": {"S": "2026-01-02T05:00:00.000000+00:00"},
                        ":version": {"N": "1"},
                    },
                },
            },
        ],
    )


@pytest.mark.parametrize("kind", ["null_digest", "empty_agent", "both_missing"])
def test_discovery_presente_mas_slot_nulo_ou_vazio_falha_fechado(
    dynamodb_case: tuple[Any, Any],
    kind: str,
) -> None:
    boundary, _ = dynamodb_case
    seed_item(boundary, _discovery_item())
    if kind == "null_digest":
        slots = (None, _agent_item())
    elif kind == "empty_agent":
        slots = (_discovery_item(), {})
    else:
        slots = (None, None)
    override = _response(*slots)
    boundary.response_overrides["transact_get_items"] = override
    with pytest.raises(RefreshTokenUnavailable):
        fresh_store(boundary).validate(TOKEN, _context())
    request = requests_for(boundary, "transact_get_items")[0]
    assert "TableName" not in request
    assert [entry["Get"]["Key"] for entry in request["TransactItems"]] == [
        item_key(TENANT, DIGEST, "DIGEST"),
        item_key(TENANT, DIGEST, "AGENT"),
    ]
    assert all(entry["Get"]["TableName"] == "refresh-tokens" for entry in request["TransactItems"])


def test_digest_ausente_na_descoberta_retorna_none_sem_snapshot(
    dynamodb_case: tuple[Any, Any],
) -> None:
    boundary, _ = dynamodb_case
    assert fresh_store(boundary).validate(TOKEN, _context()) is None
    assert requests_for(boundary, "transact_get_items") == []


@pytest.mark.parametrize(
    "response",
    [
        None,
        {},
        {"ResponseMetadata": {"HTTPStatusCode": 200}},
        {"Responses": []},
        {"Responses": [None, None], "ResponseMetadata": {"HTTPStatusCode": 200}},
        {"Responses": [None], "ResponseMetadata": {"HTTPStatusCode": 200}},
        {"Responses": "bad", "ResponseMetadata": {"HTTPStatusCode": 200}},
        {"Responses": [{}, {}, {}], "ResponseMetadata": {"HTTPStatusCode": 200}},
    ],
)
def test_envelope_snapshot_incompleto_nao_vira_ausencia_ou_sucesso(
    dynamodb_case: tuple[Any, Any],
    response: Any,
) -> None:
    boundary, _ = dynamodb_case
    seed_item(boundary, _discovery_item())
    boundary.response_overrides["transact_get_items"] = response
    with pytest.raises(RefreshTokenUnavailable):
        fresh_store(boundary).validate(TOKEN, _context())


def test_transicao_atomica_ativa_para_revogada_decide_pelo_snapshot(
    dynamodb_case: tuple[Any, Any],
) -> None:
    boundary, _ = dynamodb_case
    seed_item(boundary, _discovery_item())
    seed_item(boundary, _agent_item())
    original = boundary.transact_get_items

    def interleave(**kwargs: Any) -> Any:
        _revoke_pair(boundary)
        return original(**kwargs)

    boundary.transact_get_items = interleave  # type: ignore[method-assign]
    assert fresh_store(boundary).validate(TOKEN, _context()) is None
    digest = read_item(boundary, item_key(TENANT, DIGEST, "DIGEST"))
    agent = read_item(boundary, item_key(TENANT, DIGEST, "AGENT"))
    assert digest is not None
    assert digest["revoked_at"] == {"S": "2026-01-02T05:00:00.000000+00:00"}
    assert digest["version"] == {"N": "1"}
    assert agent is not None
    assert agent["revoked_at"] == {"S": "2026-01-02T05:00:00.000000+00:00"}
    assert agent["version"] == {"N": "1"}
    writes = requests_for(boundary, "transact_write_items")
    assert len(writes) == 1
    assert "TableName" not in writes[0]
    assert [action["Update"]["Key"] for action in writes[0]["TransactItems"]] == [
        item_key(TENANT, DIGEST, "DIGEST"),
        item_key(TENANT, DIGEST, "AGENT"),
    ]


def test_snapshot_chave_de_agente_contraditoria_falha_sem_autorizar(
    dynamodb_case: tuple[Any, Any],
) -> None:
    boundary, _ = dynamodb_case
    seed_item(boundary, _discovery_item())
    seed_item(boundary, _agent_item())
    original = boundary.transact_get_items

    def interleave(**kwargs: Any) -> Any:
        seed_item(
            boundary, literal_item("DIGEST", agent_id={"S": "44444444-4444-4444-8444-444444444444"})
        )
        return original(**kwargs)

    boundary.transact_get_items = interleave  # type: ignore[method-assign]
    with pytest.raises(RefreshTokenUnavailable):
        fresh_store(boundary).validate(TOKEN, _context())
