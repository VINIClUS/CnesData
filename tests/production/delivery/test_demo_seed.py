"""Seed usa somente itens canônicos, role dedicada e writes condicionais."""

from copy import deepcopy
from datetime import UTC, datetime, timedelta
from unittest.mock import Mock

import pytest
from botocore.exceptions import ClientError

from scripts.promotion.seed_demo import (
    SeedRefused,
    assume_seed,
    build_items,
    seed,
    validate_contract,
)
from tests.production.infra.conftest import prod_root, tofu_context  # noqa: F401
from tests.production.infra.test_demo_seed_iam import (  # noqa: F401
    _allows,
    _documents,
    _plan,
    demo_events,
)

ACCOUNT = "123456789012"
CONTRACT = {
    "contract_version": "cnesdata.demo-seed-iam/v1",
    "role_arn": f"arn:aws:iam::{ACCOUNT}:role/cnesdata-prod-demo-seed",
    "policy_name": "cnesdata-prod-demo-seed",
    "policy_sha256": "a" * 64,
    "table_arn": f"arn:aws:dynamodb:us-east-2:{ACCOUNT}:table/control",
    "tenant_partition_key": "TENANT#64656d6f",
    "role_session_name": "demo-seed",
    "max_session_duration_seconds": 3600,
    "allowed_actions": ["dynamodb:GetItem", "dynamodb:PutItem"],
}
SPEC = {
    "tenant_id": "demo",
    "user_id": "synthetic-user",
    "oidc_issuer": "https://cognito-idp.us-east-2.amazonaws.com/example",
}
NOW = datetime(2026, 11, 9, tzinfo=UTC)


class Store:
    def __init__(self):
        self.items = {}
        self.writes = []

    def get_item(self, **request):
        assert request["ConsistentRead"] is True
        assert request["TableName"] == CONTRACT["table_arn"]
        return {"Item": deepcopy(self.items.get(request["Key"]["sk"]["S"]))}

    def put_item(self, **request):
        assert (
            request["ConditionExpression"]
            == "attribute_not_exists(pk) AND attribute_not_exists(sk)"
        )
        item = request["Item"]
        key = item["sk"]["S"]
        if key in self.items:
            raise ClientError({"Error": {"Code": "ConditionalCheckFailedException"}}, "PutItem")
        self.writes.append(request)
        self.items[key] = deepcopy(item)


def test_seed_replay_idempotente_sem_scan_ou_semaforo():
    db = Store()
    seed(CONTRACT, SPEC, db)
    seed(CONTRACT, SPEC, db)
    assert len(db.writes) == 4
    assert {item["pk"]["S"] for item in db.items.values()} == {CONTRACT["tenant_partition_key"]}
    assert {item["entity"]["S"] for item in db.items.values()} == {
        "TENANT",
        "MEMBERSHIP",
        "DATASETVERSION",
        "DATASETPOINTER",
    }


@pytest.mark.parametrize("index", range(4))
def test_seed_recusa_colisao_antes_de_qualquer_escrita(index):
    db = Store()
    item = deepcopy(build_items(CONTRACT, SPEC)[index])
    item.pop("demo_seed_owner")
    db.items[item["sk"]["S"]] = item
    with pytest.raises(SeedRefused):
        seed(CONTRACT, SPEC, db)
    assert db.writes == []


def test_recusa_mistura_orfa_sem_tenant_e_payload_alterado():
    db = Store()
    items = build_items(CONTRACT, SPEC)
    db.items[items[1]["sk"]["S"]] = items[1]
    with pytest.raises(SeedRefused):
        seed(CONTRACT, SPEC, db)
    db.items = {x["sk"]["S"]: deepcopy(x) for x in items}
    db.items[items[0]["sk"]["S"]]["payload"] = {"S": "{}"}
    with pytest.raises(SeedRefused):
        seed(CONTRACT, SPEC, db)
    assert not db.writes


@pytest.mark.parametrize(
    "change",
    [
        {"tenant_partition_key": "TENANT#_GLOBAL"},
        {"allowed_actions": ["dynamodb:Scan"]},
        {"role_session_name": "other"},
        {"max_session_duration_seconds": 900},
        {"extra": True},
        {"role_arn": "arn:aws:iam::999999999999:role/demo-seed"},
    ],
)
def test_recusa_contrato_fora_da_fronteira(change):
    with pytest.raises(SeedRefused):
        validate_contract(CONTRACT | change)


def test_sessao_solicita_900_e_valida_expiracao_e_role():
    sts = Mock()
    sts.get_caller_identity.return_value = {"Account": ACCOUNT}
    sts.assume_role.return_value = {
        "Credentials": {
            "AccessKeyId": "temporary",
            "SecretAccessKey": "secret",
            "SessionToken": "token",
            "Expiration": NOW + timedelta(seconds=900),
        },
        "AssumedRoleUser": {
            "Arn": f"arn:aws:sts::{ACCOUNT}:assumed-role/cnesdata-prod-demo-seed/demo-seed"
        },
    }
    assume_seed(CONTRACT, sts, NOW)
    assert sts.assume_role.call_args.kwargs == {
        "RoleArn": CONTRACT["role_arn"],
        "RoleSessionName": "demo-seed",
        "DurationSeconds": 900,
    }
    sts.assume_role.return_value["Credentials"]["Expiration"] = NOW + timedelta(seconds=3600)
    with pytest.raises(SeedRefused):
        assume_seed(CONTRACT, sts, NOW)


def test_nega_tenant_ou_campos_arbitrarios():
    for changes in ({"tenant_id": "real"}, {"items": []}, {"user_id": ""}):
        with pytest.raises(SeedRefused):
            build_items(CONTRACT, SPEC | changes)


def test_itens_e_acoes_consumem_policy_e_contrato_renderizados(request):
    events = request.getfixturevalue("demo_events")
    contract = _plan(events, "ativo")["planned_values"]["outputs"]["demo_seed_iam_contract"][
        "value"
    ]
    validate_contract(contract)
    policy, trust = _documents(events)
    items = build_items(contract, SPEC)
    for item in items:
        for action in contract["allowed_actions"]:
            assert _allows(
                policy,
                {
                    "action": action,
                    "resource": contract["table_arn"],
                    "context": {"dynamodb:LeadingKeys": [item["pk"]["S"]]},
                },
            )
    for action in ("dynamodb:Scan", "dynamodb:Query", "dynamodb:DeleteItem", "dynamodb:UpdateItem"):
        assert not _allows(
            policy,
            {
                "action": action,
                "resource": contract["table_arn"],
                "context": {"dynamodb:LeadingKeys": [contract["tenant_partition_key"]]},
            },
        )
    assert not _allows(
        policy,
        {
            "action": "dynamodb:PutItem",
            "resource": contract["table_arn"],
            "context": {"dynamodb:LeadingKeys": ["TENANT#_GLOBAL"]},
        },
    )
    statement = trust["Statement"][0]
    assert statement["Condition"]["StringEquals"]["sts:RoleSessionName"] == "demo-seed"


def test_corrida_nao_sobrescreve_item_e_interrupcao_reinicia_condicionalmente():
    db = Store()
    original = db.put_item

    def interrupted(**request):
        original(**request)
        raise TimeoutError("private")

    db.put_item = interrupted
    with pytest.raises(TimeoutError):
        seed(CONTRACT, SPEC, db)
    db.put_item = original
    seed(CONTRACT, SPEC, db)
    assert len(db.writes) == 4


def test_sessao_curta_nao_comprova_duracao_de_900_segundos():
    sts = Mock()
    sts.get_caller_identity.return_value = {"Account": ACCOUNT}
    sts.assume_role.return_value = {
        "Credentials": {
            "AccessKeyId": "temporary",
            "SecretAccessKey": "secret",
            "SessionToken": "token",
            "Expiration": NOW + timedelta(seconds=60),
        },
        "AssumedRoleUser": {
            "Arn": f"arn:aws:sts::{ACCOUNT}:assumed-role/cnesdata-prod-demo-seed/demo-seed"
        },
    }
    with pytest.raises(SeedRefused):
        assume_seed(CONTRACT, sts, NOW)
