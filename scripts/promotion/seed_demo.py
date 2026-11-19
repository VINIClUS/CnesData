"""Seed demo canônico com role dedicada e replay estritamente condicional."""

import argparse
import json
import logging
import re
import sys
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

import boto3
from botocore.exceptions import ClientError

from cnes_domain.control_plane.entities import DatasetPointer, DatasetVersion, Membership, Tenant
from cnes_infra.control_plane.dynamodb_codec import encode_model
from cnes_infra.control_plane.dynamodb_keys import (
    entity_key,
    membership_key,
    pointer_key,
    tenant_partition,
    version_key,
)
from cnes_infra.control_plane.dynamodb_tenants import membership_item
from scripts.promotion.reconcile_cloudfront_free import NO_RETRY

FIELDS = frozenset(
    {
        "contract_version",
        "role_arn",
        "policy_name",
        "policy_sha256",
        "table_arn",
        "tenant_partition_key",
        "role_session_name",
        "max_session_duration_seconds",
        "allowed_actions",
    }
)
STAMP = datetime(2026, 1, 1, tzinfo=UTC)
VERSION = "demo-seed-v1"


class SeedRefused(ValueError):
    """Recusa sem serializar registros ou identidades."""


def _require(condition: bool) -> None:
    if not condition:
        raise SeedRefused("reason=seed_contract_or_collision")


def validate_contract(contract: dict[str, Any]) -> None:
    """Args: contrato de nove campos do produtor #481.
    Raises: SeedRefused para deriva de role, conta, tabela, PK ou permissões.
    """
    _require(set(contract) == FIELDS)
    _require(contract["contract_version"] == "cnesdata.demo-seed-iam/v1")
    role = re.fullmatch(
        r"arn:aws:iam::([0-9]{12}):role/([A-Za-z0-9+=,.@_/-]+-demo-seed)", contract["role_arn"]
    )
    _require(role is not None)
    assert role is not None
    _require(len(role[2].split("/")[-1]) <= 64)
    _require(
        re.fullmatch(
            rf"arn:aws:dynamodb:[a-z0-9-]+:{role[1]}:table/[A-Za-z0-9_.-]+", contract["table_arn"]
        )
        is not None
    )
    pk = contract["tenant_partition_key"]
    _require(re.fullmatch(r"TENANT#(?:[0-9a-f]{2})+", pk) is not None)
    try:
        _require(tenant_partition(bytes.fromhex(pk[7:]).decode()) == pk)
    except (ValueError, UnicodeError):
        raise SeedRefused("reason=seed_partition") from None
    _require(contract["allowed_actions"] == ["dynamodb:GetItem", "dynamodb:PutItem"])
    _require(contract["role_session_name"] == "demo-seed")
    _require(
        type(contract["max_session_duration_seconds"]) is int
        and contract["max_session_duration_seconds"] == 3600
    )
    _require(re.fullmatch(r"[a-f0-9]{64}", contract["policy_sha256"]) is not None)
    _require(re.fullmatch(r"[A-Za-z0-9+=,.@_-]{1,128}", contract["policy_name"]) is not None)


def build_items(contract: dict[str, Any], spec: dict[str, Any]) -> list[dict[str, Any]]:
    """Args: contrato e identidades privadas do fixture sintético fixo.
    Returns: quatro itens canônicos com marca determinística de propriedade.
    """
    validate_contract(contract)
    _require(set(spec) == {"tenant_id", "user_id", "oidc_issuer"})
    _require(all(isinstance(x, str) and bool(x) for x in spec.values()))
    tenant, user = spec["tenant_id"], spec["user_id"]
    _require(tenant_partition(tenant) == contract["tenant_partition_key"])
    _require(
        re.fullmatch(
            r"https://cognito-idp\.[a-z0-9-]+\.amazonaws\.com/[A-Za-z0-9_-]+", spec["oidc_issuer"]
        )
        is not None
    )
    models = (
        Tenant(tenant_id=tenant, municipality_name="Demonstração sintética", created_at=STAMP),
        Membership(
            tenant_id=tenant,
            user_id=user,
            role="viewer",
            created_at=STAMP,
            oidc_issuer=spec["oidc_issuer"],
        ),
        DatasetVersion(
            tenant_id=tenant,
            dataset_name="cnes",
            version_id=VERSION,
            run_id=VERSION,
            run_manifest_key=f"reconciliation/{tenant}/2026-01/{VERSION}/run-manifest.json",
            created_at=STAMP,
        ),
        DatasetPointer(
            tenant_id=tenant,
            dataset_name="cnes",
            pointer_name="active",
            version_id=VERSION,
            updated_at=STAMP,
        ),
    )
    items = [
        encode_model(models[0], "TENANT", entity_key(tenant, "TENANT", tenant)),
        membership_item(models[1]),
        encode_model(models[2], "DATASETVERSION", version_key(tenant, "cnes", VERSION)),
        encode_model(models[3], "DATASETPOINTER", pointer_key(tenant, "cnes", "active")),
    ]
    owner = sha256(json.dumps(spec, sort_keys=True).encode()).hexdigest()
    for item in items:
        item["demo_seed_owner"] = {"S": f"cnesdata.demo-seed/v1:{owner}"}
    _require(items[1]["sk"] == {"S": membership_key(tenant, user)[1]})
    return items


def _read(db: Any, table: str, item: dict[str, Any]) -> Any:
    return db.get_item(
        TableName=table, Key={k: item[k] for k in ("pk", "sk")}, ConsistentRead=True
    ).get("Item")


def _put(db: Any, table: str, item: dict[str, Any]) -> None:
    try:
        db.put_item(
            TableName=table,
            Item=item,
            ConditionExpression="attribute_not_exists(pk) AND attribute_not_exists(sk)",
        )
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") != "ConditionalCheckFailedException":
            raise SeedRefused("reason=seed_write_uncertain") from None
        _require(_read(db, table, item) == item)


def seed(contract: dict[str, Any], spec: dict[str, Any], db: Any) -> None:
    """Args: contrato revisado, fixture e cliente da sessão dedicada.
    Raises: SeedRefused se qualquer chave conhecida tiver conteúdo incompatível.
    """
    items = build_items(contract, spec)
    table = contract["table_arn"]
    prior = [_read(db, table, item) for item in items]
    _require(all(old is None or old == item for old, item in zip(prior, items, strict=True)))
    _require(prior[0] is not None or all(old is None for old in prior))
    for old, item in zip(prior, items, strict=True):
        _require(
            _read(db, table, items[0]) in (None, items[0])
            if item == items[0]
            else _read(db, table, items[0]) == items[0]
        )
        if old is None:
            _put(db, table, item)
    _require(all(_read(db, table, item) == item for item in items))


def assume_seed(contract: dict[str, Any], sts: Any, now: datetime) -> dict[str, Any]:
    """Args: contrato, STS do operador e instante UTC anterior à chamada.
    Returns: credenciais efêmeras verificadas; nunca registrar o retorno.
    """
    validate_contract(contract)
    account = contract["role_arn"].split(":")[4]
    _require(sts.get_caller_identity()["Account"] == account)
    reply = sts.assume_role(
        RoleArn=contract["role_arn"], RoleSessionName="demo-seed", DurationSeconds=900
    )
    credentials = reply["Credentials"]
    seconds = (credentials["Expiration"] - now).total_seconds()
    _require(895 <= seconds <= 905)
    role = contract["role_arn"].split("/")[-1]
    expected = f"arn:aws:sts::{account}:assumed-role/{role}/demo-seed"
    _require(reply["AssumedRoleUser"]["Arn"] == expected)
    _require(all(credentials.get(k) for k in ("AccessKeyId", "SecretAccessKey", "SessionToken")))
    return credentials


def main() -> int:
    """Returns: status sem registros, tokens, IDs ou traceback público."""
    logging.disable(logging.CRITICAL)
    parser = argparse.ArgumentParser()
    parser.add_argument("--contract", type=Path, required=True)
    parser.add_argument("--spec", type=Path, required=True)
    parser.add_argument("--release-hash", required=True)
    args = parser.parse_args()
    if re.fullmatch(r"[a-f0-9]{64}", args.release_hash) is None:
        return 1
    passed = False
    try:
        contract, spec = json.loads(args.contract.read_text()), json.loads(args.spec.read_text())
        build_items(contract, spec)
        region = contract["table_arn"].split(":")[3]
        sts = boto3.client("sts", region_name=region, config=NO_RETRY)
        credentials = assume_seed(contract, sts, datetime.now(UTC))
        session = boto3.Session(
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"],
            region_name=region,
        )
        identity = session.client("sts", config=NO_RETRY).get_caller_identity()
        expected = contract["role_arn"].split("/")[-1] + "/demo-seed"
        _require(identity["Arn"] == f"arn:aws:sts::{identity['Account']}:assumed-role/{expected}")
        _require(identity["Account"] == contract["role_arn"].split(":")[4])
        seed(contract, spec, session.client("dynamodb", config=NO_RETRY))
        credentials.clear()
        passed = True
    except Exception:
        passed = False
    sys.stdout.write(
        json.dumps(
            {
                "assertion": "demo_seed:" + ("pass" if passed else "fail"),
                "timestamp": datetime.now(UTC).isoformat(),
                "release_hash": args.release_hash,
            }
        )
        + "\n"
    )
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
