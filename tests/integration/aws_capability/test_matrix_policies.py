"""Espelho das policies de runtime: ações, condições e valores iguais a task-roles.tf."""

import json
import re
from pathlib import Path

import pytest

from tests.integration.aws_capability import _matrix_policies as policies

TERRAFORM = (
    Path(__file__).resolve().parents[3]
    / "infra" / "opentofu" / "modules" / "runtime-iam" / "task-roles.tf"
)
TABLE = "arn:aws:dynamodb:us-east-2:111111111111:table/cnesdata-cap-a1b2c3d4"
BUCKET = "arn:aws:s3:::cnesdata-cap-a1b2c3d4"


def block(start: str, end: str) -> str:
    text = TERRAFORM.read_text()
    first = text.index(start)
    return text[first : text.index(end, first)]


AUDIT_TF = block("    audit = [", "  api_data_statements")
API_TF = block("  api_data_statements", "  api_sfn_statements")


def test_audit_dynamodb_tem_query_no_gsi6_e_get_update_na_tabela() -> None:
    query, item = policies.audit_dynamodb(TABLE)
    assert query == {
        "Effect": "Allow",
        "Action": "dynamodb:Query",
        "Resource": f"{TABLE}/index/gsi6",
        "Condition": {
            "ForAllValues:StringEquals": {"dynamodb:LeadingKeys": ["OUTBOX#PENDING"]},
            "Null": {"dynamodb:LeadingKeys": "false"},
        },
    }
    assert item == {
        "Effect": "Allow",
        "Action": ["dynamodb:GetItem", "dynamodb:UpdateItem"],
        "Resource": TABLE,
        "Condition": {
            "ForAllValues:StringEquals": {
                "dynamodb:LeadingKeys": ["TENANT#_GLOBAL", "OUTBOX#CURSOR"]
            },
            "Null": {"dynamodb:LeadingKeys": "false"},
        },
    }


def test_api_dynamodb_tem_query_no_gsi1_e_get_item_na_tabela_com_string_like() -> None:
    query, item = policies.api_dynamodb(TABLE)
    assert query == {
        "Effect": "Allow",
        "Action": "dynamodb:Query",
        "Resource": f"{TABLE}/index/gsi1",
        "Condition": {
            "ForAllValues:StringLike": {"dynamodb:LeadingKeys": ["USER#*"]},
            "Null": {"dynamodb:LeadingKeys": "false"},
        },
    }
    assert item == {
        "Effect": "Allow",
        "Action": "dynamodb:GetItem",
        "Resource": TABLE,
        "Condition": {
            "ForAllValues:StringLike": {"dynamodb:LeadingKeys": ["TENANT#*"]},
            "Null": {"dynamodb:LeadingKeys": "false"},
        },
    }


def test_audit_s3_tem_lock_config_no_bucket_e_quatro_acoes_so_em_audit() -> None:
    config, objects = policies.audit_s3(BUCKET)
    assert config == {
        "Effect": "Allow",
        "Action": "s3:GetBucketObjectLockConfiguration",
        "Resource": BUCKET,
    }
    assert objects == {
        "Effect": "Allow",
        "Action": [
            "s3:PutObject", "s3:PutObjectRetention", "s3:GetObject", "s3:GetObjectRetention",
        ],
        "Resource": f"{BUCKET}/audit/*",
    }


def test_api_s3_le_reconciliation_e_serving_e_lista_so_com_esses_prefixos() -> None:
    read, listing = policies.api_s3(BUCKET)
    assert read == {
        "Effect": "Allow",
        "Action": "s3:GetObject",
        "Resource": [f"{BUCKET}/reconciliation/*", f"{BUCKET}/serving/*"],
    }
    assert listing == {
        "Effect": "Allow",
        "Action": "s3:ListBucket",
        "Resource": BUCKET,
        "Condition": {"StringLike": {"s3:prefix": ["reconciliation/*", "serving/*"]}},
    }


@pytest.mark.parametrize(
    ("source", "statements"),
    [
        (AUDIT_TF, policies.audit_dynamodb(TABLE) + policies.audit_s3(BUCKET)),
        (API_TF, policies.api_dynamodb(TABLE) + policies.api_s3(BUCKET)),
    ],
)
def test_toda_acao_chave_e_valor_do_espelho_existe_no_terraform(
    source: str, statements: list
) -> None:
    literals: set[str] = set()
    for statement in statements:
        literals.update(re.findall(r'"([^"]+)"', json.dumps(statement)))
    literals -= {"Effect", "Allow", "Action", "Resource", "Condition", "false"}
    literals = {value for value in literals if not value.startswith("arn:")}
    missing = sorted(value for value in literals if value not in source)
    assert missing == []
    assert "Null" in source
