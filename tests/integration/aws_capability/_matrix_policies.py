"""Statements das policies de runtime espelhadas de task-roles.tf; só os ARNs mudam."""

from typing import Any

AUDIT_DDB = "cap-audit-ddb"
API_DDB = "cap-api-ddb"
AUDIT_S3 = "cap-audit-s3"
API_S3 = "cap-api-s3"
UNIT_S3 = "cap-unit-s3"
RECOVERY_S3 = "cap-recovery-s3"
DATA_PREFIXES = ("raw/*", "tmp/*", "normalized/*", "reconciliation/*", "serving/*")
PUBLISH_PREFIXES = ("tmp/*", "normalized/*", "reconciliation/*", "serving/*")
LEADING_KEYS = "dynamodb:LeadingKeys"
PRESENT = {"Null": {LEADING_KEYS: "false"}}


def table_arn(region: str, account: str, name: str) -> str:
    return f"arn:aws:dynamodb:{region}:{account}:table/{name}"


def index_arn(table: str, index: str) -> str:
    return f"{table}/index/{index}"


def audit_dynamodb(table: str) -> list[dict[str, Any]]:
    return [
        {
            "Effect": "Allow",
            "Action": "dynamodb:Query",
            "Resource": index_arn(table, "gsi6"),
            "Condition": {
                "ForAllValues:StringEquals": {LEADING_KEYS: ["OUTBOX#PENDING"]},
                **PRESENT,
            },
        },
        {
            "Effect": "Allow",
            "Action": ["dynamodb:GetItem", "dynamodb:UpdateItem"],
            "Resource": table,
            "Condition": {
                "ForAllValues:StringEquals": {LEADING_KEYS: ["TENANT#_GLOBAL", "OUTBOX#CURSOR"]},
                **PRESENT,
            },
        },
    ]


def api_dynamodb(table: str) -> list[dict[str, Any]]:
    return [
        {
            "Effect": "Allow",
            "Action": "dynamodb:Query",
            "Resource": index_arn(table, "gsi1"),
            "Condition": {"ForAllValues:StringLike": {LEADING_KEYS: ["USER#*"]}, **PRESENT},
        },
        {
            "Effect": "Allow",
            "Action": "dynamodb:GetItem",
            "Resource": table,
            "Condition": {"ForAllValues:StringLike": {LEADING_KEYS: ["TENANT#*"]}, **PRESENT},
        },
    ]


def audit_s3(bucket: str) -> list[dict[str, Any]]:
    return [
        {"Effect": "Allow", "Action": "s3:GetBucketObjectLockConfiguration", "Resource": bucket},
        {
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:PutObjectRetention",
                "s3:GetObject",
                "s3:GetObjectRetention",
            ],
            "Resource": f"{bucket}/audit/*",
        },
    ]


def api_s3(bucket: str) -> list[dict[str, Any]]:
    prefixes = ["reconciliation/*", "serving/*"]
    return [
        {
            "Effect": "Allow",
            "Action": "s3:GetObject",
            "Resource": [f"{bucket}/reconciliation/*", f"{bucket}/serving/*"],
        },
        {
            "Effect": "Allow",
            "Action": "s3:ListBucket",
            "Resource": bucket,
            "Condition": {"StringLike": {"s3:prefix": prefixes}},
        },
    ]


def _objects(bucket: str, prefixes: tuple[str, ...]) -> list[str]:
    return [f"{bucket}/{prefix}" for prefix in prefixes]


def _listing(bucket: str, prefixes: tuple[str, ...]) -> dict[str, Any]:
    return {
        "Effect": "Allow",
        "Action": "s3:ListBucket",
        "Resource": bucket,
        "Condition": {"StringLike": {"s3:prefix": list(prefixes)}},
    }


def unit_s3(bucket: str) -> list[dict[str, Any]]:
    return [
        {"Effect": "Allow", "Action": "s3:GetObject", "Resource": _objects(bucket, DATA_PREFIXES)},
        {
            "Effect": "Allow",
            "Action": "s3:PutObject",
            "Resource": _objects(bucket, PUBLISH_PREFIXES),
        },
        _listing(bucket, DATA_PREFIXES),
    ]


def recovery_s3(bucket: str) -> list[dict[str, Any]]:
    return [
        {
            "Effect": "Allow",
            "Action": ["s3:GetObject", "s3:PutObject"],
            "Resource": _objects(bucket, PUBLISH_PREFIXES),
        },
        _listing(bucket, PUBLISH_PREFIXES),
    ]
