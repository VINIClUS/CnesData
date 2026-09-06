#!/usr/bin/env bash
set -euo pipefail

phase2_project="${PHASE2_COMPOSE_PROJECT:-cnesdata-phase2}"
compose=(docker compose --project-name "$phase2_project" --profile aws-test)

cleanup() {
  phase2_status=$?
  trap - EXIT INT TERM
  set +e
  "${compose[@]}" down -v --remove-orphans
  phase2_cleanup_status=$?
  if (( phase2_status != 0 )); then
    exit "$phase2_status"
  fi
  exit "$phase2_cleanup_status"
}

trap cleanup EXIT INT TERM

"${compose[@]}" config --images
"${compose[@]}" up -d --wait --wait-timeout 120 dynamodb-local localstack

export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1
export DYNAMODB_ENDPOINT=http://127.0.0.1:18000
export S3_ENDPOINT=http://127.0.0.1:4566

uv run python - <<'PY'
import os

import boto3
from botocore.config import Config

TABLE_NAME = "cnesdata-control-plane"
INDEXES = tuple(f"gsi{number}" for number in range(1, 7))


def dynamodb_client():
    return boto3.client(
        "dynamodb",
        endpoint_url=os.environ["DYNAMODB_ENDPOINT"],
        region_name="us-east-1",
    )


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )


def create_control_plane_table(client):
    names = ("pk", "sk", *(f"{index}{suffix}" for index in INDEXES for suffix in ("pk", "sk")))
    throughput = {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}
    client.create_table(
        TableName=TABLE_NAME,
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": name, "AttributeType": "S"} for name in names
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": index,
                "KeySchema": [
                    {"AttributeName": f"{index}pk", "KeyType": "HASH"},
                    {"AttributeName": f"{index}sk", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
                "ProvisionedThroughput": throughput,
            }
            for index in INDEXES
        ],
        ProvisionedThroughput=throughput,
    )
    client.get_waiter("table_exists").wait(TableName=TABLE_NAME)
    client.update_time_to_live(
        TableName=TABLE_NAME,
        TimeToLiveSpecification={"Enabled": True, "AttributeName": "expires_at"},
    )


def create_buckets(client):
    client.create_bucket(Bucket="cnesdata-test")
    client.create_bucket(
        Bucket="cnesdata-audit-test",
        ObjectLockEnabledForBucket=True,
    )


create_control_plane_table(dynamodb_client())
create_buckets(s3_client())
PY

uv run pytest -q tests/integration/test_local_adapter_matrix.py -m local_profile
uv run pytest -q tests/integration/test_aws_adapter_matrix.py \
  -m "dynamodb_local and s3_integration"
