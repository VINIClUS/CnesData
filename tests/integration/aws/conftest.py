"""Bootstrap dos emuladores da suíte AWS-014: tabela, buckets e state machines isolados."""
from __future__ import annotations

import json
import os
import socket
import time
import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import patch
from uuid import uuid4

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from fastapi.testclient import TestClient

from central_api import composition as api_composition
from central_api import deps as api_deps
from central_api.composition import build_runtime
from data_processor import composition as processor_composition
from data_processor.composition import build_processor_runtime
from packages.cnes_infra.tests.contracts.clock import MutableClock
from tests.integration.aws._doubles import BearerSubjectVerifier
from tests.integration.aws._harness import (
    ISSUER,
    AwsTestRuntime,
    EmulatorResources,
    new_session,
    runtime_values,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

    from boto3.session import Session

_READY_TIMEOUT_SECONDS = 60
_REGION = "us-east-1"
_INDEXES = tuple(f"gsi{number}" for number in range(1, 7))
_STATE_MACHINES = (
    Path(__file__).resolve().parents[3] / "packages/cnes_infra/tests/fixtures/step_functions"
)
_ROLE_ARN = "arn:aws:iam::000000000000:role/aws014-processor"
# Express rejects .sync integrations at creation, so the Express probe is a bare Pass state;
# the validator checks the workflow type before reading the definition.
_EXPRESS_DEFINITION = {"StartAt": "Done", "States": {"Done": {"Type": "Pass", "End": True}}}


@dataclass(frozen=True, slots=True)
class _Endpoints:
    dynamodb: str
    services: str


def _client(service: str, endpoint: str) -> Any:
    return boto3.client(
        cast("Any", service), endpoint_url=endpoint, region_name=_REGION,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
        config=Config(retries={"max_attempts": 2}),
    )


def _ready(endpoints: _Endpoints) -> bool:
    try:
        _client("dynamodb", endpoints.dynamodb).list_tables(Limit=1)
        health = f"{endpoints.services}/_localstack/health"
        with urllib.request.urlopen(health, timeout=5) as response:
            services = json.loads(response.read())["services"]
    except (BotoCoreError, ClientError, OSError, ValueError, KeyError):
        return False
    return all(services.get(name) in {"available", "running"} for name in ("s3", "stepfunctions"))


@pytest.fixture(scope="session")
def emulator_endpoints() -> _Endpoints:
    endpoints = _Endpoints(
        dynamodb=os.getenv("DYNAMODB_ENDPOINT_URL", "http://127.0.0.1:18000"),
        services=os.getenv("AWS_ENDPOINT_URL", "http://127.0.0.1:4566"),
    )
    deadline = time.monotonic() + _READY_TIMEOUT_SECONDS
    while not _ready(endpoints):
        if time.monotonic() >= deadline:
            pytest.fail(
                f"emulators_unready dynamodb={endpoints.dynamodb} services={endpoints.services}"
            )
        time.sleep(1)
    return endpoints


@pytest.fixture(scope="session")
def state_machines(emulator_endpoints: _Endpoints) -> Iterator[dict[str, str]]:
    client = _client("stepfunctions", emulator_endpoints.services)
    suffix = uuid4().hex[:8]
    definitions = {
        "standard": ((_STATE_MACHINES / "standard_inline_ecs.json").read_text(), "STANDARD"),
        "express": (json.dumps(_EXPRESS_DEFINITION), "EXPRESS"),
        "distributed": ((_STATE_MACHINES / "distributed_map.json").read_text(), "STANDARD"),
    }
    arns = {
        kind: client.create_state_machine(
            name=f"aws014-{kind}-{suffix}", definition=definition, roleArn=_ROLE_ARN,
            type=workflow_type,
        )["stateMachineArn"]
        for kind, (definition, workflow_type) in definitions.items()
    }
    yield arns
    for arn in arns.values():
        client.delete_state_machine(stateMachineArn=arn)


@pytest.fixture(scope="session")
def invalid_state_machines(state_machines: dict[str, str]) -> dict[str, str]:
    return {kind: state_machines[kind] for kind in ("express", "distributed")}


def _create_table(client: Any, table_name: str) -> None:
    names = ("pk", "sk", *(f"{index}{suffix}" for index in _INDEXES for suffix in ("pk", "sk")))
    throughput = {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}
    client.create_table(
        TableName=table_name,
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[{"AttributeName": name, "AttributeType": "S"} for name in names],
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
            for index in _INDEXES
        ],
        ProvisionedThroughput=throughput,
    )
    client.get_waiter("table_exists").wait(TableName=table_name)
    client.update_time_to_live(
        TableName=table_name,
        TimeToLiveSpecification={"Enabled": True, "AttributeName": "expires_at"},
    )


def _delete_bucket(client: Any, bucket: str) -> None:
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket):
        for item in page.get("Contents", ()):
            client.delete_object(Bucket=bucket, Key=item["Key"])
    client.delete_bucket(Bucket=bucket)


@pytest.fixture
def aws_resources(
    emulator_endpoints: _Endpoints, state_machines: dict[str, str],
) -> Iterator[EmulatorResources]:
    suffix = uuid4().hex[:12]
    resources = EmulatorResources(
        table_name=f"aws014-{suffix}", data_bucket=f"aws014-data-{suffix}",
        audit_bucket=f"aws014-audit-{suffix}", state_machine_arn=state_machines["standard"],
        dynamodb_endpoint=emulator_endpoints.dynamodb,
        service_endpoint=emulator_endpoints.services,
    )
    dynamodb = _client("dynamodb", emulator_endpoints.dynamodb)
    s3 = _client("s3", emulator_endpoints.services)
    _create_table(dynamodb, resources.table_name)
    s3.create_bucket(Bucket=resources.data_bucket)
    # The audit bucket stays behind: COMPLIANCE retention forbids deletion; `down -v` drops it.
    s3.create_bucket(Bucket=resources.audit_bucket, ObjectLockEnabledForBucket=True)
    yield resources
    dynamodb.delete_table(TableName=resources.table_name)
    _delete_bucket(s3, resources.data_bucket)


def _build_runtime(
    resources: EmulatorResources, clock: MutableClock, overrides: dict[str, str] | None = None,
) -> AwsTestRuntime:
    session = new_session(resources)
    values = runtime_values(resources, overrides)
    return AwsTestRuntime(
        api=build_runtime("aws", values, cast("Session", session)),
        processor=build_processor_runtime("aws", values, cast("Session", session)),
        clock=clock, resources=resources, s3=session.client("s3"),
        step_functions=session.client("stepfunctions"), dynamodb=session.client("dynamodb"),
    )


@pytest.fixture
def aws_runtime(
    monkeypatch: pytest.MonkeyPatch, aws_resources: EmulatorResources,
) -> AwsTestRuntime:
    clock = MutableClock(datetime.now(UTC).replace(microsecond=0))
    for module in (api_composition, processor_composition, api_deps):
        monkeypatch.setattr(module, "_utc_now", clock.now)
    return _build_runtime(aws_resources, clock)


def _closed_loopback_endpoint() -> str:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    return f"http://127.0.0.1:{port}"


@pytest.fixture
def dynamodb_outage_runtime(
    monkeypatch: pytest.MonkeyPatch, aws_runtime: AwsTestRuntime,
) -> AwsTestRuntime:
    # boto3 backs off for seconds per call against a refused port; a single attempt keeps the
    # fail-closed check fast and leaves the production client configuration untouched.
    monkeypatch.setenv("AWS_MAX_ATTEMPTS", "1")
    endpoint = _closed_loopback_endpoint()
    return _build_runtime(
        aws_runtime.resources, aws_runtime.clock, {"DYNAMODB_ENDPOINT_URL": endpoint},
    )


@pytest.fixture
def serving_client(
    monkeypatch: pytest.MonkeyPatch, aws_runtime: AwsTestRuntime,
) -> Iterator[TestClient]:
    for name, value in runtime_values(aws_runtime.resources).items():
        monkeypatch.setenv(name, value)
    with patch("central_api.app.init_telemetry"):
        from central_api.app import create_app

        app = create_app()
    with (
        patch("central_api.deps.build_runtime", return_value=aws_runtime.api),
        TestClient(app) as client,
    ):
        app.state.oidc_verifier = BearerSubjectVerifier(ISSUER)
        yield client
