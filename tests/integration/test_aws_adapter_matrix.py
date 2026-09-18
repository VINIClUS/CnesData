"""Matriz de capability smoke dos adapters AWS Phase 2."""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from io import BytesIO
from typing import Any

import boto3
import pytest
from botocore.config import Config

from cnes_domain.control_plane.entities import Job, OutboxEvent
from cnes_domain.control_plane.enums import JobState
from cnes_domain.control_plane.errors import Conflict
from cnes_domain.outbox_dispatcher import DispatchResult, dispatch_once

NOW = datetime(2026, 9, 6, 12, tzinfo=UTC)
DELIVERED_AT = NOW + timedelta(seconds=1)
TABLE_NAME = "cnesdata-control-plane"
OBJECT_BUCKET = "cnesdata-test"
AUDIT_BUCKET = "cnesdata-audit-test"


def _client(service: str, endpoint: str) -> Any:
    return boto3.client(
        service,
        endpoint_url=endpoint,
        region_name="us-east-1",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
        config=Config(retries={"max_attempts": 2}, s3={"addressing_style": "path"}),
    )


def _job() -> Job:
    return Job(
        tenant_id="354130",
        job_id="job-aws",
        agent_id="agent-aws",
        source_type="CNES",
        file_subtype="ST",
        competencia="2026-09",
        requested_snapshot_mode="FULL",
        state=JobState.PENDING,
        attempt=0,
        fencing_token=0,
        lease_owner=None,
        lease_until=None,
        result_manifest_id=None,
        result_manifest_key=None,
        error_code=None,
        created_at=NOW,
    )


def _event() -> OutboxEvent:
    return OutboxEvent(
        tenant_id="354130",
        event_id="event-aws",
        event_type="job.created",
        aggregate_id="job-aws",
        payload={"job_id": "job-aws"},
        created_at=NOW,
        delivered_at=None,
    )


class _InterruptedControlPlane:
    def __init__(self, control_plane: Any) -> None:
        self._control_plane = control_plane

    def pending_outbox(self, limit: int) -> tuple[OutboxEvent, ...]:
        return self._control_plane.pending_outbox(limit)

    def mark_outbox_delivered(self, event_id: str, delivered_at: datetime) -> None:
        raise OSError("dispatch=interrupted")


def _audit_keys(client: Any) -> set[str]:
    response = client.list_objects_v2(Bucket=AUDIT_BUCKET, Prefix="audit/")
    return {item["Key"] for item in response.get("Contents", [])}


def _assert_provisioned_resources(dynamodb: Any, s3: Any) -> None:
    table = dynamodb.describe_table(TableName=TABLE_NAME)["Table"]
    ttl = dynamodb.describe_time_to_live(TableName=TABLE_NAME)["TimeToLiveDescription"]
    lock = s3.get_object_lock_configuration(Bucket=AUDIT_BUCKET)
    assert {index["IndexName"] for index in table["GlobalSecondaryIndexes"]} == {
        f"gsi{number}" for number in range(1, 7)
    }
    assert ttl == {"TimeToLiveStatus": "ENABLED", "AttributeName": "expires_at"}
    assert lock["ObjectLockConfiguration"]["ObjectLockEnabled"] == "Enabled"


def _publish_object(store: Any) -> Any:
    body = b"phase-2-aws"
    digest = sha256(body).hexdigest()
    store.put("staging/job-aws.parquet", BytesIO(body), digest)
    published = store.promote(
        "staging/job-aws.parquet", "raw/354130/job-aws.parquet", digest
    )
    assert store.put("raw/354130/job-aws.parquet", BytesIO(body), digest) == published
    with pytest.raises(Conflict, match="object=immutable"):
        store.put(
            "raw/354130/job-aws.parquet",
            BytesIO(b"divergente"),
            sha256(b"divergente").hexdigest(),
        )
    return published


@pytest.mark.dynamodb_local
@pytest.mark.s3_integration
def test_replay_integrado_preserva_objetos_e_evidencia_de_retencao() -> None:
    from cnes_infra.audit import S3ObjectLockAuditSink
    from cnes_infra.control_plane import DynamoDBControlPlane
    from cnes_infra.object_store import S3ObjectStore

    dynamodb = _client(
        "dynamodb", os.getenv("DYNAMODB_ENDPOINT", "http://127.0.0.1:18000")
    )
    s3 = _client("s3", os.getenv("S3_ENDPOINT", "http://127.0.0.1:4566"))
    _assert_provisioned_resources(dynamodb, s3)
    control_plane = DynamoDBControlPlane(dynamodb, TABLE_NAME, lambda: NOW)
    created = control_plane.create_job(_job(), _event())
    assert control_plane.create_job(_job(), _event()) == created

    published = _publish_object(S3ObjectStore(s3, OBJECT_BUCKET))

    sink = S3ObjectLockAuditSink(s3, AUDIT_BUCKET, retention_days=30)
    first = dispatch_once(_InterruptedControlPlane(control_plane), sink, DELIVERED_AT)
    keys_after_interruption = _audit_keys(s3)

    reopened_control_plane = DynamoDBControlPlane(dynamodb, TABLE_NAME, lambda: DELIVERED_AT)
    reopened_store = S3ObjectStore(s3, OBJECT_BUCKET)
    reopened_sink = S3ObjectLockAuditSink(s3, AUDIT_BUCKET, retention_days=30)
    second = dispatch_once(reopened_control_plane, reopened_sink, DELIVERED_AT)
    event_key = "audit/354130/2026/09/06/event-aws.json"
    stored = s3.get_object(Bucket=AUDIT_BUCKET, Key=event_key)
    retention = s3.get_object_retention(
        Bucket=AUDIT_BUCKET,
        Key=event_key,
        VersionId=stored["VersionId"],
    )["Retention"]

    assert first == DispatchResult(delivered=0, failed=1)
    assert second == DispatchResult(delivered=1, failed=0)
    assert reopened_control_plane.pending_outbox(10) == ()
    assert reopened_store.stat("raw/354130/job-aws.parquet") == published
    assert _audit_keys(s3) == keys_after_interruption
    assert keys_after_interruption == {
        "audit/.event-id/event-aws.json",
        event_key,
    }
    assert stored["Body"].read()
    assert retention["Mode"] == "COMPLIANCE"
    assert retention["RetainUntilDate"] >= NOW + timedelta(days=30)
