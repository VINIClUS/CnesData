"""Sink de auditoria com retenção S3 Object Lock."""

from __future__ import annotations

import json
from datetime import timedelta
from hashlib import sha256
from io import BytesIO
from typing import TYPE_CHECKING

from cnes_infra.object_store.s3 import S3ObjectStore, S3Retention

if TYPE_CHECKING:
    from botocore.client import BaseClient

    from cnes_domain.control_plane.entities import OutboxEvent


def _safe_component(value: str) -> str:
    invalid = value in {"", ".", ".."} or any(char in value for char in "#/\\\0")
    if invalid:
        raise ValueError("audit_path=invalid")
    return value


def _canonical_event(event: OutboxEvent) -> bytes:
    return json.dumps(
        event.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
    ).encode()


class S3ObjectLockAuditSink:
    """Entrega ao menos uma vez por ``event_id`` estável, sem validar WORM."""

    def __init__(self, client: BaseClient, bucket: str, retention_days: int) -> None:
        if not bucket or "/" in bucket or "\\" in bucket:
            raise ValueError("bucket=invalid")
        if retention_days <= 0:
            raise ValueError("retention_days=invalid")
        response = client.get_object_lock_configuration(Bucket=bucket)
        configuration = response.get("ObjectLockConfiguration", {})
        if configuration.get("ObjectLockEnabled") != "Enabled":
            raise ValueError("object_lock=disabled")
        self._client = client
        self._bucket = bucket
        self._retention_days = retention_days

    def append(self, event: OutboxEvent) -> None:
        """Armazena o evento com retenção COMPLIANCE.

        Args:
            event: Evento validado com identidade estável.
        """
        tenant_id = _safe_component(event.tenant_id)
        event_id = _safe_component(event.event_id)
        key = (
            f"audit/{tenant_id}/{event.created_at:%Y}/{event.created_at:%m}/"
            f"{event.created_at:%d}/{event_id}.json"
        )
        body = _canonical_event(event)
        digest = sha256(body).hexdigest()
        retention = S3Retention(
            "COMPLIANCE", event.created_at + timedelta(days=self._retention_days)
        )
        store = S3ObjectStore(self._client, self._bucket, retention=retention)
        identity_key = f"audit/.event-id/{event_id}.json"
        store.put(identity_key, BytesIO(body), digest)
        store.put(key, BytesIO(body), digest)
