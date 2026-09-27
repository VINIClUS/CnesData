"""Contrato Object Lock do runtime AWS sobre o sink de auditoria estável."""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from typing import Any
from unittest.mock import Mock

import pytest
from botocore.exceptions import ClientError

from cnes_domain.outbox_dispatcher import dispatch_once
from cnes_domain.ports.control_plane import ControlPlanePort
from cnes_infra.audit.s3_object_lock_sink import S3ObjectLockAuditSink
from packages.cnes_infra.tests.contracts.audit_sink_contract import audit_event, canonical_body

NOW = datetime(2026, 8, 23, 12, 0, tzinfo=UTC)
IDENTITY_KEY = "audit/.event-id/evt-01.json"
DATED_KEY = "audit/tenant-a/2026/08/23/evt-01.json"


def _client() -> Mock:
    client = Mock()
    client.get_object_lock_configuration.return_value = {
        "ObjectLockConfiguration": {"ObjectLockEnabled": "Enabled"}
    }
    client.bodies = []

    def put_object(**kwargs: Any) -> dict[str, str]:
        client.bodies.append(kwargs["Body"].read())
        return {"ChecksumSHA256": kwargs["ChecksumSHA256"]}

    client.put_object.side_effect = put_object
    return client


def _requests(client: Mock) -> list[dict[str, Any]]:
    return [call.kwargs for call in client.put_object.call_args_list]


def test_audit_sink_envia_object_lock_e_chave_append_only() -> None:
    client = _client()
    event = audit_event("evt-01", created_at=NOW)
    sink = S3ObjectLockAuditSink(client, "audit-bucket", retention_days=365)

    sink.append(event)

    requests = _requests(client)
    assert [request["Key"] for request in requests] == [IDENTITY_KEY, DATED_KEY]
    expected_digest = sha256(canonical_body(event)).hexdigest()
    for request in requests:
        assert request["Bucket"] == "audit-bucket"
        assert request["IfNoneMatch"] == "*"
        assert request["ObjectLockMode"] == "COMPLIANCE"
        assert request["ObjectLockRetainUntilDate"] == NOW + timedelta(days=365)
        assert re.fullmatch(r"[0-9a-f]{64}", request["Metadata"]["sha256"])
        assert request["Metadata"]["sha256"] == expected_digest
    assert client.bodies == [canonical_body(event)] * 2


def test_entrega_duplicada_gera_mesma_chave_e_checksum() -> None:
    event = audit_event("evt-01", created_at=NOW)
    deliveries = []
    for _ in range(2):
        client = _client()
        S3ObjectLockAuditSink(client, "audit-bucket", retention_days=365).append(event)
        deliveries.append(
            [(request["Key"], request["Metadata"]["sha256"]) for request in _requests(client)]
        )

    assert deliveries[0] == deliveries[1]


def test_erro_aws_mantem_evento_pendente() -> None:
    client = _client()
    client.put_object.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "denied"}}, "PutObject"
    )
    sink = S3ObjectLockAuditSink(client, "audit-bucket", retention_days=365)
    event = audit_event("evt-01", created_at=NOW)
    control_plane = Mock(spec=ControlPlanePort)
    control_plane.pending_outbox.return_value = [event]

    with pytest.raises(ClientError, match="AccessDenied"):
        sink.append(event)
    result = dispatch_once(control_plane, sink, NOW)

    assert (result.delivered, result.failed) == (0, 1)
    control_plane.mark_outbox_delivered.assert_not_called()
