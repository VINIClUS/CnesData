"""Conformidade do sink de auditoria S3 Object Lock."""

from __future__ import annotations

from base64 import b64encode
from datetime import timedelta
from hashlib import sha256
from io import BytesIO
from typing import Any

import boto3
import pytest
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import ClientError
from botocore.stub import ANY, Stubber

from cnes_domain.control_plane.errors import Conflict
from cnes_infra.audit.s3_object_lock_sink import S3ObjectLockAuditSink
from packages.cnes_infra.tests.contracts.audit_sink_contract import (
    AuditSinkCase,
    StoredAuditEvent,
    audit_event,
    audit_sink_cases,
    canonical_body,
)


def _client() -> Any:
    return boto3.client(
        "s3", region_name="us-east-1", config=Config(signature_version=UNSIGNED)
    )


class _MemoryS3:
    def __init__(self) -> None:
        self.objects: dict[str, tuple[bytes, dict[str, str], Any]] = {}
        self.fail = False

    def get_object_lock_configuration(self, **kwargs: Any) -> dict[str, Any]:
        return {"ObjectLockConfiguration": {"ObjectLockEnabled": "Enabled"}}

    def put_object(self, **kwargs: Any) -> dict[str, str]:
        if self.fail:
            self.fail = False
            raise OSError("s3=unavailable")
        key = kwargs["Key"]
        body = kwargs["Body"].read()
        current = self.objects.get(key)
        if current is not None:
            raise _client_error("PreconditionFailed", 412, "PutObject")
        self.objects[key] = (body, kwargs["Metadata"], kwargs["ObjectLockRetainUntilDate"])
        return {"ChecksumSHA256": kwargs["ChecksumSHA256"]}

    def get_object(self, **kwargs: Any) -> dict[str, Any]:
        body, metadata, _ = self.objects[kwargs["Key"]]
        return {"Body": BytesIO(body), "Metadata": metadata, "VersionId": "version-1"}

    def get_object_retention(self, **kwargs: Any) -> dict[str, Any]:
        _, _, retain_until = self.objects[kwargs["Key"]]
        return {"Retention": {"Mode": "COMPLIANCE", "RetainUntilDate": retain_until}}


class _S3Probe:
    def __init__(self) -> None:
        self.client = _MemoryS3()
        self.sink = S3ObjectLockAuditSink(self.client, "audit-bucket", 30)

    def expected_location(self, event: Any) -> str:
        return f"audit/{event.tenant_id}/2026/07/15/{event.event_id}.json"

    def stored(self) -> tuple[StoredAuditEvent, ...]:
        return tuple(
            StoredAuditEvent(key, body, metadata["sha256"])
            for key, (body, metadata, _) in self.client.objects.items()
        )

    def fail_next(self) -> None:
        self.client.fail = True


@pytest.mark.parametrize("case", audit_sink_cases(), ids=lambda case: case.name)
def test_cumpre_contrato_compartilhado(case: AuditSinkCase) -> None:
    case.run(_S3Probe())


def _client_error(code: str, status: int, operation: str) -> ClientError:
    return ClientError(
        {"Error": {"Code": code, "Message": code}, "ResponseMetadata": {"HTTPStatusCode": status}},
        operation,
    )


def _enabled(stubber: Stubber) -> None:
    stubber.add_response(
        "get_object_lock_configuration",
        {"ObjectLockConfiguration": {"ObjectLockEnabled": "Enabled"}},
        {"Bucket": "audit-bucket"},
    )


def _put_params(event: Any) -> dict[str, Any]:
    body = canonical_body(event)
    digest = sha256(body).hexdigest()
    return {
        "Body": ANY,
        "Bucket": "audit-bucket",
        "IfNoneMatch": "*",
        "Key": f"audit/tenant-a/2026/07/15/{event.event_id}.json",
        "Metadata": {"sha256": digest},
        "ChecksumAlgorithm": "SHA256",
        "ChecksumSHA256": b64encode(bytes.fromhex(digest)).decode(),
        "ObjectLockMode": "COMPLIANCE",
        "ObjectLockRetainUntilDate": event.created_at + timedelta(days=30),
    }


def test_envia_requisicao_exata_sem_newline() -> None:
    event = audit_event()
    params = _put_params(event)
    client = _client()
    with Stubber(client) as stubber:
        _enabled(stubber)
        stubber.add_response(
            "put_object", {"ChecksumSHA256": params["ChecksumSHA256"]}, params
        )
        S3ObjectLockAuditSink(client, "audit-bucket", 30).append(event)
        stubber.assert_no_pending_responses()


@pytest.mark.parametrize("configuration", [{}, {"ObjectLockEnabled": "Disabled"}])
def test_rejeita_bucket_sem_object_lock(configuration: dict[str, str]) -> None:
    client = _client()
    with Stubber(client) as stubber:
        stubber.add_response(
            "get_object_lock_configuration",
            {"ObjectLockConfiguration": configuration},
            {"Bucket": "audit-bucket"},
        )
        with pytest.raises(ValueError, match="object_lock=disabled"):
            S3ObjectLockAuditSink(client, "audit-bucket", 30)


def test_propaga_erro_ao_consultar_object_lock() -> None:
    client = _client()
    with Stubber(client) as stubber:
        stubber.add_client_error(
            "get_object_lock_configuration",
            service_error_code="AccessDenied",
            http_status_code=403,
            expected_params={"Bucket": "audit-bucket"},
        )
        with pytest.raises(ClientError, match="AccessDenied"):
            S3ObjectLockAuditSink(client, "audit-bucket", 30)


@pytest.mark.parametrize(
    ("response", "message"),
    [({}, "missing"), ({"ChecksumSHA256": "AAAA"}, "mismatch")],
)
def test_rejeita_checksum_ausente_ou_divergente(
    response: dict[str, str], message: str
) -> None:
    event, client = audit_event(), _client()
    with Stubber(client) as stubber:
        _enabled(stubber)
        stubber.add_response("put_object", response, _put_params(event))
        sink = S3ObjectLockAuditSink(client, "audit-bucket", 30)
        with pytest.raises(ValueError, match=f"checksum_response={message}"):
            sink.append(event)


def test_rejeita_baddigest() -> None:
    event, client = audit_event(), _client()
    with Stubber(client) as stubber:
        _enabled(stubber)
        stubber.add_client_error(
            "put_object",
            service_error_code="BadDigest",
            http_status_code=400,
            expected_params=_put_params(event),
        )
        sink = S3ObjectLockAuditSink(client, "audit-bucket", 30)
        with pytest.raises(ValueError, match="checksum=rejected"):
            sink.append(event)


def _object_response(body: bytes, digest: str) -> dict[str, Any]:
    return {
        "Body": BytesIO(body),
        "Metadata": {"sha256": digest},
        "VersionId": "version-1",
    }


def test_aceita_replay_412_com_conteudo_e_retencao_integrais() -> None:
    event, client = audit_event(), _client()
    body, params = canonical_body(event), _put_params(event)
    digest = sha256(body).hexdigest()
    with Stubber(client) as stubber:
        _enabled(stubber)
        stubber.add_client_error(
            "put_object", service_error_code="PreconditionFailed", http_status_code=412,
            expected_params=params)
        stubber.add_response(
            "get_object", _object_response(body, digest),
            {"Bucket": "audit-bucket", "Key": params["Key"]})
        stubber.add_response(
            "get_object_retention",
            {
                "Retention": {
                    "Mode": "COMPLIANCE",
                    "RetainUntilDate": params["ObjectLockRetainUntilDate"],
                }
            },
            {"Bucket": "audit-bucket", "Key": params["Key"], "VersionId": "version-1"},
        )
        S3ObjectLockAuditSink(client, "audit-bucket", 30).append(event)


def test_rejeita_replay_412_com_conteudo_divergente() -> None:
    event, client = audit_event(), _client()
    params = _put_params(event)
    different = b"divergente"
    with Stubber(client) as stubber:
        _enabled(stubber)
        stubber.add_client_error(
            "put_object", service_error_code="PreconditionFailed", http_status_code=412,
            expected_params=params)
        stubber.add_response(
            "get_object", _object_response(different, sha256(different).hexdigest()),
            {"Bucket": "audit-bucket", "Key": params["Key"]})
        sink = S3ObjectLockAuditSink(client, "audit-bucket", 30)
        with pytest.raises(Conflict, match="object=immutable"):
            sink.append(event)


@pytest.mark.parametrize("attempts", [1, 3])
def test_limita_retry_409_e_releitura(attempts: int) -> None:
    event, client = audit_event(), _client()
    params = _put_params(event)
    with Stubber(client) as stubber:
        _enabled(stubber)
        for _ in range(attempts):
            stubber.add_client_error(
                "put_object", service_error_code="ConditionalRequestConflict",
                http_status_code=409, expected_params=params)
            stubber.add_client_error(
                "get_object", service_error_code="NoSuchKey", http_status_code=404,
                expected_params={"Bucket": "audit-bucket", "Key": params["Key"]})
        if attempts == 1:
            stubber.add_response(
                "put_object", {"ChecksumSHA256": params["ChecksumSHA256"]}, params)
        sink = S3ObjectLockAuditSink(client, "audit-bucket", 30)
        if attempts == 1:
            sink.append(event)
        else:
            with pytest.raises(Conflict, match="conditional_request=conflict"):
                sink.append(event)


def test_rejeita_parametros_e_componentes_inseguros() -> None:
    client = _MemoryS3()
    with pytest.raises(ValueError, match="retention_days=invalid"):
        S3ObjectLockAuditSink(client, "audit-bucket", 0)
    with pytest.raises(ValueError, match="bucket=invalid"):
        S3ObjectLockAuditSink(client, "", 30)
    unsafe = audit_event().model_copy()
    object.__setattr__(unsafe, "event_id", "../event")
    sink = S3ObjectLockAuditSink(client, "audit-bucket", 30)
    with pytest.raises(ValueError, match="audit_path=invalid"):
        sink.append(unsafe)
