"""Object store imutável sobre S3."""

from __future__ import annotations

from base64 import b64encode
from dataclasses import dataclass
from tempfile import TemporaryFile
from typing import TYPE_CHECKING, Any, Literal

from botocore.exceptions import ClientError

from cnes_domain.control_plane.errors import Conflict
from cnes_domain.ports.object_store import ObjectStat
from cnes_infra.object_store._common import require_digest, stream_with_digest, validate_key

if TYPE_CHECKING:
    from contextlib import AbstractContextManager as ContextManager
    from datetime import datetime
    from typing import BinaryIO


def _error_code(error: ClientError) -> str:
    return str(error.response.get("Error", {}).get("Code", ""))


def _is_missing(error: ClientError) -> bool:
    return _error_code(error) in {"404", "NoSuchKey", "NotFound"}


@dataclass(frozen=True, slots=True)
class _StoredObject:
    stat: ObjectStat
    metadata_sha256: str | None


@dataclass(frozen=True, slots=True)
class S3Retention:
    """Configura retenção enviada ao S3."""

    mode: Literal["GOVERNANCE", "COMPLIANCE"]
    retain_until: datetime


class S3ObjectStore:
    def __init__(
        self, client: Any, bucket: str, prefix: str = "", retention: S3Retention | None = None
    ) -> None:
        self._client = client
        self._bucket = bucket
        self._prefix = prefix.strip("/")
        self._retention = retention

    def _key(self, key: str) -> str:
        validate_key(key)
        return f"{self._prefix}/{key}" if self._prefix else key

    def _read_stored(self, key: str) -> _StoredObject | None:
        try:
            response = self._client.get_object(Bucket=self._bucket, Key=self._key(key))
        except ClientError as error:
            if _is_missing(error):
                return None
            raise
        with response["Body"] as body:
            size, digest = stream_with_digest(body)
        stat = ObjectStat(key=key, size_bytes=size, sha256=digest)
        metadata_sha256 = response.get("Metadata", {}).get("sha256")
        return _StoredObject(stat=stat, metadata_sha256=metadata_sha256)

    def _read_stat(self, key: str) -> ObjectStat | None:
        stored = self._read_stored(key)
        return None if stored is None else stored.stat

    def _put_request(self, key: str, staged: BinaryIO, expected_sha256: str) -> dict[str, Any]:
        request = {
            "Body": staged,
            "Bucket": self._bucket,
            "IfNoneMatch": "*",
            "Key": self._key(key),
            "Metadata": {"sha256": expected_sha256},
        }
        if self._retention is not None:
            request.update(
                ChecksumAlgorithm="SHA256",
                ChecksumSHA256=b64encode(bytes.fromhex(expected_sha256)).decode(),
                ObjectLockMode=self._retention.mode,
                ObjectLockRetainUntilDate=self._retention.retain_until,
            )
        return request

    def _validate_response_checksum(self, response: dict[str, Any], expected_sha256: str) -> None:
        if self._retention is None:
            return
        actual = response.get("ChecksumSHA256")
        if actual is None:
            raise ValueError("checksum_response_missing")
        expected = b64encode(bytes.fromhex(expected_sha256)).decode()
        if actual != expected:
            raise ValueError("checksum_response_mismatch")

    def _put_staged(
        self, key: str, staged: BinaryIO, size: int, expected_sha256: str
    ) -> ObjectStat:
        attempt = 0
        while True:
            staged.seek(0)
            try:
                response = self._client.put_object(
                    **self._put_request(key, staged, expected_sha256)
                )
            except ClientError as error:
                code = _error_code(error)
                if code == "BadDigest":
                    raise ValueError("checksum_rejected") from error
                if code not in {"ConditionalRequestConflict", "PreconditionFailed"}:
                    raise
                existing = self._read_stored(key)
                if existing is not None:
                    values = (
                        existing.stat.size_bytes,
                        existing.stat.sha256,
                        existing.metadata_sha256,
                    )
                    if values != (size, expected_sha256, expected_sha256):
                        raise Conflict("immutable_object") from error
                    return existing.stat
                if code == "PreconditionFailed" or attempt == 2:
                    raise Conflict("conditional_request_conflict") from error
                attempt += 1
                continue
            self._validate_response_checksum(response, expected_sha256)
            return ObjectStat(key=key, size_bytes=size, sha256=expected_sha256)

    def put(self, key: str, body: BinaryIO, expected_sha256: str) -> ObjectStat:
        validate_key(key)
        with TemporaryFile("w+b") as staged:
            size, digest = stream_with_digest(body, staged)
            require_digest(digest, expected_sha256)
            staged.seek(0)
            return self._put_staged(key, staged, size, digest)

    def open(self, key: str) -> ContextManager[BinaryIO]:
        response = self._client.get_object(Bucket=self._bucket, Key=self._key(key))
        return response["Body"]

    def stat(self, key: str) -> ObjectStat | None:
        validate_key(key)
        return self._read_stat(key)

    def promote(self, source_key: str, destination_key: str, expected_sha256: str) -> ObjectStat:
        validate_key(source_key)
        validate_key(destination_key)
        with self.open(source_key) as source:
            return self.put(destination_key, source, expected_sha256)

    def delete(self, key: str) -> None:
        self._client.delete_object(Bucket=self._bucket, Key=self._key(key))
