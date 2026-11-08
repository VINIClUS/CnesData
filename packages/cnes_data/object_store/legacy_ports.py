"""Port para object storage (MinIO, S3, Supabase Storage)."""

import logging
from typing import Protocol, runtime_checkable

logger = logging.getLogger(__name__)


@runtime_checkable
class ObjectStoragePort(Protocol):

    def generate_presigned_upload_url(  # pragma: no cover - Protocol stub
        self, bucket: str, object_key: str,
        expires_secs: int = 3600,
    ) -> str: ...

    def object_exists(  # pragma: no cover - Protocol stub
        self, bucket: str, object_key: str,
    ) -> bool: ...

    def get_presigned_download_url(  # pragma: no cover - Protocol stub
        self, bucket: str, object_key: str,
        expires_secs: int = 3600,
    ) -> str: ...


class NullObjectStoragePort:

    def generate_presigned_upload_url(
        self, bucket: str, object_key: str,
        expires_secs: int = 3600,
    ) -> str:
        logger.warning("object_storage_not_configured")
        return f"null://{bucket}/{object_key}"

    def object_exists(
        self, bucket: str, object_key: str,
    ) -> bool:
        return False

    def get_presigned_download_url(
        self, bucket: str, object_key: str,
        expires_secs: int = 3600,
    ) -> str:
        logger.warning("object_storage_not_configured")
        return f"null://{bucket}/{object_key}"
