"""MinIO adapter — implementação de ObjectStoragePort."""

import logging
from datetime import timedelta

from minio import Minio
from minio.error import S3Error

logger = logging.getLogger(__name__)


class MinioObjectStorage:

    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        secure: bool = False,
    ) -> None:
        self._client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )

    def generate_presigned_upload_url(
        self, bucket: str, object_key: str,
        expires_secs: int = 3600,
    ) -> str:
        self._ensure_bucket(bucket)
        return self._client.presigned_put_object(
            bucket, object_key,
            expires=timedelta(seconds=expires_secs),
        )

    def object_exists(
        self, bucket: str, object_key: str,
    ) -> bool:
        try:
            self._client.stat_object(bucket, object_key)
            return True
        except S3Error:
            return False

    def get_presigned_download_url(
        self, bucket: str, object_key: str,
        expires_secs: int = 3600,
    ) -> str:
        return self._client.presigned_get_object(
            bucket, object_key,
            expires=timedelta(seconds=expires_secs),
        )

    def _ensure_bucket(self, bucket: str) -> None:
        if not self._client.bucket_exists(bucket):
            self._client.make_bucket(bucket)
            logger.info("bucket_created name=%s", bucket)
