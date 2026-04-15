"""Testes unitários para MinioObjectStorage."""

from unittest.mock import MagicMock, patch

from cnes_infra.storage.object_storage import MinioObjectStorage


class TestMinioObjectStorage:

    def _adapter(self) -> MinioObjectStorage:
        with patch(
            "cnes_infra.storage.object_storage.Minio",
        ) as mock_cls:
            adapter = MinioObjectStorage(
                "localhost:9000", "key", "secret",
            )
            adapter._client = mock_cls.return_value
            return adapter

    def test_generate_presigned_upload_url(self):
        adapter = self._adapter()
        adapter._client.bucket_exists.return_value = True
        adapter._client.presigned_put_object.return_value = (
            "http://minio:9000/bucket/key?sig=abc"
        )
        url = adapter.generate_presigned_upload_url(
            "bucket", "key.parquet.gz",
        )
        assert "minio" in url
        adapter._client.presigned_put_object.assert_called_once()

    def test_object_exists_true(self):
        adapter = self._adapter()
        adapter._client.stat_object.return_value = MagicMock()
        assert adapter.object_exists("bucket", "key") is True

    def test_object_exists_false(self):
        from minio.error import S3Error

        adapter = self._adapter()
        adapter._client.stat_object.side_effect = S3Error(
            "NoSuchKey", "not found", "", "", "", "",
        )
        assert adapter.object_exists("bucket", "key") is False

    def test_ensure_bucket_creates_if_missing(self):
        adapter = self._adapter()
        adapter._client.bucket_exists.return_value = False
        adapter._client.presigned_put_object.return_value = "url"
        adapter.generate_presigned_upload_url("new-bucket", "k")
        adapter._client.make_bucket.assert_called_once_with(
            "new-bucket",
        )
