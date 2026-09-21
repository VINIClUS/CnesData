"""Testa o endpoint público usado por URLs pre-assinadas do MinIO."""
from __future__ import annotations

from unittest.mock import patch

from central_api.deps import MinioWrapper


def test_presigned_put_usa_public_endpoint_quando_definido():
    wrapper = MinioWrapper(
        bucket="cnesdata-landing",
        endpoint="minio:9000",
        access_key="minioadmin",
        secret_key="minioadmin",  # noqa: S106 - dev MinIO fixture credential
        secure=False,
        public_endpoint="edge.cnesdata.gov.br:9000",
    )
    with patch("minio.Minio") as fake_minio_cls:
        fake_minio_cls.return_value.presigned_put_object.return_value = "https://x/presigned"
        wrapper.presigned_put("key.parquet.gz")

    fake_minio_cls.assert_called_once()
    called_endpoint = fake_minio_cls.call_args[0][0]
    assert called_endpoint == "edge.cnesdata.gov.br:9000"


def test_presigned_put_usa_endpoint_quando_public_endpoint_ausente():
    wrapper = MinioWrapper(
        bucket="cnesdata-landing",
        endpoint="localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin",  # noqa: S106 - dev MinIO fixture credential
        secure=False,
    )
    with patch("minio.Minio") as fake_minio_cls:
        fake_minio_cls.return_value.presigned_put_object.return_value = "https://x/presigned"
        wrapper.presigned_put("key.parquet.gz")

    called_endpoint = fake_minio_cls.call_args[0][0]
    assert called_endpoint == "localhost:9000"


def test_presigned_put_usa_secure_publico_separado_do_endpoint_interno():
    wrapper = MinioWrapper(
        bucket="cnesdata-landing",
        endpoint="minio:9000",
        access_key="minioadmin",
        secret_key="minioadmin",  # noqa: S106 - dev MinIO fixture credential
        secure=False,
        public_endpoint="storage.example.com",
        public_secure=True,
    )
    with patch("minio.Minio") as fake_minio_cls:
        fake_minio_cls.return_value.presigned_put_object.return_value = "https://x/presigned"
        wrapper.presigned_put("key.parquet.gz")

    assert fake_minio_cls.call_args.kwargs["secure"] is True
