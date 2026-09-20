"""Testes de MinioWrapper.presigned_put usando public_endpoint (H9).

docs/edge-agent-audit-2026-09-20.md: central_api roda com MINIO_ENDPOINT
apontando para um alias interno (ex.: "minio:9000" no docker compose), que
um edge agent em outra rede não consegue resolver. As URLs pre-assinadas
devem usar um endpoint separado, publicamente alcançável.
"""
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
