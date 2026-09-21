"""Testes unitários para S3PresignedStorage."""

from __future__ import annotations

from typing import Any

import boto3
import pytest
from botocore import UNSIGNED
from botocore.config import Config
from botocore.stub import Stubber

from cnes_infra.storage.s3_presigned import S3PresignedStorage, build_s3_client


def _client() -> Any:
    return boto3.client(
        "s3",
        region_name="sa-east-1",
        config=Config(signature_version=UNSIGNED),
    )


def _signed_client() -> Any:
    return boto3.client(
        "s3",
        region_name="sa-east-1",
        aws_access_key_id="key",
        aws_secret_access_key="secret",  # noqa: S106
        config=Config(signature_version="s3v4"),
    )


class TestGeneratePresignedUploadUrl:

    def test_usa_sigv4(self):
        adapter = S3PresignedStorage(_signed_client())
        url = adapter.generate_presigned_upload_url("bucket", "key.parquet.gz")
        assert "X-Amz-Algorithm=AWS4-HMAC-SHA256" in url
        assert "bucket" in url
        assert "key.parquet.gz" in url

    def test_respeita_expires_secs(self):
        adapter = S3PresignedStorage(_signed_client())
        url = adapter.generate_presigned_upload_url(
            "bucket", "key", expires_secs=120,
        )
        assert "X-Amz-Expires=120" in url


class TestGetPresignedDownloadUrl:

    def test_usa_sigv4(self):
        adapter = S3PresignedStorage(_signed_client())
        url = adapter.get_presigned_download_url("bucket", "key.parquet.gz")
        assert "X-Amz-Algorithm=AWS4-HMAC-SHA256" in url
        assert "bucket" in url


class TestObjectExists:

    def test_true_quando_head_object_responde(self):
        client = _client()
        with Stubber(client) as stubber:
            stubber.add_response(
                "head_object", {}, {"Bucket": "bucket", "Key": "key"},
            )
            adapter = S3PresignedStorage(client)
            assert adapter.object_exists("bucket", "key") is True

    def test_false_quando_404(self):
        client = _client()
        with Stubber(client) as stubber:
            stubber.add_client_error(
                "head_object",
                service_error_code="404",
                http_status_code=404,
                expected_params={"Bucket": "bucket", "Key": "key"},
            )
            adapter = S3PresignedStorage(client)
            assert adapter.object_exists("bucket", "key") is False

    def test_false_quando_403_sem_list_bucket(self):
        """Sem s3:ListBucket, head_object de chave ausente devolve 403."""
        client = _client()
        with Stubber(client) as stubber:
            stubber.add_client_error(
                "head_object",
                service_error_code="403",
                http_status_code=403,
                expected_params={"Bucket": "bucket", "Key": "key"},
            )
            adapter = S3PresignedStorage(client)
            assert adapter.object_exists("bucket", "key") is False

    def test_propaga_outros_erros(self):
        client = _client()
        with Stubber(client) as stubber:
            stubber.add_client_error(
                "head_object",
                service_error_code="500",
                http_status_code=500,
                expected_params={"Bucket": "bucket", "Key": "key"},
            )
            adapter = S3PresignedStorage(client)
            with pytest.raises(Exception, match="500"):
                adapter.object_exists("bucket", "key")

class TestBuildS3Client:

    def test_fixa_sigv4_e_addressing_style(self, monkeypatch):
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "key")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret")
        client = build_s3_client(
            "sa-east-1", endpoint_url="http://localhost:4566", addressing_style="path",
        )
        assert client.meta.region_name == "sa-east-1"
        assert client.meta.endpoint_url == "http://localhost:4566"

    def test_endpoint_url_none_usa_resolvedor_padrao(self, monkeypatch):
        monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
        monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
        client = build_s3_client("sa-east-1")
        assert client.meta.region_name == "sa-east-1"

    def test_recusa_endpoint_customizado_sem_credenciais_explicitas(self, monkeypatch):
        """Sem isso, boto3 cai silenciosamente para ~/.aws/credentials (ou
        IMDS) e assina o presign com a credencial AWS real de quem estiver
        rodando — reproduzido manualmente contra AIStor: 403 assinado com
        uma access key alheia em vez de uma falha alta e óbvia."""
        monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
        monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
        with pytest.raises(ValueError, match="aws_credentials=missing"):
            build_s3_client("sa-east-1", endpoint_url="http://localhost:4566")

