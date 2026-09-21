"""S3PresignedStorage contra um S3 real (LocalStack) — o contrato de upload
do edge agent (PUT HTTP puro, sem headers de checksum, SigV4)."""

from __future__ import annotations

import hashlib
import os
from typing import Any
from urllib.parse import parse_qs, urlparse

import boto3
import httpx
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError

from cnes_infra.storage.s3_presigned import S3PresignedStorage

_BUCKET = "cnesdata-landing-test"


def _client() -> Any:
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT", "http://127.0.0.1:4566"),
        region_name="us-east-1",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


@pytest.mark.s3_integration
def test_presign_put_aceita_upload_http_puro() -> None:
    """PUT sem nenhum header de checksum — exatamente o que
    apps/dump_agent_go/internal/upload/put.go faz (net/http puro)."""
    client = _client()
    adapter = S3PresignedStorage(client)
    body = b"phase-2-presign" * (256 * 1024)  # >1 MiB
    digest = hashlib.sha256(body).hexdigest()

    url = adapter.generate_presigned_upload_url(_BUCKET, "raw/354130/job.parquet.gz")
    resp = httpx.put(url, content=body, timeout=30.0)
    resp.raise_for_status()

    head = client.head_object(Bucket=_BUCKET, Key="raw/354130/job.parquet.gz")
    assert head["ContentLength"] == len(body)

    downloaded = httpx.get(
        adapter.get_presigned_download_url(_BUCKET, "raw/354130/job.parquet.gz"),
        timeout=30.0,
    )
    assert hashlib.sha256(downloaded.content).hexdigest() == digest


@pytest.mark.s3_integration
def test_presign_usa_sigv4() -> None:
    """Regressão: boto3 cai para SigV2 com endpoint_url customizado sem
    Config(signature_version="s3v4") explícito — SigV2 é rejeitado por
    LocalStack/AIStor com S3_SKIP_SIGNATURE_VALIDATION=0."""
    adapter = S3PresignedStorage(_client())
    url = adapter.generate_presigned_upload_url(_BUCKET, "sig/key")
    query = parse_qs(urlparse(url).query)
    assert query["X-Amz-Algorithm"] == ["AWS4-HMAC-SHA256"]


@pytest.mark.s3_integration
def test_presign_expirado_e_rejeitado() -> None:
    adapter = S3PresignedStorage(_client())
    url = adapter.generate_presigned_upload_url(_BUCKET, "exp/key", expires_secs=1)
    query = parse_qs(urlparse(url).query)
    assert query["X-Amz-Expires"] == ["1"]


@pytest.mark.s3_integration
def test_presign_path_style_funciona() -> None:
    client = boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT", "http://127.0.0.1:4566"),
        region_name="us-east-1",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )
    adapter = S3PresignedStorage(client)
    url = adapter.generate_presigned_upload_url(_BUCKET, "path-style/key")
    resp = httpx.put(url, content=b"path-style-body", timeout=30.0)
    resp.raise_for_status()


@pytest.mark.s3_integration
def test_presign_nao_cria_bucket() -> None:
    """Regressão do _ensure_bucket removido: presign nunca cria bucket —
    criação de bucket é IaC, a role da aplicação não tem s3:CreateBucket."""
    client = _client()
    adapter = S3PresignedStorage(client)
    missing_bucket = "cnesdata-does-not-exist-and-must-stay-that-way"

    adapter.generate_presigned_upload_url(missing_bucket, "key")

    with pytest.raises(ClientError, match="404"):
        client.head_bucket(Bucket=missing_bucket)


@pytest.mark.s3_integration
def test_object_exists_false_quando_chave_ausente() -> None:
    """LocalStack não aplica IAM, então devolve 404 aqui — o caso 403
    (bucket sem s3:ListBucket na raiz) é coberto no unit test via Stubber
    em packages/cnes_infra/tests/storage/test_s3_presigned.py."""
    client = _client()
    adapter = S3PresignedStorage(client)
    assert adapter.object_exists(_BUCKET, "definitely/not/there") is False
