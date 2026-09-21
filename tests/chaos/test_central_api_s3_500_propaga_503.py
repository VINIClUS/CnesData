"""S3 500 durante presign -> exceção propaga limpa, não crash silencioso."""
from __future__ import annotations

import pytest


@pytest.mark.chaos
def test_s3_500_nao_crasha_servidor(inject_s3_failure):
    """Invariant: falha do backend durante presign vira exceção limpa, não crash."""
    import boto3
    from botocore.config import Config

    client = boto3.client(
        "s3", region_name="sa-east-1",
        aws_access_key_id="x", aws_secret_access_key="y",  # noqa: S106
        config=Config(signature_version="s3v4"),
    )
    with inject_s3_failure(client, mode="get_500"):
        with pytest.raises(RuntimeError, match="500"):
            client.generate_presigned_url(
                "put_object", Params={"Bucket": "bucket", "Key": "key"},
            )
