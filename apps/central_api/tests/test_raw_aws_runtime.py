from typing import Any, cast

import pytest

from central_api import raw_aws_runtime
from central_api.raw_aws_runtime import RawAWSConfig


def test_config_raw_aws_exige_tabela_bucket_regiao_e_credenciais() -> None:
    with pytest.raises(ValueError, match="raw_aws_config_missing"):
        RawAWSConfig.from_env({"RAW_BACKEND": "aws"})

    config = RawAWSConfig.from_env({
        "RAW_BACKEND": "aws", "RAW_DYNAMODB_TABLE": "raw-dev",
        "RAW_S3_BUCKET": "raw-dev", "RAW_AWS_REGION": "sa-east-1",
        "RAW_AWS_ACCESS_KEY_ID": "id", "RAW_AWS_SECRET_ACCESS_KEY": "secret",
    })
    assert config.table == "raw-dev"
    assert config.bucket == "raw-dev"


def test_confere_proprietario_do_bucket_raw_antes_de_iniciar(monkeypatch) -> None:
    class FakeSession:
        def __init__(self) -> None:
            self.dynamodb: Any = None
            self.s3: Any = None

        def client(self, service: str):
            from unittest.mock import Mock

            client = Mock()
            setattr(self, service, client)
            return client

    session = FakeSession()
    monkeypatch.setattr(raw_aws_runtime.boto3.session, "Session", lambda **_: session)
    config = RawAWSConfig("raw-dev", "cnesdata-raw-dev-836651842853", "sa-east-1", "id", "key")

    raw_aws_runtime.build_raw_aws_runtime(config, cast("Any", lambda: None))

    session.s3.head_bucket.assert_called_once_with(
        Bucket=config.bucket, ExpectedBucketOwner="836651842853",
    )
