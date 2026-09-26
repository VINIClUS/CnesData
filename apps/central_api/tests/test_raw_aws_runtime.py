import pytest

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
