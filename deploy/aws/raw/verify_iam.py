"""Valida acesso raw isolado entre ambientes sem imprimir credenciais."""

import logging
import sys
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


def read_env(path: Path) -> dict[str, str]:
    return dict(line.split("=", 1) for line in path.read_text().splitlines())


for environment in ("dev", "prod"):
    other = "prod" if environment == "dev" else "dev"
    values = read_env(Path(sys.argv[1]) / f"{environment}.env.raw")
    session = boto3.session.Session(
        aws_access_key_id=values["RAW_AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=values["RAW_AWS_SECRET_ACCESS_KEY"],
        region_name=values["RAW_AWS_REGION"],
    )
    dynamodb = session.client("dynamodb")
    s3 = session.client("s3")
    dynamodb.describe_table(TableName=values["RAW_DYNAMODB_TABLE"])
    s3.head_bucket(Bucket=values["RAW_S3_BUCKET"])
    item_key = {"pk": {"S": "VERIFY#IAM"}, "sk": {"S": "VERIFY#IAM"}}
    dynamodb.put_item(TableName=values["RAW_DYNAMODB_TABLE"], Item=item_key)
    assert dynamodb.get_item(
        TableName=values["RAW_DYNAMODB_TABLE"], Key=item_key,
    )["Item"] == item_key
    dynamodb.delete_item(TableName=values["RAW_DYNAMODB_TABLE"], Key=item_key)
    key = "verification/iam-check"
    response = s3.put_object(Bucket=values["RAW_S3_BUCKET"], Key=key, Body=b"raw-check")
    assert s3.get_object(Bucket=values["RAW_S3_BUCKET"], Key=key)["Body"].read() == b"raw-check"
    boto3.client("s3", region_name=values["RAW_AWS_REGION"]).delete_object(
        Bucket=values["RAW_S3_BUCKET"], Key=key, VersionId=response["VersionId"],
    )
    other_table = f"cnesdata-raw-{other}"
    other_bucket = f"cnesdata-raw-{other}-836651842853"
    for call in (
        lambda db=dynamodb, name=other_table: db.describe_table(TableName=name),
        lambda client=s3, name=other_bucket: client.head_bucket(Bucket=name),
    ):
        try:
            call()
        except ClientError as error:
            if error.response["Error"]["Code"] not in {
                "AccessDenied", "AccessDeniedException", "403",
            }:
                raise
        else:
            raise AssertionError(f"raw_cross_environment_access={environment}")
    logger.info("raw_iam_verified environment=%s", environment)
