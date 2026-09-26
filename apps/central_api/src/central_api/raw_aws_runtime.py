"""Composição isolada do control plane e object store raw na VPS."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import boto3

from central_api.services.delta_policy import DeltaPolicy
from central_api.services.raw_ingestion import RawIngestionService
from central_api.services.raw_upload import RawUploadService
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.object_store import S3ObjectStore

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from datetime import datetime


@dataclass(frozen=True, slots=True)
class RawAWSConfig:
    table: str
    bucket: str
    region: str
    access_key: str
    secret_key: str

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> RawAWSConfig:
        names = (
            "RAW_DYNAMODB_TABLE", "RAW_S3_BUCKET", "RAW_AWS_REGION",
            "RAW_AWS_ACCESS_KEY_ID", "RAW_AWS_SECRET_ACCESS_KEY",
        )
        if any(not env.get(name, "").strip() for name in names):
            raise ValueError("raw_aws_config_missing")
        return cls(*(env[name] for name in names))


def build_raw_aws_runtime(config: RawAWSConfig, clock: Callable[[], datetime]):
    session = boto3.session.Session(
        aws_access_key_id=config.access_key,
        aws_secret_access_key=config.secret_key,
        region_name=config.region,
    )
    dynamodb = session.client("dynamodb")
    s3 = session.client("s3")
    dynamodb.describe_table(TableName=config.table)
    s3.head_bucket(Bucket=config.bucket)
    control = DynamoDBControlPlane(dynamodb, config.table, clock)
    objects = S3ObjectStore(s3, config.bucket)
    return control, RawUploadService(control, objects, clock), RawIngestionService(
        control, objects, DeltaPolicy(),
    )
