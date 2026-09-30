"""Bundle de clientes boto3 e adapters estáveis do profile aws."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from botocore.config import Config

from cnes_infra.audit.s3_object_lock_sink import S3ObjectLockAuditSink
from cnes_infra.billing.settings import LOCAL_BILLING_SETTINGS, BillingSettings
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.object_store.s3 import S3ObjectStore

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import datetime

    from boto3.session import Session
    from botocore.client import BaseClient

    from cnes_domain.ports.audit import AuditSinkPort
    from cnes_domain.ports.control_plane import ControlPlanePort
    from cnes_domain.ports.object_store import ObjectStorePort
    from cnes_infra.aws.settings import AwsRuntimeSettings


@dataclass(frozen=True, slots=True)
class AwsClients:
    dynamodb: BaseClient
    s3: BaseClient
    step_functions: BaseClient


@dataclass(frozen=True, slots=True)
class AwsRuntimeComponents:
    control_plane: ControlPlanePort
    object_store: ObjectStorePort
    audit_sink: AuditSinkPort


def create_aws_clients(settings: AwsRuntimeSettings, session: Session) -> AwsClients:
    """Args: settings: Configuração do profile aws. session: Sessão boto3 da provider chain.
    Returns: Clientes DynamoDB, S3 (SigV4) e Step Functions.
    """
    return AwsClients(
        dynamodb=cast("BaseClient", session.client(
            "dynamodb", region_name=settings.region,
            endpoint_url=settings.dynamodb_endpoint_url,
        )),
        # With endpoint_url set, boto3 may fall back to SigV2 presigning.
        s3=cast("BaseClient", session.client(
            "s3", region_name=settings.region,
            endpoint_url=settings.service_endpoint_url,
            config=Config(signature_version="s3v4"),
        )),
        step_functions=cast("BaseClient", session.client(
            "stepfunctions", region_name=settings.region,
            endpoint_url=settings.service_endpoint_url,
        )),
    )


def build_aws_runtime(
    settings: AwsRuntimeSettings, clients: AwsClients, clock: Callable[[], datetime],
    billing: BillingSettings = LOCAL_BILLING_SETTINGS,
) -> AwsRuntimeComponents:
    """Args: settings: Configuração. clients: Clientes boto3. clock: Relógio injetado.
        billing: Settings de billing aplicados ao claim de unidades.
    Returns: Adapters sobre os recursos configurados, sem provisionamento.
    Raises: ValueError: Quando o bucket de auditoria não tem Object Lock.
    """
    return AwsRuntimeComponents(
        control_plane=DynamoDBControlPlane(
            client=clients.dynamodb, table_name=settings.control_plane_table, clock=clock,
            billing=billing,
        ),
        object_store=S3ObjectStore(client=clients.s3, bucket=settings.data_bucket, prefix=""),
        audit_sink=S3ObjectLockAuditSink(
            client=clients.s3,
            bucket=settings.audit_bucket,
            retention_days=settings.audit_retention_days,
        ),
    )


__all__ = ["AwsClients", "AwsRuntimeComponents", "build_aws_runtime", "create_aws_clients"]
