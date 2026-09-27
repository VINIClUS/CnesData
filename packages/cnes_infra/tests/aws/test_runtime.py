"""Contrato do bundle de clientes e adapters do profile aws."""

from dataclasses import replace
from datetime import UTC, datetime
from unittest.mock import Mock, call, sentinel

import pytest
from botocore.exceptions import ClientError

from cnes_infra.audit.s3_object_lock_sink import S3ObjectLockAuditSink
from cnes_infra.aws.runtime import AwsClients, build_aws_runtime, create_aws_clients
from cnes_infra.aws.settings import AwsRuntimeSettings
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.object_store.s3 import S3ObjectStore

_NOW = datetime(2026, 9, 27, 12, tzinfo=UTC)
_LOCKED = {"ObjectLockConfiguration": {"ObjectLockEnabled": "Enabled"}}


def _settings(
    dynamodb_endpoint_url: str | None = None, service_endpoint_url: str | None = None
) -> AwsRuntimeSettings:
    settings = AwsRuntimeSettings.from_mapping(
        {
            "PROFILE": "aws",
            "AUTH_MODE": "oidc",
            "AWS_REGION": "us-east-1",
            "AWS_CONTROL_PLANE_TABLE": "cnesdata-test-control-plane",
            "AWS_DATA_BUCKET": "cnesdata-test-data",
            "AWS_AUDIT_BUCKET": "cnesdata-test-audit",
            "AWS_STATE_MACHINE_ARN": (
                "arn:aws:states:us-east-1:000000000000:stateMachine:cnesdata-test"
            ),
            "AWS_PROCESSOR_CONTAINER_NAME": "processor",
            "AWS_AUDIT_RETENTION_DAYS": "365",
            "OIDC_ISSUER": "https://id.example.test",
            "OIDC_AUDIENCE": "cnesdata-dashboard",
        }
    )
    return replace(
        settings,
        dynamodb_endpoint_url=dynamodb_endpoint_url,
        service_endpoint_url=service_endpoint_url,
    )


def _clients(lock_configuration: dict[str, object]) -> AwsClients:
    s3 = Mock()
    s3.get_object_lock_configuration.return_value = lock_configuration
    return AwsClients(dynamodb=Mock(), s3=s3, step_functions=Mock())


def _session() -> Mock:
    session = Mock()
    session.client.side_effect = [sentinel.dynamodb, sentinel.s3, sentinel.sfn]
    return session


def test_cria_clientes_na_regiao_e_endpoint_configurados() -> None:
    session = _session()

    clients = create_aws_clients(
        _settings(
            dynamodb_endpoint_url="http://dynamodb-local:8000",
            service_endpoint_url="http://aws-emulator:4566",
        ),
        session,
    )

    assert clients == AwsClients(
        dynamodb=sentinel.dynamodb, s3=sentinel.s3, step_functions=sentinel.sfn,
    )
    dynamodb, s3, sfn = session.client.call_args_list
    assert dynamodb == call(
        "dynamodb", region_name="us-east-1", endpoint_url="http://dynamodb-local:8000",
    )
    assert s3.args == ("s3",)
    assert s3.kwargs["region_name"] == "us-east-1"
    assert s3.kwargs["endpoint_url"] == "http://aws-emulator:4566"
    assert s3.kwargs["config"].signature_version == "s3v4"
    assert set(s3.kwargs) == {"region_name", "endpoint_url", "config"}
    assert sfn == call(
        "stepfunctions", region_name="us-east-1", endpoint_url="http://aws-emulator:4566",
    )


def test_usa_endpoints_padrao_da_aws_em_producao() -> None:
    session = _session()

    create_aws_clients(_settings(), session)

    assert session.client.call_count == 3
    assert [c.kwargs["endpoint_url"] for c in session.client.call_args_list] == [None] * 3
    assert session.client.call_args_list[1].kwargs["config"].signature_version == "s3v4"


def test_compoe_adapters_com_recursos_configurados() -> None:
    clients = _clients(_LOCKED)

    components = build_aws_runtime(_settings(), clients, clock=lambda: _NOW)

    assert isinstance(components.control_plane, DynamoDBControlPlane)
    assert isinstance(components.object_store, S3ObjectStore)
    assert isinstance(components.audit_sink, S3ObjectLockAuditSink)
    assert clients.dynamodb.mock_calls == []
    assert clients.step_functions.mock_calls == []
    assert clients.s3.mock_calls == [
        call.get_object_lock_configuration(Bucket="cnesdata-test-audit"),
    ]


def test_encaminha_tabela_e_bucket_configurados_aos_adapters() -> None:
    clients = _clients(_LOCKED)
    clients.dynamodb.get_item.return_value = {}
    clients.s3.get_object.side_effect = ClientError(
        {"Error": {"Code": "NoSuchKey"}}, "GetObject",
    )
    components = build_aws_runtime(_settings(), clients, clock=lambda: _NOW)

    assert components.control_plane.get_membership("354130", "user-1") is None
    assert components.object_store.stat("serving/354130/run-1/a.parquet") is None
    assert clients.dynamodb.get_item.call_args.kwargs["TableName"] == (
        "cnesdata-test-control-plane"
    )
    clients.s3.get_object.assert_called_once_with(
        Bucket="cnesdata-test-data", Key="serving/354130/run-1/a.parquet",
    )


def test_rejeita_bucket_de_auditoria_sem_object_lock() -> None:
    clients = _clients({})

    with pytest.raises(ValueError, match="object_lock=disabled"):
        build_aws_runtime(_settings(), clients, clock=lambda: _NOW)
