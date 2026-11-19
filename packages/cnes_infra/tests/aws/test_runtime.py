"""Contrato do bundle de clientes e adapters do profile aws."""

from collections.abc import Mapping
from dataclasses import replace
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import Mock, call, sentinel
from urllib.parse import urlsplit

import boto3
import pytest
from botocore.exceptions import ClientError

from cnes_domain.orchestration.dispatch_admission import EnvironmentAdmission
from cnes_domain.ports.environment_gate import UNGATED_ADMISSION, ExecutionPresence
from cnes_infra.audit.s3_object_lock_sink import S3ObjectLockAuditSink
from cnes_infra.aws.runtime import (
    AwsClients,
    build_aws_runtime,
    build_dispatch_admission,
    create_aws_clients,
)
from cnes_infra.aws.settings import AwsRuntimeConfigurationError, AwsRuntimeSettings
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.object_store.s3 import S3ObjectStore
from packages.cnes_infra.tests.aws.production_values import (
    PRODUCTION_STATE_MACHINE_ARN,
    PRODUCTION_VALUES,
)

if TYPE_CHECKING:
    from types_boto3_s3.client import S3Client

    from cnes_domain.ports.control_plane import ControlPlanePort

_NOW = datetime(2026, 9, 27, 12, tzinfo=UTC)
_LOCKED = {"ObjectLockConfiguration": {"ObjectLockEnabled": "Enabled"}}
_SERVING_KEY = "serving/354130/run-01/overview.json"


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


def _clients(lock_configuration: Mapping[str, object]) -> AwsClients:
    s3 = Mock()
    s3.get_object_lock_configuration.return_value = lock_configuration
    return AwsClients(dynamodb=Mock(), s3=s3, step_functions=Mock())


def _mock(client: object) -> Mock:
    return cast("Mock", client)


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
    assert s3.kwargs["config"].s3 is None
    assert set(s3.kwargs) == {"region_name", "endpoint_url", "config"}
    assert sfn == call(
        "stepfunctions", region_name="us-east-1", endpoint_url="http://aws-emulator:4566",
    )


def test_usa_endpoints_padrao_da_aws_em_producao() -> None:
    session = _session()

    create_aws_clients(_settings(), session)

    assert session.client.call_count == 3
    assert [c.kwargs["endpoint_url"] for c in session.client.call_args_list] == [None] * 3
    config = session.client.call_args_list[1].kwargs["config"]
    assert config.signature_version == "s3v4"
    assert config.s3 == {"addressing_style": "virtual"}


def test_url_assinada_de_serving_usa_host_regional_do_bucket(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name in ("AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3"):
        monkeypatch.delenv(name, raising=False)
    settings = AwsRuntimeSettings.from_mapping(PRODUCTION_VALUES)
    session = boto3.Session(
        aws_access_key_id="test", aws_secret_access_key="test",  # noqa: S106
    )

    signer = cast("S3Client", create_aws_clients(settings, session).s3)

    url = signer.generate_presigned_url(
        "get_object", Params={"Bucket": settings.data_bucket, "Key": _SERVING_KEY},
        ExpiresIn=settings.serving_url_ttl_seconds,
    )

    parts = urlsplit(url)
    assert (parts.scheme, parts.netloc, parts.path) == (
        "https", "cnesdata-prod-data.s3.us-east-2.amazonaws.com", f"/{_SERVING_KEY}",
    )


def test_compoe_adapters_com_recursos_configurados() -> None:
    clients = _clients(_LOCKED)

    components = build_aws_runtime(_settings(), clients, clock=lambda: _NOW)

    assert isinstance(components.control_plane, DynamoDBControlPlane)
    assert isinstance(components.object_store, S3ObjectStore)
    assert isinstance(components.audit_sink, S3ObjectLockAuditSink)
    assert _mock(clients.dynamodb).mock_calls == []
    assert _mock(clients.step_functions).mock_calls == []
    assert _mock(clients.s3).mock_calls == [
        call.get_object_lock_configuration(Bucket="cnesdata-test-audit"),
    ]


def test_encaminha_tabela_e_bucket_configurados_aos_adapters() -> None:
    clients = _clients(_LOCKED)
    _mock(clients.dynamodb).get_item.return_value = {}
    _mock(clients.s3).get_object.side_effect = ClientError(
        cast("Any", {"Error": {"Code": "NoSuchKey"}}), "GetObject",
    )
    components = build_aws_runtime(_settings(), clients, clock=lambda: _NOW)

    assert components.control_plane.get_membership("354130", "user-1") is None
    assert components.object_store.stat("serving/354130/run-1/a.parquet") is None
    assert _mock(clients.dynamodb).get_item.call_args.kwargs["TableName"] == (
        "cnesdata-test-control-plane"
    )
    _mock(clients.s3).get_object.assert_called_once_with(
        Bucket="cnesdata-test-data", Key="serving/354130/run-1/a.parquet",
    )


def test_rejeita_bucket_de_auditoria_sem_object_lock() -> None:
    clients = _clients({})

    with pytest.raises(ValueError, match="object_lock=disabled"):
        build_aws_runtime(_settings(), clients, clock=lambda: _NOW)


def test_admissao_fora_da_politica_de_producao_e_sem_gate() -> None:
    clients = _clients(_LOCKED)

    admission = build_dispatch_admission(
        _settings(), clients, cast("ControlPlanePort", object()),
    )

    assert admission is UNGATED_ADMISSION
    assert _mock(clients.step_functions).mock_calls == []


def test_admissao_de_producao_usa_o_gate_e_o_probe_do_executor() -> None:
    settings = AwsRuntimeSettings.from_mapping(PRODUCTION_VALUES)
    clients = _clients(_LOCKED)
    control_plane = DynamoDBControlPlane(
        clients.dynamodb, settings.control_plane_table, lambda: _NOW,
    )
    _mock(clients.step_functions).describe_execution.return_value = {"status": "RUNNING"}

    admission = build_dispatch_admission(settings, clients, control_plane)

    assert isinstance(admission, EnvironmentAdmission)
    assert admission._gate is control_plane
    assert (admission._environment, admission._lease_seconds) == ("prod", 3600)
    observation = admission._probe.probe("0123456789abcdef")
    assert observation.presence is ExecutionPresence.RUNNING
    prefix = PRODUCTION_STATE_MACHINE_ARN.replace(":stateMachine:", ":execution:")
    _mock(clients.step_functions).describe_execution.assert_called_once_with(
        executionArn=f"{prefix}:0123456789abcdef",
    )
    assert _mock(clients.dynamodb).mock_calls == []


def test_admissao_de_producao_exige_control_plane_com_gate() -> None:
    settings = AwsRuntimeSettings.from_mapping(PRODUCTION_VALUES)
    clients = _clients(_LOCKED)

    with pytest.raises(AwsRuntimeConfigurationError, match=r"^environment_gate_unsupported$"):
        build_dispatch_admission(settings, clients, cast("ControlPlanePort", object()))

    assert _mock(clients.step_functions).mock_calls == []
