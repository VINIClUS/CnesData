"""Main AWS com DynamoDB/S3 reais offline e sink canônico Object Lock."""

import importlib
import io
import json
import logging
import os
import socket
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any
from unittest.mock import Mock, patch

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from apps.data_processor.tests.test_audit_dispatch_mode import _MINIMUM
from cnes_domain.control_plane.entities import OutboxEvent
from cnes_infra.billing.dynamodb_items import outbox_item
from cnes_infra.control_plane.dynamodb_codec import decode_model
from cnes_infra.control_plane.dynamodb_keys import item_key, outbox_key
from packages.cnes_infra.tests.contracts.clock import _NOW
from packages.cnes_infra.tests.control_plane.outbox_cursor_support import evento
from packages.cnes_infra.tests.control_plane.test_dynamodb_adapter import _create_table

_TABLE = "audit-control-configurado"
_BUCKET = "audit-bucket-configurado"
_OFFLINE = {
    "AWS_ACCESS_KEY_ID": "offline",
    "AWS_SECRET_ACCESS_KEY": "offline",
    "AWS_EC2_METADATA_DISABLED": "true",
    "PYTHON_DOTENV_DISABLED": "1",
    "AWS_SHARED_CREDENTIALS_FILE": "/dev/null",
    "AWS_CONFIG_FILE": "/dev/null",
}
_POISON_BILLING = {
    "BILLING_MODE": "invalid",
    "BILLING_ENFORCEMENT_MODE": "invalid",
    "STRIPE_SECRET_KEY": "invalid",
    "BILLING_SECRET_ARN": "invalid",
}
_IRRELEVANT = {
    "AUTH_MODE": "invalid",
    "OIDC_ISSUER": "invalid",
    "OIDC_AUDIENCE": "",
    "OIDC_DOMAIN": "invalid",
    "AWS_DATA_BUCKET": "invalid/data",
    "AWS_STATE_MACHINE_ARN": "invalid",
    "AWS_PUBLIC_SUBNET_IDS": "invalid",
    "AWS_SECURITY_GROUP_ID": "invalid",
    "AWS_PROCESSOR_CONTAINER_NAME": "",
    "AWS_PROCESSOR_MAX_CONCURRENCY": "invalid",
    "AWS_PROCESSOR_LEASE_SECONDS": "invalid",
    "AWS_PROCESSOR_RECOVERY_BATCH_SIZE": "invalid",
    "AWS_SERVING_URL_TTL_SECONDS": "invalid",
}


class _Sdk:
    def __init__(self, client: Any, kind: str) -> None:
        self.client, self.kind = client, kind
        self.phase = "seed"
        self.calls: list[Any] = []
        self.failure: tuple[str, BaseException] | None = None
        self.failure_key: dict[str, Any] | None = None

    def __getattr__(self, name: str) -> Any:
        target = getattr(self.client, name)
        if not callable(target):
            return target

        def forward(**request: Any) -> Any:
            if self.kind == "dynamodb" and self.phase == "seed" and "TableName" in request:
                request = request | {"TableName": _TABLE}
            self.calls.append((self.phase, name, dict(request)))
            if self.phase == "runtime" and self.kind == "dynamodb":
                assert name in {"query", "get_item", "update_item"}, f"ddb_forbidden={name}"
                assert request["TableName"] == _TABLE
            if self.failure is not None and self.phase == "runtime":
                operation, error = self.failure
                if name == operation and (
                    self.failure_key is None or request.get("Key") == self.failure_key
                ):
                    raise error
            return target(**request)

        return forward


@dataclass
class _Aws:
    app: Any
    dynamodb: _Sdk
    s3: _Sdk
    session: Mock
    constructor: Mock
    trace: list[Any] = field(default_factory=list)

    def seed(self, event: OutboxEvent) -> None:
        self.dynamodb.put_item(TableName=_TABLE, Item=outbox_item(event))

    def stored(self, event_id: str) -> OutboxEvent:
        self.dynamodb.phase = "observation"
        response = self.dynamodb.get_item(
            TableName=_TABLE,
            Key=item_key(*outbox_key(event_id)),
            ConsistentRead=True,
        )
        return decode_model(response["Item"], OutboxEvent)


@contextmanager
def _deadline(trace: list[Any], policy: Any) -> Iterator[None]:
    trace.append(("arm", policy.mode, policy.seconds))
    try:
        yield
    finally:
        trace.append(("disarm", policy.mode, policy.seconds))


def _proibir_composicao(monkeypatch: pytest.MonkeyPatch) -> None:
    forbidden = {
        "data_processor.aws_composition": (
            "AwsExecutionPolicy",
            "build_aws_unit_runtime",
            "build_aws_recovery_runtime",
            "build_aws_processor_core",
            "build_dispatch_admission",
            "build_execution_callbacks",
            "StepFunctionsExecutor",
            "PipelineCoordinator",
            "DatasetPublisher",
            "UnitWorker",
            "StageProcessor",
            "build_source_registry",
            "build_publication_policy",
            "S3ObjectStore",
        ),
        "data_processor.main": (
            "AwsExecutionPolicy",
            "build_aws_unit_runtime",
            "build_aws_recovery_runtime",
        ),
        "cnes_infra.billing.composition": ("build_secret_provider",),
        "cnes_infra.aws.runtime": ("S3ObjectStore", "StepFunctionsExecutor"),
    }
    for module_name, names in forbidden.items():
        module = importlib.import_module(module_name)
        for name in names:
            if hasattr(module, name):
                monkeypatch.setattr(
                    module,
                    name,
                    Mock(
                        side_effect=AssertionError(f"construction_forbidden={name}"),
                    ),
                )
    settings = importlib.import_module("cnes_infra.billing.settings")
    monkeypatch.setattr(
        settings.BillingSettings,
        "from_mapping",
        Mock(
            side_effect=AssertionError("billing_settings=forbidden"),
        ),
    )


def _observabilidade(monkeypatch: pytest.MonkeyPatch, instance: _Aws) -> None:
    original = logging.Logger.exception

    def emit(self: Any, message: Any, *args: Any, **kwargs: Any) -> None:
        instance.trace.append("diagnostic")
        original(self, message, *args, **kwargs)

    monkeypatch.setattr(logging.Logger, "exception", emit)
    monkeypatch.setattr(instance.app, "_utc_now", lambda: _NOW)
    monkeypatch.setattr(
        instance.app, "init_telemetry", lambda *args: instance.trace.append("telemetry")
    )
    monkeypatch.setattr(
        instance.app, "processor_deadline", lambda policy: _deadline(instance.trace, policy)
    )


@pytest.fixture
def aws(monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest) -> Iterator[_Aws]:
    with patch.dict(os.environ, _MINIMUM | _OFFLINE, clear=True), mock_aws():
        app = importlib.import_module("data_processor.main")
        dynamodb = _Sdk(boto3.client("dynamodb", region_name="us-east-2"), "dynamodb")
        s3 = _Sdk(boto3.client("s3", region_name="us-east-2"), "s3")
        _create_table(dynamodb)
        s3.create_bucket(
            Bucket=_BUCKET,
            ObjectLockEnabledForBucket=True,
            CreateBucketConfiguration={"LocationConstraint": "us-east-2"},
        )
        clients = {"dynamodb": dynamodb, "s3": s3}
        session = Mock(spec_set=["client"])
        instance = _Aws(app, dynamodb, s3, session, Mock())

        def client(service: str, **kwargs: Any) -> Any:
            instance.trace.append(("client", service))
            return clients[service]

        def construct() -> Any:
            instance.trace.append("Session")
            return session

        configure = app.configure_json_stdout

        def logging_boundary(*args: Any) -> None:
            instance.trace.append("logging")
            configure(*args)

        session.client.side_effect = client
        instance.constructor.side_effect = construct
        monkeypatch.setattr(app, "Session", instance.constructor)
        monkeypatch.setattr(app, "configure_json_stdout", logging_boundary)
        _proibir_composicao(monkeypatch)
        _observabilidade(monkeypatch, instance)
        yield instance
        request.node.user_properties.extend(
            [
                ("ddb_seed_runtime_observation", repr(dynamodb.calls)),
                ("s3_seed_runtime_observation", repr(s3.calls)),
                ("deadline_bootstrap_trace", repr(instance.trace)),
            ]
        )


@pytest.fixture(autouse=True)
def _bloquear_rede(monkeypatch: pytest.MonkeyPatch) -> None:
    def negar(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("network=forbidden")

    for name in ("create_connection", "getaddrinfo", "gethostbyname", "gethostbyname_ex"):
        monkeypatch.setattr(socket, name, negar)
    for name in ("connect", "connect_ex", "sendto"):
        monkeypatch.setattr(socket.socket, name, negar)


async def _invoke(aws: _Aws, values: dict[str, str]) -> tuple[int, list[Any]]:
    output = io.StringIO()
    root = logging.getLogger()
    handlers, level = root.handlers[:], root.level
    root.handlers[:] = []
    aws.dynamodb.phase = aws.s3.phase = "runtime"
    try:
        with (
            patch.dict(os.environ, values | _OFFLINE, clear=True),
            patch.object(sys, "argv", ["data_processor", "dispatch-outbox-once"]),
            patch.object(sys, "stdout", output),
        ):
            rc = await aws.app.main()
        return rc, [json.loads(line) for line in output.getvalue().splitlines()]
    finally:
        for handler in root.handlers[:]:
            handler.close()
        root.handlers[:] = handlers
        root.setLevel(level)


def _assert_session(aws: _Aws, endpoints: bool) -> None:
    aws.constructor.assert_called_once_with()
    calls = aws.session.client.call_args_list
    assert sorted(call.args[0] for call in calls) == ["dynamodb", "s3"]
    for call in calls:
        assert call.args in (("dynamodb",), ("s3",))
        assert call.kwargs["region_name"] == "us-east-2"
        endpoint = (
            "https://dynamodb.offline.invalid"
            if call.args == ("dynamodb",)
            else ("https://s3.offline.invalid")
        )
        assert call.kwargs.get("endpoint_url") == (endpoint if endpoints else None)
        assert not {"aws_access_key_id", "aws_secret_access_key", "aws_session_token"} & (
            call.kwargs.keys()
        )
        if call.args == ("s3",):
            config = call.kwargs["config"]
            assert config.signature_version == "s3v4"
            assert config.s3 == (None if endpoints else {"addressing_style": "virtual"})


def _assert_sink(
    aws: _Aws,
    event: OutboxEvent,
    retained_until: str = "2027-07-15T12:00:00+00:00",
) -> None:
    aws.s3.phase = "observation"
    key = "audit/354130/2026/07/15/entregue.json"
    response = aws.s3.get_object(Bucket=_BUCKET, Key=key)
    assert OutboxEvent.model_validate_json(response["Body"].read()) == event
    assert response["ObjectLockMode"] == "COMPLIANCE"
    assert response["ObjectLockRetainUntilDate"].isoformat() == retained_until
    identity = aws.s3.get_object(Bucket=_BUCKET, Key="audit/.event-id/entregue.json")
    assert OutboxEvent.model_validate_json(identity["Body"].read()) == event


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "variant", ["minimo", "producao", "irrelevantes", "endpoints", "retencao30"]
)
async def test_main_entrega_no_bucket_e_tabela_configurados_sem_outros_recursos(
    aws: _Aws,
    variant: str,
    record_property: Any,
) -> None:
    event = evento("entregue")
    aws.seed(event)
    values = _MINIMUM | _POISON_BILLING
    if variant == "producao":
        values |= {"AWS_PRODUCTION_POLICY": "true"}
    if variant == "irrelevantes":
        values |= _IRRELEVANT
    if variant == "endpoints":
        values |= {
            "DYNAMODB_ENDPOINT_URL": "https://dynamodb.offline.invalid",
            "AWS_ENDPOINT_URL": "https://s3.offline.invalid",
        }
    if variant == "retencao30":
        values |= {"AWS_AUDIT_RETENTION_DAYS": "30"}

    rc, events = await _invoke(aws, values)

    assert rc == 0, events
    _assert_session(aws, variant == "endpoints")
    assert aws.trace[:4] == [("arm", "dispatch-outbox-once", 60), "logging", "telemetry", "Session"]
    assert set(aws.trace[4:-1]) == {("client", "dynamodb"), ("client", "s3")}
    assert aws.trace[-1] == ("disarm", "dispatch-outbox-once", 60)
    assert aws.stored(event.event_id) == event.model_copy(update={"delivered_at": _NOW})
    retained_until = (
        "2026-08-14T12:00:00+00:00" if variant == "retencao30" else ("2027-07-15T12:00:00+00:00")
    )
    _assert_sink(aws, event, retained_until)
    runtime = [(name, request) for phase, name, request in aws.dynamodb.calls if phase == "runtime"]
    assert {name for name, _ in runtime} == {"query", "get_item", "update_item"}
    assert [request["Limit"] for name, request in runtime if name == "query"] == [100]
    assert all(request["TableName"] == _TABLE for _, request in runtime)
    assert all(
        request["Bucket"] == _BUCKET for phase, _, request in aws.s3.calls if phase == "runtime"
    )
    record_property("ddb_seed_runtime_observation", repr(aws.dynamodb.calls))
    record_property("s3_seed_runtime_observation", repr(aws.s3.calls))


@pytest.mark.asyncio
async def test_main_vazio_retorna_zero_sem_escrita_de_evento(aws: _Aws) -> None:
    rc, events = await _invoke(aws, _MINIMUM | _POISON_BILLING)

    assert rc == 0, events
    _assert_session(aws, False)
    assert not any(name == "put_object" for phase, name, _ in aws.s3.calls if phase == "runtime")
    aws.s3.phase = "observation"
    assert aws.s3.list_objects_v2(Bucket=_BUCKET).get("Contents", []) == []


def _error(code: str, operation: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": "offline"}}, operation)


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["query", "get_item", "update_item"])
async def test_erro_sdk_de_query_cursor_ou_marcacao_retorna_um_e_preserva_pending(
    aws: _Aws,
    operation: str,
) -> None:
    event = evento("entregue")
    aws.seed(event)
    aws.dynamodb.failure = (operation, _error("AccessDenied", operation))

    rc, events = await _invoke(aws, _MINIMUM)

    assert rc == 1, events
    assert any(phase == "runtime" and name == operation for phase, name, _ in aws.dynamodb.calls)
    assert aws.stored(event.event_id) == event


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("code", "expected"),
    [
        ("NoSuchBucket", 1),
        ("AccessDenied", 1),
    ],
)
async def test_falha_operacional_do_bucket_nao_vira_configuracao(
    aws: _Aws,
    code: str,
    expected: int,
) -> None:
    aws.s3.failure = ("get_object_lock_configuration", _error(code, "GetObjectLockConfiguration"))

    rc, events = await _invoke(aws, _MINIMUM)

    assert rc == expected, events
    assert any(
        phase == "runtime" and name == "get_object_lock_configuration"
        for phase, name, _ in aws.s3.calls
    )


@pytest.mark.asyncio
async def test_object_lock_desabilitado_e_configuracao_terminal(aws: _Aws) -> None:
    aws.s3.put_object_lock_configuration(
        Bucket=_BUCKET,
        ObjectLockConfiguration={"ObjectLockEnabled": "Enabled"},
    )
    original = aws.s3.client.get_object_lock_configuration

    def disabled(**request: Any) -> dict[str, Any]:
        original(**request)
        return {"ObjectLockConfiguration": {}}

    with patch.object(aws.s3.client, "get_object_lock_configuration", disabled):
        rc, events = await _invoke(aws, _MINIMUM)

    assert rc == 2, events
    assert any(
        phase == "runtime" and name == "get_object_lock_configuration"
        for phase, name, _ in aws.s3.calls
    )


@pytest.mark.asyncio
async def test_valueerror_operacional_nao_vira_configuracao_e_diagnostico_tem_deadline(
    aws: _Aws,
) -> None:
    aws.s3.failure = ("get_object_lock_configuration", ValueError("remote_payload=invalid"))

    rc, events = await _invoke(aws, _MINIMUM)

    assert rc == 1, events
    assert aws.trace[0] == ("arm", "dispatch-outbox-once", 60)
    assert aws.trace[-2:] == ["diagnostic", ("disarm", "dispatch-outbox-once", 60)]


@pytest.mark.asyncio
async def test_cursor_corrupto_preserva_item_e_retorna_falha_operacional(aws: _Aws) -> None:
    cursor = {
        "pk": {"S": "OUTBOX#CURSOR"},
        "sk": {"S": "CURRENT"},
        "entity": {"S": "OUTBOXCURSOR"},
        "version": {"N": "1"},
        "position": {"N": "42"},
    }
    aws.dynamodb.put_item(TableName=_TABLE, Item=cursor)

    rc, events = await _invoke(aws, _MINIMUM)

    assert rc == 1, events
    aws.dynamodb.phase = "observation"
    assert (
        aws.dynamodb.get_item(
            TableName=_TABLE, Key=item_key("OUTBOX#CURSOR", "CURRENT"), ConsistentRead=True
        )["Item"]
        == cursor
    )
    assert not any(
        name == "update_item" for phase, name, _ in aws.dynamodb.calls if phase == "runtime"
    )


@pytest.mark.asyncio
async def test_falha_no_cas_do_cursor_nao_desfaz_evento_entregue(aws: _Aws) -> None:
    event = evento("entregue")
    aws.seed(event)
    aws.dynamodb.failure_key = item_key("OUTBOX#CURSOR", "CURRENT")
    aws.dynamodb.failure = ("update_item", _error("AccessDenied", "UpdateItem"))

    rc, events = await _invoke(aws, _MINIMUM)

    assert rc == 1, events
    assert aws.stored(event.event_id) == event.model_copy(update={"delivered_at": _NOW})
    _assert_sink(aws, event)
    assert "Item" not in aws.dynamodb.get_item(
        TableName=_TABLE,
        Key=item_key("OUTBOX#CURSOR", "CURRENT"),
        ConsistentRead=True,
    )
