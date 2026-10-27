"""Fronteira moto e oráculos literais de tokens de atualização DynamoDB."""

import json
import os
import socket
from collections.abc import Callable, Iterator
from copy import deepcopy
from datetime import UTC, datetime
from logging import DEBUG, INFO, LogRecord
from pathlib import Path
from traceback import format_exception
from typing import TYPE_CHECKING, Any, cast

import boto3
import botocore
import pytest
from moto import mock_aws

from cnes_domain.refresh_tokens import RefreshTokenInput, RefreshTokenRecord
from cnes_infra.auth.dynamodb_refresh_tokens import DynamoDBRefreshTokenStore

if TYPE_CHECKING:
    from botocore.client import BaseClient

_TABLE = "refresh-tokens"
_FIXTURE = Path(__file__).parents[1] / "fixtures/auth/refresh_dynamodb_schema1.json"
with _FIXTURE.open(encoding="utf-8") as _stream:
    VALUES: dict[str, Any] = json.loads("".join(json.load(_stream)))
TENANT = VALUES["tenant_id"]
OTHER_TENANT = VALUES["other_tenant_id"]
AGENT = VALUES["agent_id"]
OTHER_AGENT = VALUES["other_agent_id"]
TOKEN = VALUES["token"]
DIGEST = VALUES["token_sha256"]
FINGERPRINT = VALUES["machine_fingerprint"]
ISSUED = datetime.fromisoformat(VALUES["issued_at"])
EXPIRES = datetime.fromisoformat(VALUES["token_expires_at"])
LAST_USED = datetime.fromisoformat(VALUES["last_used_at"])
REVOKED = datetime.fromisoformat(VALUES["revoked_at"])
NOW = datetime(2026, 1, 2, 6, 0, tzinfo=UTC)
CANARY = "synthetic-refresh-ddb-canary"
SECRET_CANARY = "synthetic-refresh-secret-canary-very-long"  # noqa: S105
RECORD_CANARY = "synthetic-refresh-record-shaped-canary-very-long"
FINGERPRINT_CANARY = "synthetic-refresh-fingerprint-canary-very-long"
_OFFLINE_COUNTS: list[dict[str, Any]] = []


class DynamoBoundary:
    """Registra pedidos de baixo nível e encaminha todas as chamadas bem-sucedidas ao moto."""

    def __init__(self, client: Any) -> None:
        self.client = client
        self.requests: list[tuple[str, dict[str, Any]]] = []
        self.fail_operation = ""
        self.fail_error: BaseException | None = None
        self.response_overrides: dict[str, Any] = {}
        self.forwarded_errors: list[tuple[str, BaseException]] = []

    def __getattr__(self, name: str) -> Any:
        operation = getattr(self.client, name)
        if not callable(operation):
            return operation

        def invoke(**kwargs: Any) -> Any:
            self.requests.append((name, dict(kwargs)))
            if name == self.fail_operation and self.fail_error is not None:
                error = self.fail_error
                self.forwarded_errors.append((name, error))
                raise error
            try:
                if name in self.response_overrides:
                    return self.response_overrides[name]
                return operation(**kwargs)
            except BaseException as error:
                self.forwarded_errors.append((name, error))
                raise

        return invoke

    def independent(self) -> "DynamoBoundary":
        client = boto3.client(
            "dynamodb",
            region_name="us-east-1",
            aws_access_key_id="synthetic",
            aws_secret_access_key="synthetic-secret",  # noqa: S106
        )
        return DynamoBoundary(client)


@pytest.fixture
def dynamodb_case() -> Iterator[tuple[DynamoBoundary, DynamoDBRefreshTokenStore]]:
    with mock_aws():
        client = boto3.client(
            "dynamodb",
            region_name="us-east-1",
            aws_access_key_id="synthetic",
            aws_secret_access_key="synthetic-secret",  # noqa: S106
        )
        client.create_table(
            TableName=_TABLE,
            KeySchema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        client.update_time_to_live(
            TableName=_TABLE,
            TimeToLiveSpecification={"Enabled": True, "AttributeName": "expires_at"},
        )
        boundary = DynamoBoundary(client)
        yield (
            boundary,
            DynamoDBRefreshTokenStore(
                cast("BaseClient", boundary),
                _TABLE,
                lambda: TOKEN,
            ),
        )


def token_input(**overrides: Any) -> RefreshTokenInput:
    values: dict[str, Any] = {
        "agent_id": AGENT,
        "tenant_id": TENANT,
        "machine_fingerprint": FINGERPRINT,
        "issued_at": ISSUED,
        "expires_at": EXPIRES,
    }
    values.update(overrides)
    return RefreshTokenInput(**values)


def token_record(**overrides: Any) -> RefreshTokenRecord:
    values: dict[str, Any] = {
        "token_sha256": DIGEST,
        "agent_id": AGENT,
        "tenant_id": TENANT,
        "machine_fingerprint": FINGERPRINT,
        "issued_at": ISSUED,
        "expires_at": EXPIRES,
        "last_used_at": LAST_USED,
        "revoked_at": REVOKED,
        "version": 7,
    }
    values.update(overrides)
    return RefreshTokenRecord(**values)


def key_component(value: str) -> str:
    return value.encode("utf-8").hex()


def item_key(
    tenant_id: str,
    digest: str,
    projection: str,
    agent_id: str = AGENT,
) -> dict[str, dict[str, str]]:
    tenant = f"TENANT#{key_component(tenant_id)}"
    digest_key = f"REFRESH_TOKEN#{key_component(digest)}"
    if projection == "DIGEST":
        return {"pk": {"S": tenant}, "sk": {"S": digest_key}}
    return {"pk": {"S": f"{tenant}#AGENT#{key_component(agent_id)}"}, "sk": {"S": digest_key}}


def ttl(expires_at: datetime = EXPIRES) -> int:
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    elapsed = expires_at - epoch
    seconds = elapsed.days * 86_400 + elapsed.seconds
    return int(seconds + bool(elapsed.microseconds))


def literal_item(projection: str, **overrides: Any) -> dict[str, dict[str, Any]]:
    values = deepcopy(VALUES["imported_items"][projection.lower()])
    values.update(overrides)
    return values


def active_item(projection_name: str, **overrides: Any) -> dict[str, dict[str, Any]]:
    values = literal_item(projection_name)
    values.update(
        last_used_at={"NULL": True},
        revoked_at={"NULL": True},
        version={"N": "0"},
    )
    values.update(overrides)
    return values


def literal_key(projection: str) -> dict[str, dict[str, str]]:
    return deepcopy(VALUES["wire_keys"][projection.lower()])


def seed_item(boundary: DynamoBoundary, item: dict[str, dict[str, Any]]) -> None:
    boundary.put_item(TableName=_TABLE, Item=item)


def read_item(boundary: DynamoBoundary, key: dict[str, dict[str, str]]) -> dict[str, Any] | None:
    response = boundary.get_item(TableName=_TABLE, Key=key, ConsistentRead=True)
    return response.get("Item")


def requests_for(boundary: DynamoBoundary, operation: str) -> list[dict[str, Any]]:
    return [request for name, request in boundary.requests if name == operation]


def raw_text(boundary: DynamoBoundary) -> str:
    return repr(boundary.requests)


def log_record_text(record: LogRecord) -> str:
    fields = [record.getMessage(), record.exc_text or "", record.stack_info or ""]
    if record.exc_info:
        fields.extend(format_exception(*record.exc_info))
    return "\n".join(fields)


def assert_ttl_ceiling() -> None:
    assert ttl() == VALUES["ttl_epoch"]


def fresh_store(
    boundary: DynamoBoundary,
    factory: Callable[[], str] | None = None,
) -> DynamoDBRefreshTokenStore:
    return DynamoDBRefreshTokenStore(
        cast("BaseClient", boundary),
        _TABLE,
        factory or (lambda: TOKEN),
    )


@pytest.fixture(autouse=True)
def offline_process(
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> Iterator[list[str]]:
    for name in ("AWS_EC2_METADATA_DISABLED", "AWS_CONFIG_FILE", "AWS_SHARED_CREDENTIALS_FILE"):
        monkeypatch.setenv(name, "true" if name == "AWS_EC2_METADATA_DISABLED" else "/dev/null")
    attempts: list[str] = []

    def blocked(*_: Any, **__: Any) -> None:
        attempts.append("socket")
        raise AssertionError("offline_socket_attempt")

    monkeypatch.setattr(socket, "create_connection", blocked)
    monkeypatch.setattr(socket, "getaddrinfo", blocked)
    monkeypatch.setattr(socket.socket, "connect", blocked)
    monkeypatch.setattr(socket.socket, "connect_ex", blocked)
    monkeypatch.setattr(socket.socket, "sendto", blocked)
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "synthetic")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "synthetic-secret")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("NO_PROXY", "*")
    monkeypatch.setenv("PYTHONHASHSEED", os.environ.get("PYTHONHASHSEED", "0"))
    try:
        yield attempts
    finally:
        count = {"nodeid": request.node.nodeid, "attempts": len(attempts)}
        _OFFLINE_COUNTS.append(count)
        assert not attempts, f"forbidden_io nodeid={request.node.nodeid}"


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    path = os.environ.get("CNES_REFRESH_OFFLINE_EVIDENCE")
    if path:
        Path(path).write_text(
            json.dumps(
                {
                    "exitstatus": exitstatus,
                    "cases": _OFFLINE_COUNTS,
                    "case_count": len(_OFFLINE_COUNTS),
                    "attempt_count": sum(item["attempts"] for item in _OFFLINE_COUNTS),
                },
                sort_keys=True,
            )
        )


_BOTOCORE_DIR = Path(botocore.__file__).resolve().parent
_SDK_LOG_FAMILIES = (
    (
        "botocore.endpoint",
        "endpoint.py",
        "make_request",
        114,
        "Making request for %s with params: %s",
        2,
    ),
    ("botocore.parsers", "parsers.py", "parse", 256, "Response body:\n%r", 2),
    ("botocore.endpoint", "endpoint.py", "_do_get_response", 265, "Sending http request: %s", 1),
    ("botocore.parsers", "parsers.py", "parse", 255, "Response headers: %r", 1),
    ("botocore.auth", "auth.py", "add_auth", 437, "CanonicalRequest:\n%s", 1),
    ("botocore.auth", "auth.py", "add_auth", 439, "StringToSign:\n%s", 1),
    ("botocore.auth", "auth.py", "add_auth", 441, "Signature:\n%s", 1),
    ("botocore.hooks", "hooks.py", "_emit", 238, "Event %s: calling handler %s", 1),
)


def _sdk_log_allowance(record: LogRecord) -> int:
    if record.exc_info is not None or record.exc_text or record.stack_info:
        return 0
    fields = ("name", "pathname", "funcName", "msg")
    strings = tuple(getattr(record, field, None) for field in fields)
    if not all(type(value) is str for value in strings):
        return 0
    if type(getattr(record, "lineno", None)) is not int:
        return 0
    if type(getattr(record, "levelno", None)) is not int:
        return 0
    origin = (record.name, record.pathname, record.funcName, record.lineno, record.msg)
    for name, filename, function, line, message, allowance in _SDK_LOG_FAMILIES:
        expected = (name, str(_BOTOCORE_DIR / filename), function, line, message)
        if record.levelno == DEBUG and origin == expected:
            return allowance
    return 0


def assert_log_privacy(records: list[LogRecord]) -> None:
    for record in records:
        rendered = log_record_text(record)
        for value in (CANARY, SECRET_CANARY, RECORD_CANARY, FINGERPRINT_CANARY):
            assert value not in rendered
        allowance = _sdk_log_allowance(record)
        if allowance < 2:
            assert FINGERPRINT not in rendered
        if allowance < 1:
            assert TOKEN not in rendered


def privacy_protocol_record(family: str, value: str) -> LogRecord:
    vectors = {
        "pedido": (
            "endpoint",
            "make_request",
            114,
            "Making request for %s with params: %s",
            ("OperationModel(name=TransactWriteItems)", {"body": value}),
        ),
        "corpo": ("parsers", "parse", 256, "Response body:\n%r", (value.encode(),)),
        "preparado": (
            "endpoint",
            "_do_get_response",
            265,
            "Sending http request: %s",
            (f"<AWSPreparedRequest headers={{'Authorization': '{value}'}}>",),
        ),
        "cabecalhos": (
            "parsers",
            "parse",
            255,
            "Response headers: %r",
            ({"x-amzn-requestid": value},),
        ),
        "canonico": ("auth", "add_auth", 437, "CanonicalRequest:\n%s", (value,)),
        "assinatura_base": ("auth", "add_auth", 439, "StringToSign:\n%s", (value,)),
        "assinatura": ("auth", "add_auth", 441, "Signature:\n%s", (value,)),
        "handler": (
            "hooks",
            "_emit",
            238,
            "Event %s: calling handler %s",
            ("before-send.dynamodb.TransactWriteItems", f"<handler at 0x{value}>"),
        ),
    }
    module, function, line, message, args = vectors[family]
    return LogRecord(
        name=f"botocore.{module}",
        level=DEBUG,
        pathname=str(Path(botocore.__file__).resolve().parent / f"{module}.py"),
        lineno=line,
        msg=message,
        args=args,
        exc_info=None,
        func=function,
    )


PRIVACY_POSITIVE_LOGS = [
    pytest.param(privacy_protocol_record("pedido", f"abc {FINGERPRINT}"), id="pedido"),
    pytest.param(privacy_protocol_record("corpo", f"abc {FINGERPRINT}"), id="corpo"),
    pytest.param(privacy_protocol_record("preparado", "0123abc456"), id="preparado"),
    pytest.param(
        privacy_protocol_record("cabecalhos", "00000000-0000-4000-8000-000000000abc"),
        id="cabecalhos",
    ),
    pytest.param(privacy_protocol_record("canonico", "0123abc456"), id="canonico"),
    pytest.param(privacy_protocol_record("assinatura_base", "0123abc456"), id="assinatura-base"),
    pytest.param(privacy_protocol_record("assinatura", "0123abc456"), id="assinatura"),
    pytest.param(privacy_protocol_record("handler", "abc"), id="handler"),
]
PRIVACY_NEGATIVE_CASES = [
    "produto-nomeado",
    "produto-root",
    "produto-nome-sdk",
    "basename-fora-sdk",
    "mensagem-desconhecida",
    "logger-errado",
    "funcao-errada",
    "linha-errada",
    "nivel-info",
    "pathname-vazio",
    "fingerprint-preparado",
    "fingerprint-cabecalhos",
    "fingerprint-canonico",
    "fingerprint-assinatura_base",
    "fingerprint-assinatura",
    "fingerprint-handler",
    "canario-erro",
    "canario-segredo",
    "canario-registro",
    "canario-fingerprint",
    "exc_info-token",
    "exc_info-fingerprint",
    "exc_text-token",
    "exc_text-fingerprint",
    "stack_info-token",
    "stack_info-fingerprint",
]


def _provenance_negative(case: str) -> LogRecord:
    record = privacy_protocol_record("pedido", TOKEN)
    product = str(Path(__file__).parents[2] / "src/cnes_infra/auth/dynamodb_refresh_tokens.py")
    changes = {
        "produto-nomeado": {
            "pathname": product,
            "name": "cnes_infra.auth.refresh",
            "args": ("TransactWriteItems", {"body": FINGERPRINT}),
        },
        "produto-root": {"pathname": product, "name": "root"},
        "produto-nome-sdk": {
            "pathname": product,
            "args": ("TransactWriteItems", {"body": FINGERPRINT}),
        },
        "basename-fora-sdk": {"pathname": str(Path(__file__).parent / "endpoint.py")},
        "mensagem-desconhecida": {
            "msg": "unknown %s %s",
            "args": ("TransactWriteItems", {"body": FINGERPRINT}),
        },
        "logger-errado": {"name": "botocore.other"},
        "funcao-errada": {"funcName": "other_function"},
        "linha-errada": {"lineno": 115},
        "nivel-info": {"levelno": INFO},
        "pathname-vazio": {"pathname": ""},
    }
    for field, value in changes[case].items():
        setattr(record, field, value)
    return record


def privacy_negative_record(case: str) -> LogRecord:
    kind, detail = case.split("-", 1)
    if kind == "fingerprint":
        return privacy_protocol_record(detail, FINGERPRINT)
    if kind == "canario":
        canaries = {
            "erro": CANARY,
            "segredo": SECRET_CANARY,
            "registro": RECORD_CANARY,
            "fingerprint": FINGERPRINT_CANARY,
        }
        return privacy_protocol_record("corpo", canaries[detail])
    if kind in {"exc_info", "exc_text", "stack_info"}:
        record = privacy_protocol_record("pedido", "clean-body")
        value = TOKEN if detail == "token" else FINGERPRINT
        attached = (RuntimeError, RuntimeError(value), None) if kind == "exc_info" else value
        setattr(record, kind, attached)
        return record
    return _provenance_negative(case)
