"""Dublês de fronteira boto3/OIDC da suíte AWS-014: gravam chamadas e injetam falhas."""
from __future__ import annotations

import json
import os
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import boto3
from botocore.exceptions import ClientError

from cnes_infra.auth.oidc import OidcPrincipal

if TYPE_CHECKING:
    from collections.abc import Iterator

    from boto3.session import Session

    from cnes_domain.control_plane.commands import BindRunDispatch
    from cnes_domain.control_plane.entities import RunDispatch
    from cnes_domain.ports.control_plane import ControlPlanePort

_PAGE_KEYS_IGNORED = frozenset({"ExclusiveStartKey", "Limit"})


def client_error(code: str, operation: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": code}}, operation)


class _Delegating:
    def __init__(self, delegate: Any) -> None:
        self._delegate = delegate

    def __getattr__(self, name: str) -> Any:
        return getattr(self._delegate, name)


@dataclass(slots=True)
class _Execution:
    input: str
    status: str


def _execution_arn(state_machine_arn: str, name: str) -> str:
    prefix, _, machine = state_machine_arn.rpartition(":stateMachine:")
    return f"{prefix}:execution:{machine}:{name}"


class RecordingStepFunctionsClient(_Delegating):
    """Start/Stop/DescribeExecution in-process com a semântica Standard; o resto vai ao emulador."""

    def __init__(self, delegate: Any) -> None:
        super().__init__(delegate)
        self._executions: dict[str, _Execution] = {}
        self._throttled_starts = 0
        self.started: list[tuple[str, str]] = []
        self.stopped: list[str] = []

    def start_execution(self, **request: Any) -> dict[str, Any]:
        if self._throttled_starts:
            self._throttled_starts -= 1
            raise client_error("ThrottlingException", "StartExecution")
        arn = _execution_arn(request["stateMachineArn"], request["name"])
        existing = self._executions.get(arn)
        if existing is None:
            self._executions[arn] = _Execution(request["input"], "RUNNING")
            self.started.append((arn, request["input"]))
        elif (existing.status, existing.input) != ("RUNNING", request["input"]):
            raise client_error("ExecutionAlreadyExists", "StartExecution")
        return {"executionArn": arn}

    def stop_execution(self, **request: Any) -> dict[str, Any]:
        execution = self._require(request["executionArn"], "StopExecution")
        self.stopped.append(request["executionArn"])
        if execution.status == "RUNNING":
            execution.status = "ABORTED"
        return {}

    def describe_execution(self, **request: Any) -> dict[str, Any]:
        arn = request["executionArn"]
        execution = self._require(arn, "DescribeExecution")
        return {"executionArn": arn, "input": execution.input, "status": execution.status}

    def set_status(self, execution_arn: str, status: str) -> None:
        self._require(execution_arn, "DescribeExecution").status = status

    def throttle_next_start(self) -> None:
        self._throttled_starts += 1

    def _require(self, execution_arn: str, operation: str) -> _Execution:
        execution = self._executions.get(execution_arn)
        if execution is None:
            raise client_error("ExecutionDoesNotExist", operation)
        return execution


class RecordingS3Client(_Delegating):
    """Grava presigns e puts do cliente S3 composto; injeta falhas de put por bucket."""

    def __init__(self, delegate: Any, audit_bucket: str) -> None:
        super().__init__(delegate)
        self._audit_bucket = audit_bucket
        self._failing_bucket: str | None = None
        self._puts_before_failure = 0
        self._audit_failures = 0
        self.presigned: list[dict[str, Any]] = []
        self.puts: list[tuple[str, str]] = []
        self.injected_failures: list[tuple[str, str]] = []

    def generate_presigned_url(self, operation: str, **request: Any) -> str:
        self.presigned.append({"operation": operation, **request})
        return self._delegate.generate_presigned_url(operation, **request)

    def put_object(self, **request: Any) -> dict[str, Any]:
        target = (request["Bucket"], request["Key"])
        if self._should_fail(request["Bucket"]):
            self.injected_failures.append(target)
            raise client_error("ServiceUnavailable", "PutObject")
        response = self._delegate.put_object(**request)
        self.puts.append(target)
        return response

    @contextmanager
    def failing_puts_after(self, bucket: str, successful_puts: int) -> Iterator[None]:
        self._failing_bucket, self._puts_before_failure = bucket, successful_puts
        try:
            yield
        finally:
            self._failing_bucket = None

    @contextmanager
    def failing_audit_put_once(self) -> Iterator[None]:
        self._audit_failures = 1
        try:
            yield
        finally:
            self._audit_failures = 0

    def _should_fail(self, bucket: str) -> bool:
        if bucket == self._audit_bucket and self._audit_failures:
            self._audit_failures -= 1
            return True
        if bucket != self._failing_bucket:
            return False
        if self._puts_before_failure:
            self._puts_before_failure -= 1
            return False
        return True


class RecordingSession:
    """Sessão boto3 que entrega os mesmos dublês às duas composition roots."""

    REGION = "us-east-1"

    @classmethod
    def for_emulator(cls, audit_bucket: str) -> RecordingSession:
        delegate = boto3.Session(
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
            region_name=cls.REGION,
        )
        return cls(delegate, audit_bucket)

    def __init__(self, delegate: Session, audit_bucket: str) -> None:
        self._delegate = delegate
        self._audit_bucket = audit_bucket
        self._clients: dict[str, Any] = {}

    def client(self, service_name: str, **options: Any) -> Any:
        if service_name not in self._clients:
            client = self._delegate.client(service_name, **options)
            self._clients[service_name] = self._wrap(service_name, client)
        return self._clients[service_name]

    def _wrap(self, service_name: str, client: Any) -> Any:
        if service_name == "stepfunctions":
            return RecordingStepFunctionsClient(client)
        if service_name == "s3":
            return RecordingS3Client(client, self._audit_bucket)
        return client


def _page_key(request: dict[str, Any]) -> str:
    stable = {name: value for name, value in request.items() if name not in _PAGE_KEYS_IGNORED}
    return json.dumps(stable, sort_keys=True)


class QueryRecorder(_Delegating):
    """Captura a página de Query devolvida pelo DynamoDB Local para cada requisição."""

    def __init__(self, delegate: Any) -> None:
        super().__init__(delegate)
        self.pages: dict[str, dict[str, Any]] = {}

    def query(self, **request: Any) -> dict[str, Any]:
        response = self._delegate.query(**request)
        self.pages.setdefault(_page_key(request), deepcopy(response))
        return response


class StaleQueryClient(_Delegating):
    """Serve a página capturada antes da mudança na base; nunca espera nem muta o GSI."""

    def __init__(self, delegate: Any, pages: dict[str, dict[str, Any]]) -> None:
        super().__init__(delegate)
        self._pages = pages
        self.served_items: list[dict[str, Any]] = []

    def query(self, **request: Any) -> dict[str, Any]:
        captured = self._pages.get(_page_key(request))
        if captured is None:
            return self._delegate.query(**request)
        page = {
            name: value for name, value in deepcopy(captured).items()
            if name != "LastEvaluatedKey"
        }
        self.served_items.extend(page.get("Items", ()))
        return page


class BindFailsOnce(_Delegating):
    """Control plane real, exceto um bind_run_dispatch que levanta o erro canônico injetado."""

    def __init__(self, delegate: ControlPlanePort, error: Exception) -> None:
        super().__init__(delegate)
        self._error: Exception | None = error
        self.fired = False

    def bind_run_dispatch(self, command: BindRunDispatch) -> RunDispatch:
        if self._error is None:
            return self._delegate.bind_run_dispatch(command)
        error, self._error = self._error, None
        self.fired = True
        raise error


@dataclass(frozen=True, slots=True)
class BearerSubjectVerifier:
    """Verificador OIDC de teste: o bearer já é o subject autenticado pelo issuer."""

    issuer: str

    def verify(self, token: str) -> OidcPrincipal:
        return OidcPrincipal(issuer=self.issuer, subject=token, email=None, display_name=None)
