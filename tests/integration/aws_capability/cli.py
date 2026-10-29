"""Transporte CLI limitado, sem persistência de requests ou respostas sensíveis."""

import json
import math
import os
import re
import stat
import subprocess
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path

from tests.integration.aws_capability._service_contracts import ApiError, Reply
from tests.integration.aws_capability.contracts import RequestContext


@dataclass(frozen=True)
class Request:
    service: str
    operation: str
    payload: Mapping[str, object] = field(repr=False)


class CliFailure(Exception):
    pass


@dataclass(frozen=True)
class Cli:
    region: str
    timeout: float
    environment: Mapping[str, str] = field(repr=False)
    context: RequestContext

    def invoke(self, request: Request) -> Mapping[str, object]:
        reply = self.call(request)
        if not reply.ok:
            code = "operation_failed"
            if reply.error is not None and reply.error.code in {
                "invalid_timeout",
                "invalid_argument",
                "invalid_body",
                "deadline",
            }:
                code = reply.error.code
            raise CliFailure(f"cli={code}")
        return reply.data

    def call(self, request: Request) -> Reply:
        if not math.isfinite(self.timeout) or self.timeout <= 0:
            return _local_failure("invalid_timeout")
        if not all(
            re.fullmatch(r"[a-z][a-z0-9-]*", part)
            for part in (request.service, request.operation, self.region)
        ):
            return _local_failure("invalid_argument")
        try:
            payload = _input_payload(request)
        except (OSError, ValueError, TypeError):
            payload = None
        if payload is None:
            return _local_failure("invalid_body")
        self.context.budget.acquire(self.context.lane)
        timeout = min(self.timeout, self.context.budget.remaining_seconds(self.context.lane))
        if timeout <= 0:
            return _local_failure("deadline")
        try:
            completed = subprocess.run(  # noqa: S603
                self.argv(request, timeout),
                input=payload,
                capture_output=True,
                text=True,
                timeout=timeout,
                check=False,
                shell=False,
                env=self.child_environment(),
            )
            return _process_reply(completed, request)
        except (OSError, subprocess.SubprocessError, ValueError, TypeError):
            pass
        return _local_failure("operation_failed")

    def argv(self, request: Request, timeout: float) -> list[str]:
        seconds = str(max(1, math.floor(timeout)))
        return [
            "aws",
            request.service,
            request.operation,
            "--region",
            self.region,
            "--cli-input-json",
            "file:///dev/stdin",
            "--output",
            "json",
            "--cli-error-format",
            "json",
            "--color",
            "off",
            "--no-cli-pager",
            "--no-cli-auto-prompt",
            "--no-paginate",
            "--cli-connect-timeout",
            seconds,
            "--cli-read-timeout",
            seconds,
        ] + (["--body", str(request.payload["Body"])] if "Body" in request.payload else [])

    def child_environment(self) -> dict[str, str]:
        environment = {
            key: value
            for key, value in self.environment.items()
            if not key.startswith("AWS_ENDPOINT_URL")
            and key not in {"AWS_PROFILE", "AWS_DEFAULT_PROFILE"}
        }
        environment.update(
            {
                "AWS_REGION": self.region,
                "AWS_DEFAULT_REGION": self.region,
                "AWS_IGNORE_CONFIGURED_ENDPOINT_URLS": "true",
                "AWS_PAGER": "",
                "AWS_CLI_AUTO_PROMPT": "off",
                "AWS_MAX_ATTEMPTS": "1",
                "AWS_CONFIG_FILE": os.devnull,
                "AWS_SHARED_CREDENTIALS_FILE": os.devnull,
                "AWS_EC2_METADATA_DISABLED": "true",
            }
        )
        return environment


def _local_failure(code: str) -> Reply:
    return Reply(False, {}, ApiError(code, {}, "local"))


def _input_payload(request: Request) -> str | None:
    payload = dict(request.payload)
    if "Body" in payload:
        body = Path(__file__).parent / "fixtures" / "body.json"
        if (request.service, request.operation) != ("s3api", "put-object"):
            return None
        if payload["Body"] != str(body) or not _approved_body(body):
            return None
        del payload["Body"]
    return json.dumps(payload)


def _approved_body(body: Path) -> bool:
    if not stat.S_ISREG(body.lstat().st_mode):
        return False
    descriptor = os.open(body, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        metadata = os.fstat(descriptor)
        return (
            stat.S_ISREG(metadata.st_mode)
            and metadata.st_size == 2
            and os.read(descriptor, 3) == b"{}"
        )
    finally:
        os.close(descriptor)


def _process_reply(completed: subprocess.CompletedProcess[str], request: Request) -> Reply:
    if completed.returncode == 0:
        if not completed.stdout.strip() and (
            request.operation.startswith("delete-")
            or (request.service, request.operation) == ("cognito-idp", "update-user-pool")
            or (request.service, request.operation)
            in {("dynamodb", "transact-write-items"), ("dynamodb", "get-item")}
        ):
            return Reply(True, {}, None)
        data = json.loads(completed.stdout)
        return Reply(True, data, None) if isinstance(data, dict) else _local_failure("invalid_json")
    details = json.loads(completed.stderr)
    if not isinstance(details, dict):
        return _local_failure("invalid_error")
    code = details.get("Code")
    if not isinstance(code, str) or not code:
        return _local_failure("invalid_error")
    origin = "service" if completed.returncode == 254 else "local"
    return Reply(False, {}, ApiError(code, details, origin))
