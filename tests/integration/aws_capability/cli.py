"""Transporte CLI limitado, sem persistência de requests ou respostas sensíveis."""

import json
import math
import os
import re
import subprocess
from collections.abc import Mapping
from dataclasses import dataclass

from tests.integration.aws_capability.contracts import RequestContext


@dataclass(frozen=True)
class Request:
    service: str
    operation: str
    payload: Mapping[str, object]


class CliFailure(Exception):
    pass


@dataclass(frozen=True)
class Cli:
    region: str
    timeout: float
    environment: Mapping[str, str]
    context: RequestContext

    def invoke(self, request: Request) -> Mapping[str, object]:
        if not math.isfinite(self.timeout) or self.timeout <= 0:
            raise CliFailure("cli=invalid_timeout")
        if not all(
            re.fullmatch(r"[a-z][a-z0-9-]*", part)
            for part in (request.service, request.operation, self.region)
        ):
            raise CliFailure("cli=invalid_argument")
        self.context.budget.acquire(self.context.lane)
        timeout = min(self.timeout, self.context.budget.remaining_seconds(self.context.lane))
        if timeout <= 0:
            raise CliFailure("cli=deadline")
        result = None
        try:
            completed = subprocess.run(  # noqa: S603
                self.argv(request, timeout),
                input=json.dumps(request.payload),
                capture_output=True,
                text=True,
                timeout=timeout,
                check=False,
                shell=False,
                env=self.child_environment(),
            )
            if completed.returncode == 0:
                result = json.loads(completed.stdout)
        except (OSError, subprocess.SubprocessError, ValueError, TypeError):
            pass
        if not isinstance(result, dict):
            raise CliFailure("cli=operation_failed")
        return result

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
            "--no-cli-pager",
            "--no-cli-auto-prompt",
            "--no-paginate",
            "--cli-connect-timeout",
            seconds,
            "--cli-read-timeout",
            seconds,
        ]

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
