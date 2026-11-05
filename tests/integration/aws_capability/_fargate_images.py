"""Porta de imagem real: docker com DOCKER_CONFIG temporário e token só por stdin."""

import os
import re
import shutil
import subprocess
import tempfile
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from tests.integration.aws_capability._service_contracts import RegistryLogin
from tests.integration.aws_capability.contracts import (
    BudgetExhausted,
    RequestContext,
    ServiceFailure,
)

SHARED_MEMORY = Path("/dev/shm")  # noqa: S108
COMMAND_SECONDS = 300.0
INHERITED = ("PATH", "HOME", "DOCKER_HOST", "XDG_RUNTIME_DIR", "TMPDIR")
DIGEST = re.compile(r"digest: (sha256:[0-9a-f]{64})")


@dataclass(frozen=True)
class DockerImages:
    run: Callable[..., Any] = field(default=subprocess.run, repr=False)
    root: Path = SHARED_MEMORY

    def push(
        self, image: str, target: str, login: RegistryLogin, context: RequestContext
    ) -> str:
        """Returns: digest publicado; Raises: ServiceFailure sem expor o token."""
        with self._private_config() as environment:
            login_argv = ["docker", "login", "--username", login.username]
            login_argv += ["--password-stdin", login.registry]
            self._execute("login", login_argv, environment, context, login.token)
            self._execute("tag", ["docker", "tag", image, target], environment, context)
            output = self._execute("push", ["docker", "push", target], environment, context)
            digest = DIGEST.findall(output)
            if not digest:
                raise ServiceFailure("service=docker_push_digest_missing")
            self._best_effort(["docker", "rmi", target], environment, context)
            return digest[-1]

    @contextmanager
    def _private_config(self) -> Iterator[dict[str, str]]:
        directory = Path(tempfile.mkdtemp(prefix="cnesdata-cap-docker-", dir=self.root))
        try:
            environment = {k: os.environ[k] for k in INHERITED if k in os.environ}
            environment["DOCKER_CONFIG"] = str(directory)
            yield environment
        finally:
            shutil.rmtree(directory, ignore_errors=True)

    def _execute(
        self,
        verb: str,
        argv: list[str],
        environment: dict[str, str],
        context: RequestContext,
        secret: str | None = None,
    ) -> str:
        timeout = min(COMMAND_SECONDS, context.budget.remaining_seconds(context.lane))
        if timeout <= 0:
            raise BudgetExhausted("budget=deadline")
        options: dict[str, Any] = {
            "env": environment, "capture_output": True, "text": True,
            "timeout": timeout, "check": False, "shell": False,
        }
        if secret is not None:
            options["input"] = secret + "\n"
        try:
            done = self.run(argv, **options)
        except (subprocess.SubprocessError, OSError):
            raise ServiceFailure(f"service=docker_{verb}_failed") from None
        if done.returncode != 0:
            raise ServiceFailure(f"service=docker_{verb}_failed")
        return str(done.stdout) + str(done.stderr)

    def _best_effort(
        self, argv: list[str], environment: dict[str, str], context: RequestContext
    ) -> None:
        try:
            self._execute("rmi", argv, environment, context)
        except (ServiceFailure, BudgetExhausted):
            return
