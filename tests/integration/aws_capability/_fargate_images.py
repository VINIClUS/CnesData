"""Porta de imagem real: docker com DOCKER_CONFIG temporário e token só por stdin."""

import subprocess
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from tests.integration.aws_capability._service_contracts import RegistryLogin
from tests.integration.aws_capability.contracts import RequestContext, ServiceFailure

SHARED_MEMORY = Path("/dev/shm")  # noqa: S108


@dataclass(frozen=True)
class DockerImages:
    run: Callable[..., Any] = field(default=subprocess.run, repr=False)
    root: Path = SHARED_MEMORY

    def push(
        self, image: str, target: str, login: RegistryLogin, context: RequestContext
    ) -> str:
        """Returns: digest publicado; Raises: ServiceFailure sem expor o token."""
        raise ServiceFailure("service=not_implemented")
