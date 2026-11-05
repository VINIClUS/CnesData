"""Script de probe dentro do container e parser fechado da linha CAPPROBE."""
# ruff: noqa: S108

from tests.integration.aws_capability.contracts import ServiceFailure


def probe_script(tmp: str = "/tmp", logs: str = "/logs", data: str = "/data") -> str:
    """Args: caminhos dos três mounts. Returns: fonte Python do entrypoint do probe."""
    raise ServiceFailure("service=not_implemented")


def parse_probe_line(output: str) -> dict[str, object]:
    """Args: saída completa do container. Returns: corpo fechado da única linha CAPPROBE."""
    raise ServiceFailure("service=not_implemented")
