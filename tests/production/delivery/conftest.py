"""Ferramentas dos contratos de entrega: obrigatórias em CI."""

from __future__ import annotations

import os
from shutil import which

import pytest


def require_tool(name: str) -> str:
    executable = which(name)
    if executable:
        return executable
    if os.environ.get("CI", "").lower() == "true":
        pytest.fail(f"delivery_tool_missing={name}")
    pytest.skip(f"delivery_tool_missing={name}")
