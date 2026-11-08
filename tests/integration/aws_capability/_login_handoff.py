"""Handoff privado com URL, usuário e senha sintéticos para o humano."""

import os
import re
import secrets
import tempfile
from pathlib import Path

from tests.integration.aws_capability.contracts import RunSpec

_LOWER, _UPPER, _DIGIT = "abcdefghijkmnopqrstuvwxyz", "ABCDEFGHJKLMNPQRSTUVWXYZ", "23456789"
_SYMBOL = "!#$%^*-_+"
_ALL = _LOWER + _UPPER + _DIGIT + _SYMBOL


def handoff_path(spec: RunSpec) -> Path:
    return spec.journal_dir / f"{spec.run_id}.login"


def new_password() -> str:
    chosen = [secrets.choice(group) for group in (_LOWER, _UPPER, _DIGIT, _SYMBOL)]
    chosen += [secrets.choice(_ALL) for _ in range(20)]
    secrets.SystemRandom().shuffle(chosen)
    return "".join(chosen)


def write_handoff(spec: RunSpec, url: str, username: str, password: str) -> None:
    values = (("url", url), ("username", username), ("password", password))
    if any("\n" in value or "\r" in value for _, value in values):
        raise ValueError("handoff=invalid_field")
    content = "".join(f"{key}={value}\n" for key, value in values).encode("utf-8")
    descriptor, temporary = tempfile.mkstemp(
        dir=spec.journal_dir, prefix=f".handoff-{spec.run_id}-"
    )
    try:
        os.write(descriptor, content)
        os.fsync(descriptor)
        os.replace(temporary, handoff_path(spec))
    finally:
        os.close(descriptor)
        Path(temporary).unlink(missing_ok=True)


def remove_handoff(spec: RunSpec) -> None:
    if re.fullmatch(r"[0-9a-f]{8}", spec.run_id) is not None:
        handoff_path(spec).unlink(missing_ok=True)
        for orphan in spec.journal_dir.glob(f".handoff-{spec.run_id}-*"):
            orphan.unlink(missing_ok=True)
