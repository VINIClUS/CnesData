"""Script de probe dentro do container e parser fechado da linha CAPPROBE."""

# ruff: noqa: S108
import json
import re
from collections.abc import Mapping, Sequence

from tests.integration.aws_capability.contracts import ServiceFailure

MARKER = "CAPPROBE "
LOGICAL = ("/tmp", "/logs", "/data")
BODY_KEYS = frozenset({"uid", "user", "root_write", "tempfile", *LOGICAL})
MOUNT_KEYS = frozenset({"mode", "uid", "gid", "write"})
DENIAL_MARKERS = ("PermissionError", "Errno 13", "Read-only file system", "No usable temporary")
_WRITE = re.compile(r"ok|E[A-Z0-9]+")
_MODE = re.compile(r"0o[0-7]{3,4}")
_TEMPFILE = re.compile(r"ok:(/tmp|/logs|/data|other)|[A-Za-z]+Error")
_USER = re.compile(r"[a-z_][a-z0-9_-]{0,31}")

PROBE_SCRIPT = """
import errno, json, os, pwd, stat, tempfile
PATHS = {"/tmp": __TMP__, "/logs": __LOGS__, "/data": __DATA__}
ROOT = "/cap-root-probe"


def attempt(path):
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        os.close(descriptor)
        os.unlink(path)
    except OSError as error:
        return errno.errorcode.get(error.errno, "EUNKNOWN")
    return "ok"


def inspect(path):
    info = os.stat(path)
    return {
        "mode": "0o%03o" % stat.S_IMODE(info.st_mode),
        "uid": info.st_uid,
        "gid": info.st_gid,
        "write": attempt(os.path.join(path, ".cap-probe-%d" % os.getpid())),
    }


def temporary():
    try:
        with tempfile.NamedTemporaryFile() as handle:
            where = os.path.realpath(os.path.dirname(handle.name))
    except Exception as error:
        return type(error).__name__
    for logical, path in PATHS.items():
        if os.path.realpath(path) == where:
            return "ok:" + logical
    return "ok:other"


def account():
    try:
        return pwd.getpwuid(os.getuid()).pw_name
    except KeyError:
        return "unknown"


body = {logical: inspect(path) for logical, path in PATHS.items()}
body.update(
    uid=os.getuid(), user=account(), root_write=attempt(ROOT), tempfile=temporary()
)
print("CAPPROBE " + json.dumps(body, sort_keys=True))
"""


def probe_script(tmp: str = "/tmp", logs: str = "/logs", data: str = "/data") -> str:
    """Args: caminhos dos três mounts. Returns: fonte Python do entrypoint do probe."""
    script = PROBE_SCRIPT
    for token, path in (("__TMP__", tmp), ("__LOGS__", logs), ("__DATA__", data)):
        script = script.replace(token, json.dumps(path))
    return script


def parse_probe_line(output: str) -> dict[str, object]:
    """Args: saída completa do container. Returns: corpo fechado da única linha CAPPROBE.

    Raises: ServiceFailure sem ecoar o conteúdo recebido.
    """
    lines = [line for line in output.splitlines() if line.startswith(MARKER)]
    if len(lines) != 1:
        raise ServiceFailure("probe=log_invalid")
    try:
        body = json.loads(lines[0][len(MARKER) :])
    except ValueError:
        raise ServiceFailure("probe=log_invalid") from None
    if not _valid_body(body):
        raise ServiceFailure("probe=log_invalid")
    return dict(body)


def _is_int(value: object) -> bool:
    return type(value) is int


def _is_text(value: object, pattern: re.Pattern[str]) -> bool:
    return isinstance(value, str) and pattern.fullmatch(value) is not None


def _valid_mount(entry: object) -> bool:
    return (
        isinstance(entry, Mapping)
        and set(entry) == MOUNT_KEYS
        and _is_text(entry["mode"], _MODE)
        and _is_int(entry["uid"])
        and _is_int(entry["gid"])
        and _is_text(entry["write"], _WRITE)
    )


def _valid_body(body: object) -> bool:
    return (
        isinstance(body, Mapping)
        and set(body) == BODY_KEYS
        and _is_int(body["uid"])
        and _is_text(body["user"], _USER)
        and _is_text(body["root_write"], _WRITE)
        and _is_text(body["tempfile"], _TEMPFILE)
        and all(_valid_mount(body[logical]) for logical in LOGICAL)
    )


def count_denials(messages: Sequence[str]) -> int:
    """Returns: linhas com negação de disco, erro de somente leitura ou tmp inutilizável."""
    lines = [line for message in messages for line in message.splitlines()]
    return sum(any(marker in line for marker in DENIAL_MARKERS) for line in lines)
