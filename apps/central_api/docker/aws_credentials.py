#!/usr/local/bin/python3 -IS
"""credential_process da imagem: valida o JSON escrito pelo host e falha fechado com key=value."""

from __future__ import annotations

import errno
import json
import os
import re
import stat
import sys
import time
from datetime import UTC, datetime

SIZE_LIMIT = 8192
MARGIN_SECONDS = 960
MAX_LIFETIME_SECONDS = 3600
REFUSAL_EXIT = 2
FIELDS = frozenset({"Version", "AccessKeyId", "SecretAccessKey", "SessionToken", "Expiration"})
ACCESS_KEY = re.compile(r"ASIA[0-9A-Z]{16}")
INSTANT = re.compile(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ")
INSTANT_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
OPEN_FLAGS = os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK


class Refused(Exception):
    pass


def _open(path: str) -> int:
    try:
        return os.open(path, OPEN_FLAGS)
    except (FileNotFoundError, NotADirectoryError):
        raise Refused("credentials_json=missing") from None
    except OSError as error:
        code = "symlink" if error.errno == errno.ELOOP else "unreadable"
        raise Refused(f"credentials_json={code}") from None


def _require_private_regular(mode: int) -> None:
    if not stat.S_ISREG(mode):
        raise Refused("credentials_json=not_regular")
    if mode & 0o007:
        raise Refused("credentials_json=insecure_mode")


def read(path: str) -> bytes:
    descriptor = _open(path)
    try:
        _require_private_regular(os.fstat(descriptor).st_mode)
    except Refused:
        os.close(descriptor)
        raise
    with os.fdopen(descriptor, "rb") as handle:
        raw = handle.read(SIZE_LIMIT + 1)
    if not 0 < len(raw) <= SIZE_LIMIT:
        raise Refused("credentials_json=size")
    return raw


def _unique(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise Refused("credentials_json=duplicate_key")
        result[key] = value
    return result


def parse(raw: bytes) -> dict[str, object]:
    try:
        value: object = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique)
    except (UnicodeDecodeError, ValueError):
        raise Refused("credentials_json=invalid_json") from None
    if not isinstance(value, dict) or set(value) != FIELDS:
        raise Refused("credentials_json=fields")
    return value


def _require_text(value: object, code: str) -> None:
    if not isinstance(value, str) or not value:
        raise Refused(f"credentials_json={code}")


def _expires_at(value: object) -> float:
    if not isinstance(value, str) or INSTANT.fullmatch(value) is None:
        raise Refused("credentials_json=expiration_format")
    try:
        moment = datetime.strptime(value, INSTANT_FORMAT).replace(tzinfo=UTC)
    except ValueError:
        raise Refused("credentials_json=expiration_format") from None
    return moment.timestamp()


def validate(document: dict[str, object], now: float) -> None:
    version = document["Version"]
    if type(version) is not int or version != 1:
        raise Refused("credentials_json=version")
    access_key = document["AccessKeyId"]
    if not isinstance(access_key, str) or ACCESS_KEY.fullmatch(access_key) is None:
        raise Refused("credentials_json=access_key")
    _require_text(document["SecretAccessKey"], "secret_access_key")
    _require_text(document["SessionToken"], "session_token")
    expires = _expires_at(document["Expiration"])
    if not now + MARGIN_SECONDS < expires <= now + MAX_LIFETIME_SECONDS:
        raise Refused("credentials_json=expiration_window")


def main(argv: list[str]) -> int:
    if len(argv) != 2 or not argv[1].startswith("/"):
        sys.stderr.write("credentials_json=usage\n")
        return REFUSAL_EXIT
    path = argv[1]
    try:
        document = parse(read(path))
        validate(document, time.time())
    except Refused as refusal:
        sys.stderr.write(f"{refusal} path={path}\n")
        return REFUSAL_EXIT
    canonical = {key: document[key] for key in sorted(FIELDS)}
    sys.stdout.write(json.dumps(canonical, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
