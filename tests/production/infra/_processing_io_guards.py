"""Recusas de I/O preservam o bootstrap genuíno e os aliases de socket."""

import builtins
import importlib
import io
import os
import socket
from collections.abc import Callable
from contextlib import ExitStack
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest


def _socket_refusal(
    calls: list[str], bootstrap: list[dict[str, object]], phase: list[bool]
) -> Callable[..., Any]:
    deny = _denied(calls)

    def refuse(_cls: type, *args: Any, **kwargs: Any) -> Any:
        if phase[0] and args == (socket.AF_INET6,) and not kwargs:
            bootstrap.append(
                {
                    "phase": "dependency-bootstrap",
                    "operation": "socket.construct",
                    "family": "AF_INET6",
                    "outcome": "refused_oserror",
                }
            )
            raise OSError("processing_dependency_ipv6=unavailable")
        return deny(*args, **kwargs)

    return refuse


def _denied(calls: list[str]) -> Callable[..., Any]:
    def deny(*_args: Any, **_kwargs: Any) -> Any:
        calls.append("external_io")
        pytest.fail("setup_error processing_import_external_io")

    return deny


def _guard_open(original: Callable[..., Any], calls: list[str]) -> Callable[..., Any]:
    def guarded(file: Any, *args: Any, **kwargs: Any) -> Any:
        if isinstance(file, (str, os.PathLike)) and Path(file).name.startswith(".env"):
            calls.append("dotenv_file")
            pytest.fail("setup_error processing_import_dotenv_file")
        return original(file, *args, **kwargs)

    return guarded


def _io_guards(
    stack: ExitStack, calls: list[str], bootstrap: list[dict[str, object]] | None = None
) -> None:
    deny = _denied(calls)
    phase = [True]
    refuse = _socket_refusal(calls, bootstrap if bootstrap is not None else [], phase)
    original_socket = socket.socket
    for name in (
        "create_connection",
        "getaddrinfo",
        "gethostbyname",
        "gethostbyname_ex",
        "gethostbyaddr",
        "getnameinfo",
    ):
        stack.enter_context(patch.object(socket, name, deny))
    for name in ("connect", "connect_ex", "sendto", "sendmsg"):
        stack.enter_context(patch.object(socket.socket, name, deny))
    stack.enter_context(patch.object(original_socket, "__new__", staticmethod(refuse)))
    for name in ("socket", "SocketType"):
        stack.enter_context(patch.object(socket, name, original_socket))
    stack.enter_context(patch.object(builtins, "open", _guard_open(builtins.open, calls)))
    stack.enter_context(patch.object(io, "open", _guard_open(io.open, calls)))
    try:
        boto = importlib.import_module("boto3")
        if boto.DEFAULT_SESSION is not None:
            pytest.fail("setup_error processing_existing_default_session")
        botocore_session = importlib.import_module("botocore.session")
        botocore_client = importlib.import_module("botocore.client")
        dotenv = importlib.import_module("dotenv")
        dotenv_main = importlib.import_module("dotenv.main")
    finally:
        phase[0] = False
    for owner in (boto.session.Session, botocore_session.Session):
        stack.enter_context(patch.object(owner, "__init__", deny))
    for owner, name in (
        (boto, "client"),
        (boto, "resource"),
        (boto.session.Session, "client"),
        (boto.session.Session, "resource"),
        (botocore_session.Session, "create_client"),
        (botocore_client.BaseClient, "_make_api_call"),
        (dotenv_main.DotEnv, "__init__"),
    ):
        stack.enter_context(patch.object(owner, name, deny))
    for module in (dotenv, dotenv_main):
        for name in ("load_dotenv", "dotenv_values", "find_dotenv"):
            stack.enter_context(patch.object(module, name, deny))
