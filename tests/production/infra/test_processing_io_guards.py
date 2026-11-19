"""Bootstrap real do SDK preserva as recusas públicas sem alocar sockets."""

import builtins
import importlib
import io
import json
import socket
import sys
import sysconfig
from collections.abc import Callable
from contextlib import AbstractContextManager, ExitStack
from pathlib import Path
from types import ModuleType
from typing import cast
from unittest.mock import patch

import pytest

from tests.production.infra._fresh_process import run_fresh_process

STRICT = "setup_error processing_import_external_io"
SDK_ROOTS = ("boto3", "botocore", "s3transfer", "urllib3", "dotenv")
SUT_ROOTS = ("data_processor", "cnes_contracts", "cnes_domain", "cnes_infra")
Attempt = dict[str, str]
Boundary = tuple[str, Callable[[], object]]
Restoration = tuple[object, str, object]


def _socket_sentinel(events: list[str]) -> None:
    watched = {"socket.__new__", "socket.bind", "socket.connect", "socket.getaddrinfo"}

    def refuse(event: str, _arguments: tuple[object, ...]) -> None:
        if event in watched:
            events.append(event)
            pytest.fail(f"setup_error processing_real_socket_audit={event}")

    sys.addaudithook(refuse)


def _socket_originals() -> list[Restoration]:
    targets = [(socket, name) for name in ("socket", "SocketType", "getaddrinfo")]
    targets += [(socket, name) for name in ("create_connection", "gethostbyname")]
    targets += [(socket, name) for name in ("gethostbyname_ex", "gethostbyaddr", "getnameinfo")]
    targets += [(socket.socket, name) for name in ("__new__", "connect", "connect_ex")]
    targets += [(socket.socket, name) for name in ("sendto", "sendmsg")]
    targets += [(builtins, "open"), (io, "open")]
    return [(owner, name, getattr(owner, name)) for owner, name in targets]


def _sdk_originals(name: str, module: ModuleType) -> list[Restoration]:
    targets: list[tuple[object, str]] = []
    if name == "boto3":
        targets = [(module, "client"), (module, "resource")]
        targets += [(module.session.Session, item) for item in ("__init__", "client", "resource")]
    elif name == "botocore.session":
        targets = [(module.Session, item) for item in ("__init__", "create_client")]
    elif name == "botocore.client":
        targets = [(module.BaseClient, "_make_api_call")]
    elif name in ("dotenv", "dotenv.main"):
        targets = [(module, item) for item in ("load_dotenv", "dotenv_values", "find_dotenv")]
        if name == "dotenv.main":
            targets.append((module.DotEnv, "__init__"))
    return [(owner, item, getattr(owner, item)) for owner, item in targets]


def _assert_restored(originals: list[Restoration]) -> None:
    for owner, name, original in originals:
        assert getattr(owner, name) is original, f"processing_guard_restore={name}"


def _refused(
    operation: str,
    invoke: Callable[[], object],
    attempts: list[Attempt],
    marker: str = STRICT,
) -> None:
    attempts.append({"operation": operation, "expected": marker})
    with pytest.raises(pytest.fail.Exception, match=marker):
        invoke()
    attempts[-1]["outcome"] = "strict_refusal"


def _genuine(module: ModuleType, relative: str, category: str = "purelib") -> None:
    origin = Path(str(module.__file__)).resolve()
    expected = Path(sysconfig.get_paths()[category]) / relative
    assert origin == expected.resolve(), f"processing_dependency_origin={module.__name__}"


def _socket_boundaries(
    warm: ModuleType, warm_classes: tuple[type, type], captured: list[type]
) -> list[Boundary]:
    cold = importlib.import_module("ssl")
    assert cold is not warm, "processing_ssl_cold_module"
    _genuine(cold, "ssl.py", "stdlib")
    factories = [("socket", socket.socket), ("SocketType", socket.SocketType)]
    factories += [("warm_ssl.socket", warm_classes[0]), ("warm_ssl.SSLSocket", warm_classes[1])]
    factories += [("cold_ssl.socket", cold.socket), ("cold_ssl.SSLSocket", cold.SSLSocket)]
    factories += [(f"bootstrap_alias_{index}", item) for index, item in enumerate(captured)]
    boundaries: list[Boundary] = [
        (name, lambda factory=factory: factory(socket.AF_INET6)) for name, factory in factories
    ]
    boundaries += [
        ("socket_ipv4", lambda: socket.socket(socket.AF_INET)),
        ("socket_fileno", lambda: socket.socket(fileno=0)),
        ("dns", lambda: socket.getaddrinfo("example.invalid", 443)),
        ("create_connection", lambda: socket.create_connection(("127.0.0.1", 9))),
        ("connect", lambda: socket.socket.connect(cast("socket.socket", None), ("127.0.0.1", 9))),
    ]
    return boundaries


def _sdk_boundaries() -> list[Boundary]:
    boto = importlib.import_module("boto3")
    session = importlib.import_module("botocore.session")
    dotenv = importlib.import_module("dotenv")
    _genuine(boto, "boto3/__init__.py")
    _genuine(session, "botocore/session.py")
    _genuine(dotenv, "dotenv/__init__.py")
    _genuine(importlib.import_module("urllib3"), "urllib3/__init__.py")
    assert boto.DEFAULT_SESSION is None, "processing_unexpected_default_session"
    return [
        ("boto3.Session", lambda: boto.Session()),
        ("botocore.Session", lambda: session.Session()),
        ("boto3.client", lambda: boto.client("s3")),
        ("boto3.resource", lambda: boto.resource("s3")),
        ("dotenv.load_dotenv", lambda: dotenv.load_dotenv()),
        ("dotenv.dotenv_values", lambda: dotenv.dotenv_values()),
        ("dotenv.find_dotenv", lambda: dotenv.find_dotenv()),
    ]


def _open_env(opener: Callable[[str], AbstractContextManager[object]]) -> None:
    with opener(".env.processing"):
        pass


def _bootstrap_forbidden_contract(attempts: list[Attempt]) -> list[str]:
    from tests.production.infra.test_processing_runtime import _io_guards

    calls: list[str] = []
    observed: list[str] = []
    original_import = importlib.import_module

    def observe(name: str, package: str | None = None) -> ModuleType:
        if name == "boto3":
            observed.append(name)
            boundaries: list[Boundary] = [
                ("bootstrap_ipv4", lambda: socket.socket(socket.AF_INET)),
                ("bootstrap_fileno", lambda: socket.socket(socket.AF_INET6, fileno=0)),
                ("bootstrap_type", lambda: socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)),
                ("bootstrap_protocol", lambda: socket.socket(socket.AF_INET6, proto=17)),
                (
                    "bootstrap_type_protocol",
                    lambda: socket.socket(socket.AF_INET6, socket.SOCK_DGRAM, 17),
                ),
            ]
            for operation, invoke in boundaries:
                _refused(operation, invoke, attempts)
            raise ImportError("processing_bootstrap_forbidden_calibration")
        return original_import(name, package)

    with ExitStack() as stack:
        with patch.object(importlib, "import_module", observe):
            with pytest.raises(ImportError, match="processing_bootstrap_forbidden_calibration"):
                _io_guards(stack, calls)
        assert observed == ["boto3"], "processing_bootstrap_calibration_phase"
        assert calls == ["external_io"] * 5, "processing_bootstrap_forbidden_ledger"
        assert not [name for name in sys.modules if name.split(".")[0] in SDK_ROOTS]
    return calls


def _session_fallback(factory: str, attempts: list[Attempt]) -> Callable[..., None]:
    def refuse(*_args: object, **_kwargs: object) -> None:
        attempts.append(
            {
                "operation": factory + ".Session_fallback",
                "expected": "unreachable",
                "outcome": "nonallocating_refusal",
            }
        )
        pytest.fail(f"setup_error processing_factory_session_fallback={factory}")

    return refuse


def _factory_probe(
    boto: ModuleType, name: str, attempts: list[Attempt], fallbacks: list[Attempt]
) -> None:
    with patch.object(boto, "Session", _session_fallback(name, fallbacks)):
        _refused("factory." + name, lambda: getattr(boto, name)("s3"), attempts)


def _factory_contract(attempts: list[Attempt], fallbacks: list[Attempt]) -> list[str]:
    from tests.production.infra.test_processing_runtime import _io_guards

    boto = importlib.import_module("boto3")
    original_session = boto.Session
    calls: list[str] = []
    assert boto.DEFAULT_SESSION is None, "processing_factory_existing_session"
    try:
        with ExitStack() as stack:
            _io_guards(stack, calls)
            for name in ("client", "resource"):
                _factory_probe(boto, name, attempts, fallbacks)
            assert calls == ["external_io"] * 2, "processing_factory_refusal_ledger"
            assert not fallbacks, "processing_factory_session_fallback_reached"
    finally:
        assert boto.Session is original_session, "processing_factory_session_restore"
        assert boto.DEFAULT_SESSION is None, "processing_factory_default_session_created"
    return calls


def _bootstrap_contract(
    warm: ModuleType, warm_classes: tuple[type, type], attempts: list[Attempt]
) -> list[str]:
    from tests.production.infra.test_processing_runtime import _io_guards

    calls: list[str] = []
    captured: list[type] = []
    sdk_originals: list[Restoration] = []
    original_import = importlib.import_module

    def observe(name: str, package: str | None = None) -> ModuleType:
        if name == "boto3":
            captured.extend((socket.socket, socket.SocketType))
        module = original_import(name, package)
        sdk_originals.extend(_sdk_originals(name, module))
        return module

    try:
        with ExitStack() as stack:
            with patch.object(importlib, "import_module", observe):
                _io_guards(stack, calls)
            assert not calls, "processing_bootstrap_polluted_strict_ledger"
            assert len(captured) == 2, "processing_bootstrap_alias_count"
            boundaries = _socket_boundaries(warm, warm_classes, captured) + _sdk_boundaries()
            for name, invoke in boundaries:
                _refused(name, invoke, attempts)
            assert calls == ["external_io"] * len(boundaries), "processing_guard_attempt_ledger"
            _refused(
                "builtins.env",
                lambda: _open_env(builtins.open),
                attempts,
                "setup_error processing_import_dotenv_file",
            )
            _refused(
                "io.env",
                lambda: _open_env(io.open),
                attempts,
                "setup_error processing_import_dotenv_file",
            )
            assert calls == ["external_io"] * len(boundaries) + ["dotenv_file"] * 2
    finally:
        _assert_restored(sdk_originals)
    return calls


def _import_failure_contract(attempts: list[Attempt]) -> list[str]:
    from tests.production.infra.test_processing_runtime import _io_guards

    captured: list[type] = []
    calls: list[str] = []
    original_import = importlib.import_module

    def interrupt(name: str, package: str | None = None) -> ModuleType:
        if name == "boto3":
            captured.append(socket.socket)
            raise ImportError("processing_bootstrap_calibration")
        return original_import(name, package)

    with ExitStack() as stack:
        with patch.object(importlib, "import_module", interrupt):
            with pytest.raises(ImportError, match="processing_bootstrap_calibration"):
                _io_guards(stack, calls)
        assert len(captured) == 1, "processing_failed_bootstrap_alias_count"
        _refused("failed_bootstrap_alias", lambda: captured[0](socket.AF_INET6), attempts)
        assert calls == ["external_io"], "processing_failed_bootstrap_attempt_ledger"
    return calls


def _test_importa_sdk_real_sem_sockets_e_mantem_guardas_estritas(
    record_testsuite_property: Callable[[str, object], None],
) -> None:
    present = [name for name in sys.modules if name.split(".")[0] in SDK_ROOTS + SUT_ROOTS]
    assert not present, f"processing_bootstrap_preloaded={present}"
    assert socket.has_ipv6 is True, "processing_bootstrap_ipv6_precondition"
    events: list[str] = []
    attempts: list[Attempt] = []
    bootstrap_attempts: list[Attempt] = []
    factory_attempts: list[Attempt] = []
    factory_fallbacks: list[Attempt] = []
    _socket_sentinel(events)
    warm = importlib.import_module("ssl")
    _genuine(warm, "ssl.py", "stdlib")
    warm_classes = (warm.socket, warm.SSLSocket)
    originals = _socket_originals()
    originals += [(warm, "socket", warm_classes[0]), (warm, "SSLSocket", warm_classes[1])]
    assert sys.modules.pop("ssl") is warm, "processing_ssl_eviction"
    try:
        bootstrap_calls = _bootstrap_forbidden_contract(bootstrap_attempts)
        _assert_restored(originals)
        calls = _bootstrap_contract(warm, warm_classes, attempts)
        _assert_restored(originals)
        factory_calls = _factory_contract(factory_attempts, factory_fallbacks)
        _assert_restored(originals)
        failed_calls = _import_failure_contract(attempts)
        _assert_restored(originals)
        assert len(calls) + len(failed_calls) == len(attempts)
        assert len(bootstrap_calls) == len(bootstrap_attempts)
        assert len(factory_calls) == len(factory_attempts)
        assert not [name for name in sys.modules if name.split(".")[0] in SUT_ROOTS]
    finally:
        sys.modules["ssl"] = warm
        _assert_restored(originals)
        record_testsuite_property("processing_calibration_attempts", json.dumps(attempts))
        record_testsuite_property("processing_bootstrap_attempts", json.dumps(bootstrap_attempts))
        record_testsuite_property("processing_factory_attempts", json.dumps(factory_attempts))
        record_testsuite_property("processing_factory_fallbacks", json.dumps(factory_fallbacks))
        record_testsuite_property("processing_socket_audit_events", json.dumps(events))
        record_testsuite_property("processing_first_bootstrap_ledger", "not_caller_owned_two_args")
        assert not events, f"processing_real_socket_events={events}"


def test_importa_sdk_real_sem_sockets_e_mantem_guardas_estritas(
    tmp_path: Path,
    record_testsuite_property: Callable[[str, object], None],
) -> None:
    node = (
        "tests/production/infra/test_processing_io_guards.py::"
        "test_importa_sdk_real_sem_sockets_e_mantem_guardas_estritas"
    )
    if run_fresh_process(node, tmp_path):
        return
    _test_importa_sdk_real_sem_sockets_e_mantem_guardas_estritas(record_testsuite_property)
    record_testsuite_property("fresh_process_completed", node)
