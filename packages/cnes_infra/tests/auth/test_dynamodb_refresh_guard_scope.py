"""O plugin de refresh preserva as fronteiras de módulos que não o declaram."""

import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from xml.etree import ElementTree

import pytest

_PLUGIN = "packages.cnes_infra.tests.auth.dynamodb_refresh_support"
_PRIMITIVES = ["create_connection", "getaddrinfo", "connect", "connect_ex", "sendto"]
_RUNNER = """import hashlib
import inspect
import json
import os
from pathlib import Path
import socket
import sys

case = Path(sys.argv[1]).resolve()
config = json.loads((case / "config.json").read_text())
root = Path(config["root"]).resolve()
state = {"sentinel_calls": [], "reports": [], "bindings": {}}

def sentinel(name):
    def invoke(*args, **kwargs):
        state["sentinel_calls"].append({"primitive": name, "args": repr(args)})
        return "native:" + name
    return invoke

for name in ("create_connection", "getaddrinfo"):
    setattr(socket, name, sentinel(name))
for name in ("connect", "connect_ex", "sendto"):
    setattr(socket.socket, name, sentinel(name))

import pytest

class Evidence:
    def pytest_collection_finish(self, session):
        plugin = session.config.pluginmanager.getplugin(config["plugin"])
        state["helper_registered"] = plugin is not None
        state["helper_path"] = str(Path(plugin.__file__).resolve()) if plugin else None
        state["items"] = [{"nodeid": item.nodeid,
            "function_path": inspect.getsourcefile(item.function)} for item in session.items]

    def pytest_runtest_logreport(self, report):
        state["reports"].append({"nodeid": report.nodeid, "when": report.when,
            "outcome": report.outcome, "longrepr": str(report.longrepr)})

try:
    rc = pytest.main(config["pytest_args"], plugins=[Evidence()])
finally:
    for name, module in sorted(sys.modules.items()):
        file = getattr(module, "__file__", None)
        if not file:
            continue
        path = Path(file).resolve()
        relevant = name.startswith(("cnes_", "packages.cnes_", "test_owner", "test_foreign"))
        if path.is_file() and (relevant or path.is_relative_to(case)):
            state["bindings"][name] = {"path": str(path),
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "within_checkout": path.is_relative_to(root)}
    (case / "runtime.json").write_text(json.dumps(state, indent=2) + "\\n")
raise SystemExit(rc)
"""
_OWNER = """import json
import socket
import pytest

pytest_plugins = ("packages.cnes_infra.tests.auth.dynamodb_refresh_support",)

def test_modulo_dono_bloqueia_cinco_primitivas(record_property):
    functions = [("create_connection", lambda: socket.create_connection(("localhost", 5433))),
        ("getaddrinfo", lambda: socket.getaddrinfo("localhost", 5433))]
    with socket.socket() as sock:
        functions += [("connect", lambda: sock.connect(("localhost", 5433))),
            ("connect_ex", lambda: sock.connect_ex(("localhost", 5433))),
            ("sendto", lambda: sock.sendto(b"probe", ("localhost", 5433)))]
        blocked = []
        for name, invoke in functions:
            with pytest.raises(AssertionError, match="^offline_socket_attempt$"):
                invoke()
            blocked.append(name)
    record_property("blocked_primitives", json.dumps(blocked))
    assert len(blocked) == 5
"""
_FOREIGN = """import json
import os
from pathlib import Path
import socket

def test_modulo_alheio_preserva_fronteiras(record_property):
    config = json.loads((Path(os.environ["SCOPE_CASE_DIR"]) / "config.json").read_text())
    endpoint = ("localhost", config["port"])
    functions = [("create_connection", lambda: socket.create_connection(endpoint)),
        ("getaddrinfo", lambda: socket.getaddrinfo(*endpoint))]
    with socket.socket() as sock:
        functions += [("connect", lambda: sock.connect(endpoint)),
            ("connect_ex", lambda: sock.connect_ex(endpoint)),
            ("sendto", lambda: sock.sendto(b"probe", endpoint))]
        outcomes = []
        for name, invoke in functions:
            try:
                outcomes.append({"primitive": name, "result": invoke(), "error": None})
            except AssertionError as error:
                outcomes.append({"primitive": name, "result": None, "error": str(error)})
    incoming = config["incoming_env"]
    actual = {name: os.environ.get(name) for name in incoming}
    record_property("native_outcomes", json.dumps(outcomes, sort_keys=True))
    record_property("incoming_env", json.dumps(incoming, sort_keys=True))
    record_property("actual_env", json.dumps(actual, sort_keys=True))
    assert [outcome["error"] for outcome in outcomes] == [None] * 5
    assert [outcome["result"] for outcome in outcomes] == [
        "native:create_connection", "native:getaddrinfo", "native:connect",
        "native:connect_ex", "native:sendto"]
    assert actual == incoming
"""


def _environment(root: Path, directory: Path) -> dict[str, str]:
    sources = [
        "packages/cnes_contracts/src",
        "packages/cnes_domain/src",
        "packages/cnes_infra/src",
        ".",
    ]
    return {
        "PATH": str(Path(sys.executable).parent) + os.pathsep + os.defpath,
        "HOME": str(directory),
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "PYTHONPATH": os.pathsep.join(str(root / source) for source in sources),
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
        "PYTHON_DOTENV_DISABLED": "1",
        "BILLING_MODE": "disabled",
        "AWS_ACCESS_KEY_ID": "incoming-synthetic-key",
        "AWS_SECRET_ACCESS_KEY": "incoming-synthetic-secret",
        "AWS_SESSION_TOKEN": "incoming-synthetic-session",
        "AWS_DEFAULT_REGION": "sa-east-1",
        "AWS_EC2_METADATA_DISABLED": "true",
        "AWS_CONFIG_FILE": str(directory / "incoming-config"),
        "AWS_SHARED_CREDENTIALS_FILE": str(directory / "incoming-credentials"),
        "AWS_PROFILE": "incoming-synthetic-profile",
        "NO_PROXY": "incoming.invalid",
        "SCOPE_CASE_DIR": str(directory),
        "CNES_REFRESH_OFFLINE_EVIDENCE": str(directory / "offline-counts.json"),
    }


def _prepare_case(directory: Path, kind: str) -> tuple[Path, dict[str, str]]:
    root = Path(__file__).resolve().parents[4]
    environment = _environment(root, directory)
    for name in ("incoming-config", "incoming-credentials"):
        (directory / name).write_text("")
    (directory / "runner.py").write_text(_RUNNER)
    owner = directory / "test_owner.py"
    owner.write_text(_OWNER)
    foreign = directory / "test_foreign.py"
    foreign.write_text(_FOREIGN)
    args = [str(owner)]
    if kind != "owned":
        args += [str(foreign), "-k", "test_modulo_alheio_preserva_fronteiras"]
    args += [
        "-q",
        "-p",
        "no:cacheprovider",
        "--rootdir=" + str(root),
        "--junitxml=" + str(directory / "child.xml"),
        "--basetemp=" + str(directory / "child-tmp"),
    ]
    incoming = {
        name: value
        for name, value in environment.items()
        if name.startswith("AWS_") or name == "NO_PROXY"
    }
    config = {
        "root": str(root),
        "plugin": _PLUGIN,
        "pytest_args": args,
        "port": 5433 if kind == "postgres" else 8000,
        "incoming_env": incoming,
    }
    (directory / "config.json").write_text(json.dumps(config, indent=2) + "\n")
    return root, environment


def _run_child(directory: Path, kind: str) -> tuple[int, dict[str, Any], Any]:
    root, environment = _prepare_case(directory, kind)
    argv = [sys.executable, "-B", str(directory / "runner.py"), str(directory)]
    metadata = {
        "argv": argv,
        "cwd": str(root),
        "synthetic_env": environment,
        "timeout": 60,
        "started_utc": datetime.now(UTC).isoformat(),
    }
    result = subprocess.run(  # noqa: S603 - canonical interpreter and locally authored runner
        argv, cwd=root, env=environment, capture_output=True, timeout=60, check=False
    )
    metadata.update(rc=result.returncode, ended_utc=datetime.now(UTC).isoformat())
    (directory / "command.json").write_text(json.dumps(metadata, indent=2) + "\n")
    (directory / "child.stdout").write_bytes(result.stdout)
    (directory / "child.stderr").write_bytes(result.stderr)
    runtime = json.loads((directory / "runtime.json").read_text())
    assert runtime["helper_registered"] is True
    assert runtime["helper_path"] == str(
        root / "packages/cnes_infra/tests/auth/dynamodb_refresh_support.py"
    )
    assert runtime["bindings"]
    assert all(binding["within_checkout"] for binding in runtime["bindings"].values())
    suite = ElementTree.parse(directory / "child.xml").getroot().find("testsuite")  # noqa: S314
    assert suite is not None
    assert len(runtime["items"]) == 1
    setup = next(report for report in runtime["reports"] if report["when"] == "setup")
    assert setup["outcome"] == "passed"
    assert not suite.findall(".//error[@type='pytest.Collector.CollectError']")
    return result.returncode, runtime, suite


def test_modulo_dono_mantem_bloqueio_offline(tmp_path: Path) -> None:
    rc, runtime, suite = _run_child(tmp_path, "owned")
    assert rc == 1
    assert suite.attrib["failures"] == "0"
    assert {report["when"]: report["outcome"] for report in runtime["reports"]} == {
        "setup": "passed",
        "call": "passed",
        "teardown": "failed",
    }
    errors = suite.findall(".//error")
    assert len(errors) == 1
    assert "forbidden_io" in errors[0].attrib["message"]
    assert runtime["sentinel_calls"] == []
    properties = suite.find(".//properties")
    assert properties is not None
    blocked = next(
        item.attrib["value"] for item in properties if item.attrib["name"] == "blocked_primitives"
    )
    assert json.loads(blocked) == _PRIMITIVES
    counts = json.loads((tmp_path / "offline-counts.json").read_text())
    assert counts["cases"] == [{"nodeid": runtime["items"][0]["nodeid"], "attempts": 5}]


@pytest.mark.parametrize("backend", ["postgres", "dynamodb_local"])
def test_modulo_alheio_preserva_fronteiras_nativas(tmp_path: Path, backend: str) -> None:
    rc, runtime, suite = _run_child(tmp_path, backend)
    assert rc == 0, {
        "child_failures": suite.attrib["failures"],
        "child_errors": suite.attrib["errors"],
        "sentinel_calls": runtime["sentinel_calls"],
        "reports": runtime["reports"],
    }
    assert suite.attrib["failures"] == suite.attrib["errors"] == "0"
    assert [call["primitive"] for call in runtime["sentinel_calls"]] == _PRIMITIVES
