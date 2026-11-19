"""Deadline nativo do main audit em subprocesso isolado e limitado."""

# ruff: noqa: S603

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from apps.data_processor.tests.test_audit_dispatch_mode import _MINIMUM

_ROOT = Path(__file__).parents[3]
_CHILD = """import asyncio, importlib, json, os, socket, sys, time
from pathlib import Path
from contextlib import contextmanager
from unittest.mock import Mock
BASE = Path.cwd()
PHASE = sys.argv[1]
def note(kind, **values):
    with (BASE / "evidence.jsonl").open("a") as stream:
        stream.write(json.dumps({"kind": kind, "time": time.monotonic(), **values}) + "\\n")
        stream.flush()
def deny(*args, **kwargs):
    note("network-forbidden")
    raise AssertionError("network=forbidden")
for name in ("create_connection", "getaddrinfo", "gethostbyname", "gethostbyname_ex"):
    setattr(socket, name, deny)
for name in ("connect", "connect_ex", "sendto"):
    setattr(socket.socket, name, deny)
def block(operation, **request):
    note("blocked", operation=operation, bucket=request.get("Bucket"))
    time.sleep(10)
    (BASE / "continued").touch()
    raise OSError("boundary=still_blocked")
import boto3
from moto import mock_aws
from cnes_infra.billing.dynamodb_items import outbox_item
from packages.cnes_infra.tests.control_plane.outbox_cursor_support import evento
from packages.cnes_infra.tests.control_plane.test_dynamodb_adapter import _create_table
from data_processor.deadline import DeadlinePolicy, processor_deadline
app = importlib.import_module("data_processor.main")
note("binding", module=app.__file__, interpreter=sys.executable)
@contextmanager
def accelerated(policy):
    note("requested", mode=policy.mode, seconds=policy.seconds)
    with processor_deadline(DeadlinePolicy(policy.mode, 1.0)):
        note("native-arm", seconds=1.0)
        yield
def run():
    with mock_aws():
        session = boto3.Session()
        ddb = session.client("dynamodb", region_name="us-east-2")
        class Seed:
            def __getattr__(self, name):
                def forward(**request):
                    request["TableName"] = os.environ["AWS_CONTROL_PLANE_TABLE"]
                    return getattr(ddb, name)(**request)
                return forward
        _create_table(Seed())
        ddb.put_item(TableName=os.environ["AWS_CONTROL_PLANE_TABLE"],
                     Item=outbox_item(evento("entregue")))
        s3 = session.client("s3", region_name="us-east-2")
        s3.create_bucket(Bucket=os.environ["AWS_AUDIT_BUCKET"], ObjectLockEnabledForBucket=True,
                         CreateBucketConfiguration={"LocationConstraint": "us-east-2"})
        def client(service, **request):
            note("client", service=service)
            result = session.client(service, **request)
            if PHASE == "sdk" and service == "dynamodb":
                result.query = lambda **request: block("query", **request)
            if PHASE == "storage" and service == "s3":
                result.put_object = lambda **request: block("put_object", **request)
            return result
        app.Session = lambda: Mock(spec_set=["client"], client=client)
        app.init_telemetry = lambda *args: None
        app.processor_deadline = accelerated
        sys.argv[:] = ["data_processor", "dispatch-outbox-once"]
        note("main-entry")
        rc = asyncio.run(app.main())
        note("returned", rc=rc)
        return rc
try:
    raise SystemExit(run())
except Exception as error:
    note("setup-or-execution-error", error=type(error).__name__, detail=str(error))
    raise
"""


def _environment() -> dict[str, str]:
    sources = (
        "apps/data_processor",
        "packages/cnes_contracts",
        "packages/cnes_domain",
        "packages/cnes_infra",
    )
    return _MINIMUM | {
        "PYTHONPATH": os.pathsep.join(
            [str(_ROOT), *(str(_ROOT / path / "src") for path in sources)]
        ),
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHON_DOTENV_DISABLED": "1",
        "AWS_EC2_METADATA_DISABLED": "true",
        "AWS_ACCESS_KEY_ID": "offline",
        "AWS_SECRET_ACCESS_KEY": "offline",
        "AWS_SHARED_CREDENTIALS_FILE": "/dev/null",
        "AWS_CONFIG_FILE": "/dev/null",
        "POLARS_MAX_THREADS": "2",
    }


def _events(tmp_path: Path) -> list[Any]:
    path = tmp_path / "evidence.jsonl"
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


def _run(tmp_path: Path, phase: str) -> dict[str, Any]:
    argv = [sys.executable, "-B", "-c", _CHILD, phase]
    process = subprocess.Popen(
        argv,
        cwd=tmp_path,
        env=_environment(),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    proof: dict[str, Any] = {"accelerated_native_seconds": 1.0, "phase": phase}
    try:
        try:
            process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            proof["timeout"] = True
    finally:
        proof["killed"] = process.poll() is None
        if proof["killed"]:
            process.kill()
        stdout, stderr = process.communicate(timeout=3)
        for stream in (process.stdout, process.stderr):
            if stream is not None:
                stream.close()
        proof.update(
            rc=process.returncode,
            events=_events(tmp_path),
            stdout=stdout.decode(),
            stderr=stderr.decode(),
            continued=(tmp_path / "continued").exists(),
            reaped=process.poll() is not None,
        )
        (tmp_path / "result.json").write_text(json.dumps(proof, indent=2))
    return proof


@pytest.mark.parametrize("phase", ["sdk", "storage"])
def test_main_bloqueado_encerra_com_tres_sem_continuar_apos_deadline_nativo(
    tmp_path: Path,
    phase: str,
    record_property: Any,
) -> None:
    proof = _run(tmp_path, phase)
    record_property("subprocesso_deadline_acelerado", json.dumps(proof))
    assert proof["reaped"] is True
    assert not proof.get("timeout"), proof
    assert not any(
        event["kind"] in {"setup-or-execution-error", "network-forbidden"}
        for event in proof["events"]
    ), proof
    assert (proof["rc"], proof["killed"], proof["continued"]) == (3, False, False), proof
    requested = [event for event in proof["events"] if event["kind"] == "requested"]
    assert [(event["mode"], event["seconds"]) for event in requested] == [
        ("dispatch-outbox-once", 60),
    ]
    arms = [event for event in proof["events"] if event["kind"] == "native-arm"]
    blocked = [event for event in proof["events"] if event["kind"] == "blocked"]
    assert len(arms) == len(blocked) == 1
    assert arms[0]["seconds"] == 1.0
    assert requested[0]["time"] <= arms[0]["time"] <= blocked[0]["time"]
    assert blocked[0]["operation"] == ("query" if phase == "sdk" else "put_object")
    if phase == "storage":
        assert blocked[0]["bucket"] == "audit-bucket-configurado"
    assert not any(event["kind"] == "returned" for event in proof["events"])
