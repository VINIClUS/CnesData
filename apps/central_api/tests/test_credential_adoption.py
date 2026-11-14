"""Aceite #473: o JSON novo do host passa a assinar todos os clients da API no mesmo processo."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import socket
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any
from unittest.mock import patch

import boto3
import pytest
from fastapi.testclient import TestClient
from moto import mock_aws

from apps.central_api.tests.test_aws_composition import _make_app
from central_api import deps
from packages.cnes_infra.tests.aws.production_values import (
    PRODUCTION_STATE_MACHINE_ARN,
    PRODUCTION_VALUES,
)
from packages.cnes_infra.tests.control_plane.test_dynamodb_adapter import (
    _TABLE_NAME,
    _create_table,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

REPO_ROOT = Path(__file__).resolve().parents[3]
DOCKER_DIRECTORY = REPO_ROOT / "apps" / "central_api" / "docker"
AWS_CONFIG = DOCKER_DIRECTORY / "aws-config"
READER_SOURCE = DOCKER_DIRECTORY / "aws_credentials.py"
READER_TARGET = "/usr/local/bin/cnesdata-aws-credentials"
CREDENTIAL_PATH = "/run/credentials/aws-credentials.json"
STATE_MACHINE_DEFINITION = (
    REPO_ROOT / "packages/cnes_infra/tests/fixtures/step_functions/production_public_ip.json"
)
ACCOUNT_ID = PRODUCTION_STATE_MACHINE_ARN.split(":")[4]
REGION = PRODUCTION_VALUES["AWS_REGION"]
KEY_A = "ASIAGENA000000000001"
KEY_B = "ASIAGENB000000000002"
GENERATION_SECONDS = 1800
NEXT_GENERATION_SECONDS = 3590
ADVANCE_SECONDS = 960
API_SERVICES = {"dynamodb", "s3", "stepfunctions", "secretsmanager"}
SIGNING_NAMES = {"dynamodb", "s3", "states", "secretsmanager"}
LOGGER = "cnes_infra.aws.credentials"
REFRESHED = re.compile(
    r"aws_credentials_refreshed key_fp=(?P<fp>[0-9a-f]{12}) "
    r"previous_fp=(?P<previous>[0-9a-f]{12}|none) pid=(?P<pid>[0-9]+)"
)
SIGNED = re.compile(
    r"aws_credentials_signed key_fp=(?P<fp>[0-9a-f]{12}) service=(?P<service>[a-z0-9-]+) "
    r"pid=(?P<pid>[0-9]+)"
)
READER_WRAPPER = """\
#!/bin/sh
exec "{python}" -I -S "{source}" "$@"
"""
BILLING = {
    "BILLING_MODE": "stripe",
    "BILLING_SUCCESS_URL": "https://cnesdata.com.br/billing/success",
    "BILLING_CANCEL_URL": "https://cnesdata.com.br/billing/cancel",
    "BILLING_PORTAL_RETURN_URL": "https://cnesdata.com.br/billing",
    "BILLING_RETURN_ORIGINS": "https://cnesdata.com.br",
}
STRIPE_SECRETS = {
    "STRIPE_SECRET_KEY_SECRET_ARN": ("cnesdata/stripe-api", "sk_" + "test_acceptance"),
    "STRIPE_WEBHOOK_SECRET_SECRET_ARN": ("cnesdata/stripe-webhook", "wh" + "sec_acceptance"),
}


def _fingerprint(key: str) -> str:
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:12]


def _secret(key: str) -> str:
    return f"secret-of-{key}"


def _token(key: str) -> str:
    return f"token-of-{key}"


@dataclass
class _Host:
    credentials: Path

    def publish(self, key: str, seconds: int) -> None:
        expiration = datetime.now(UTC) + timedelta(seconds=seconds)
        document = {
            "Version": 1, "AccessKeyId": key, "SecretAccessKey": _secret(key),
            "SessionToken": _token(key), "Expiration": expiration.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        staged = self.credentials.with_name(".write")
        staged.write_text(json.dumps(document), encoding="utf-8")
        staged.chmod(0o440)
        staged.replace(self.credentials)


class _RecordingSession:
    def __init__(self, **kwargs: Any) -> None:
        self._session: Any = boto3.Session(**kwargs)
        self.clients: list[tuple[str, Any]] = []

    def client(self, service_name: str, **kwargs: Any) -> Any:
        created = self._session.client(service_name, **kwargs)
        self.clients.append((service_name, created))
        return created

    def __getattr__(self, name: str) -> Any:
        return getattr(self._session, name)


@dataclass
class _Api:
    host: _Host
    resources: dict[str, str]
    sessions: list[_RecordingSession] = field(default_factory=list[_RecordingSession])

    def session(self, **kwargs: Any) -> _RecordingSession:
        created = _RecordingSession(**kwargs)
        self.sessions.append(created)
        return created


def _image_config(tmp_path: Path, credentials: Path) -> Path:
    reader = tmp_path / "cnesdata-aws-credentials"
    reader.write_text(
        READER_WRAPPER.format(python=sys.executable, source=READER_SOURCE), encoding="utf-8",
    )
    reader.chmod(0o755)
    config = tmp_path / "aws-config"
    text = AWS_CONFIG.read_text(encoding="utf-8")
    assert READER_TARGET in text
    assert CREDENTIAL_PATH in text
    rewritten = text.replace(READER_TARGET, str(reader)).replace(CREDENTIAL_PATH, str(credentials))
    config.write_text(rewritten, encoding="utf-8")
    return config


def _seed(values: dict[str, str]) -> dict[str, str]:
    seed = boto3.Session(region_name=REGION)
    _create_table(seed.client("dynamodb"))
    s3: Any = seed.client("s3")
    location = {"LocationConstraint": REGION}
    s3.create_bucket(Bucket=values["AWS_DATA_BUCKET"], CreateBucketConfiguration=location)
    s3.create_bucket(
        Bucket=values["AWS_AUDIT_BUCKET"], CreateBucketConfiguration=location,
        ObjectLockEnabledForBucket=True,
    )
    seed.client("stepfunctions").create_state_machine(
        name=PRODUCTION_STATE_MACHINE_ARN.rpartition(":")[2],
        definition=STATE_MACHINE_DEFINITION.read_text(encoding="utf-8"),
        roleArn=f"arn:aws:iam::{ACCOUNT_ID}:role/cnesdata-prod-sfn", type="STANDARD",
    )
    secrets = seed.client("secretsmanager")
    return {
        variable: secrets.create_secret(Name=name, SecretString=value)["ARN"]
        for variable, (name, value) in STRIPE_SECRETS.items()
    }


def _isolate_network(monkeypatch: pytest.MonkeyPatch) -> None:
    def deny(*_: Any, **__: Any) -> None:
        raise AssertionError("network=forbidden")

    for name in ("create_connection", "getaddrinfo", "gethostbyname", "gethostbyname_ex"):
        monkeypatch.setattr(socket, name, deny)
    for name in ("connect", "connect_ex", "sendto"):
        monkeypatch.setattr(socket.socket, name, deny)


@pytest.fixture
def api(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[_Api]:
    mount = tmp_path / "credentials"
    mount.mkdir(mode=0o710)
    host = _Host(mount / Path(CREDENTIAL_PATH).name)
    host.publish(KEY_A, GENERATION_SECONDS)
    values = dict(PRODUCTION_VALUES) | BILLING | {"AWS_CONTROL_PLANE_TABLE": _TABLE_NAME}
    environment = values | {
        "AWS_CONFIG_FILE": str(_image_config(tmp_path, host.credentials)),
        "AWS_SHARED_CREDENTIALS_FILE": os.devnull,
        "AWS_EC2_METADATA_DISABLED": "true",
        "MOTO_ACCOUNT_ID": ACCOUNT_ID,
        "DB_URL": "sqlite+pysqlite:///:memory:",
    }
    root = logging.getLogger()
    handlers, level = root.handlers[:], root.level
    with (
        patch.dict("os.environ", environment, clear=True),
        mock_aws(config={"core": {"mock_credentials": False}}),
    ):
        os.environ.update(_seed(values))
        _isolate_network(monkeypatch)
        recorder = _Api(host, values)
        monkeypatch.setattr(deps, "Session", recorder.session)
        yield recorder
    root.handlers[:] = handlers
    root.setLevel(level)


def _events(output: str) -> list[dict[str, Any]]:
    return [json.loads(line) for line in output.splitlines()]


def _adoption(lines: list[dict[str, Any]]) -> list[str]:
    return [line["event"] for line in lines if line["logger"] == LOGGER]


def _signed(events: list[str], key: str) -> set[str]:
    matches = (SIGNED.fullmatch(event) for event in events)
    return {match["service"] for match in matches if match and match["fp"] == _fingerprint(key)}


def _call_each_client(api: _Api) -> set[str]:
    [session] = api.sessions
    names = {name for name, _ in session.clients}
    for name, client in session.clients:
        if name == "dynamodb":
            client.describe_table(TableName=_TABLE_NAME)
        elif name == "s3":
            client.head_bucket(Bucket=api.resources["AWS_DATA_BUCKET"])
        elif name == "stepfunctions":
            client.describe_state_machine(stateMachineArn=PRODUCTION_STATE_MACHINE_ARN)
        elif name == "secretsmanager":
            client.describe_secret(SecretId=os.environ["STRIPE_SECRET_KEY_SECRET_ARN"])
    return names


def _advance_botocore_clock(app: Any, seconds: int) -> None:
    credentials = app.state.aws_credential_adoption.credentials
    credentials._time_fetcher = lambda: datetime.now(UTC) + timedelta(seconds=seconds)


def test_json_novo_assina_todos_os_clients_no_mesmo_processo_sem_novo_lifespan(
    api: _Api, capsys: pytest.CaptureFixture[str],
) -> None:
    app = _make_app()
    capsys.readouterr()
    pid = os.getpid()

    with TestClient(app):
        runtime = app.state.runtime
        adoption = app.state.aws_credential_adoption
        boot = _adoption(_events(capsys.readouterr().out))
        api.host.publish(KEY_B, NEXT_GENERATION_SECONDS)
        _advance_botocore_clock(app, ADVANCE_SECONDS)
        adoption.tick()
        services = _call_each_client(api)
        rotated = _events(capsys.readouterr().out)
        assert app.state.runtime is runtime
        assert app.state.aws_credential_adoption is adoption

    after_rotation = _adoption(rotated)
    assert boot[0] == (
        f"aws_credentials_refreshed key_fp={_fingerprint(KEY_A)} previous_fp=none pid={pid}"
    )
    assert _signed(boot, KEY_A) == {"states", "secretsmanager"}
    assert after_rotation[0] == (
        f"aws_credentials_refreshed key_fp={_fingerprint(KEY_B)} "
        f"previous_fp={_fingerprint(KEY_A)} pid={pid}"
    )
    assert services == API_SERVICES
    assert _signed(after_rotation, KEY_B) == SIGNING_NAMES
    assert len(api.sessions) == 1
    assert [event for event in boot + after_rotation if REFRESHED.fullmatch(event)] == [
        boot[0], after_rotation[0],
    ]
    assert all(str(pid) == match["pid"] for match in map(SIGNED.fullmatch, after_rotation) if match)


def test_nenhuma_linha_do_stdout_leva_chave_secret_ou_token(
    api: _Api, capsys: pytest.CaptureFixture[str],
) -> None:
    app = _make_app()
    capsys.readouterr()

    with TestClient(app):
        api.host.publish(KEY_B, NEXT_GENERATION_SECONDS)
        _advance_botocore_clock(app, ADVANCE_SECONDS)
        app.state.aws_credential_adoption.tick()
        _call_each_client(api)

    output = capsys.readouterr().out
    lines = _events(output)
    assert any(line["logger"] == LOGGER for line in lines)
    assert all(line["service"] == "central-api" for line in lines)
    forbidden = [KEY_A, KEY_B, *map(_secret, (KEY_A, KEY_B)), *map(_token, (KEY_A, KEY_B))]
    assert [value for value in forbidden if value in output] == []
    assert not re.search(r"\bASIA[0-9A-Z]{16}\b", output)
