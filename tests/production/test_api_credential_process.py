"""Perfil AWS da imagem: credential_process renova no mesmo client e PID, com o relógio fake."""

from __future__ import annotations

import json
import os
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import pytest
from botocore.awsrequest import AWSPreparedRequest, AWSResponse
from botocore.compat import HTTPHeaders, compat_shell_split
from botocore.configloader import load_config
from botocore.credentials import RefreshableCredentials
from botocore.exceptions import CredentialRetrievalError
from botocore.session import Session

if TYPE_CHECKING:
    from collections.abc import Iterator

REPO_ROOT = Path(__file__).resolve().parents[2]
AWS_CONFIG = REPO_ROOT / "apps" / "central_api" / "docker" / "aws-config"
READER_SOURCE = REPO_ROOT / "apps" / "central_api" / "docker" / "aws_credentials.py"
TIMEOUT_BINARY = "/usr/bin/timeout"
TIMEOUT_SECONDS = 5
READER_TARGET = "/usr/local/bin/cnesdata-aws-credentials"
CREDENTIAL_PATH = "/run/credentials/aws-credentials.json"
PROFILE_ARGV = [TIMEOUT_BINARY, str(TIMEOUT_SECONDS), READER_TARGET, CREDENTIAL_PATH]
EXPECTED_CONFIG = f"[default]\ncredential_process = {' '.join(PROFILE_ARGV)}\n"
SHELL_SYNTAX = re.compile(r"[$`;|&\\'\"\n]")
SIGNED_ACCESS_KEY = re.compile(r"Credential=(?P<key>[^/]+)/")
ADVISORY_SECONDS = 15 * 60
MANDATORY_SECONDS = 10 * 60
READER_MARGIN_SECONDS = 960
GENERATION_SECONDS = 1800
NEXT_GENERATION_SECONDS = 3590
HUNG_READER_SECONDS = 30
KEY_A = "ASIAGENA000000000001"
KEY_B = "ASIAGENB000000000002"
TIMED_OUT = "Error when retrieving credentials from custom-process: "
CREDENTIAL_ENVIRONMENT = (
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_SECURITY_TOKEN",
    "AWS_CREDENTIAL_EXPIRATION",
    "AWS_PROFILE",
    "AWS_DEFAULT_PROFILE",
    "AWS_ROLE_ARN",
    "AWS_WEB_IDENTITY_TOKEN_FILE",
    "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
    "AWS_CONTAINER_CREDENTIALS_FULL_URI",
    "AWS_CONTAINER_AUTHORIZATION_TOKEN",
    "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
    "AWS_CREDENTIAL_FILE",
    "BOTO_CONFIG",
    "AWS_CA_BUNDLE",
    "AWS_ENDPOINT_URL",
    "AWS_ENDPOINT_URL_DYNAMODB",
)
WRAPPER = """\
#!/bin/sh
printf '%s\\n' "$1" >> "{log}"
if [ -e "{hang}" ]; then
    exec sleep {seconds}
fi
exec "{python}" -I -S "{source}" "$@"
"""


def _instant(moment: datetime) -> str:
    return moment.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


class _Clock:
    def __init__(self, start: datetime) -> None:
        self.start = start
        self._now = start

    def __call__(self) -> datetime:
        return self._now

    def elapse(self, seconds: int) -> None:
        self._now = self.start + timedelta(seconds=seconds)


class _Body:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def stream(self, **_: object) -> Iterator[bytes]:
        yield self._payload


@dataclass
class _Host:
    state: Path
    credentials: Path
    clock: _Clock
    signed: list[str] = field(default_factory=list[str])

    def publish(self, key: str, expiration: datetime, **changes: object) -> None:
        document = {
            "Version": 1,
            "AccessKeyId": key,
            "SecretAccessKey": f"secret-{key}",
            "SessionToken": f"token-{key}",
            "Expiration": _instant(expiration),
        } | changes
        staged = self.credentials.with_name(".write")
        staged.write_text(json.dumps(document), encoding="utf-8")
        staged.chmod(0o440)
        staged.replace(self.credentials)

    def invocations(self) -> list[str]:
        log = self.state / "invocations"
        if not log.exists():
            return []
        return log.read_text(encoding="utf-8").splitlines()

    def hang_reader(self) -> None:
        (self.state / "hang").touch()

    def connect(self) -> Any:
        session = Session()
        client: Any = session.create_client("dynamodb", region_name="us-east-2")
        credentials = session.get_credentials()
        assert isinstance(credentials, RefreshableCredentials)
        assert credentials.method == "custom-process"
        cast("Any", credentials)._time_fetcher = self.clock
        client.meta.events.register("before-send.dynamodb.ListTables", self._respond)
        return client

    def _respond(self, request: AWSPreparedRequest, **_: object) -> AWSResponse:
        match = SIGNED_ACCESS_KEY.search(str(request.headers["Authorization"]))
        assert match is not None
        self.signed.append(match["key"])
        return AWSResponse(request.url, 200, HTTPHeaders(), _Body(b'{"TableNames": []}'))


def _image_config(wrapper: Path, credentials: Path) -> str:
    text = AWS_CONFIG.read_text(encoding="utf-8")
    assert text == EXPECTED_CONFIG
    return text.replace(READER_TARGET, str(wrapper)).replace(CREDENTIAL_PATH, str(credentials))


def _require_executable(path: str) -> None:
    if os.access(path, os.X_OK):
        return
    message = f"tool_missing path={path}"
    if "CI" in os.environ:
        pytest.fail(message)
    pytest.skip(message)


@pytest.fixture
def host(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> _Host:
    _require_executable(TIMEOUT_BINARY)
    state = tmp_path / "state"
    state.mkdir()
    mount = tmp_path / "credentials"
    mount.mkdir(mode=0o710)
    wrapper = tmp_path / "cnesdata-aws-credentials"
    wrapper.write_text(
        WRAPPER.format(
            log=state / "invocations", hang=state / "hang", seconds=HUNG_READER_SECONDS,
            python=sys.executable, source=READER_SOURCE,
        ),
        encoding="utf-8",
    )
    wrapper.chmod(0o755)
    credentials = mount / "aws-credentials.json"
    config = tmp_path / "aws-config"
    config.write_text(_image_config(wrapper, credentials), encoding="utf-8")
    for name in CREDENTIAL_ENVIRONMENT:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("AWS_CONFIG_FILE", str(config))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", os.devnull)
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    clock = _Clock(datetime.now(UTC).replace(microsecond=0))
    published = _Host(state, credentials, clock)
    published.publish(KEY_A, clock.start + timedelta(seconds=GENERATION_SECONDS))
    return published


def test_aws_config_da_imagem_tem_so_o_default_com_o_leitor_sob_timeout() -> None:
    profiles = load_config(str(AWS_CONFIG))["profiles"]
    command = profiles["default"]["credential_process"]

    assert AWS_CONFIG.read_bytes() == EXPECTED_CONFIG.encode("ascii")
    assert profiles == {"default": {"credential_process": command}}
    assert not SHELL_SYNTAX.search(command)
    assert compat_shell_split(command) == PROFILE_ARGV


def test_margem_do_leitor_cobre_a_janela_advisory_do_botocore() -> None:
    refreshable = cast("Any", RefreshableCredentials)

    assert refreshable._advisory_refresh_timeout == ADVISORY_SECONDS
    assert refreshable._mandatory_refresh_timeout == MANDATORY_SECONDS
    assert ADVISORY_SECONDS < READER_MARGIN_SECONDS < GENERATION_SECONDS


def test_primeira_chamada_invoca_o_leitor_uma_vez(host: _Host) -> None:
    assert host.invocations() == []

    client = host.connect()
    resolved_on_client_creation = len(host.invocations())
    client.list_tables()

    assert resolved_on_client_creation == 1
    assert host.invocations() == [str(host.credentials)]
    assert host.signed == [KEY_A]


def test_antes_da_janela_advisory_a_geracao_nova_nao_e_lida(host: _Host) -> None:
    client = host.connect()
    host.publish(KEY_B, datetime.now(UTC) + timedelta(seconds=NEXT_GENERATION_SECONDS))

    for elapsed in (0, 60, GENERATION_SECONDS // 3, GENERATION_SECONDS - ADVISORY_SECONDS - 1):
        host.clock.elapse(elapsed)
        client.list_tables()

    assert len(host.invocations()) == 1
    assert host.signed == [KEY_A] * 4


def test_na_janela_advisory_a_geracao_nova_assina_no_mesmo_client_e_pid(host: _Host) -> None:
    client = host.connect()
    credentials = client._request_signer._credentials
    pid = os.getpid()
    client.list_tables()
    host.publish(KEY_B, datetime.now(UTC) + timedelta(seconds=NEXT_GENERATION_SECONDS))

    host.clock.elapse(GENERATION_SECONDS - ADVISORY_SECONDS + 1)
    client.list_tables()
    host.clock.elapse(GENERATION_SECONDS)
    client.list_tables()

    assert host.signed == [KEY_A, KEY_B, KEY_B]
    assert len(host.invocations()) == 2
    assert client._request_signer._credentials is credentials
    assert os.getpid() == pid


@pytest.mark.parametrize(
    ("code", "corrupt"),
    [
        ("missing", lambda host: host.credentials.unlink()),
        ("access_key", lambda host: host.publish(
            "AKIAGENB000000000002",
            datetime.now(UTC) + timedelta(seconds=NEXT_GENERATION_SECONDS),
        )),
        ("expiration_window", lambda host: host.publish(
            KEY_B, datetime.now(UTC) + timedelta(seconds=ADVISORY_SECONDS),
        )),
    ],
    ids=["arquivo-ausente", "chave-de-longo-prazo", "geracao-sem-margem"],
)
def test_falha_do_leitor_segue_na_advisory_e_fecha_na_mandatoria(
    host: _Host, caplog: pytest.LogCaptureFixture, code: str, corrupt: Any,
) -> None:
    client = host.connect()
    corrupt(host)

    host.clock.elapse(GENERATION_SECONDS - ADVISORY_SECONDS + 1)
    client.list_tables()
    host.clock.elapse(GENERATION_SECONDS - MANDATORY_SECONDS + 1)
    with pytest.raises(CredentialRetrievalError) as error:
        client.list_tables()

    assert host.signed == [KEY_A]
    assert "advisory refresh period" in caplog.text
    assert str(error.value) == f"{TIMED_OUT}credentials_json={code} path={host.credentials}"


def test_leitor_travado_e_morto_pelo_timeout_da_imagem(host: _Host) -> None:
    host.hang_reader()
    started = time.monotonic()

    with pytest.raises(CredentialRetrievalError) as error:
        host.connect()

    assert TIMEOUT_SECONDS <= time.monotonic() - started < HUNG_READER_SECONDS
    assert str(error.value) == TIMED_OUT
    assert host.invocations() == [str(host.credentials)]
