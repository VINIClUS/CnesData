"""Contrato credential_process: config de produção e refresh real do botocore com relógio fake."""

from __future__ import annotations

import json
import os
import re
import shlex
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
CONFIG_PATH = REPO_ROOT / "deploy" / "aws" / "config"
PROFILE_NAME = "cnesdata-production"
SESSION_SECONDS = 3600
# botocore 1.43.82 (uv.lock): with < 15 min left the refresh is advisory and a helper failure keeps
# the still-valid session; with < 10 min left it is mandatory and the failure propagates.
ADVISORY_SECONDS = 15 * 60
MANDATORY_SECONDS = 10 * 60
TIMEOUT_BINARY = "/usr/bin/timeout"
TIMEOUT_SECONDS = 60
TEST_TIMEOUT_SECONDS = 3
HUNG_HELPER_SECONDS = 20
HELPER_PATH = "/opt/aws/aws_signing_helper"
HELPER_ARGUMENTS = [
    "credential-process",
    "--certificate",
    "/etc/cnesdata/aws/leaf-certificate.pem",
    "--private-key",
    "/etc/cnesdata/aws/leaf-private-key.pem",
    "--trust-anchor-arn",
    "<TRUST_ANCHOR_ARN>",
    "--profile-arn",
    "<PROFILE_ARN>",
    "--role-arn",
    "<ROLE_ARN>",
    "--session-duration",
    str(SESSION_SECONDS),
]
EXPECTED_ARGV = [TIMEOUT_BINARY, str(TIMEOUT_SECONDS), HELPER_PATH, *HELPER_ARGUMENTS]
TIMED_OUT = "Error when retrieving credentials from custom-process: "
SHELL_SYNTAX = re.compile(r"[$`;|&\\'\"\n]")
SIGNED_ACCESS_KEY = re.compile(r"Credential=(?P<key>[^/]+)/")
CREDENTIAL_ENVIRONMENT = (
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_SECURITY_TOKEN",
    "AWS_CREDENTIAL_EXPIRATION",
    "AWS_ACCOUNT_ID",
    "AWS_DEFAULT_PROFILE",
    "AWS_ROLE_ARN",
    "AWS_WEB_IDENTITY_TOKEN_FILE",
    "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
    "AWS_CONTAINER_CREDENTIALS_FULL_URI",
    "AWS_CONTAINER_AUTHORIZATION_TOKEN",
    "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
    "AWS_CA_BUNDLE",
    "AWS_ENDPOINT_URL",
    "AWS_ENDPOINT_URL_DYNAMODB",
)
FAKE_HELPER = """\
import json
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

state = Path(sys.argv[1])
arguments = sys.argv[2:]
with (state / "invocations").open("a", encoding="utf-8") as log:
    log.write(json.dumps(arguments) + "\\n")
if (state / "hang").exists():
    time.sleep(float((state / "hang").read_text(encoding="utf-8")))
if (state / "fail").exists():
    sys.stderr.write("helper_failed\\n")
    raise SystemExit(1)
count = len((state / "invocations").read_text(encoding="utf-8").splitlines())
duration = int(arguments[arguments.index("--session-duration") + 1])
issued = datetime.fromisoformat((state / "now").read_text(encoding="utf-8"))
print(json.dumps({
    "Version": 1,
    "AccessKeyId": f"fake-access-{count}",
    "SecretAccessKey": f"fake-secret-{count}",
    "SessionToken": f"fake-session-{count}",
    "Expiration": (issued + timedelta(seconds=duration)).isoformat(),
}))
"""


class _Clock:
    def __init__(self, state: Path, start: datetime) -> None:
        self._state = state
        self._start = start
        self._now = start
        self.elapse(0)

    def __call__(self) -> datetime:
        return self._now

    def elapse(self, seconds: int) -> None:
        self._now = self._start + timedelta(seconds=seconds)
        (self._state / "now").write_text(self._now.isoformat(), encoding="utf-8")


class _Body:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def stream(self, **_: object) -> Iterator[bytes]:
        yield self._payload


@dataclass
class _RolesAnywhere:
    state: Path
    clock: _Clock
    helper: Path
    config: Path
    signed: list[str] = field(default_factory=list[str])

    def invocations(self) -> list[list[str]]:
        log = self.state / "invocations"
        if not log.exists():
            return []
        return [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines()]

    def fail_helper(self) -> None:
        (self.state / "fail").touch()

    def hang_helper(self, seconds: int) -> None:
        (self.state / "hang").write_text(str(seconds), encoding="utf-8")

    def bound_by_timeout(self, seconds: int) -> None:
        _require_executable(TIMEOUT_BINARY)
        self.config.write_text(_fake_config(self.helper, self.state, seconds), encoding="utf-8")

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


def _require_executable(path: str) -> None:
    if os.access(path, os.X_OK):
        return
    message = f"tool_missing path={path}"
    if "CI" in os.environ:
        pytest.fail(message)
    pytest.skip(message)


def _production_profile() -> dict[str, str]:
    return load_config(str(CONFIG_PATH))["profiles"][PROFILE_NAME]


def _shortened(wrapper: list[str], seconds: int) -> list[str]:
    return [str(seconds) if token == str(TIMEOUT_SECONDS) else token for token in wrapper]


def _fake_config(helper: Path, state: Path, timeout_seconds: int | None) -> str:
    argv = compat_shell_split(_production_profile()["credential_process"])
    helper_at = argv.index(HELPER_PATH)
    wrapper = [] if timeout_seconds is None else _shortened(argv[:helper_at], timeout_seconds)
    fake = [sys.executable, "-I", str(helper), str(state)]
    command = shlex.join([*wrapper, *fake, *argv[helper_at + 1 :]])
    return f"[profile {PROFILE_NAME}]\ncredential_process = {command}\n"


@pytest.fixture
def roles_anywhere(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> _RolesAnywhere:
    state = tmp_path / "state"
    state.mkdir()
    helper = tmp_path / "fake_aws_signing_helper.py"
    helper.write_text(FAKE_HELPER, encoding="utf-8")
    config = tmp_path / "config"
    config.write_text(_fake_config(helper, state, None), encoding="utf-8")
    for name in CREDENTIAL_ENVIRONMENT:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("AWS_CONFIG_FILE", str(config))
    monkeypatch.setenv("AWS_PROFILE", PROFILE_NAME)
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "credentials-ausente"))
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    clock = _Clock(state, datetime.now(UTC).replace(microsecond=0))
    return _RolesAnywhere(state, clock, helper, config)


def test_config_de_producao_chama_helper_sem_shell_com_sessao_de_3600() -> None:
    profiles = load_config(str(CONFIG_PATH))["profiles"]
    command = profiles[PROFILE_NAME]["credential_process"]

    assert set(profiles) == {PROFILE_NAME}
    assert set(profiles[PROFILE_NAME]) == {"credential_process"}
    assert not SHELL_SYNTAX.search(command)
    assert compat_shell_split(command) == EXPECTED_ARGV


def test_config_de_producao_versiona_so_placeholders() -> None:
    text = CONFIG_PATH.read_text(encoding="utf-8")

    assert re.findall(r"<[A-Z_]+>", text) == ["<TRUST_ANCHOR_ARN>", "<PROFILE_ARN>", "<ROLE_ARN>"]
    assert not re.search(r"\d{12}", text)
    assert "arn:" not in text.lower()
    assert "-----BEGIN" not in text
    assert not re.search(r"aws_(access_key_id|secret_access_key|session_token)", text, re.I)


def test_botocore_instalado_renova_com_15_min_e_exige_com_10_min() -> None:
    refreshable = cast("Any", RefreshableCredentials)

    assert refreshable._advisory_refresh_timeout == ADVISORY_SECONDS
    assert refreshable._mandatory_refresh_timeout == MANDATORY_SECONDS


def test_primeira_chamada_invoca_helper(roles_anywhere: _RolesAnywhere) -> None:
    assert roles_anywhere.invocations() == []

    client = roles_anywhere.connect()
    resolved_on_client_creation = len(roles_anywhere.invocations())
    client.list_tables()

    assert resolved_on_client_creation == 1
    assert roles_anywhere.invocations() == [HELPER_ARGUMENTS]
    assert roles_anywhere.signed == ["fake-access-1"]


def test_chamadas_antes_da_janela_advisory_nao_invocam_helper(
    roles_anywhere: _RolesAnywhere,
) -> None:
    client = roles_anywhere.connect()

    for elapsed in (0, 60, SESSION_SECONDS // 2, SESSION_SECONDS - ADVISORY_SECONDS):
        roles_anywhere.clock.elapse(elapsed)
        client.list_tables()

    assert len(roles_anywhere.invocations()) == 1
    assert roles_anywhere.signed == ["fake-access-1"] * 4


def test_renova_uma_vez_na_janela_advisory(roles_anywhere: _RolesAnywhere) -> None:
    client = roles_anywhere.connect()
    refreshed_at = SESSION_SECONDS - ADVISORY_SECONDS + 1

    roles_anywhere.clock.elapse(refreshed_at)
    for _ in range(3):
        client.list_tables()
    roles_anywhere.clock.elapse(refreshed_at + SESSION_SECONDS - ADVISORY_SECONDS)
    client.list_tables()

    assert roles_anywhere.invocations() == [HELPER_ARGUMENTS] * 2
    assert roles_anywhere.signed == ["fake-access-2"] * 4


def test_falha_do_helper_na_primeira_chamada_nao_cria_cliente(
    roles_anywhere: _RolesAnywhere,
) -> None:
    roles_anywhere.fail_helper()

    with pytest.raises(CredentialRetrievalError, match="helper_failed"):
        roles_anywhere.connect()

    assert len(roles_anywhere.invocations()) == 1
    assert roles_anywhere.signed == []


@pytest.mark.parametrize(
    "elapsed",
    [
        pytest.param(SESSION_SECONDS - MANDATORY_SECONDS + 1, id="janela-mandatoria"),
        pytest.param(SESSION_SECONDS, id="expira-agora"),
        pytest.param(SESSION_SECONDS + 60, id="expirada"),
    ],
)
def test_falha_do_helper_fecha(roles_anywhere: _RolesAnywhere, elapsed: int) -> None:
    client = roles_anywhere.connect()
    client.list_tables()
    roles_anywhere.fail_helper()
    roles_anywhere.clock.elapse(elapsed)

    with pytest.raises(CredentialRetrievalError, match="helper_failed"):
        client.list_tables()

    assert len(roles_anywhere.invocations()) == 2
    assert roles_anywhere.signed == ["fake-access-1"]


def test_falha_na_janela_advisory_segue_com_sessao_valida_ate_a_mandatoria(
    roles_anywhere: _RolesAnywhere,
) -> None:
    client = roles_anywhere.connect()
    roles_anywhere.fail_helper()

    roles_anywhere.clock.elapse(SESSION_SECONDS - ADVISORY_SECONDS + 1)
    client.list_tables()
    advisory_attempts = len(roles_anywhere.invocations()) - 1
    roles_anywhere.clock.elapse(SESSION_SECONDS - MANDATORY_SECONDS + 1)
    with pytest.raises(CredentialRetrievalError, match="helper_failed"):
        client.list_tables()

    assert advisory_attempts >= 1
    assert len(roles_anywhere.invocations()) == advisory_attempts + 2
    assert roles_anywhere.signed == ["fake-access-1"]


def test_helper_travado_no_boot_falha_dentro_do_timeout(roles_anywhere: _RolesAnywhere) -> None:
    roles_anywhere.bound_by_timeout(TEST_TIMEOUT_SECONDS)
    roles_anywhere.hang_helper(HUNG_HELPER_SECONDS)
    started = time.monotonic()

    with pytest.raises(CredentialRetrievalError) as error:
        roles_anywhere.connect()

    assert TEST_TIMEOUT_SECONDS <= time.monotonic() - started < HUNG_HELPER_SECONDS
    assert str(error.value) == TIMED_OUT
    assert roles_anywhere.invocations() == [HELPER_ARGUMENTS]


def test_helper_travado_na_renovacao_mandatoria_falha_dentro_do_timeout(
    roles_anywhere: _RolesAnywhere,
) -> None:
    roles_anywhere.bound_by_timeout(TEST_TIMEOUT_SECONDS)
    client = roles_anywhere.connect()
    roles_anywhere.hang_helper(HUNG_HELPER_SECONDS)
    roles_anywhere.clock.elapse(SESSION_SECONDS - MANDATORY_SECONDS + 1)
    started = time.monotonic()

    with pytest.raises(CredentialRetrievalError) as error:
        client.list_tables()

    assert TEST_TIMEOUT_SECONDS <= time.monotonic() - started < HUNG_HELPER_SECONDS
    assert str(error.value) == TIMED_OUT
    assert roles_anywhere.invocations() == [HELPER_ARGUMENTS] * 2
    assert roles_anywhere.signed == []
