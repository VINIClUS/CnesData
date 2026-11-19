"""Adoção da credencial do credential_process: boot, watcher, observer e logs sem segredo."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import shlex
import sys
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, cast

import boto3
import pytest
from botocore.awsrequest import AWSPreparedRequest, AWSResponse
from botocore.compat import HTTPHeaders
from botocore.credentials import Credentials, RefreshableCredentials
from botocore.exceptions import CredentialRetrievalError

from cnes_infra.aws import AwsRuntimeConfigurationError
from cnes_infra.aws.credentials import (
    WATCH_INTERVAL_SECONDS,
    CredentialAdoption,
    key_fingerprint,
    register_signing_observer,
    require_process_credentials,
    watch_credentials,
)

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

    from boto3.session import Session

LOGGER = "cnes_infra.aws.credentials"
KEY_A = "ASIAGENA000000000001"
KEY_B = "ASIAGENB000000000002"
SECRET = "segredo-nunca-em-log"  # noqa: S105
TOKEN = "token-nunca-em-log"  # noqa: S105
SESSION_SECONDS = 3600
ADVISORY_SECONDS = 15 * 60
MANDATORY_SECONDS = 10 * 60
REFRESHED = re.compile(
    r"aws_credentials_refreshed key_fp=(?P<fp>[0-9a-f]{12}) "
    r"previous_fp=(?P<previous>[0-9a-f]{12}|none) pid=(?P<pid>[0-9]+)"
)
SCOPE = "20261114/us-east-2/{service}/aws4_request"
FAKE_PROCESS = """\
import json, sys
from datetime import UTC, datetime, timedelta
document = {"Version": 1, "AccessKeyId": "ASIAGENA000000000001", "SecretAccessKey": "s"}
document["SessionToken"] = "t"
if sys.argv[1] != "0":
    expiration = datetime.now(UTC) + timedelta(seconds=int(sys.argv[1]))
    document["Expiration"] = expiration.strftime("%Y-%m-%dT%H:%M:%SZ")
print(json.dumps(document))
"""
CREDENTIAL_ENVIRONMENT = (
    "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_SECURITY_TOKEN",
    "AWS_PROFILE", "AWS_DEFAULT_PROFILE", "AWS_ROLE_ARN", "AWS_WEB_IDENTITY_TOKEN_FILE",
    "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_CONTAINER_CREDENTIALS_FULL_URI",
)


class _Clock:
    def __init__(self) -> None:
        self.start = datetime.now(UTC).replace(microsecond=0)
        self.now = self.start

    def __call__(self) -> datetime:
        return self.now

    def elapse(self, seconds: int) -> None:
        self.now = self.start + timedelta(seconds=seconds)


class _Host:
    def __init__(self, clock: _Clock) -> None:
        self.clock = clock
        self.key = KEY_B
        self.failure: Exception | None = None
        self.calls = 0

    def metadata(self, key: str, seconds: int) -> dict[str, str]:
        expiry = self.clock.start + timedelta(seconds=seconds)
        return {
            "access_key": key, "secret_key": SECRET, "token": TOKEN,
            "expiry_time": expiry.isoformat(),
        }

    def refresh(self) -> dict[str, str]:
        self.calls += 1
        if self.failure is not None:
            raise self.failure
        return self.metadata(self.key, 2 * SESSION_SECONDS)


@pytest.fixture(autouse=True)
def _restaura_root() -> Iterator[None]:
    root = logging.getLogger()
    handlers, level = root.handlers[:], root.level
    yield
    root.handlers[:] = handlers
    root.setLevel(level)


@pytest.fixture
def clock() -> _Clock:
    return _Clock()


@pytest.fixture
def host(clock: _Clock) -> _Host:
    return _Host(clock)


@pytest.fixture
def credentials(host: _Host, clock: _Clock) -> RefreshableCredentials:
    expiry = clock.start + timedelta(seconds=SESSION_SECONDS)
    return RefreshableCredentials(
        KEY_A, SECRET, TOKEN, expiry, host.refresh, "custom-process", time_fetcher=clock,
    )


@pytest.fixture
def adoption(credentials: RefreshableCredentials) -> CredentialAdoption:
    return CredentialAdoption(credentials)


def _events(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [record.getMessage() for record in caplog.records if record.name == LOGGER]


def _request(authorization: object) -> AWSPreparedRequest:
    headers = HTTPHeaders()
    if authorization is not None:
        headers["Authorization"] = cast("str", authorization)
    return AWSPreparedRequest("POST", "https://example.invalid/", headers, b"", True)


def _sigv4(key: str, service: str) -> str:
    scope = SCOPE.format(service=service)
    return (
        f"AWS4-HMAC-SHA256 Credential={key}/{scope}, "
        "SignedHeaders=host;x-amz-date, Signature=" + "0" * 64
    )


class _Session:
    def __init__(self, credentials: Credentials | None) -> None:
        self._credentials = credentials

    def get_credentials(self) -> Credentials | None:
        return self._credentials


def _session(credentials: Credentials | None) -> Session:
    return cast("Session", _Session(credentials))


def test_fingerprint_e_sha256_do_access_key_em_12_hex() -> None:
    expected = hashlib.sha256(KEY_A.encode("utf-8")).hexdigest()[:12]

    assert key_fingerprint(KEY_A) == expected
    assert re.fullmatch(r"[0-9a-f]{12}", expected)
    assert key_fingerprint(KEY_B) != expected


def test_watcher_roda_a_cada_60_segundos() -> None:
    assert WATCH_INTERVAL_SECONDS == 60


def test_exige_credencial_renovavel_do_credential_process(
    credentials: RefreshableCredentials,
) -> None:
    assert require_process_credentials(_session(credentials)) is credentials


@pytest.mark.parametrize(
    ("resolved", "error"),
    [
        (None, "aws_credentials=missing method=none"),
        (Credentials(KEY_A, SECRET, TOKEN, method="env"), "aws_credentials=forbidden method=env"),
        (
            Credentials(KEY_A, SECRET, TOKEN, method="shared-credentials-file"),
            "aws_credentials=forbidden method=shared-credentials-file",
        ),
        (
            Credentials(KEY_A, SECRET, TOKEN, method="custom-process"),
            "aws_credentials=not_refreshable method=custom-process",
        ),
    ],
    ids=["sem-credencial", "env", "arquivo-compartilhado", "process-sem-expiration"],
)
def test_recusa_outro_provider_ou_credencial_estatica(
    resolved: Credentials | None, error: str,
) -> None:
    with pytest.raises(AwsRuntimeConfigurationError) as caught:
        require_process_credentials(_session(resolved))

    assert str(caught.value) == error
    assert SECRET not in str(caught.value)


@pytest.mark.parametrize(
    ("lifetime", "refreshable"), [("1800", True), ("0", False)], ids=["com-expiration", "sem"],
)
def test_sessao_boto3_real_so_passa_com_expiration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, lifetime: str, refreshable: bool,
) -> None:
    process = tmp_path / "process.py"
    process.write_text(FAKE_PROCESS, encoding="utf-8")
    config = tmp_path / "config"
    command = shlex.join([sys.executable, "-I", str(process), lifetime])
    config.write_text(f"[default]\ncredential_process = {command}\n", encoding="utf-8")
    for name in CREDENTIAL_ENVIRONMENT:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("AWS_CONFIG_FILE", str(config))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", os.devnull)
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    session = boto3.Session(region_name="us-east-2")

    if refreshable:
        assert isinstance(require_process_credentials(session), RefreshableCredentials)
        return
    with pytest.raises(AwsRuntimeConfigurationError, match="not_refreshable"):
        require_process_credentials(session)


def test_primeira_observacao_loga_a_linha_de_boot(
    adoption: CredentialAdoption, caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, LOGGER)

    fingerprint = adoption.observe()

    assert fingerprint == key_fingerprint(KEY_A)
    assert _events(caplog) == [
        f"aws_credentials_refreshed key_fp={fingerprint} previous_fp=none pid={os.getpid()}",
    ]
    assert adoption.credentials.access_key == KEY_A


def test_observacao_loga_so_quando_a_chave_muda(
    adoption: CredentialAdoption, clock: _Clock, host: _Host,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, LOGGER)
    adoption.observe()
    adoption.observe()

    clock.elapse(SESSION_SECONDS - ADVISORY_SECONDS + 1)
    adoption.observe()
    adoption.observe()

    first, second = (REFRESHED.fullmatch(event) for event in _events(caplog))
    assert first is not None
    assert second is not None
    assert second.groupdict() == {
        "fp": key_fingerprint(KEY_B), "previous": key_fingerprint(KEY_A), "pid": str(os.getpid()),
    }
    assert host.calls == 1


def test_tick_renova_na_advisory_sem_logar_falha(
    adoption: CredentialAdoption, clock: _Clock, caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, LOGGER)
    adoption.tick()
    clock.elapse(SESSION_SECONDS - ADVISORY_SECONDS + 1)

    adoption.tick()

    assert [REFRESHED.fullmatch(event) is not None for event in _events(caplog)] == [True, True]


def test_tick_loga_falha_engolida_pelo_botocore_na_advisory(
    adoption: CredentialAdoption, clock: _Clock, host: _Host,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, LOGGER)
    adoption.tick()
    host.failure = CredentialRetrievalError(
        provider="custom-process", error_msg="credentials_json=expiration_window path=/x",
    )
    clock.elapse(SESSION_SECONDS - ADVISORY_SECONDS + 1)

    adoption.tick()

    assert _events(caplog)[1:] == ["aws_credentials_refresh_failed error=advisory_refresh"]
    assert adoption.credentials.access_key == KEY_A


@pytest.mark.parametrize(
    "failure",
    [
        CredentialRetrievalError(provider="custom-process", error_msg="credentials_json=missing"),
        RuntimeError("expired"),
    ],
    ids=["leitor-recusa", "expirada"],
)
def test_tick_loga_o_tipo_da_falha_na_mandatoria_e_segue(
    adoption: CredentialAdoption, clock: _Clock, host: _Host,
    caplog: pytest.LogCaptureFixture, failure: Exception,
) -> None:
    caplog.set_level(logging.INFO, LOGGER)
    adoption.tick()
    host.failure = failure
    clock.elapse(SESSION_SECONDS - MANDATORY_SECONDS + 1)

    adoption.tick()

    assert _events(caplog)[1:] == [
        f"aws_credentials_refresh_failed error={type(failure).__name__}",
    ]


def test_observer_loga_a_chave_que_assina_cada_servico_uma_vez(
    adoption: CredentialAdoption, caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, LOGGER)
    pid = os.getpid()
    fp_a, fp_b = key_fingerprint(KEY_A), key_fingerprint(KEY_B)

    results = [
        adoption.record_signature(_request(_sigv4(key, service).encode("ascii")))
        for key, service in [
            (KEY_A, "states"), (KEY_A, "states"), (KEY_A, "dynamodb"),
            (KEY_B, "states"), (KEY_B, "dynamodb"), (KEY_B, "dynamodb"),
        ]
    ]

    assert results == [None] * 6
    assert _events(caplog) == [
        f"aws_credentials_signed key_fp={fp_a} service=states pid={pid}",
        f"aws_credentials_signed key_fp={fp_a} service=dynamodb pid={pid}",
        f"aws_credentials_signed key_fp={fp_b} service=states pid={pid}",
        f"aws_credentials_signed key_fp={fp_b} service=dynamodb pid={pid}",
    ]


def test_observer_aceita_o_header_em_texto(
    adoption: CredentialAdoption, caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, LOGGER)

    adoption.record_signature(_request(_sigv4(KEY_A, "s3")))

    assert _events(caplog) == [
        f"aws_credentials_signed key_fp={key_fingerprint(KEY_A)} service=s3 pid={os.getpid()}",
    ]


@pytest.mark.parametrize(
    "authorization",
    [None, "Bearer abc", "AWS4-HMAC-SHA256 Credential=ASIA/short", b"Bearer abc", 7],
    ids=["sem-header", "outro-esquema", "escopo-invalido", "bytes-outro-esquema", "nao-texto"],
)
def test_observer_ignora_request_sem_assinatura_sigv4(
    adoption: CredentialAdoption, caplog: pytest.LogCaptureFixture,
    authorization: object,
) -> None:
    caplog.set_level(logging.INFO, LOGGER)

    assert adoption.record_signature(_request(authorization)) is None
    assert _events(caplog) == []


class _Body:
    def stream(self, **_: object) -> Iterator[bytes]:
        yield b'{"TableNames": []}'


def _respond(request: AWSPreparedRequest, **_: object) -> AWSResponse:
    return AWSResponse(request.url, 200, HTTPHeaders(), _Body())


def test_observer_registrado_na_sessao_vale_para_clients_criados_depois(
    adoption: CredentialAdoption, caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, LOGGER)
    session = boto3.Session(
        aws_access_key_id=KEY_A, aws_secret_access_key=SECRET, aws_session_token=TOKEN,
        region_name="us-east-2",
    )

    register_signing_observer(session, adoption)
    client: Any = session.client("dynamodb")
    client.meta.events.register("before-send.dynamodb.ListTables", _respond)
    client.list_tables()

    assert _events(caplog) == [
        f"aws_credentials_signed key_fp={key_fingerprint(KEY_A)} service=dynamodb "
        f"pid={os.getpid()}",
    ]


class _Ticks:
    def __init__(self, stop_after: int) -> None:
        self.count = 0
        self.stop_after = stop_after
        self.done = asyncio.Event()
        self.loop = asyncio.get_running_loop()

    def tick(self) -> None:
        self.count += 1
        if self.count == self.stop_after:
            self.loop.call_soon_threadsafe(self.done.set)


@pytest.mark.asyncio
async def test_watcher_dorme_antes_do_primeiro_tick() -> None:
    ticks = _Ticks(stop_after=1)
    task = asyncio.create_task(watch_credentials(cast("Any", ticks), 3600))

    for _ in range(5):
        await asyncio.sleep(0)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert ticks.count == 0


@pytest.mark.asyncio
async def test_watcher_repete_o_tick_fora_do_loop_ate_ser_cancelado() -> None:
    ticks = _Ticks(stop_after=3)
    task = asyncio.create_task(watch_credentials(cast("Any", ticks), 0))

    await asyncio.wait_for(ticks.done.wait(), timeout=5)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert ticks.count >= 3


def test_logs_nunca_levam_segredo_token_nem_access_key(
    adoption: CredentialAdoption, clock: _Clock, host: _Host,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    adoption.tick()
    adoption.record_signature(_request(_sigv4(KEY_A, "s3")))
    host.failure = CredentialRetrievalError(provider="custom-process", error_msg="x")
    clock.elapse(SESSION_SECONDS - ADVISORY_SECONDS + 1)
    adoption.tick()
    clock.elapse(SESSION_SECONDS - MANDATORY_SECONDS + 1)
    adoption.tick()

    text = json.dumps([record.getMessage() for record in caplog.records if record.name == LOGGER])
    assert all(value not in text for value in (KEY_A, KEY_B, SECRET, TOKEN))
