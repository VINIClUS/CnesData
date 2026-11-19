"""Lifespan aws de produção: credential_process obrigatório, observer, watcher e log JSON."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from unittest.mock import ANY, Mock, call, patch

import pytest
from botocore.credentials import Credentials, RefreshableCredentials
from fastapi.testclient import TestClient

from apps.central_api.tests.test_aws_composition import (
    _aws_components,
    _aws_values,
    _make_app,
    _set_env,
)
from cnes_infra.aws import AwsRuntimeConfigurationError, CredentialAdoption
from packages.cnes_infra.tests.aws.production_values import PRODUCTION_VALUES

if TYPE_CHECKING:
    from collections.abc import Iterator

KEY_A = "ASIAGENA000000000001"
SECRET = "segredo-nunca-em-log"  # noqa: S105
TOKEN = "token-nunca-em-log"  # noqa: S105
FP_A = "d26dc7039eeb"
BOOT_EVENT = re.compile(
    r"aws_credentials_refreshed key_fp=([0-9a-f]{12}) previous_fp=([0-9a-f]{12}|none) "
    r"pid=([0-9]+)"
)
ADOPTION_LOGGER = "cnes_infra.aws.credentials"


@pytest.fixture(autouse=True)
def _restaura_root() -> Iterator[None]:
    root = logging.getLogger()
    handlers, level = root.handlers[:], root.level
    yield
    root.handlers[:] = handlers
    root.setLevel(level)


def _refreshable() -> RefreshableCredentials:
    expiry = datetime.now(UTC) + timedelta(hours=1)
    return RefreshableCredentials(KEY_A, SECRET, TOKEN, expiry, Mock(), "custom-process")


def _session(credentials: Credentials | None) -> Mock:
    session = Mock(name="session")
    session.get_credentials.return_value = credentials
    return session


@pytest.fixture
def production(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_env(monkeypatch, dict(PRODUCTION_VALUES) | {"BILLING_MODE": "disabled"})


@pytest.mark.usefixtures("production")
@pytest.mark.parametrize(
    ("resolved", "error"),
    [
        (None, "aws_credentials=missing method=none"),
        (Credentials(KEY_A, SECRET, TOKEN, method="env"), "aws_credentials=forbidden method=env"),
        (
            Credentials(KEY_A, SECRET, TOKEN, method="custom-process"),
            "aws_credentials=not_refreshable method=custom-process",
        ),
    ],
    ids=["sem-credencial", "env", "process-sem-expiration"],
)
def test_producao_nao_sobe_sem_credential_process_renovavel(
    resolved: Credentials | None, error: str,
) -> None:
    app = _make_app()

    with (
        patch("central_api.deps.Session", return_value=_session(resolved)),
        patch("central_api.deps.build_runtime") as build,
        pytest.raises(AwsRuntimeConfigurationError) as caught,
        TestClient(app),
    ):
        pass

    assert str(caught.value) == error
    build.assert_not_called()


@pytest.mark.usefixtures("production")
def test_producao_configura_log_e_registra_o_observer_antes_dos_clients() -> None:
    session = _session(_refreshable())
    manager = Mock()
    manager.attach_mock(session, "session")
    app = _make_app()

    with (
        patch("central_api.deps.configure_json_stdout", manager.configure_json_stdout),
        patch("central_api.deps.Session", manager.Session),
        patch("central_api.deps.build_runtime", manager.build_runtime),
        patch("central_api.deps.watch_credentials", manager.watch_credentials),
    ):
        manager.Session.return_value = session
        manager.build_runtime.return_value = _aws_components()
        manager.watch_credentials.side_effect = lambda adoption: asyncio.sleep(0)
        with TestClient(app):
            adoption = app.state.aws_credential_adoption

    assert isinstance(adoption, CredentialAdoption)
    assert adoption.credentials is session.get_credentials.return_value
    assert manager.mock_calls[:5] == [
        call.configure_json_stdout("central-api"),
        call.Session(region_name=PRODUCTION_VALUES["AWS_REGION"]),
        call.session.get_credentials(),
        call.session.events.register("before-send", adoption.record_signature),
        call.build_runtime("aws", os.environ, session),
    ]


@pytest.mark.usefixtures("production")
def test_producao_inicia_o_watcher_e_cancela_no_shutdown() -> None:
    session = _session(_refreshable())
    started: list[CredentialAdoption] = []
    cancelled: list[bool] = []

    async def watch(adoption: CredentialAdoption) -> None:
        started.append(adoption)
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.append(True)
            raise

    app = _make_app()
    with (
        patch("central_api.deps.Session", return_value=session),
        patch("central_api.deps.build_runtime", return_value=_aws_components()),
        patch("central_api.deps.watch_credentials", watch),
        TestClient(app),
    ):
        assert cancelled == []

    assert started == [app.state.aws_credential_adoption]
    assert cancelled == [True]


def test_fora_da_producao_o_lifespan_aws_nao_muda(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_env(monkeypatch, _aws_values())
    session = _session(None)
    root = logging.getLogger()
    handlers = root.handlers[:]
    app = _make_app()

    with (
        patch("central_api.deps.Session", return_value=session),
        patch("central_api.deps.build_runtime", return_value=_aws_components()) as build,
        patch("central_api.deps.watch_credentials") as watch,
        TestClient(app),
    ):
        pass

    build.assert_called_once_with("aws", os.environ, ANY)
    session.get_credentials.assert_not_called()
    session.events.register.assert_not_called()
    watch.assert_not_called()
    assert root.handlers == handlers
    assert not hasattr(app.state, "aws_credential_adoption")


@pytest.mark.usefixtures("production")
def test_linha_de_adocao_sai_em_json_no_stdout_no_formato_do_contrato(
    capsys: pytest.CaptureFixture[str],
) -> None:
    app = _make_app()
    capsys.readouterr()

    with (
        patch("central_api.deps.Session", return_value=_session(_refreshable())),
        patch("central_api.deps.build_runtime", return_value=_aws_components()),
        TestClient(app),
    ):
        pass

    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    adoption = [line for line in lines if line.get("logger") == ADOPTION_LOGGER]
    assert len(adoption) == 1
    [boot] = adoption
    assert set(boot) == {"timestamp", "level", "service", "logger", "event"}
    assert (boot["level"], boot["service"]) == ("INFO", "central-api")
    match = BOOT_EVENT.fullmatch(boot["event"])
    assert match is not None
    assert match.groups() == (FP_A, "none", str(os.getpid()))
    text = json.dumps(lines)
    assert all(value not in text for value in (KEY_A, SECRET, TOKEN))


def test_fingerprint_do_contrato_e_sha256_do_access_key() -> None:
    assert hashlib.sha256(KEY_A.encode()).hexdigest()[:12] == FP_A
