"""GET /health/ready: corpo constante para o readiness_sha256 do host gate."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import TYPE_CHECKING, Any
from unittest.mock import ANY, MagicMock, Mock, patch

import pytest
from botocore.exceptions import ClientError, NoCredentialsError
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.exc import OperationalError

from apps.central_api.tests.test_aws_composition import _aws_components, _make_app, _set_env
from apps.central_api.tests.test_credential_lifespan import _refreshable, _session
from central_api.deps import get_health_engine
from central_api.middleware import AuthMiddleware
from central_api.routes.readiness import StsReadiness
from cnes_infra.auth import TokenInvalid
from packages.cnes_infra.tests.aws.production_values import PRODUCTION_VALUES

if TYPE_CHECKING:
    from collections.abc import Iterator

ROOT = Path(__file__).resolve().parents[3]
READY = b'{"status":"ready"}'
NOT_READY = b'{"status":"not_ready"}'


class _Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def _engine(*, fails: bool = False) -> MagicMock:
    engine = MagicMock(name="engine")
    if fails:
        engine.connect.side_effect = OperationalError("SELECT 1", {}, Exception("down"))
    return engine


def _sts(error: Exception | None = None) -> Mock:
    sts = Mock(name="sts")
    sts.get_caller_identity.side_effect = error
    return sts


@pytest.fixture
def app() -> Iterator[FastAPI]:
    application = _make_app()
    application.dependency_overrides[get_health_engine] = _engine
    yield application
    application.dependency_overrides.clear()


def _probe(app: FastAPI, sts: Mock, clock: _Clock | None = None, **headers: str) -> Any:
    app.state.sts_readiness = StsReadiness(sts, clock or _Clock())
    return TestClient(app).get("/health/ready", headers=headers, follow_redirects=False)


def test_pronto_devolve_200_com_o_corpo_constante_exato(app: FastAPI) -> None:
    response = _probe(app, _sts())

    assert response.status_code == 200
    assert response.content == READY
    assert response.headers["content-type"] == "application/json"


def test_banco_sem_resposta_devolve_503_com_outro_corpo_constante(app: FastAPI) -> None:
    app.dependency_overrides[get_health_engine] = lambda: _engine(fails=True)

    response = _probe(app, _sts())

    assert response.status_code == 503
    assert response.content == NOT_READY


@pytest.mark.parametrize(
    "error",
    [
        ClientError({"Error": {"Code": "ExpiredToken", "Message": "x"}}, "GetCallerIdentity"),
        NoCredentialsError(),
    ],
    ids=["sts-recusa", "credencial-nao-resolve"],
)
def test_sts_falhando_devolve_503(app: FastAPI, error: Exception) -> None:
    response = _probe(app, _sts(error))

    assert response.status_code == 503
    assert response.content == NOT_READY


def test_sucesso_do_sts_fica_em_cache_por_no_maximo_60_segundos(app: FastAPI) -> None:
    sts, clock = _sts(), _Clock()
    app.state.sts_readiness = StsReadiness(sts, clock)
    client = TestClient(app)
    calls: list[int] = []
    for offset in (0.0, 59.9, 60.0):
        clock.now = 1000.0 + offset
        assert client.get("/health/ready").content == READY
        calls.append(sts.get_caller_identity.call_count)

    assert calls == [1, 1, 2]


def test_falha_do_sts_nao_entra_no_cache(app: FastAPI) -> None:
    sts, clock = _sts(NoCredentialsError()), _Clock()
    app.state.sts_readiness = StsReadiness(sts, clock)
    client = TestClient(app)

    assert client.get("/health/ready").status_code == 503
    sts.get_caller_identity.side_effect = None
    clock.now += 1
    assert client.get("/health/ready").status_code == 200
    assert sts.get_caller_identity.call_count == 2


def test_dispensa_auth_mesmo_com_verificador_oidc_e_bearer_invalido(app: FastAPI) -> None:
    verifier = Mock(name="verifier")
    verifier.verify.side_effect = TokenInvalid("expired")
    app.state.oidc_verifier = verifier

    anonymous = _probe(app, _sts())
    invalid = _probe(app, _sts(), Authorization="Bearer bad")

    assert (anonymous.status_code, anonymous.content) == (200, READY)
    assert (invalid.status_code, invalid.content) == (200, READY)
    verifier.verify.assert_not_called()


def test_dispensa_de_auth_vale_so_para_o_path_exato() -> None:
    verifier = Mock(name="verifier")
    verifier.verify.side_effect = TokenInvalid("expired")
    application = FastAPI()
    application.add_middleware(AuthMiddleware)
    application.state.oidc_verifier = verifier

    @application.get("/health/other")
    def other() -> dict[str, str]:
        return {}

    response = TestClient(application).get(
        "/health/other", headers={"Authorization": "Bearer bad"},
    )

    assert response.status_code == 401


def test_rota_fica_fora_do_openapi_e_o_snapshot_nao_muda() -> None:
    schema = json.loads(json.dumps(_make_app().openapi()))
    committed = json.loads((ROOT / "docs/contracts/openapi.json").read_text(encoding="utf-8"))

    assert "/health/ready" not in schema["paths"]
    assert schema == committed


def test_lifespan_aws_de_producao_liga_o_sts_da_sessao_e_dispensa_o_banco(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_env(monkeypatch, dict(PRODUCTION_VALUES) | {"BILLING_MODE": "disabled"})
    session = _session(_refreshable())
    application = _make_app()

    with (
        patch("central_api.deps.configure_json_stdout"),
        patch("central_api.deps.Session", return_value=session),
        patch("central_api.deps.build_runtime", return_value=_aws_components()),
        patch("central_api.deps.watch_credentials", lambda _adoption: asyncio.sleep(0)),
        TestClient(application) as client,
    ):
        response = client.get("/health/ready")

    assert (response.status_code, response.content) == (200, READY)
    session.client.assert_called_once_with(
        "sts", region_name=PRODUCTION_VALUES["AWS_REGION"], endpoint_url=None, config=ANY,
    )
    session.client.return_value.get_caller_identity.assert_called_once_with()
