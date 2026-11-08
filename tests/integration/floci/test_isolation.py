"""Isolamento da suíte Floci: endpoint loopback de FLOCI_ENDPOINT_URL e credenciais fictícias."""
from __future__ import annotations

import socket
import sys
from typing import TYPE_CHECKING, Any

import httpx
import pytest

from tests.integration.floci._floci import (
    ADMIN,
    DEFAULT_ENDPOINT,
    EMULATOR_VERSION,
    REGION,
    FlociIsolationError,
    floci_endpoint,
    health_problem,
    http_client,
    isolated_session,
    new_client,
)

if TYPE_CHECKING:
    from pathlib import Path

    from tests.integration.floci._floci import FlociContext

pytestmark = [pytest.mark.floci]

_UNROUTABLE = "http://192.0.2.1:9"
_AWS_ENDPOINTS = {
    "AWS_ENDPOINT_URL": "http://127.0.0.1:4566",
    "DYNAMODB_ENDPOINT_URL": "http://127.0.0.1:18000",
}
_HEALTHY = {
    "version": EMULATOR_VERSION,
    "services": dict.fromkeys(("cognito-idp", "dynamodb", "iam", "s3"), "running"),
}


def _hostile_environment(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    marker = tmp_path / "credential_process_executado"
    process = f"{sys.executable} -c \"open(r'{marker}', 'w').close()\""
    config = tmp_path / "config"
    config.write_text(
        f"[default]\ncredential_process = {process}\nendpoint_url = {_UNROUTABLE}\n"
        f"[profile hostil]\ncredential_process = {process}\nregion = sa-east-1\n",
        encoding="utf-8",
    )
    hostile = {
        "AWS_PROFILE": "hostil",
        "AWS_CONFIG_FILE": str(config),
        "AWS_SHARED_CREDENTIALS_FILE": str(config),
        "AWS_ACCESS_KEY_ID": "chave-do-ambiente-hostil",
        "AWS_SECRET_ACCESS_KEY": "valor-do-ambiente",
        "AWS_SESSION_TOKEN": "token-do-ambiente",
        "AWS_DEFAULT_REGION": "sa-east-1",
        "AWS_ENDPOINT_URL": _UNROUTABLE,
        "AWS_ENDPOINT_URL_DYNAMODB": _UNROUTABLE,
        "HTTP_PROXY": _UNROUTABLE,
        "http_proxy": _UNROUTABLE,
        "NO_PROXY": "",
        "no_proxy": "",
    }
    for name, value in hostile.items():
        monkeypatch.setenv(name, value)
    return marker


def _closed_loopback_endpoint() -> str:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    return f"http://127.0.0.1:{port}"


def _health_client(body: dict[str, Any]) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(lambda _: httpx.Response(200, json=body)))


def test_endpoint_padrao_e_loopback_4577() -> None:
    assert floci_endpoint({}) == DEFAULT_ENDPOINT == "http://127.0.0.1:4577"


def test_endpoint_ignora_variaveis_de_endpoint_aws() -> None:
    assert floci_endpoint(_AWS_ENDPOINTS) == DEFAULT_ENDPOINT


@pytest.mark.parametrize(
    "endpoint", ["http://localhost:4577", "http://[::1]:4577", "http://127.0.0.2:4577/"],
)
def test_endpoint_aceita_loopback_explicito(endpoint: str) -> None:
    assert floci_endpoint({"FLOCI_ENDPOINT_URL": endpoint}) == endpoint.rstrip("/")


@pytest.mark.parametrize(
    "endpoint",
    ["http://10.0.0.5:4577", "http://floci:4566", "https://floci.example.com", _UNROUTABLE],
)
def test_endpoint_fora_de_loopback_falha_fechado(endpoint: str) -> None:
    with pytest.raises(FlociIsolationError, match="reason=non_loopback"):
        floci_endpoint({"FLOCI_ENDPOINT_URL": endpoint})


@pytest.mark.parametrize(
    "endpoint",
    [
        "ftp://127.0.0.1:4577",
        "http://usuario:senha@127.0.0.1:4577",
        "http://127.0.0.1:4577/prefixo",
        "http://127.0.0.1:4577?x=1",
    ],
)
def test_endpoint_com_esquema_credencial_ou_caminho_falha_fechado(endpoint: str) -> None:
    with pytest.raises(FlociIsolationError, match="floci_endpoint=rejected"):
        floci_endpoint({"FLOCI_ENDPOINT_URL": endpoint})


def test_sessao_isolada_nao_le_perfil_config_nem_credenciais_do_ambiente(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    marker = _hostile_environment(monkeypatch, tmp_path)
    session = isolated_session()

    assert session.get_credentials() is None
    assert session.get_scoped_config() == {}
    assert not marker.exists()


def test_cliente_usa_endpoint_e_credencial_ficticia_explicitos_em_ambiente_hostil(
    floci: FlociContext, monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    marker = _hostile_environment(monkeypatch, tmp_path)
    client = new_client(isolated_session(), "dynamodb", floci.endpoint, ADMIN)
    signatures: list[str] = []

    def _record(request: Any, **_: Any) -> None:
        signatures.append(str(request.headers.get("Authorization")))

    client.meta.events.register("before-send.dynamodb.ListTables", _record)
    client.list_tables(Limit=1)

    assert client.meta.endpoint_url == floci.endpoint
    assert client.meta.region_name == REGION
    assert len(signatures) == 1
    assert f"Credential={ADMIN.access_key_id}/" in signatures[0]
    assert "chave-do-ambiente-hostil" not in signatures[0]
    assert not marker.exists()


def test_health_de_emulador_inacessivel_reporta_unreachable() -> None:
    with http_client() as http:
        assert health_problem(http, _closed_loopback_endpoint()) == "unreachable"


def test_health_rejeita_versao_diferente_da_fixada() -> None:
    with _health_client({**_HEALTHY, "version": "2.1.0"}) as http:
        assert health_problem(http, DEFAULT_ENDPOINT) == "version=2.1.0"


def test_health_exige_servicos_da_suite_rodando() -> None:
    services = {**_HEALTHY["services"], "iam": "disabled", "s3": "starting"}
    with _health_client({**_HEALTHY, "services": services}) as http:
        assert health_problem(http, DEFAULT_ENDPOINT) == "services_down=iam,s3"


def test_health_aceita_emulador_fixado_com_servicos_rodando() -> None:
    with _health_client(_HEALTHY) as http:
        assert health_problem(http, DEFAULT_ENDPOINT) is None


def test_floci_em_execucao_e_a_versao_fixada(floci: FlociContext) -> None:
    with http_client() as http:
        assert health_problem(http, floci.endpoint) is None
