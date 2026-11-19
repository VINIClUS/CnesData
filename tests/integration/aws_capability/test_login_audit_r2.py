"""Verificação do desvio: token_use, escopo exato da API e timeout do cliente HTTP real."""

from pathlib import Path

import httpx
import pytest

from tests.integration.aws_capability import _production as production
from tests.integration.aws_capability._login_lab_testing import create_root, make_login_world
from tests.integration.aws_capability._login_ports import API_ORIGIN


def observe(tmp_path: Path, **option: object) -> dict[str, object]:
    world = make_login_world(tmp_path, **option)
    root = create_root(world.lab)
    return dict(world.lab.services.probe([root], world.lab.spec, world.lab.context))


def test_token_use_divergente_aparece_no_rotulo(tmp_path: Path) -> None:
    observed = observe(tmp_path, access_kind="id")
    assert observed.get("a_token_use") == "id"


@pytest.mark.parametrize(
    "scope",
    [
        f"openid email profile {API_ORIGIN}/other.access",
        "openid email profile https://other.example/api.access",
        f"openid email profile {API_ORIGIN}/api.access.extra",
        "openid email profile",
    ],
)
def test_escopo_sem_o_da_api_exata_vira_falso(tmp_path: Path, scope: str) -> None:
    observed = observe(tmp_path, access_scope=scope)
    assert observed["a_scope_api"] is False


def test_escopo_da_api_em_qualquer_posicao_vira_verdadeiro(tmp_path: Path) -> None:
    observed = observe(tmp_path, access_scope=f"{API_ORIGIN}/api.access openid")
    assert observed["a_scope_api"] is True


def test_cliente_http_real_tem_timeout_finito_de_15_segundos() -> None:
    transport = httpx.MockTransport(lambda request: httpx.Response(200, json={}))
    client = production.build_http_client(transport)
    assert production.HTTP_TIMEOUT == 15.0
    assert client.timeout == httpx.Timeout(15.0)
