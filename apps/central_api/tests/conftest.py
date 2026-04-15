"""Fixtures de integração — sobe stack Docker via pytest-docker."""

import urllib.request

import pytest


@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig):
    return str(pytestconfig.rootpath / "docker-compose.yml")


@pytest.fixture(scope="session")
def api_url(docker_services):
    port = docker_services.port_for("central-api", 8000)
    url = f"http://localhost:{port}"
    docker_services.wait_until_responsive(
        check=lambda: _is_healthy(url),
        timeout=120.0,
        pause=3.0,
    )
    return url


def _is_healthy(url: str) -> bool:
    try:
        resp = urllib.request.urlopen(
            f"{url}/api/v1/system/health", timeout=5,
        )
        return resp.status == 200
    except Exception:
        return False
