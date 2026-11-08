import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
OIDC_SETTINGS = REPO_ROOT / "apps/web_dashboard/src/auth/oidcSettings.ts"
DASHBOARD_ENV = REPO_ROOT / "apps/web_dashboard/src/lib/env.ts"
DEPLOY_MAIN = REPO_ROOT / ".github/workflows/deploy-main.yml"
DASHBOARD_ENDPOINTS = {
    "authorization_endpoint": "authorize_url", "token_endpoint": "token_url",
    "end_session_endpoint": "logout_url",
}


def _contract(path: Path, pattern: str, name: str) -> re.Match[str]:
    match = re.search(pattern, path.read_text(encoding="utf-8"), re.MULTILINE)
    if match is None:
        pytest.fail(f"dashboard_contract_missing name={name}")
    return match


def _dashboard_constant(name: str) -> str:
    return _contract(OIDC_SETTINGS, rf'^const {name} = (["`])(?P<value>[^"`]+)\1;$', name)["value"]


def _dashboard_scopes() -> tuple[str, list[str]]:
    resource = _dashboard_constant("API_RESOURCE")
    api_scope = _dashboard_constant("API_SCOPE").replace("${API_RESOURCE}", resource)
    return resource, sorted([*_dashboard_constant("OIDC_SCOPE").split(), api_scope])


def _dashboard_schema() -> tuple[set[str], re.Pattern[str], dict[str, str]]:
    shape = r"^const PRODUCTION_OIDC_DOMAIN =\s*/(?P<pattern>[^\n]+)/;$"
    domain = _contract(DASHBOARD_ENV, shape, "PRODUCTION_OIDC_DOMAIN")["pattern"]
    env, settings = (path.read_text(encoding="utf-8") for path in (DASHBOARD_ENV, OIDC_SETTINGS))
    return (
        set(re.findall(r"^\s+(VITE_OIDC_[A-Z_]+):", env, re.MULTILINE)),
        re.compile(domain.replace("\\/", "/")),
        dict(re.findall(r"(\w+): `\$\{domain\}(/[^`]+)`", settings)),
    )


def _deployed_redirect_uri() -> str:
    pattern = r"VITE_OIDC_REDIRECT_URI=\$\{\{ matrix\.app == 'web_dashboard' && '(?P<uri>[^']+)'"
    return _contract(DEPLOY_MAIN, pattern, "VITE_OIDC_REDIRECT_URI")["uri"]


def _assert_dashboard_env(outputs: Mapping[str, str], client: Mapping[str, Any]) -> None:
    keys, domain_shape, endpoints = _dashboard_schema()
    redirect = urlsplit(_deployed_redirect_uri())
    public_env = {
        "VITE_OIDC_AUTHORITY": outputs["issuer"], "VITE_OIDC_CLIENT_ID": outputs["client_id"],
        "VITE_OIDC_REDIRECT_URI": client["callback_urls"][0], "VITE_OIDC_DOMAIN": outputs["domain"],
    }

    assert set(public_env) == keys
    assert domain_shape.fullmatch(public_env["VITE_OIDC_DOMAIN"])
    assert client["callback_urls"] == [redirect.geturl()]
    assert client["logout_urls"] == [f"{redirect.scheme}://{redirect.netloc}"]
    assert {name: outputs["domain"] + endpoints[name] for name in DASHBOARD_ENDPOINTS} == {
        name: outputs[output] for name, output in DASHBOARD_ENDPOINTS.items()
    }
