"""O ensaio espelha infra/opentofu/modules/cognito/main.tf, exceto callback, logout e domain."""

import json
import re
from pathlib import Path
from typing import cast

from tests.integration.aws_capability._login_lab_testing import create_root, make_login_lab
from tests.integration.aws_capability._login_ports import API_ORIGIN, API_SCOPE, SCOPES
from tests.integration.aws_capability._login_setup import client_payload
from tests.integration.aws_capability._service_cognito_create import _payload as pool_payload
from tests.integration.aws_capability._service_cognito_server_create import (
    _payload as server_payload,
)

MODULE = Path(__file__).parents[3] / "infra" / "opentofu" / "modules" / "cognito" / "main.tf"
TEXT = MODULE.read_text()


def block(header: str) -> str:
    start = TEXT.index(header)
    depth, index = 0, TEXT.index("{", start)
    first = index
    while True:
        depth += {"{": 1, "}": -1}.get(TEXT[index], 0)
        if depth == 0:
            return TEXT[first : index + 1]
        index += 1


def scalar(body: str, name: str) -> object:
    match = re.search(rf"^\s*{name}\s*=\s*(.+?)\s*$", body, re.MULTILINE)
    assert match, f"attr_missing={name}"
    raw = match.group(1)
    if raw in {"true", "false"}:
        return raw == "true"
    if re.fullmatch(r"\d+", raw):
        return int(raw)
    if raw.startswith("["):
        return json.loads(raw)
    return json.loads(raw) if raw.startswith('"') else raw


POOL = block('resource "aws_cognito_user_pool" "this"')
CLIENT = block('resource "aws_cognito_user_pool_client" "dashboard"')
DOMAIN = block('resource "aws_cognito_user_pool_domain" "this"')
BRANDING = block('resource "aws_cognito_managed_login_branding" "dashboard"')
SERVER = block('resource "aws_cognito_resource_server" "api"')
LOCALS = block("locals")


def test_pool_espelha_tier_mfa_protecao_e_atributos_de_usuario(tmp_path: Path) -> None:
    payload = pool_payload(make_login_lab(tmp_path).spec)
    assert payload["UserPoolTier"] == scalar(POOL, "user_pool_tier") == "ESSENTIALS"
    assert payload["MfaConfiguration"] == scalar(POOL, "mfa_configuration") == "OFF"
    assert payload["DeletionProtection"] == scalar(POOL, "deletion_protection")
    assert payload["UsernameAttributes"] == scalar(POOL, "username_attributes") == ["email"]
    config = payload["UsernameConfiguration"]
    assert config == {"CaseSensitive": scalar(POOL, "case_sensitive")} == {"CaseSensitive": False}


def test_pool_espelha_politica_de_senha_e_recuperacao(tmp_path: Path) -> None:
    payload = pool_payload(make_login_lab(tmp_path).spec)
    policies = cast("dict[str, dict[str, object]]", payload["Policies"])
    policy = policies["PasswordPolicy"]
    hcl = block("password_policy")
    assert policy == {
        "MinimumLength": scalar(hcl, "minimum_length"),
        "RequireLowercase": scalar(hcl, "require_lowercase"),
        "RequireUppercase": scalar(hcl, "require_uppercase"),
        "RequireNumbers": scalar(hcl, "require_numbers"),
        "RequireSymbols": scalar(hcl, "require_symbols"),
    }
    assert policy["MinimumLength"] == 14
    recovery = block("recovery_mechanism")
    assert payload["AccountRecoverySetting"] == {
        "RecoveryMechanisms": [
            {"Name": scalar(recovery, "name"), "Priority": scalar(recovery, "priority")}
        ]
    }
    admin = block("admin_create_user_config")
    assert payload["AdminCreateUserConfig"] == {
        "AllowAdminCreateUserOnly": scalar(admin, "allow_admin_create_user_only")
    }


def test_resource_server_espelha_identificador_e_escopo(tmp_path: Path) -> None:
    lab = make_login_lab(tmp_path)
    payload = server_payload(create_root(lab))
    assert scalar(LOCALS, "api_origin") == payload["Identifier"] == API_ORIGIN
    scopes = payload["Scopes"]
    assert isinstance(scopes, list)
    assert scopes[0]["ScopeName"] == scalar(LOCALS, "api_scope") == "api.access"
    assert scalar(block("  scope {"), "scope_description") == scopes[0]["ScopeDescription"]


def test_client_espelha_fluxos_escopos_provedores_e_revogacao(tmp_path: Path) -> None:
    lab = make_login_lab(tmp_path)
    payload = client_payload(lab.spec, create_root(lab))
    assert payload["GenerateSecret"] is scalar(CLIENT, "generate_secret") is False
    assert payload["AllowedOAuthFlows"] == scalar(CLIENT, "allowed_oauth_flows") == ["code"]
    assert payload["AllowedOAuthFlowsUserPoolClient"] is scalar(
        CLIENT, "allowed_oauth_flows_user_pool_client"
    )
    assert payload["SupportedIdentityProviders"] == scalar(
        CLIENT, "supported_identity_providers"
    )
    assert payload["ExplicitAuthFlows"] == scalar(CLIENT, "explicit_auth_flows")
    assert payload["PreventUserExistenceErrors"] == scalar(
        CLIENT, "prevent_user_existence_errors"
    )
    assert payload["EnableTokenRevocation"] is scalar(CLIENT, "enable_token_revocation")
    oidc = scalar(LOCALS, "oidc_scopes")
    assert isinstance(oidc, list)
    assert list(SCOPES) == [*oidc, f"{scalar(LOCALS, 'api_origin')}/{scalar(LOCALS, 'api_scope')}"]
    assert SCOPES[-1] == API_SCOPE


def test_unicas_diferencas_do_client_sao_callback_e_logout_locais(tmp_path: Path) -> None:
    lab = make_login_lab(tmp_path)
    payload = client_payload(lab.spec, create_root(lab))
    assert "callback_urls" in CLIENT
    assert "logout_urls" in CLIENT
    assert payload["CallbackURLs"] == ["http://localhost:53682/callback"]
    assert payload["LogoutURLs"] == ["http://localhost:53682/"]
    hcl_keys = set(re.findall(r"^\s*([a-z_]+)\s*=", CLIENT, re.MULTILINE))
    assert hcl_keys - {"name", "user_pool_id", "callback_urls", "logout_urls"} == {
        "generate_secret",
        "allowed_oauth_flows_user_pool_client",
        "allowed_oauth_flows",
        "allowed_oauth_scopes",
        "supported_identity_providers",
        "explicit_auth_flows",
        "prevent_user_existence_errors",
        "enable_token_revocation",
    }


def test_domain_managed_login_v2_e_branding_do_cognito() -> None:
    assert scalar(DOMAIN, "managed_login_version") == 2
    assert scalar(BRANDING, "use_cognito_provided_values") is True
    assert "managed_login_version" in DOMAIN
    assert "classic" not in TEXT.lower()


def test_nome_do_servidor_e_prefixo_do_pool_mais_api(tmp_path: Path) -> None:
    lab = make_login_lab(tmp_path)
    payload = server_payload(create_root(lab))
    assert scalar(SERVER, "name") == "${var.name_prefix}-api"
    assert payload["Name"] == "cnesdata-cap-a1b2c3d4-api"
