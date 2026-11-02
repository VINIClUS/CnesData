"""Provisionamento do login: server, client público, usuário sintético, domain v2 e branding."""

from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import pytest

from tests.integration.aws_capability._aws_fake_types_testing import good
from tests.integration.aws_capability._login_lab_testing import API, create_root, make_login_lab
from tests.integration.aws_capability._login_ports import CALLBACK_URL, LOGOUT_URL, SCOPES
from tests.integration.aws_capability._login_setup import provision
from tests.integration.aws_capability._service_contracts import Reply
from tests.integration.aws_capability.contracts import RequestContext, ServiceFailure

if TYPE_CHECKING:
    from collections.abc import Mapping

NAME = "cnesdata-cap-a1b2c3d4"


def run_setup(tmp_path: Path):
    lab = make_login_lab(tmp_path)
    root = create_root(lab)
    mark = len(lab.aws.requests)
    env = provision(lab.services.ports.commands, lab.spec, root, lab.context)
    return lab, root, env, lab.aws.requests[mark:]


def payload_of(requests, operation: str):
    return next(dict(r.payload) for r in requests if r.operation == operation)


def test_ordem_de_criacao_e_so_as_chamadas_necessarias(tmp_path: Path) -> None:
    _, _, _, requests = run_setup(tmp_path)
    created = [r.operation for r in requests if r.operation.startswith(("create-", "admin-"))]
    assert created == [
        "create-resource-server",
        "create-user-pool-client",
        "admin-create-user",
        "admin-set-user-password",
        "create-user-pool-domain",
        "create-managed-login-branding",
    ]


def test_client_publico_espelha_o_modulo(tmp_path: Path) -> None:
    lab, root, env, requests = run_setup(tmp_path)
    client = payload_of(requests, "create-user-pool-client")
    assert client == {
        "UserPoolId": root.resource_id,
        "ClientName": f"{NAME}-dashboard",
        "GenerateSecret": False,
        "AllowedOAuthFlowsUserPoolClient": True,
        "AllowedOAuthFlows": ["code"],
        "AllowedOAuthScopes": list(SCOPES),
        "CallbackURLs": [CALLBACK_URL],
        "LogoutURLs": [LOGOUT_URL],
        "SupportedIdentityProviders": ["COGNITO"],
        "ExplicitAuthFlows": ["ALLOW_REFRESH_TOKEN_AUTH"],
        "PreventUserExistenceErrors": "ENABLED",
        "EnableTokenRevocation": True,
    }
    assert SCOPES[-1] == f"{API}/api.access"
    assert env.client_id in lab.aws.pools[root.resource_id].clients


def test_usuario_sintetico_sem_email_e_com_senha_permanente(tmp_path: Path) -> None:
    _, root, env, requests = run_setup(tmp_path)
    create = payload_of(requests, "admin-create-user")
    assert create == {
        "UserPoolId": root.resource_id,
        "Username": "cap-a1b2c3d4@example.invalid",
        "UserAttributes": [
            {"Name": "email", "Value": "cap-a1b2c3d4@example.invalid"},
            {"Name": "email_verified", "Value": "true"},
        ],
        "MessageAction": "SUPPRESS",
    }
    password = payload_of(requests, "admin-set-user-password")
    assert password["Permanent"] is True
    assert password["Password"] == env.password
    assert password["Username"] == "cap-a1b2c3d4@example.invalid"
    assert env.username == "cap-a1b2c3d4@example.invalid"
    assert len(env.password) >= 14


def test_domain_managed_login_v2_e_branding_do_cognito(tmp_path: Path) -> None:
    _, root, env, requests = run_setup(tmp_path)
    assert payload_of(requests, "create-user-pool-domain") == {
        "UserPoolId": root.resource_id,
        "Domain": NAME,
        "ManagedLoginVersion": 2,
    }
    assert payload_of(requests, "create-managed-login-branding") == {
        "UserPoolId": root.resource_id,
        "ClientId": env.client_id,
        "UseCognitoProvidedValues": True,
    }
    assert env.origin == "https://cnesdata-cap-a1b2c3d4.auth.us-east-2.amazoncognito.com"


def test_repr_do_ambiente_nao_vaza_segredos(tmp_path: Path) -> None:
    _, _, env, _ = run_setup(tmp_path)
    text = repr(env)
    for secret in (env.client_id, env.password, env.username, env.origin):
        assert secret not in text


def test_recusa_fora_da_lane_de_trabalho(tmp_path: Path) -> None:
    lab = make_login_lab(tmp_path)
    root = create_root(lab)
    context = RequestContext(lab.budget, "cleanup")
    with pytest.raises(ServiceFailure) as error:
        provision(lab.services.ports.commands, lab.spec, root, context)
    assert error.value.args == ("probe=login_setup_invalid",)


def test_readback_do_client_com_fluxo_ampliado_aborta_antes_do_usuario(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lab = make_login_lab(tmp_path)
    root = create_root(lab)
    original = lab.aws._cognito_idp_describe_user_pool_client

    def widened(payload: dict, who: str) -> Reply:
        reply = original(payload, who)
        row: dict[str, object] = dict(cast("Mapping[str, Any]", reply.data["UserPoolClient"]))
        row["ExplicitAuthFlows"] = ["ALLOW_REFRESH_TOKEN_AUTH", "ALLOW_USER_PASSWORD_AUTH"]
        return good({"UserPoolClient": row})

    monkeypatch.setattr(lab.aws, "_cognito_idp_describe_user_pool_client", widened)
    with pytest.raises(ServiceFailure) as error:
        provision(lab.services.ports.commands, lab.spec, root, lab.context)
    assert error.value.args == ("probe=step_failed step=client_readback",)
    assert "admin-create-user" not in lab.aws.operations()


def test_resposta_de_client_sem_id_aborta_sem_domain(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lab = make_login_lab(tmp_path)
    root = create_root(lab)
    monkeypatch.setattr(
        lab.aws, "_cognito_idp_create_user_pool_client", lambda payload, who: good({})
    )
    with pytest.raises(ServiceFailure) as error:
        provision(lab.services.ports.commands, lab.spec, root, lab.context)
    assert error.value.args == ("probe=step_failed step=client",)
    assert "create-user-pool-domain" not in lab.aws.operations()
