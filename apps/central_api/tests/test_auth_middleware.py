"""Tests for AuthMiddleware — JWT validation, request.state.user population, AUTH_REQUIRED flag."""
from unittest.mock import MagicMock
from uuid import uuid4

from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from central_api.middleware import AuthMiddleware


def _build_app(validator, repo, auth_required: str = "required") -> FastAPI:
    app = FastAPI()
    app.state.jwt_validator = validator
    app.state.dashboard_repo = repo
    app.state.auth_required = auth_required
    app.add_middleware(AuthMiddleware)

    @app.get("/who")
    def who(request: Request) -> dict:
        u = getattr(request.state, "user", None)
        if u is None:
            return {"user": None}
        return {"user_id": str(u.user_id), "tenants": list(u.tenant_ids)}

    return app


def test_passa_request_sem_authorization_sem_user() -> None:
    app = _build_app(MagicMock(), MagicMock())
    client = TestClient(app)
    r = client.get("/who")
    assert r.status_code == 200
    assert r.json() == {"user": None}


def test_popula_request_state_user_quando_token_valido() -> None:
    validator = MagicMock()
    validator.verify.return_value = {
        "sub": "sub-1", "iss": "https://kc.local",
        "email": "g@m", "name": "Gestor",
    }
    user_id = uuid4()
    repo = MagicMock()
    repo.upsert_user.return_value = MagicMock(
        user_id=user_id, email="g@m", display_name="Gestor",
        role="gestor", tenant_ids=["354130"],
    )
    app = _build_app(validator, repo)
    client = TestClient(app)
    r = client.get("/who", headers={"Authorization": "Bearer abc.def.ghi"})
    assert r.status_code == 200
    assert r.json() == {"user_id": str(user_id), "tenants": ["354130"]}
    repo.upsert_user.assert_called_once_with(
        oidc_subject="sub-1", oidc_issuer="https://kc.local",
        email="g@m", display_name="Gestor",
    )


def test_responde_401_quando_token_invalido_em_modo_required() -> None:
    from cnes_infra.auth import TokenInvalid
    validator = MagicMock()
    validator.verify.side_effect = TokenInvalid("expired")
    app = _build_app(validator, MagicMock(), auth_required="required")
    client = TestClient(app)
    r = client.get("/who", headers={"Authorization": "Bearer bad"})
    assert r.status_code == 401
    assert r.json()["detail"] == "token_invalid"


def test_passa_anonimo_quando_token_invalido_em_modo_optional() -> None:
    from cnes_infra.auth import TokenInvalid
    validator = MagicMock()
    validator.verify.side_effect = TokenInvalid("expired")
    app = _build_app(validator, MagicMock(), auth_required="optional")
    client = TestClient(app)
    r = client.get("/who", headers={"Authorization": "Bearer bad"})
    assert r.status_code == 200
    assert r.json() == {"user": None}


def test_ignora_authorization_sem_prefixo_bearer() -> None:
    app = _build_app(MagicMock(), MagicMock())
    client = TestClient(app)
    r = client.get("/who", headers={"Authorization": "Basic abc"})
    assert r.status_code == 200
    assert r.json() == {"user": None}


def test_passa_anonimo_quando_validator_nao_configurado() -> None:
    app = _build_app(None, None)
    client = TestClient(app)
    r = client.get("/who", headers={"Authorization": "Bearer abc"})
    assert r.status_code == 200
    assert r.json() == {"user": None}


def test_email_e_name_ausentes_no_token_usam_defaults() -> None:
    validator = MagicMock()
    validator.verify.return_value = {
        "sub": "sub-2", "iss": "https://kc.local",
    }
    user_id = uuid4()
    repo = MagicMock()
    repo.upsert_user.return_value = MagicMock(
        user_id=user_id, email="", display_name=None,
        role="gestor", tenant_ids=[],
    )
    app = _build_app(validator, repo)
    client = TestClient(app)
    r = client.get("/who", headers={"Authorization": "Bearer abc"})
    assert r.status_code == 200
    repo.upsert_user.assert_called_once_with(
        oidc_subject="sub-2", oidc_issuer="https://kc.local",
        email="", display_name=None,
    )


def test_pula_validacao_jwks_para_paths_oauth_e_provision() -> None:
    """AuthMiddleware ignora Authorization em /oauth/* e /provision/*.

    Agente usa Bearer opaco (não JWT); rota parseia header própria.
    """
    from cnes_infra.auth import TokenInvalid
    validator = MagicMock()
    validator.verify.side_effect = TokenInvalid("not_jwt")
    app = _build_app(validator, MagicMock(), auth_required="required")

    @app.get("/oauth/test")
    def oauth_test(request: Request) -> dict:
        u = getattr(request.state, "user", None)
        return {"user_set": u is not None,
                "auth_header": request.headers.get("Authorization", "")}

    @app.get("/provision/test")
    def provision_test(request: Request) -> dict:
        return {"auth_header": request.headers.get("Authorization", "")}

    client = TestClient(app)
    r1 = client.get("/oauth/test", headers={"Authorization": "Bearer opaque"})
    assert r1.status_code == 200
    assert r1.json() == {"user_set": False, "auth_header": "Bearer opaque"}

    r2 = client.get("/provision/test", headers={"Authorization": "Bearer opaque"})
    assert r2.status_code == 200
    assert r2.json() == {"auth_header": "Bearer opaque"}

    validator.verify.assert_not_called()
