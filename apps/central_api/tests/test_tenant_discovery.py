"""Descoberta AWS atravessa JWT/JWKS, middleware e persistência canônica."""

import re
import time
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import httpx
import pytest
from botocore.exceptions import ClientError
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient
from jose import jwt
from jose.utils import base64url_encode

from central_api.auth.aws_oidc import MembershipAuthorizer
from central_api.middleware import AuthMiddleware
from central_api.routes import dashboard
from cnes_domain.control_plane.entities import Membership, Tenant
from cnes_infra.auth.dynamodb_memberships import DynamoDBMembershipCandidates
from cnes_infra.auth.oidc import OidcPrincipal, OidcVerifier
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.control_plane.dynamodb_codec import encode_model
from cnes_infra.control_plane.dynamodb_keys import entity_key, key_component, membership_key

if TYPE_CHECKING:
    from botocore.client import BaseClient

ISSUER = "https://idp.example.test"
SUBJECT = "cognito-subject-nao-uuid"
ROOT = "/api/v1/dashboard"
NOW = datetime(2026, 10, 25, tzinfo=UTC)
PATHS = (f"{ROOT}/auth/me", f"{ROOT}/memberships")


class DynamoBoundary:
    def __init__(self) -> None:
        self.pages: list[list[str]] = [[]]
        self.items: dict[tuple[str, str], Any] = {}
        self.queries: list[dict[str, Any]] = []
        self.reads: list[dict[str, Any]] = []
        self.failure: str | None = None

    def query(self, **request: Any) -> dict[str, Any]:
        self.queries.append(request)
        self._fail("query")
        page = int(request.get("ExclusiveStartKey", {}).get("page", {"N": "0"})["N"])
        result: dict[str, Any] = {
            "Items": [{"gsi1sk": {"S": f"TENANT#{key_component(t)}"}} for t in self.pages[page]]
        }
        if page + 1 < len(self.pages):
            result["LastEvaluatedKey"] = {"page": {"N": str(page + 1)}}
        return result

    def get_item(self, **request: Any) -> dict[str, Any]:
        self.reads.append(request)
        key = request["Key"]
        identity = key["pk"]["S"], key["sk"]["S"]
        self._fail("tenant" if identity[1].startswith("TENANT#") else "membership")
        item = self.items.get(identity)
        return {} if item is None else {"Item": item}

    def scan(self, **request: Any) -> None:
        pytest.fail("descoberta_nao_pode_usar_scan")

    def _fail(self, operation: str) -> None:
        if self.failure == operation or (
            self.failure == "query_late" and operation == "query" and len(self.queries) > 1
        ):
            raise ClientError(
                {"Error": {"Code": "InternalServerError", "Message": "SECRET-internal-table"}},
                operation,
            )

    def grant(self, tenant_id: str, issuer: str | None = ISSUER, role: str = "gestor") -> None:
        member = Membership(
            tenant_id=tenant_id, user_id=SUBJECT, role=role, created_at=NOW, oidc_issuer=issuer
        )
        tenant = Tenant(
            tenant_id=tenant_id, municipality_name=f"Município {tenant_id}", created_at=NOW
        )
        self.items[membership_key(tenant_id, SUBJECT)] = encode_model(
            member, "MEMBERSHIP", membership_key(tenant_id, SUBJECT)
        )
        self.items[entity_key(tenant_id, "TENANT", tenant_id)] = encode_model(
            tenant, "TENANT", entity_key(tenant_id, "TENANT", tenant_id)
        )


def _keys() -> tuple[str, dict[str, Any]]:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    numbers = key.public_key().public_numbers()

    def b64(value: int) -> str:
        return base64url_encode(value.to_bytes((value.bit_length() + 7) // 8, "big")).decode()

    pem = key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    ).decode()
    return pem, {
        "kty": "RSA",
        "kid": "test",
        "alg": "RS256",
        "use": "sig",
        "n": b64(numbers.n),
        "e": b64(numbers.e),
    }


def _app(control, authorizer):
    app = FastAPI()
    app.add_middleware(AuthMiddleware)
    app.include_router(dashboard.router, prefix=ROOT)
    app.state.runtime = SimpleNamespace(
        control_plane=control, services=SimpleNamespace(membership_authorizer=authorizer)
    )
    app.state.auth_required = "optional"

    @app.get(f"{ROOT}/produto")
    def product(request: Request):
        return {"tenant_id": request.state.authorized_tenant.tenant_id}

    return app


@pytest.fixture
def discovery(monkeypatch):
    for name, value in {
        "DB_URL": "sqlite+pysqlite:///:memory:",
        "PROFILE": "aws",
        "AWS_ACCESS_KEY_ID": "test",
        "AWS_SECRET_ACCESS_KEY": "test",
        "AWS_EC2_METADATA_DISABLED": "true",
    }.items():
        monkeypatch.setenv(name, value)
    boundary = DynamoBoundary()
    control = DynamoDBControlPlane(boundary, "test-control-plane", lambda: NOW)
    candidates = DynamoDBMembershipCandidates(cast("BaseClient", boundary), "test-control-plane")
    authorizer = MembershipAuthorizer(control, candidates)
    pem, jwk = _keys()

    def provider(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/.well-known/openid-configuration":
            return httpx.Response(200, json={"issuer": ISSUER, "jwks_uri": f"{ISSUER}/keys"})
        assert str(request.url) == f"{ISSUER}/keys"
        return httpx.Response(200, json={"keys": [jwk]})

    app = _app(control, authorizer)
    with httpx.Client(transport=httpx.MockTransport(provider)) as http:
        app.state.oidc_verifier = OidcVerifier(ISSUER, "dashboard", http)
        now = int(time.time())
        token = jwt.encode(
            {"iss": ISSUER, "sub": SUBJECT, "aud": "dashboard", "iat": now, "exp": now + 300},
            pem,
            algorithm="RS256",
            headers={"kid": "test"},
        )
        with TestClient(app, raise_server_exceptions=False) as client:
            yield SimpleNamespace(
                client=client,
                db=boundary,
                authorizer=authorizer,
                headers={"Authorization": f"Bearer {token}"},
                app=app,
            )


def _no_store(response) -> None:
    directives = {part.strip() for part in response.headers.get("cache-control", "").split(",")}
    assert directives >= {"private", "no-store"}


def test_identidade_aws_sem_uuid_tenant_ou_membership(discovery) -> None:
    response = discovery.client.get(PATHS[0], headers=discovery.headers)
    assert response.status_code == 200
    assert response.json() == {
        "subject": SUBJECT,
        "issuer": ISSUER,
        "email": None,
        "display_name": None,
    }
    _no_store(response)
    assert discovery.db.queries == discovery.db.reads == []


def test_descobre_todas_paginas_ordena_deduplica_e_revalida_fortemente(discovery) -> None:
    db = discovery.db
    db.pages = [["tenant-z", "deleted", "foreign"], ["tenant-a", "tenant-z", "legacy"]]
    db.grant("tenant-z", role="leitor")
    db.grant("tenant-a")
    db.grant("foreign", "https://other.example.test")
    db.grant("legacy", None)
    response = discovery.client.get(
        PATHS[1], headers=discovery.headers | {"X-Tenant-Id": "unrelated"}
    )
    assert response.status_code == 200
    assert response.json() == [
        {
            "tenant_id": t,
            "municipality_name": f"Município {t}",
            "role": "leitor" if t == "tenant-z" else "gestor",
        }
        for t in ("legacy", "tenant-a", "tenant-z")
    ]
    _no_store(response)
    assert len(db.queries) == 2
    assert "ExclusiveStartKey" not in db.queries[0]
    assert db.queries[1]["ExclusiveStartKey"] == {"page": {"N": "1"}}
    assert all(q["IndexName"] == "gsi1" for q in db.queries)
    assert all(
        q["ExpressionAttributeValues"][":user"] == {"S": f"USER#{key_component(SUBJECT)}"}
        for q in db.queries
    )
    assert all(r["ConsistentRead"] is True for r in db.reads)
    member_reads = [r for r in db.reads if r["Key"]["sk"]["S"].startswith("MEMBERSHIP#")]
    assert len(member_reads) == 5


@pytest.mark.parametrize("foreign", ["empty", "issuer", "user", "tenant", "deleted"])
def test_sem_grants_ou_somente_candidato_alheio_retorna_lista_vazia(discovery, foreign) -> None:
    if foreign != "empty":
        discovery.db.pages = [["foreign"]]
    if foreign in {"issuer", "user", "tenant"}:
        discovery.db.grant(
            "foreign", "https://other.example.test" if foreign == "issuer" else ISSUER
        )
    if foreign in {"user", "tenant"}:
        field = "user_id" if foreign == "user" else "tenant_id"
        member = Membership(
            tenant_id="foreign", user_id=SUBJECT, role="gestor", created_at=NOW, oidc_issuer=ISSUER
        ).model_copy(update={field: "other"})
        key = membership_key("foreign", SUBJECT)
        discovery.db.items[key] = encode_model(member, "MEMBERSHIP", key)
    response = discovery.client.get(PATHS[1], headers=discovery.headers)
    assert response.status_code == 200
    assert response.json() == []
    _no_store(response)


@pytest.mark.parametrize("path", PATHS)
@pytest.mark.parametrize("token", [None, "invalid"])
@pytest.mark.parametrize("tenant", [None, "tenant-a"])
def test_discovery_exige_bearer_mesmo_auth_optional(discovery, path, token, tenant) -> None:
    headers = {} if token is None else {"Authorization": f"Bearer {token}"}
    if tenant:
        headers["X-Tenant-Id"] = tenant
    response = discovery.client.get(path, headers=headers)
    assert response.status_code == 401
    _no_store(response)
    assert discovery.db.queries == discovery.db.reads == []


@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("GET", "/auth/me/extra"),
        ("GET", "/auth/me/"),
        ("GET", "/memberships/"),
        ("GET", "/memberships-other"),
        ("POST", "/memberships"),
        ("POST", "/auth/me"),
        ("GET", "/tenants"),
        ("GET", "/produto"),
    ],
)
def test_isencao_nao_vaza_para_metodos_ou_paths_semelhantes(discovery, method, path) -> None:
    response = discovery.client.request(method, f"{ROOT}{path}", headers=discovery.headers)
    assert response.status_code == 400
    assert response.json() == {"detail": "tenant_header_required"}


def test_produto_exige_membership_e_preserva_tenant_autorizado(discovery) -> None:
    discovery.db.grant("tenant-a")
    allowed = discovery.client.get(
        f"{ROOT}/produto", headers=discovery.headers | {"X-Tenant-Id": "tenant-a"}
    )
    assert allowed.status_code == 200
    assert allowed.json() == {"tenant_id": "tenant-a"}
    denied = discovery.client.get(
        f"{ROOT}/produto", headers=discovery.headers | {"X-Tenant-Id": "tenant-b"}
    )
    assert denied.status_code == 403


@pytest.mark.parametrize(
    "failure", ["query", "query_late", "membership", "tenant", "missing_tenant"]
)
def test_falha_de_persistencia_nao_vira_lista_parcial_ou_erro_com_segredos(
    discovery, failure
) -> None:
    discovery.db.pages = [["tenant-a"], ["tenant-z"]]
    discovery.db.grant("tenant-a")
    discovery.db.grant("tenant-z")
    discovery.db.failure = failure
    if failure == "missing_tenant":
        del discovery.db.items[entity_key("tenant-z", "TENANT", "tenant-z")]
    responses = [discovery.client.get(PATHS[1], headers=discovery.headers) for _ in range(2)]
    for response in responses:
        assert response.status_code == 503
        _no_store(response)
        assert re.fullmatch(r"[a-z][a-z0-9_]+", response.json()["detail"])
        assert "SECRET" not in response.text
        assert "InternalServerError" not in response.text
    assert responses[0].json() == responses[1].json()


def test_authorizer_real_preserva_issuer_nulo_e_rejeita_outro_usuario(discovery) -> None:
    discovery.db.pages = [["legacy", "tenant-a", "foreign", "deleted", "tenant-a"]]
    discovery.db.grant("legacy", None)
    discovery.db.grant("tenant-a")
    discovery.db.grant("foreign", "https://other.example.test")
    principal = OidcPrincipal(ISSUER, SUBJECT, None, None)
    grants = discovery.authorizer.list_authorized(principal)
    assert {g.tenant_id for g in grants} == {"legacy", "tenant-a"}
    assert (
        discovery.authorizer.list_authorized(OidcPrincipal(ISSUER, "outro-user", None, None)) == ()
    )
    assert all(r["ConsistentRead"] is True for r in discovery.db.reads)


def _schema_properties(schema, document):
    if "$ref" in schema:
        schema = document["components"]["schemas"][schema["$ref"].split("/")[-1]]
    return schema


def test_openapi_declara_alternativas_de_identidade_e_memberships_sem_header_obrigatorio(
    discovery,
) -> None:
    document = discovery.app.openapi()
    me = document["paths"][PATHS[0]]["get"]["responses"]["200"]["content"]["application/json"][
        "schema"
    ]
    alternatives = me.get("anyOf", me.get("oneOf", []))
    fields = [set(_schema_properties(s, document).get("properties", {})) for s in alternatives]
    assert {"subject", "issuer", "email", "display_name"} in fields
    assert {
        "user_id",
        "email",
        "display_name",
        "role",
        "tenant_ids",
        "has_pending_request",
    } in fields
    operation = document["paths"][PATHS[1]]["get"]
    schema = operation["responses"]["200"]["content"]["application/json"]["schema"]
    assert schema["type"] == "array"
    item = _schema_properties(schema["items"], document)
    assert (
        set(item["properties"])
        == set(item["required"])
        == {"tenant_id", "municipality_name", "role"}
    )
    for path in PATHS:
        assert not any(
            p["name"].lower() == "x-tenant-id" and p.get("required")
            for p in document["paths"][path]["get"].get("parameters", [])
        )


def test_candidato_paginado_real_nao_e_grant_sem_leitura_canonica(discovery) -> None:
    discovery.db.pages = [["tenant-z", "deleted"], ["tenant-a", "tenant-z", "foreign"]]
    discovery.db.grant("tenant-z")
    discovery.db.grant("tenant-a", None)
    discovery.db.grant("foreign", "https://other.example.test")
    grants = discovery.authorizer.list_authorized(OidcPrincipal(ISSUER, SUBJECT, None, None))
    assert {g.tenant_id for g in grants} == {"tenant-z", "tenant-a"}
    assert len(discovery.db.queries) == 2
    assert discovery.db.queries[1]["ExclusiveStartKey"] == {"page": {"N": "1"}}
    assert len(discovery.db.reads) == 4
    assert all(r["ConsistentRead"] is True for r in discovery.db.reads)


def test_memberships_e_rota_real_apos_header_valido(discovery) -> None:
    discovery.db.grant("tenant-a")
    discovery.db.pages = [["tenant-a"]]
    response = discovery.client.get(
        PATHS[1], headers=discovery.headers | {"X-Tenant-Id": "tenant-a"}
    )
    assert response.status_code == 200
    assert response.json() == [
        {"tenant_id": "tenant-a", "municipality_name": "Município tenant-a", "role": "gestor"}
    ]
