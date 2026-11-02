"""IdP Cognito sintético: discovery, JWKS, authorize, token endpoint e InitiateAuth."""

import base64
import hashlib
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from urllib.parse import parse_qs, urlsplit

import httpx
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from jose import jwk, jwt

from tests.integration.aws_capability._aws_fake_cognito_testing import REGION, FakePool
from tests.integration.aws_capability._aws_fake_types_testing import denied, good
from tests.integration.aws_capability._login_ports import API_ORIGIN, CALLBACK_URL
from tests.integration.aws_capability._service_contracts import Reply

_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)
_PRIVATE = _KEY.private_bytes(
    serialization.Encoding.PEM,
    serialization.PrivateFormat.PKCS8,
    serialization.NoEncryption(),
).decode()
_PUBLIC = _KEY.public_key().public_bytes(
    serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
)
KID = "kid-1"
JWK = {**jwk.construct(_PUBLIC, "RS256").to_dict(), "kid": KID, "use": "sig"}
HOST = f"cognito-idp.{REGION}.amazonaws.com"


def _s256(verifier: str) -> str:
    digest = hashlib.sha256(verifier.encode()).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode()


@dataclass
class FakeIdp:
    aws_pools: Callable[[], Mapping[str, FakePool]]
    advance: Callable[[float], None]
    latency: float = 1.3
    ready_after: int = 2
    consume_on_bad_verifier: bool = False
    refresh_keeps_aud: bool = True
    sdk_refresh_aud: bool = False
    codes: dict[str, dict[str, object]] = field(default_factory=dict)
    refreshes: dict[str, str | None] = field(default_factory=dict)
    polls: int = 0
    issued: int = 0
    requests: list[tuple[str, str]] = field(default_factory=list)

    @property
    def pool(self) -> FakePool:
        return next(iter(self.aws_pools().values()))

    @property
    def client_id(self) -> str:
        return next(iter(self.pool.clients))

    @property
    def issuer(self) -> str:
        return f"https://{HOST}/{self.pool.pool_id}"

    @property
    def origin(self) -> str:
        return f"https://{self.pool.domain}.auth.{REGION}.amazoncognito.com"

    def client(self) -> httpx.Client:
        return httpx.Client(transport=httpx.MockTransport(self.handle))

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.advance(self.latency)
        path = request.url.path
        self.requests.append((request.method, path))
        if request.url.host == HOST:
            return self._metadata(path)
        if path == "/oauth2/authorize":
            return self._authorize_page()
        if path == "/oauth2/token" and request.method == "POST":
            form = {k: v[0] for k, v in parse_qs(request.content.decode()).items()}
            return self._token(form)
        return httpx.Response(404, json={})

    def _metadata(self, path: str) -> httpx.Response:
        if path.endswith("/openid-configuration"):
            document = {"issuer": self.issuer, "jwks_uri": f"{self.issuer}/.well-known/jwks.json"}
            return httpx.Response(200, json=document)
        return httpx.Response(200, json={"keys": [JWK]})

    def _authorize_page(self) -> httpx.Response:
        pool = self.pool
        if pool.domain is None or not pool.brandings or self.polls < self.ready_after:
            self.polls += 1
            return httpx.Response(404, json={})
        return httpx.Response(200, text="<html>login</html>")

    def authorize(self, query: Mapping[str, str]) -> str | None:
        valid = (
            query.get("client_id") == self.client_id
            and query.get("redirect_uri") == CALLBACK_URL
            and query.get("code_challenge_method") == "S256"
            and query.get("response_type") == "code"
        )
        if not valid:
            return None
        self.issued += 1
        self.requests.append(("AUTHORIZE", "/oauth2/authorize"))
        code = f"code-{self.issued}"
        resource = query.get("resource")
        self.codes[code] = {
            "challenge": query["code_challenge"],
            "redirect": query["redirect_uri"],
            "resource": resource,
            "used": False,
        }
        return code

    def _token(self, form: Mapping[str, str]) -> httpx.Response:
        if form.get("grant_type") == "refresh_token":
            return self._refresh(form)
        record = self.codes.get(form.get("code", ""))
        error = self._grant_error(record, form)
        if error is not None:
            return httpx.Response(400, json={"error": error})
        assert record is not None
        record["used"] = True
        resource = record["resource"]
        assert resource is None or isinstance(resource, str)
        return httpx.Response(200, json=self._issue(resource, True))

    def _grant_error(self, record: dict[str, object] | None, form: Mapping[str, str]) -> str | None:
        if form.get("client_id") != self.client_id:
            return "invalid_client"
        if record is None or record["used"]:
            return "invalid_grant"
        if form.get("redirect_uri") != record["redirect"]:
            return "invalid_grant"
        if _s256(form.get("code_verifier", "")) != record["challenge"]:
            record["used"] = self.consume_on_bad_verifier
            return "invalid_grant"
        return None

    def _refresh(self, form: Mapping[str, str]) -> httpx.Response:
        token = form.get("refresh_token", "")
        if token not in self.refreshes:
            return httpx.Response(400, json={"error": "invalid_grant"})
        resource = self.refreshes[token] if self.refresh_keeps_aud else None
        return httpx.Response(200, json=self._issue(resource, False))

    def _issue(self, resource: str | None, with_refresh: bool) -> dict[str, str]:
        now = int(time.time())
        base = {"sub": "sub-1", "iss": self.issuer, "iat": now, "exp": now + 3600}
        scope = f"openid email profile {API_ORIGIN}/api.access"
        access: dict[str, object] = {
            **base,
            "client_id": self.client_id,
            "token_use": "access",
            "scope": scope,
            "jti": f"jti-{self.issued}",
        }
        if resource is not None:
            access["aud"] = resource
        identity = {**base, "aud": self.client_id, "token_use": "id", "email": "x"}
        tokens = {
            "access_token": jwt.encode(access, _PRIVATE, "RS256", headers={"kid": KID}),
            "id_token": jwt.encode(identity, _PRIVATE, "RS256", headers={"kid": KID}),
        }
        if with_refresh:
            refresh = f"refresh-{self.issued}"
            self.refreshes[refresh] = resource
            tokens["refresh_token"] = refresh
        return tokens

    def initiate_auth(self, payload: Mapping[str, object]) -> Reply:
        flow = payload.get("AuthFlow")
        params = payload.get("AuthParameters")
        if flow != "REFRESH_TOKEN_AUTH" or not isinstance(params, Mapping):
            return denied("InvalidParameterException")
        token = str(params.get("REFRESH_TOKEN", ""))
        if token not in self.refreshes:
            return denied("NotAuthorizedException")
        resource = self.refreshes[token] if self.sdk_refresh_aud else None
        issued = self._issue(resource, False)
        result = {"AccessToken": issued["access_token"], "IdToken": issued["id_token"]}
        return good({"AuthenticationResult": result})


def query_of(url: str) -> dict[str, str]:
    return {k: v[0] for k, v in parse_qs(urlsplit(url).query).items()}
