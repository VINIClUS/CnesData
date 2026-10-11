"""Isolamento da camada Floci: endpoint só de FLOCI_ENDPOINT_URL e credenciais fictícias."""
from __future__ import annotations

import ipaddress
import os
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit
from uuid import uuid4

import botocore.session
import httpx
from botocore.config import Config
from botocore.credentials import CredentialResolver
from botocore.exceptions import ClientError

if TYPE_CHECKING:
    from collections.abc import Mapping

ENDPOINT_VARIABLE = "FLOCI_ENDPOINT_URL"
DEFAULT_ENDPOINT = "http://127.0.0.1:4577"
REGION = "us-east-2"
ACCOUNT_ID = "000000000000"
EMULATOR_VERSION = "2.2.0"
NAME_PREFIX = "cnesdata-cap"
REQUIRED_SERVICES = ("cognito-idp", "dynamodb", "iam", "s3")
_SCHEMES = frozenset({"http", "https"})
_LOCALHOST = "localhost"
# AWS_PROFILE, AWS_CONFIG_FILE and AWS_SHARED_CREDENTIALS_FILE are never consulted: the
# variables are unmapped and both files resolve to os.devnull.
_SESSION_VARIABLES: dict[str, Any] = {
    "profile": (None, None, None, None),
    "config_file": (None, None, os.devnull, None),
    "credentials_file": (None, None, os.devnull, None),
}
# An empty (not None) proxies mapping stops botocore from reading HTTP(S)_PROXY.
_CLIENT_CONFIG = Config(
    retries={"max_attempts": 1, "mode": "standard"},
    s3={"addressing_style": "path"},
    proxies={},
    connect_timeout=5,
    read_timeout=30,
)


class FlociIsolationError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class Credentials:
    access_key_id: str
    secret_access_key: str = field(repr=False)
    session_token: str | None = field(default=None, repr=False)


# "test" is the access key Floci documents as always bypassing IAM enforcement: the operator.
ADMIN = Credentials("test", "test")


def floci_endpoint(environ: Mapping[str, str]) -> str:
    """Resolve o endpoint do Floci exclusivamente de FLOCI_ENDPOINT_URL.

    Raises:
        FlociIsolationError: esquema, credencial embutida, caminho ou host fora de loopback.
    """
    parts = urlsplit(environ.get(ENDPOINT_VARIABLE) or DEFAULT_ENDPOINT)
    if parts.scheme not in _SCHEMES or parts.username or parts.password:
        raise FlociIsolationError("floci_endpoint=rejected reason=scheme_or_userinfo")
    if parts.path not in {"", "/"} or parts.query or parts.fragment:
        raise FlociIsolationError("floci_endpoint=rejected reason=path")
    host = parts.hostname or ""
    if not _is_loopback(host):
        raise FlociIsolationError(f"floci_endpoint=rejected reason=non_loopback host={host}")
    return f"{parts.scheme}://{parts.netloc}"


def _is_loopback(host: str) -> bool:
    if host == _LOCALHOST:
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def isolated_session() -> botocore.session.Session:
    """Returns: sessão botocore sem perfil, arquivos de config, cadeia de credenciais nem IMDS."""
    session = botocore.session.Session(session_vars=_SESSION_VARIABLES)
    session.register_component("credential_provider", CredentialResolver(providers=[]))
    return session


def new_client(
    session: botocore.session.Session, service: str, endpoint: str, credentials: Credentials,
) -> Any:
    """Returns: cliente botocore do serviço com credenciais explícitas e sem retry."""
    return session.create_client(
        service,
        region_name=REGION,
        endpoint_url=endpoint,
        aws_access_key_id=credentials.access_key_id,
        aws_secret_access_key=credentials.secret_access_key,
        aws_session_token=credentials.session_token,
        config=_CLIENT_CONFIG,
    )


def http_client() -> httpx.Client:
    """Returns: cliente HTTP que ignora proxies, netrc e certificados do ambiente."""
    return httpx.Client(trust_env=False, follow_redirects=False, timeout=10.0)


def unique_name(suffix: str = "") -> str:
    """Returns: nome efêmero `cnesdata-cap-<uuid8>[-sufixo]`."""
    name = f"{NAME_PREFIX}-{uuid4().hex[:8]}"
    return f"{name}-{suffix}" if suffix else name


def outcome(call: Any, **kwargs: Any) -> str:
    """Returns: "OK" quando a chamada AWS passa; senão o código de erro devolvido."""
    try:
        call(**kwargs)
    except ClientError as error:
        return str(error.response.get("Error", {}).get("Code"))
    return "OK"


def health_problem(http: httpx.Client, endpoint: str) -> str | None:
    """Returns: None quando o Floci fixado está pronto; senão o código do problema."""
    try:
        response = http.get(f"{endpoint}/_floci/health")
        body = response.json()
    except (httpx.HTTPError, ValueError):
        return "unreachable"
    if response.status_code != 200 or not isinstance(body, dict):
        return f"health_status={response.status_code}"
    if body.get("version") != EMULATOR_VERSION:
        return f"version={body.get('version')}"
    services = body.get("services") or {}
    down = [name for name in REQUIRED_SERVICES if services.get(name) != "running"]
    return f"services_down={','.join(down)}" if down else None


@dataclass(frozen=True, slots=True)
class FlociContext:
    endpoint: str
    session: botocore.session.Session

    def client(self, service: str, credentials: Credentials = ADMIN) -> Any:
        return new_client(self.session, service, self.endpoint, credentials)
