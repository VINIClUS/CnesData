"""Laboratório do cognito-login sobre a fronteira com estado."""

from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from tests.integration.aws_capability._journal import prepare_directory
from tests.integration.aws_capability._login_fake_testing import FakeIdp
from tests.integration.aws_capability._login_listener_testing import FakeListener
from tests.integration.aws_capability._probe_testing import LATENCY, ProbeLab, make_probe_lab
from tests.integration.aws_capability._resource_ids import cognito_child
from tests.integration.aws_capability._safety import run_name, tags
from tests.integration.aws_capability.contracts import Creation, RequestContext, Resource
from tests.integration.aws_capability.services import AwsServices

API = "https://api.cnesdata.com.br"


def make_login_lab(tmp_path: Path, **options: Any) -> ProbeLab:
    lab = make_probe_lab(tmp_path / "private", "cognito-login", seeded=False, **options)
    prepare_directory(lab.spec.journal_dir)
    return lab


def cleanup_context(lab: ProbeLab) -> RequestContext:
    return RequestContext(lab.budget, "cleanup")


def create_root(lab: ProbeLab) -> Resource:
    spec = lab.spec
    creation = Creation(spec, run_name(spec), tags(spec))
    return lab.services.create("cognito", creation, lab.context)


def with_ports(lab: ProbeLab, **ports: Any) -> AwsServices:
    services = AwsServices(lab.spec, replace(lab.services.ports, **ports))
    lab.services = services
    return services


def server_of(lab: ProbeLab, root: Resource) -> Resource:
    return cognito_child(root, lab.spec, "cognito.server", {"identifier": API})


@dataclass
class LoginWorld:
    lab: ProbeLab
    idp: FakeIdp
    listener: FakeListener


def _pick(options: dict[str, Any], keys: set[str]) -> dict[str, Any]:
    return {key: options[key] for key in keys if key in options}


def make_login_world(tmp_path: Path, **options: Any) -> LoginWorld:
    idp_keys = {
        "ready_after", "consume_on_bad_verifier", "refresh_keeps_aud", "sdk_refresh_aud",
        "wrong_client_id", "wrong_id_aud", "bad_issuer", "http_jwks",
    }
    human_keys = {"human_seconds", "silent_seconds", "wrong_state_at", "error_at", "empty_code_at"}
    aws_options = {k: v for k, v in options.items() if k not in idp_keys | human_keys}
    lab = make_login_lab(tmp_path, latency=LATENCY, **aws_options)
    advance = lab.aws.advance
    idp = FakeIdp(lambda: lab.aws.pools, advance, LATENCY, **_pick(options, idp_keys))
    listener = FakeListener(idp, lab.spec, advance, **_pick(options, human_keys))
    lab.aws.idp = idp.initiate_auth
    with_ports(lab, http=idp.client(), listener=listener.factory)
    return LoginWorld(lab, idp, listener)
