"""Laboratório do cognito-login sobre a fronteira com estado."""

from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from tests.integration.aws_capability._journal import prepare_directory
from tests.integration.aws_capability._login_dns_fake_testing import HOSTS, FakeDns
from tests.integration.aws_capability._login_fake_testing import FakeIdp
from tests.integration.aws_capability._login_listener_testing import FakeListener
from tests.integration.aws_capability._probe_testing import LATENCY, ProbeLab, make_probe_lab
from tests.integration.aws_capability._resource_ids import cognito_child
from tests.integration.aws_capability._safety import run_name, tags
from tests.integration.aws_capability._service_contracts import Reply
from tests.integration.aws_capability.contracts import Creation, RequestContext, Resource
from tests.integration.aws_capability.services import AwsServices

API = "https://api.cnesdata.com.br"
DNS_LATENCY = 0.5
DNS_KEYS = {
    "dns_delay", "dns_silent", "dns_drop_every", "dns_zone_answer", "dns_resolvers", "dns_probe",
}


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
    dns: FakeDns


def _pick(options: dict[str, Any], keys: set[str]) -> dict[str, Any]:
    return {key: options[key] for key in keys if key in options}


def make_login_world(tmp_path: Path, **options: Any) -> LoginWorld:
    idp_keys = {
        "ready_after", "consume_on_bad_verifier", "refresh_keeps_aud", "sdk_refresh_aud",
        "wrong_client_id", "wrong_id_aud", "bad_issuer", "http_jwks", "access_kind",
        "access_scope",
    }
    human_keys = {"human_seconds", "silent_seconds", "wrong_state_at", "error_at", "empty_code_at"}
    excluded = idp_keys | human_keys | DNS_KEYS
    aws_options = {k: v for k, v in options.items() if k not in excluded}
    lab = make_login_lab(tmp_path, latency=LATENCY, **aws_options)
    advance = lab.aws.advance
    idp = FakeIdp(lambda: lab.aws.pools, advance, LATENCY, **_pick(options, idp_keys))
    listener = FakeListener(idp, lab.spec, advance, **_pick(options, human_keys))
    dns = make_dns(lab, options)
    idp.dns = dns
    lab.aws.idp = idp.initiate_auth
    with_ports(lab, http=idp.client(), listener=listener.factory, dns=dns)
    return LoginWorld(lab, idp, listener, dns)


def make_dns(lab: ProbeLab, options: dict[str, Any]) -> FakeDns:
    delays = _delays(options.get("dns_delay", 0.0))
    dns = FakeDns(
        lab.aws.clock,
        lab.aws.advance,
        latency=DNS_LATENCY,
        publish_delay=delays,
        silent=set(options.get("dns_silent", ())),
        drop_every=options.get("dns_drop_every", 0),
        zone_answer=options.get("dns_zone_answer", "ok"),
        probe_answer=dict(options.get("dns_probe", {})),
    )
    if "dns_resolvers" in options:
        dns.resolver_list = tuple(options["dns_resolvers"])
    _publish_on_domain(lab, dns)
    return dns


def _delays(delay: Any) -> dict[str, float]:
    if isinstance(delay, dict):
        return {str(key): float(value) for key, value in delay.items()}
    return {address: float(delay) for address in HOSTS.values()}


def _publish_on_domain(lab: ProbeLab, dns: FakeDns) -> None:
    original = lab.aws._cognito_idp_create_user_pool_domain

    def created(payload: dict[str, Any], who: str) -> Reply:
        reply = original(payload, who)
        if reply.ok:
            dns.create(f"{payload['Domain']}.auth.{lab.spec.region}.amazoncognito.com")
        return reply

    lab.aws._cognito_idp_create_user_pool_domain = created
