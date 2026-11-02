"""Probe cognito-login: um login humano, códigos em memória e verificações empíricas."""

from collections.abc import Callable, Mapping, Sequence

from tests.integration.aws_capability._login_chain import (
    LOGIN_SECONDS,
    Chain,
    Plan,
    follow,
    make_plan,
)
from tests.integration.aws_capability._login_checks import (
    Kit,
    Observed,
    check_discovery,
    check_negatives,
    check_password_auth,
    check_primary,
    check_refresh,
    check_sdk_refresh,
    check_without_resource,
)
from tests.integration.aws_capability._login_claims import make_verifier
from tests.integration.aws_capability._login_handoff import remove_handoff, write_handoff
from tests.integration.aws_capability._login_http import Http
from tests.integration.aws_capability._login_oauth import await_ready
from tests.integration.aws_capability._login_setup import LoginEnv, provision
from tests.integration.aws_capability._service_contracts import ServicePorts
from tests.integration.aws_capability.contracts import (
    RequestContext,
    Resource,
    RunSpec,
    ServiceFailure,
)

EXCHANGE_RESERVE = 150.0


def probe_login(
    resources: Sequence[Resource], spec: RunSpec, ports: ServicePorts, context: RequestContext
) -> Mapping[str, object]:
    """Returns: observações do login; Raises: ServiceFailure para entrada ou etapa inválida."""
    if context.lane != "work" or len(resources) != 1 or ports.http is None:
        raise ServiceFailure("probe=login_invalid")
    root = resources[0]
    http = Http(ports.http, context)
    issuer = f"https://cognito-idp.{spec.region}.amazonaws.com/{root.resource_id}"
    observed = check_discovery(http, issuer)
    env = provision(ports.commands, spec, root, context)
    kit = Kit(http, env.endpoint, make_verifier(issuer, ports.http), ports.commands, context)
    observed.update(_ready(kit, ports))
    if observed["domain_ready"] == "timeout":
        return observed
    plan = make_plan(env.endpoint)
    chain, seconds = _login(spec, ports, (env, plan), context)
    observed.update({"login": chain.outcome, "login_seconds": seconds})
    if chain.error != "none":
        observed["login_error"] = chain.error
    if chain.outcome == "ok":
        observed.update(_checks(kit, plan, chain, env))
    return observed


def _ready(kit: Kit, ports: ServicePorts) -> Observed:
    start = ports.now()
    polls, ready = await_ready(kit.http, kit.endpoint, ports.pause)
    seconds: object = round(ports.now() - start, 1) if ready else "timeout"
    return {"domain_ready": seconds, "domain_ready_polls": polls}


def _login(
    spec: RunSpec, ports: ServicePorts, material: tuple[LoginEnv, Plan], context: RequestContext
) -> tuple[Chain, float]:
    env, plan = material
    if ports.listener is None:
        raise ServiceFailure("probe=login_ports_unavailable")
    start = ports.now()
    listener = ports.listener()
    try:
        write_handoff(spec, plan.urls["a"], env.username, env.password)
        limit = _limit(context)
        chain = follow(listener, plan, limit, ports.now)
    finally:
        listener.close()
        remove_handoff(spec)
    return chain, round(ports.now() - start, 1)


def _limit(context: RequestContext) -> float:
    remaining = context.budget.remaining_seconds(context.lane)
    return max(0.0, min(LOGIN_SECONDS, remaining - EXCHANGE_RESERVE))


def _checks(kit: Kit, plan: Plan, chain: Chain, env: LoginEnv) -> Observed:
    observed, reply = check_primary(kit, plan, chain)
    observed.update(check_refresh(kit, reply.refresh))
    observed.update(check_sdk_refresh(kit, reply.refresh))
    observed.update(check_without_resource(kit, plan, chain))
    observed.update(check_negatives(kit, plan, chain))
    observed.update(check_password_auth(kit, env.username, env.password))
    return observed


Pause = Callable[[float], None]
