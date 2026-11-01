"""Probe s3-retention: GOVERNANCE, permissões aditivas e bypass com controles positivos."""

from dataclasses import dataclass

from tests.integration.aws_capability._assumed_session import assume_role_session
from tests.integration.aws_capability._iam_policy_ops import put_inline_policy
from tests.integration.aws_capability._probe_attempt import must, retry_denied, version_of
from tests.integration.aws_capability._s3_actor import Actor
from tests.integration.aws_capability._service_contracts import ServicePorts
from tests.integration.aws_capability.contracts import RunSpec

KEY = "cap-locked"
ROLE_KEY = "cap-role-locked"
P0 = ("s3:GetObject", "s3:DeleteObject", "s3:PutObject")
P1 = ("s3:DeleteObjectVersion", "s3:PutObjectRetention")
HOLD_SECONDS = 600
SHORTEN_SECONDS = 300
EXTEND_SECONDS = 900
ROLE_EXTEND_SECONDS = 1200


@dataclass(frozen=True)
class _Actors:
    operator: Actor
    role: Actor
    ports: ServicePorts
    spec: RunSpec


def probe_retention(operator: Actor, ports: ServicePorts, spec: RunSpec) -> dict[str, object]:
    """Returns: códigos e segundos observados; Raises: ServiceFailure sem negação inválida."""
    base = ports.now()
    first, second, observed = _operator_phase(operator, base)
    session, seconds = assume_role_session(ports, spec, operator.context)
    observed["assume_seconds"] = seconds
    role = Actor(session, operator.bucket, operator.account, operator.context)
    actors = _Actors(operator, role, ports, spec)
    observed.update(_denied_phase(actors, second))
    observed.update(_bypass_phase(actors, (first, base)))
    observed["operator_bypass_delete"] = operator.delete(KEY, first, True).code
    return observed


def _operator_phase(operator: Actor, base: float) -> tuple[str, str, dict[str, object]]:
    first = version_of(must(operator.put(KEY, base + HOLD_SECONDS), "put"))
    observed: dict[str, object] = {
        "retained_delete": operator.delete(KEY, first, False).code,
        "retained_shorten": operator.retention(KEY, first, base + SHORTEN_SECONDS, False).code,
        "retained_extend": operator.retention(KEY, first, base + EXTEND_SECONDS, False).code,
        "suspend_versioning": operator.suspend().code,
    }
    second = version_of(must(operator.put(KEY, None), "overwrite"))
    observed["overwrite_new_version"] = second != first
    observed["new_version_retention"] = operator.read_retention(KEY, second).code
    return first, second, observed


def _denied_phase(actors: _Actors, version: str) -> dict[str, object]:
    operator, role, ports, spec = actors.operator, actors.role, actors.ports, actors.spec
    must(put_inline_policy(ports.commands, spec, ("cap-p0", P0), operator.context), "p0_policy")
    _, ready = retry_denied(lambda: role.head(KEY), ports, operator.context, "p0")
    return {
        "p0_ready_seconds": ready,
        "p0_delete_version": role.delete(KEY, version, False).code,
        "p0_put_lock_headers": role.put(ROLE_KEY, ports.now() + HOLD_SECONDS).code,
    }


def _bypass_phase(actors: _Actors, retained: tuple[str, float]) -> dict[str, object]:
    operator, role, ports, spec = actors.operator, actors.role, actors.ports, actors.spec
    version, base = retained
    must(put_inline_policy(ports.commands, spec, ("cap-p1", P1), operator.context), "p1_policy")
    _, ready = retry_denied(
        lambda: role.retention(KEY, version, base + ROLE_EXTEND_SECONDS, False),
        ports,
        operator.context,
        "p1",
    )
    return {
        "p1_ready_seconds": ready,
        "p1_delete_bypass": role.delete(KEY, version, True).code,
        "p1_delete_retained": role.delete(KEY, version, False).code,
    }
