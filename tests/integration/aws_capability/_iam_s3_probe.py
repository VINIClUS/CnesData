"""Probe iam-s3: prefixos, retenção e ações não concedidas, cada negação pareada."""

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass

from tests.integration.aws_capability import _matrix_policies as policies
from tests.integration.aws_capability._assumed_session import assume_role_session
from tests.integration.aws_capability._bucket_policy_probe import BucketPolicyAB
from tests.integration.aws_capability._iam_policy_ops import put_statements
from tests.integration.aws_capability._probe_attempt import (
    Attempt,
    must,
    retry_denied,
    version_of,
)
from tests.integration.aws_capability._s3_actor import Actor
from tests.integration.aws_capability._safety import root_name, run_name
from tests.integration.aws_capability._service_contracts import CommandPort, ServicePorts
from tests.integration.aws_capability.contracts import (
    RequestContext,
    Resource,
    RunSpec,
    ServiceFailure,
)

SEED_KEYS = ("serving/x", "serving/blocked/x", "raw/x", "reconciliation/x")
SERVING, RAW = "serving/x", "raw/x"
PLAIN, LOCKED = "audit/a", "audit/b"
LOCK_SECONDS = 300
EXTEND_SECONDS = 540
SHORTEN_SECONDS = 360
SKIPPED = "skipped"


@dataclass(frozen=True)
class _Matrix:
    ports: ServicePorts
    spec: RunSpec
    context: RequestContext
    bucket: str

    @property
    def arn(self) -> str:
        return f"arn:aws:s3:::{self.bucket}"

    def actor(self, commands: CommandPort) -> Actor:
        return Actor(commands, self.bucket, self.spec.account, self.context)

    def control(self, action: Callable[[], Attempt], step: str) -> float:
        _, seconds = retry_denied(action, self.ports, self.context, step)
        return seconds

    def seed(self) -> None:
        operator = self.actor(self.ports.commands)
        for key in SEED_KEYS:
            must(operator.put(key, None), "seed")

    def open(
        self, suffix: str, policy: str, statements: Sequence[Mapping[str, object]]
    ) -> tuple[Actor, float]:
        session, seconds = assume_role_session(self.ports, self.spec, self.context, suffix)
        target = (root_name(self.spec, suffix), policy)
        must(put_statements(self.ports.commands, target, statements, self.context), "policy")
        return self.actor(session), seconds

    def audit(self) -> dict[str, object]:
        actor, seconds = self.open("audit", policies.AUDIT_S3, policies.audit_s3(self.arn))
        now = self.ports.now
        plain, ready = retry_denied(
            lambda: actor.put(PLAIN, None), self.ports, self.context, "audit_put"
        )
        locked = actor.put(LOCKED, now() + LOCK_SECONDS)
        observed: dict[str, object] = {
            "assume_audit_seconds": seconds,
            "audit_put_ready_seconds": ready,
            "audit_put_lock": locked.code,
            "audit_lock_config": actor.lock_config().code,
        }
        version = version_of(locked) if locked.ok else None
        observed.update(self.extension(actor, version))
        observed.update(self.reads(actor))
        observed["audit_get_retention"] = (
            SKIPPED if version is None else actor.read_retention(LOCKED, version).code
        )
        observed.update(self.audit_ungranted(actor, version_of(plain)))
        return observed

    def extension(self, actor: Actor, version: str | None) -> dict[str, object]:
        if version is None:
            return {
                "audit_retention_extend_ready_seconds": SKIPPED,
                "audit_retention_shorten": SKIPPED,
            }
        now = self.ports.now
        seconds = self.control(
            lambda: actor.retention(LOCKED, version, now() + EXTEND_SECONDS, False),
            "audit_retention_extend",
        )
        shorten = actor.retention(LOCKED, version, now() + SHORTEN_SECONDS, False)
        return {
            "audit_retention_extend_ready_seconds": seconds,
            "audit_retention_shorten": shorten.code,
        }

    def reads(self, actor: Actor) -> dict[str, object]:
        seconds = self.control(lambda: actor.head(PLAIN), "audit_get")
        return {
            "audit_get_ready_seconds": seconds,
            "audit_get_raw": actor.head(RAW).code,
            "audit_get_missing": actor.head("audit/missing").code,
        }

    def audit_ungranted(self, actor: Actor, version: str) -> dict[str, object]:
        must(actor.head(PLAIN), "audit_live_control")
        return {
            "audit_delete_version_ungranted": actor.delete(PLAIN, version, False).code,
            "audit_list_ungranted": actor.list("audit/").code,
        }

    def api(self) -> dict[str, object]:
        actor, seconds = self.open("api", policies.API_S3, policies.api_s3(self.arn))
        observed: dict[str, object] = {"assume_api_seconds": seconds}
        observed["api_get_ready_seconds"] = self.control(lambda: actor.head(SERVING), "api_get")
        observed["api_get_missing"] = actor.head("serving/missing").code
        observed["api_get_raw"] = actor.head(RAW).code
        observed["api_list_ready_seconds"] = self.control(
            lambda: actor.list("serving/"), "api_list"
        )
        observed["api_list_raw"] = actor.list("raw/").code
        must(actor.head(SERVING), "api_live_control")
        observed["api_put_object_ungranted"] = actor.put("serving/y", None).code
        policy = BucketPolicyAB(self.ports, self.spec, self.context, self.bucket)
        observed.update(policy.run(self.actor(self.ports.commands), actor))
        return observed


def probe_iam_s3(
    resources: Sequence[Resource], spec: RunSpec, ports: ServicePorts, context: RequestContext
) -> Mapping[str, object]:
    """Returns: códigos e segundos; Raises: ServiceFailure sem controle verde."""
    name = run_name(spec)
    roles = [Resource("iam", root_name(spec, s), root_name(spec, s)) for s in ("audit", "api")]
    if context.lane != "work" or list(resources) != [Resource("s3", name, name), *roles]:
        raise ServiceFailure("probe=iam_s3_invalid")
    if ports.session is None or ports.operator is None:
        raise ServiceFailure("probe=session_unavailable")
    matrix = _Matrix(ports, spec, context, name)
    matrix.seed()
    observed = matrix.audit()
    observed.update(matrix.api())
    return observed
