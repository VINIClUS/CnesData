"""Probe iam-trust: cadeia, trust exato e widening, cada negação pareada com um sucesso."""

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from tests.integration.aws_capability._assumed_session import assume_request, open_session
from tests.integration.aws_capability._iam_policy_ops import put_assume_policy
from tests.integration.aws_capability._probe_attempt import Attempt, attempt, must, retry_denied
from tests.integration.aws_capability._safety import root_name, run_name
from tests.integration.aws_capability._service_contracts import CommandPort, ServicePorts
from tests.integration.aws_capability.cli import Request
from tests.integration.aws_capability.contracts import (
    RequestContext,
    Resource,
    RunSpec,
    ServiceFailure,
)

LONG_SECONDS = 3601
SESSION_TAGS = [{"Key": "cap-tag", "Value": "cap-tag-value"}]
SOURCE_IDENTITY = "cap-source"
WIDENED_ACTIONS = ["sts:AssumeRole", "sts:TagSession", "sts:SetSourceIdentity"]


def duration_category(message: str) -> str:
    """Returns: role_chaining, max_session_duration ou other, sem expor o texto."""
    text = message.lower()
    if "role chaining" in text:
        return "role_chaining"
    if "maxsessionduration" in text.replace(" ", "") or "maximum session duration" in text:
        return "max_session_duration"
    return "other"


def widened_trust(principal: str) -> dict[str, object]:
    return {
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Principal": {"AWS": principal}, "Action": list(WIDENED_ACTIONS)}
        ],
    }


@dataclass(frozen=True)
class _Trust:
    ports: ServicePorts
    spec: RunSpec
    context: RequestContext
    operator_arn: str

    def single(self, caller: CommandPort, role: str, override: Mapping[str, object]) -> Attempt:
        return attempt(caller, assume_request(self.spec, role, override), self.context)

    def control(
        self, caller: CommandPort, step: str, flip: tuple[str, Mapping[str, object]]
    ) -> tuple[Attempt, float]:
        request = assume_request(self.spec, flip[0], flip[1])
        return retry_denied(
            lambda: attempt(caller, request, self.context), self.ports, self.context, step
        )

    def chain_phase(self) -> dict[str, object]:
        r, r2 = run_name(self.spec), root_name(self.spec, "r2")
        first, assume_seconds = self.control(self.ports.commands, "assume_r", (r, {}))
        session = open_session(self.ports, first.data)
        policy = put_assume_policy(self.ports.commands, self.spec, self.context)
        must(policy, "assume_policy")
        _, chain_seconds = self.control(session, "assume_r2", (r2, {}))
        return {
            "assume_seconds": assume_seconds,
            "chain_seconds": chain_seconds,
            "self_assume": self.single(session, r, {}).code,
        }

    def denial_phase(self) -> dict[str, object]:
        r, operator = run_name(self.spec), self.ports.commands
        long = self.single(operator, r, {"DurationSeconds": LONG_SECONDS})
        return {
            "duration_3601": long.code,
            "duration_3601_category": "none" if long.ok else duration_category(long.message),
            "session_tags_before": self.single(operator, r, {"Tags": SESSION_TAGS}).code,
            "source_identity_before": self.single(
                operator, r, {"SourceIdentity": SOURCE_IDENTITY}
            ).code,
        }

    def widen_phase(self) -> dict[str, object]:
        r, operator = run_name(self.spec), self.ports.commands
        document = json.dumps(
            widened_trust(self.operator_arn), sort_keys=True, separators=(",", ":")
        )
        update = Request(
            "iam", "update-assume-role-policy", {"RoleName": r, "PolicyDocument": document}
        )
        must(attempt(operator, update, self.context), "widen_trust")
        tags, tags_seconds = self.control(operator, "tags_flip", (r, {"Tags": SESSION_TAGS}))
        source, source_seconds = self.control(
            operator, "source_flip", (r, {"SourceIdentity": SOURCE_IDENTITY})
        )
        return {
            "session_tags_after": tags.code,
            "session_tags_ready_seconds": tags_seconds,
            "source_identity_after": source.code,
            "source_identity_ready_seconds": source_seconds,
        }


def probe_trust(
    resources: Sequence[Resource], spec: RunSpec, ports: ServicePorts, context: RequestContext
) -> Mapping[str, object]:
    """Returns: códigos, categoria e segundos; Raises: ServiceFailure sem controle verde."""
    names = (run_name(spec), root_name(spec, "r2"))
    expected = [Resource("iam", name, name) for name in names]
    if context.lane != "work" or list(resources) != expected:
        raise ServiceFailure("probe=trust_invalid")
    if ports.session is None or ports.operator is None:
        raise ServiceFailure("probe=session_unavailable")
    trust = _Trust(ports, spec, context, ports.operator.role_arn)
    observed = trust.chain_phase()
    observed.update(trust.denial_phase())
    observed.update(trust.widen_phase())
    return observed
