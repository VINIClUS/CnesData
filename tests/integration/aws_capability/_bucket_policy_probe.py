"""A/B da bucket policy: leitura 200 antes, Deny do role api depois, controle ainda verde."""

import json
from dataclasses import dataclass

from tests.integration.aws_capability._probe_attempt import (
    CONTROL_SECONDS,
    PROPAGATION_DENIALS,
    must,
    retry_denied,
)
from tests.integration.aws_capability._s3_actor import Actor
from tests.integration.aws_capability._safety import root_name
from tests.integration.aws_capability._service_contracts import ServicePorts
from tests.integration.aws_capability.contracts import RequestContext, RunSpec, ServiceFailure

BLOCKED = "serving/blocked/x"
CONTROL = "serving/x"
POLICY_ATTEMPTS = 4
POLICY_RETRY_SECONDS = 2.0
FLIP_ATTEMPTS = 12
FLIP_SECONDS = 2.0


def policy_document(spec: RunSpec, bucket: str) -> str:
    role = f"arn:aws:iam::{spec.account}:role/cnesdata-cap/{root_name(spec, 'api')}"
    document = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Deny",
                "Action": "s3:GetObject",
                "Principal": {"AWS": role},
                "Resource": f"arn:aws:s3:::{bucket}/serving/blocked/*",
            }
        ],
    }
    return json.dumps(document, sort_keys=True, separators=(",", ":"))


@dataclass(frozen=True)
class BucketPolicyAB:
    ports: ServicePorts
    spec: RunSpec
    context: RequestContext
    bucket: str

    def run(self, operator: Actor, api: Actor) -> dict[str, object]:
        """Returns: códigos e segundos; Raises: ServiceFailure se um controle não ficar verde."""
        retry_denied(lambda: api.head(BLOCKED), self.ports, self.context, "blocked_before")
        attempts = self.put(operator)
        code, seconds = self.flip(api)
        must(api.head(CONTROL), "serving_after_flip")
        return {
            "blocked_before": "ok",
            "policy_put_attempts": attempts,
            "blocked_after": code,
            "blocked_flip_seconds": seconds,
            "serving_after_flip": "ok",
        }

    def put(self, operator: Actor) -> int:
        document = policy_document(self.spec, self.bucket)
        for attempt in range(1, POLICY_ATTEMPTS + 1):
            result = operator.put_policy(document)
            if result.ok:
                return attempt
            if result.code != "MalformedPolicy":
                break
            if attempt < POLICY_ATTEMPTS:
                self.ports.pause(POLICY_RETRY_SECONDS)
        raise ServiceFailure("probe=step_failed step=bucket_policy")

    def flip(self, api: Actor) -> tuple[str, float]:
        start = self.ports.now()
        result = api.head(BLOCKED)
        for _ in range(FLIP_ATTEMPTS - 1):
            if result.code in PROPAGATION_DENIALS or self.out_of_time():
                break
            self.ports.pause(FLIP_SECONDS)
            result = api.head(BLOCKED)
        return result.code, round(self.ports.now() - start, 1)

    def out_of_time(self) -> bool:
        return self.context.budget.remaining_seconds(self.context.lane) <= CONTROL_SECONDS + 1
