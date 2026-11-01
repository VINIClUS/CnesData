"""Inline policies aditivas do role de teste, restritas aos objetos do bucket."""

import json
from collections.abc import Sequence

from tests.integration.aws_capability._probe_attempt import Attempt, attempt
from tests.integration.aws_capability._safety import run_name
from tests.integration.aws_capability._service_contracts import CommandPort
from tests.integration.aws_capability.cli import Request
from tests.integration.aws_capability.contracts import RequestContext, RunSpec


def put_inline_policy(
    commands: CommandPort,
    spec: RunSpec,
    policy: tuple[str, Sequence[str]],
    context: RequestContext,
) -> Attempt:
    name = run_name(spec)
    policy_name, actions = policy
    statement = {"Effect": "Allow", "Action": list(actions), "Resource": f"arn:aws:s3:::{name}/*"}
    document = {"Version": "2012-10-17", "Statement": [statement]}
    payload: dict[str, object] = {
        "RoleName": name,
        "PolicyName": policy_name,
        "PolicyDocument": json.dumps(document, sort_keys=True, separators=(",", ":")),
    }
    return attempt(commands, Request("iam", "put-role-policy", payload), context)
