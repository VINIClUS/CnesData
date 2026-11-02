"""Inline policies aditivas do role de teste, restritas aos objetos do bucket."""

import json
from collections.abc import Sequence

from tests.integration.aws_capability._probe_attempt import Attempt, attempt
from tests.integration.aws_capability._safety import run_name
from tests.integration.aws_capability._service_contracts import CommandPort
from tests.integration.aws_capability.cli import Request
from tests.integration.aws_capability.contracts import RequestContext, RunSpec

ASSUME_POLICY = "cap-assume"


def put_inline_policy(
    commands: CommandPort,
    spec: RunSpec,
    policy: tuple[str, Sequence[str]],
    context: RequestContext,
) -> Attempt:
    name = run_name(spec)
    policy_name, actions = policy
    statement = {"Effect": "Allow", "Action": list(actions), "Resource": f"arn:aws:s3:::{name}/*"}
    return _put(commands, (name, policy_name), statement, context)


def put_assume_policy(commands: CommandPort, spec: RunSpec, context: RequestContext) -> Attempt:
    name = run_name(spec)
    resource = f"arn:aws:iam::{spec.account}:role/cnesdata-cap/{name}*"
    statement = {"Effect": "Allow", "Action": ["sts:AssumeRole"], "Resource": resource}
    return _put(commands, (name, ASSUME_POLICY), statement, context)


def _put(
    commands: CommandPort,
    target: tuple[str, str],
    statement: dict[str, object],
    context: RequestContext,
) -> Attempt:
    document = {"Version": "2012-10-17", "Statement": [statement]}
    payload: dict[str, object] = {
        "RoleName": target[0],
        "PolicyName": target[1],
        "PolicyDocument": json.dumps(document, sort_keys=True, separators=(",", ":")),
    }
    return attempt(commands, Request("iam", "put-role-policy", payload), context)
