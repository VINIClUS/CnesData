"""Isolated native plan of the schedulers module with mocked AWS providers."""

import json
import shutil
from pathlib import Path
from typing import Any, NamedTuple

import pytest

from tests.production.infra._aws_mock_defaults import MOCK_AWS_PROVIDERS
from tests.production.infra._processing_native import _native_plan_events, _native_result

type Node = dict[str, Any]

ACCOUNT = "123456789012"
PREFIX = "cnesdata-prod"
CLUSTER = f"arn:aws:ecs:us-east-2:{ACCOUNT}:cluster/{PREFIX}-processor"
EXECUTION_ROLE = f"arn:aws:iam::{ACCOUNT}:role/{PREFIX}-processor-execution"
RECOVERY_REVISION = f"arn:aws:ecs:us-east-2:{ACCOUNT}:task-definition/{PREFIX}-processor-recovery:7"
AUDIT_REVISION = f"arn:aws:ecs:us-east-2:{ACCOUNT}:task-definition/{PREFIX}-processor-audit:3"
SUBNETS = ["subnet-0aaa1111", "subnet-0bbb2222"]
SECURITY_GROUP = "sg-0123456789abcdef0"
OUTPUTS = (
    "freeze_target_role_names",
    "freeze_excluded_role_names",
    "task_hour_budget",
    "alarm_names",
)

BASE_INPUTS: Node = {
    "name_prefix": PREFIX,
    "account_id": ACCOUNT,
    "cluster_arn": CLUSTER,
    "recovery_task_definition_arn": RECOVERY_REVISION,
    "audit_task_definition_arn": AUDIT_REVISION,
    "task_role_arns": {
        mode: f"arn:aws:iam::{ACCOUNT}:role/{PREFIX}-processor-{mode}"
        for mode in ("unit", "recovery", "audit")
    },
    "execution_role_arn": EXECUTION_ROLE,
    "public_subnet_ids": SUBNETS,
    "security_group_id": SECURITY_GROUP,
}


class SchedulersRun(NamedTuple):
    statuses: dict[str, str]
    plans: dict[str, Node]
    diagnostics: list[Node]


def _escape(value: object) -> str:
    return json.dumps(value).replace("${", "$${")


def _wrapper(module_source: Path) -> str:
    outputs = "\n".join(
        f'output "{name}" {{\n  value = module.subject.{name}\n}}' for name in OUTPUTS
    )
    arguments = "\n".join(f"  {name} = var.{name}" for name in BASE_INPUTS)
    declarations = "\n".join(
        f'variable "{name}" {{\n  type = any\n  default = null\n}}' for name in BASE_INPUTS
    )
    return (
        'terraform {\n  required_providers {\n    aws = {\n      source = "hashicorp/aws"\n'
        '      version = "6.54.0"\n    }\n  }\n}\nprovider "aws" {\n  region = "us-east-2"\n}\n'
        f'{declarations}\nmodule "subject" {{\n  source = {json.dumps(str(module_source))}\n'
        f"{arguments}\n}}\n{outputs}\n"
    )


def _suite(label: str, inputs: Node) -> str:
    literals = "\n".join(f"    {name} = {_escape(value)}" for name, value in inputs.items())
    run = f'run "{label}" {{\n  command = plan\n  variables {{\n{literals}\n  }}\n}}\n'
    return f"{MOCK_AWS_PROVIDERS}\n{run}"


def schedulers_run(
    tofu: tuple[str, dict[str, str]], prod_root: Path, case: tuple[Path, str, Node]
) -> SchedulersRun:
    tmp_path, label, overrides = case
    root = tmp_path / label
    (root / "tests").mkdir(parents=True)
    module_source = (prod_root / "../../modules/schedulers").resolve()
    (root / "main.tf").write_text(_wrapper(module_source), encoding="utf-8")
    shutil.copyfile(prod_root / ".terraform.lock.hcl", root / ".terraform.lock.hcl")
    suite = _suite(label, BASE_INPUTS | overrides)
    (root / "tests" / f"{label}.tftest.hcl").write_text(suite, encoding="utf-8")
    local = (tofu[0], tofu[1] | {"TF_DATA_DIR": str(root / ".tofu-data")})
    init = _native_result(local, root, ("init", "-backend=false", "-input=false"), f"{label}-init")
    if init.returncode != 0:
        pytest.fail(f"setup_error schedulers_init={init.stderr!r}")
    arguments = ("test", "-no-color", "-json", "-verbose", "-test-directory=tests")
    result = _native_result(local, root, arguments, label)
    return SchedulersRun(*_native_plan_events(result.stdout))


def planned(run: SchedulersRun, label: str) -> Node:
    if run.statuses.get(label) != "pass" or run.diagnostics:
        pytest.fail(f"setup_error schedulers_plan statuses={run.statuses!r} {run.diagnostics!r}")
    return run.plans[label]
