"""Focused native observations for the Slice 2 routing and IAM contract."""

import json
import shutil
from pathlib import Path
from typing import Any

import pytest

from tests.production.infra._aws_mock_defaults import MOCK_AWS_PROVIDERS
from tests.production.infra._processing_native import (
    _module_resources,
)

type Node = dict[str, Any]

POLICY_ATTACHMENT_TYPES = {
    "aws_iam_role_policy",
    "aws_iam_role_policy_attachment",
    "aws_iam_policy_attachment",
    "aws_iam_role_policy_attachments_exclusive",
    "aws_iam_role_policies_exclusive",
}


def managed_resources(plan: Node, kind: str) -> list[Node]:
    """Return managed resources of one provider type from a native plan."""

    return [
        resource
        for resource in plan.get("resource_changes", [])
        if resource.get("mode") == "managed" and resource.get("type") == kind
    ]


def module_resources_for(
    plan: Node, configuration: Node, root: Path, source: str
) -> list[Node]:
    return _module_resources(plan, configuration, root, source)


def one_resource(resources: list[Node], description: str) -> Node:
    if len(resources) != 1:
        pytest.fail(
            f"{description}_count expected=1 actual={len(resources)} "
            f"addresses={[item.get('address') for item in resources]!r}"
        )
    return resources[0]


def resource_named(plan: Node, kind: str, name: str, description: str) -> Node:
    return one_resource(
        [
            resource
            for resource in managed_resources(plan, kind)
            if resource.get("change", {}).get("after", {}).get("name") == name
        ],
        description,
    )


def output_value(plan: Node, name: str) -> Any:
    output = plan.get("planned_values", {}).get("outputs", {}).get(name)
    if not isinstance(output, dict) or not {"type", "value"} <= output.keys():
        pytest.fail(f"setup_error missing_evaluated_output={name}")
    change = plan.get("output_changes", {}).get(name)
    if not isinstance(change, dict) or "after" not in change or change.get("after_unknown"):
        pytest.fail(f"setup_error unresolved_output={name}")
    assert change["after"] == output["value"], f"output_change_mismatch={name}"
    return output["value"]


def assert_output_null(plan: Node, name: str) -> None:
    assert output_value(plan, name) is None, f"{name}_expected_null"


def expected_machine_arn(plan: Node, name_prefix: str = "cnesdata-prod") -> str:
    account = plan.get("variables", {}).get("shared_outputs", {}).get("value", {}).get(
        "account_id"
    )
    if not isinstance(account, str) or not account:
        pytest.fail("setup_error account_id_missing_for_machine_arn")
    return f"arn:aws:states:us-east-2:{account}:stateMachine:{name_prefix}"


def configuration_module(configuration: Node, root: Path, source: str) -> Node:
    matches = [
        call["module"]
        for call in configuration.get("root_module", {}).get("module_calls", {}).values()
        if (root / call["source"]).resolve() == (root / source).resolve()
    ]
    return one_resource(matches, f"configuration_module_{source}")


def configuration_variable_names(module: Node) -> set[str]:
    return set(module.get("variables", {}))


def configuration_output_names(module: Node) -> set[str]:
    return set(module.get("outputs", {}))


def exact_step_functions_trust(role: Node, account: str, machine_arn: str) -> None:
    policy = role.get("change", {}).get("after", {}).get("assume_role_policy")
    if not isinstance(policy, str):
        pytest.fail(f"step_functions_trust_missing={role.get('address')}")
    try:
        document = json.loads(policy)
    except json.JSONDecodeError as error:
        pytest.fail(f"step_functions_trust_json={error}")
    expected = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "states.amazonaws.com"},
                "Action": "sts:AssumeRole",
                "Condition": {
                    "StringEquals": {
                        "aws:SourceAccount": account,
                        "aws:SourceArn": machine_arn,
                    }
                },
            }
        ],
    }
    assert document == expected, f"step_functions_trust_exact={document!r}"


def attached_policy_resources(plan: Node, role: Node) -> list[Node]:
    values = role.get("change", {}).get("after", {})
    identities = {values.get("name"), values.get("id"), values.get("arn")}
    identities.discard(None)
    found = []
    for resource in plan.get("resource_changes", []):
        if resource.get("mode") != "managed" or resource.get("type") not in POLICY_ATTACHMENT_TYPES:
            continue
        after = resource.get("change", {}).get("after", {})
        targets = set(after.get("roles") or [])
        targets.update({after.get("role"), after.get("role_name")})
        if targets & identities:
            found.append(resource)
    return found


def assert_no_step_functions_policies(plan: Node, role: Node) -> None:
    values = role.get("change", {}).get("after", {})
    inline = values.get("inline_policy") or []
    managed = values.get("managed_policy_arns") or []
    assert not inline, f"step_functions_inline_policies={inline!r}"
    assert not managed, f"step_functions_managed_policies={managed!r}"
    assert not attached_policy_resources(plan, role), "step_functions_policy_attachments"


def _policy_actions(raw: object) -> set[Any]:
    actions: set[Any] = set()
    if not isinstance(raw, str):
        return actions
    try:
        document = json.loads(raw)
    except json.JSONDecodeError:
        return actions
    statements = document.get("Statement", [])
    if isinstance(statements, dict):
        statements = [statements]
    for statement in statements:
        if isinstance(statement, dict):
            entries = statement.get("Action", [])
            actions.update(entries if isinstance(entries, list) else [entries])
    return actions


def actions_in_runtime_iam(plan: Node, configuration: Node, root: Path) -> set[str]:
    actions: set[Any] = set()
    for resource in module_resources_for(plan, configuration, root, "../../modules/runtime-iam"):
        value = resource.get("change", {}).get("after", {})
        for field in ("policy", "assume_role_policy"):
            actions.update(_policy_actions(value.get(field)))
    return {action for action in actions if isinstance(action, str)}


def state_machine_definition(machine: Node) -> Node:
    raw = machine.get("change", {}).get("after", {}).get("definition")
    if not isinstance(raw, str):
        pytest.fail(f"state_machine_definition_missing={machine.get('address')}")
    try:
        definition = json.loads(raw)
    except json.JSONDecodeError as error:
        pytest.fail(f"state_machine_definition_json={error}")
    if not isinstance(definition, dict):
        pytest.fail("state_machine_definition_object_required")
    return definition


def native_events(stdout: str) -> list[Node]:
    events = []
    for line in stdout.splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(event, dict):
            events.append(event)
    return events


def event_for(events: list[Node], kind: str, run: str) -> Node:
    matches = [
        event
        for event in events
        if event.get("type") == kind and event.get("@testrun") == run
    ]
    return one_resource(matches, f"native_{kind}_{run}")


def prepare_isolated_case(
    root: Path, prod_root: Path, case: tuple[str, dict[str, object], str]
) -> None:
    source, variables, label = case
    (root / "tests").mkdir(parents=True)
    source_path = (prod_root / source).resolve()
    declarations = "\n".join(
        f'''variable "{name}" {{
  type = {variable_type(value)}
  nullable = true
}}'''
        for name, value in variables.items()
    )
    assignments = "\n".join(f"  {name} = var.{name}" for name in variables)
    (root / "main.tf").write_text(
        f'''terraform {{
  required_providers {{
    aws = {{
      source = "hashicorp/aws"
      version = "6.54.0"
    }}
  }}
}}
provider "aws" {{
  region = "us-east-2"
}}
{declarations}
module "subject" {{
  source = {json.dumps(str(source_path))}
{assignments}
}}
''',
        encoding="utf-8",
    )
    shutil.copyfile(prod_root / ".terraform.lock.hcl", root / ".terraform.lock.hcl")
    literals = "\n".join(
        f"    {name} = {json.dumps(value)}" for name, value in variables.items()
    )
    (root / "tests" / f"{label}.tftest.hcl").write_text(
        MOCK_AWS_PROVIDERS
        + f'''\nrun "{label}" {{
  command = plan
  variables {{
{literals}
  }}
}}
''',
        encoding="utf-8",
    )


def variable_type(value: object) -> str:
    if value is None or isinstance(value, str):
        return "string"
    if isinstance(value, dict):
        return "map(string)"
    if isinstance(value, list):
        return "list(string)"
    if isinstance(value, int):
        return "number"
    return "bool"
