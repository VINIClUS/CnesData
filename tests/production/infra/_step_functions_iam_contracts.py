"""Focused native observations for the Slice 2 routing and IAM contract."""

import json
import shutil
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pytest

from tests.production.infra._aws_mock_defaults import MOCK_AWS_PROVIDERS
from tests.production.infra._processing_native import (
    _module_resources,
)

type Node = dict[str, Any]
type Grants = frozenset[tuple[str, str, str]]

ECS_POLICY_ADDRESS = "module.processing.aws_iam_role_policy.step_functions_ecs[0]"
COMMON_POLICY_ADDRESS = "module.runtime_iam.aws_iam_role_policy.step_functions[0]"
STEP_FUNCTIONS_ROLE_ADDRESS = "module.runtime_iam.aws_iam_role.step_functions[0]"
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
        f"    {name} = {_input_literal(value)}" for name, value in variables.items()
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


def _input_literal(value: object) -> str:
    return json.dumps(value).replace("${", "$${").replace("%{", "%%{")


def variable_type(value: object) -> str:
    if value is None or isinstance(value, str):
        return "string"
    if isinstance(value, dict):
        return "any"
    if isinstance(value, list):
        return "list(string)"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "number"
    return "bool"


def _unique_policy_object(pairs: list[tuple[str, Any]]) -> Node:
    result: Node = {}
    for key, value in pairs:
        assert key not in result, f"policy_shape duplicate_key={key}"
        result[key] = value
    return result


def _policy_strings(value: Any) -> tuple[str, ...]:
    values = [value] if isinstance(value, str) else value
    assert isinstance(values, list), "policy_shape expected_strings"
    assert values, "policy_shape expected_nonempty_strings"
    seen: list[str] = []
    for item in values:
        assert isinstance(item, str), "policy_shape expected_string"
        assert item, "policy_shape empty_string"
        assert item not in seen, f"duplicate_grant value={item}"
        seen.append(item)
    return tuple(sorted(seen))


def _policy_condition(condition: Any) -> str:
    assert isinstance(condition, dict), "policy_shape condition_object"
    normalized: Node = {}
    for operator, entries in condition.items():
        assert isinstance(entries, dict), "policy_shape condition_entries"
        assert entries, "policy_shape empty_condition_entries"
        normalized[operator] = {}
        for key, value in entries.items():
            values = _policy_strings(value)
            normalized[operator][key] = values[0] if isinstance(value, str) else list(values)
    return json.dumps(normalized, sort_keys=True, separators=(",", ":"))


def literal_grants(actions: tuple[str, ...], resources: tuple[str, ...], condition: Node) -> Grants:
    """Args: Ações, recursos e condição do oráculo literal.
    Returns: Tuplas exatas sem interpretação de permissões.
    """
    encoded = _policy_condition(condition)
    return frozenset((action, resource, encoded) for action in actions for resource in resources)


def _statement_grants(statement: Any) -> list[tuple[str, str, str]]:
    assert isinstance(statement, dict), "policy_shape statement_object"
    assert {"Effect", "Action", "Resource"} <= statement.keys(), "policy_shape missing_fields"
    assert statement.keys() <= {"Sid", "Effect", "Action", "Resource", "Condition"}, (
        "policy_shape unexpected_fields"
    )
    assert statement["Effect"] == "Allow", "policy_shape effect"
    assert isinstance(statement.get("Sid", ""), str), "policy_shape sid"
    actions = _policy_strings(statement["Action"])
    resources = _policy_strings(statement["Resource"])
    condition = _policy_condition(statement.get("Condition", {}))
    return [(action, resource, condition) for action in actions for resource in resources]


def policy_grants(document: str) -> Grants:
    """Args: Documento IAM observado, sem reconstruir expectativas.
    Returns: Grants normalizados após rejeitar duplicatas.
    Raises: AssertionError para shape ou duplicatas inválidos.
    """
    try:
        parsed = json.loads(document, object_pairs_hook=_unique_policy_object)
    except (json.JSONDecodeError, TypeError) as error:
        raise AssertionError("policy_shape invalid_json") from error
    assert isinstance(parsed, dict), "policy_shape document_object"
    assert set(parsed) == {"Version", "Statement"}, "policy_shape document_fields"
    assert parsed["Version"] == "2012-10-17", "policy_shape version"
    statements = parsed["Statement"]
    assert isinstance(statements, list), "policy_shape statements_list"
    seen: list[tuple[str, str, str]] = []
    signatures: list[tuple[tuple[str, str, str], ...]] = []
    for statement in statements:
        grants = _statement_grants(statement)
        signature = tuple(sorted(grants))
        assert signature not in signatures, "duplicate_statement"
        signatures.append(signature)
        for grant in grants:
            assert grant not in seen, f"duplicate_grant grant={grant!r}"
            seen.append(grant)
    return frozenset(seen)


def resource_at(plan: Node, address: str) -> Node:
    """Args: Plano observado e endereço literal do produtor.
    Returns: Único recurso correspondente.
    """
    return one_resource(
        [item for item in plan["resource_changes"] if item["address"] == address], address
    )


def assert_policy_grants(plan: Node, role: Node, specification: Node, expected: Grants) -> None:
    """Args: Plano, role, binding literal e grants independentes.
    Returns: None após igualdade exata.
    Raises: AssertionError para ausência, binding ou grants divergentes.
    """
    address = specification["address"]
    matches = [item for item in managed_resources(plan, "aws_iam_role_policy")
               if item["address"] == address]
    observed = frozenset()
    if matches:
        observed = policy_grants(matches[0]["change"]["after"]["policy"])
    assert matches, f"missing_policy address={address} missing_grants={sorted(expected)!r}"
    assert len(matches) == 1, f"binding duplicate_policy={address}"
    values = matches[0]["change"]["after"]
    target = role["change"]["after"]
    assert values["role"] == target["name"], f"binding role={address}"
    assert values["name"] == specification["name"], f"binding name={address}"
    assert not target.get("inline_policy"), "binding unexpected_inline_policy"
    assert not target.get("managed_policy_arns"), "binding unexpected_managed_policy"
    attachments = attached_policy_resources(plan, role)
    assert all(item["type"] == "aws_iam_role_policy" for item in attachments), (
        "binding unexpected_attachment_type"
    )
    assert {item["address"] for item in attachments} <= set(specification["allowed"]), (
        "binding unexpected_policy"
    )
    assert expected <= observed, (
        f"missing_grants address={address} grants={sorted(expected - observed)!r}"
    )
    assert observed <= expected, (
        f"extra_grants address={address} grants={sorted(observed - expected)!r}"
    )


def ecs_policy_grants(run_task: Iterable[str]) -> Grants:
    """Args: Revisões unit esperadas para ecs:RunTask, vazio no bootstrap.
    Returns: Grants literais da policy ECS de Step Functions, sem condição.
    """
    return literal_grants(("ecs:DescribeTasks", "ecs:StopTask"), ("*",), {}) | literal_grants(
        ("ecs:RunTask",), tuple(sorted(set(run_task))), {}
    )


def assert_ecs_policy(plan: Node, run_task: Iterable[str]) -> str:
    """Args: Plano observado e revisões unit esperadas de producers independentes.
    Returns: Documento da policy ECS após igualdade exata e statement único de RunTask.
    Raises: AssertionError para grants ou binding divergentes.
    """
    expected = set(run_task)
    specification = {
        "address": ECS_POLICY_ADDRESS,
        "name": "cnesdata-prod-step-functions-ecs",
        "allowed": [ECS_POLICY_ADDRESS, COMMON_POLICY_ADDRESS],
    }
    role = resource_at(plan, STEP_FUNCTIONS_ROLE_ADDRESS)
    assert_policy_grants(plan, role, specification, ecs_policy_grants(expected))
    document = resource_at(plan, ECS_POLICY_ADDRESS)["change"]["after"]["policy"]
    statements = json.loads(document)["Statement"]
    assert len(statements) == 1 + bool(expected), f"ecs_policy_statements={len(statements)}"
    return document
