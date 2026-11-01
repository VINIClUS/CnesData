import json
import shutil
import subprocess
from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import Any, cast

import pytest

from tests.production.infra._aws_mock_defaults import MOCK_AWS_PROVIDERS
from tests.production.infra._processing_identities import (
    _configuration_address,
    _configuration_resources,
    _role_identity,
)
from tests.production.infra._processing_native import _native_plan_events
from tests.production.infra._root_native_identities import (
    _override_hcl,
    assert_root_identity_producers,
    merge_identity_records,
)
from tests.production.infra._root_policy_inventory import assert_root_policy_inventory
from tests.production.infra._step_functions_iam_contracts import (
    assert_policy_grants,
    attached_policy_resources,
    exact_step_functions_trust,
    literal_grants,
    resource_at,
)
from tests.production.infra._step_functions_iam_contracts import (
    managed_resources as _managed_resources,
)

type Node = dict[str, Any]
type Tofu = tuple[str, dict[str, str]]
type Artifacts = dict[str, dict[str, object]]

ROOT_RUN = "raiz_avalia_registry"
PRODUCT_NAMESPACE = "cnesdata-prod"
MOCK_PROVIDERS = MOCK_AWS_PROVIDERS


def _native_result(
    tofu: Tofu, root: Path, arguments: Sequence[str]
) -> subprocess.CompletedProcess[str]:
    argv = [tofu[0], f"-chdir={root}", *arguments]
    result = subprocess.run(  # noqa: S603
        argv, check=False, capture_output=True, cwd=root, env=tofu[1], text=True
    )
    evidence = Path(tofu[1]["TF_DATA_DIR"])
    (evidence / "registry-native.stdout").write_text(result.stdout, encoding="utf-8")
    (evidence / "registry-native.stderr").write_text(result.stderr, encoding="utf-8")
    command = {"argv": argv, "cwd": str(root), "environment": tofu[1], "rc": result.returncode}
    (evidence / "registry-native.command.json").write_text(
        json.dumps(command, indent=2) + "\n", encoding="utf-8"
    )
    return result


def _successful_plan(result: subprocess.CompletedProcess[str]) -> Node:
    if result.returncode != 0:
        pytest.fail(
            f"setup_error command=registry-plan rc={result.returncode} "
            f"stdout={result.stdout!r} stderr={result.stderr!r}"
        )
    statuses, plans, diagnostics = _native_plan_events(result.stdout)
    if statuses.get(ROOT_RUN) != "pass" or diagnostics or not plans.get(ROOT_RUN):
        pytest.fail(f"setup_error statuses={statuses!r} diagnostics={diagnostics!r}")
    plan = plans[ROOT_RUN]
    if not plan.get("resource_changes"):
        pytest.fail("setup_error native_plan=empty")
    return cast("Node", plan)


def _iam_identity_overrides(plan: Node, configuration: Node, schema: Node) -> list[Node]:
    account = plan["variables"]["shared_outputs"]["value"]["account_id"]
    return [
        _role_identity(role, configuration, schema, account)
        for role in _managed_resources(plan, "aws_iam_role")
        if _configuration_address(role["address"]).startswith("module.runtime_iam.")
    ]


def _identity_discovery(tofu: Tofu, root: Path, arguments: Sequence[str], suite: str) -> Node:
    plan = _successful_plan(_native_result(tofu, root, arguments))
    evidence = Path(tofu[1]["TF_DATA_DIR"])
    for suffix in ("stdout", "stderr", "command.json"):
        (evidence / f"registry-native.{suffix}").rename(evidence / f"registry-discovery.{suffix}")
    (evidence / "registry-discovery.tftest.hcl").write_text(suite, encoding="utf-8")
    (evidence / "registry-discovery.plan.json").write_text(
        json.dumps(plan, indent=2) + "\n", encoding="utf-8"
    )
    return plan


@pytest.fixture(scope="module")
def registry_plan(
    tofu_context: Tofu,
    tofu_artifacts: Artifacts,
    prod_root: Path,
    valid_shared_outputs: dict[str, object],
) -> Iterator[tuple[Node, Node]]:
    directory = prod_root / ".terraform" / "registry-tests"
    directory.mkdir(parents=True, exist_ok=False)
    ids = cast("dict[str, list[Node]]", tofu_artifacts["root_identities"])
    inputs = json.dumps(valid_shared_outputs, separators=(",", ":"))
    suite = (
        MOCK_PROVIDERS
        + f'\nrun "{ROOT_RUN}" {{\n  command = plan\n'
        + f"  variables {{\n    shared_outputs = {inputs}\n  }}\n}}\n"
    )
    try:
        (directory / "registry.tftest.hcl").write_text(
            suite + _override_hcl(ids["overrides"]), encoding="utf-8"
        )
        arguments = (
            "test",
            "-no-color",
            "-json",
            "-verbose",
            "-test-directory=.terraform/registry-tests",
        )
        configuration = cast("Node", tofu_artifacts["configuration"])
        discovery = _identity_discovery(
            tofu_context, prod_root, arguments, suite + _override_hcl(ids["overrides"])
        )
        assert_root_identity_producers(discovery, ids["seeds"])
        identities = merge_identity_records(
            ids["overrides"],
            _iam_identity_overrides(
                discovery, configuration, cast("Node", tofu_artifacts["schema"])
            ),
        )
        suite += _override_hcl(identities)
        (directory / "registry.tftest.hcl").write_text(suite, encoding="utf-8")
        result = _native_result(tofu_context, prod_root, arguments)
        plan = _successful_plan(result)
        assert_root_identity_producers(plan, ids["seeds"])
        evidence = Path(tofu_context[1]["TF_DATA_DIR"])
        (evidence / "registry-normalized.tftest.hcl").write_text(suite, encoding="utf-8")
        (evidence / "registry-native.plan.json").write_text(
            json.dumps(plan, indent=2) + "\n", encoding="utf-8"
        )
        (evidence / "registry-native.config.json").write_text(
            json.dumps(configuration, indent=2) + "\n", encoding="utf-8"
        )
        (evidence / "registry-native.artifacts.json").write_text(
            json.dumps(tofu_artifacts, indent=2) + "\n", encoding="utf-8"
        )
        yield plan, configuration
    finally:
        shutil.rmtree(directory)


def _assert_product_provider(resource: Node, configuration: Node) -> None:
    resources = dict(_configuration_resources(configuration["root_module"]))
    configured = resources[_configuration_address(resource["address"])]
    provider = configuration["provider_config"][configured["provider_config_key"]]
    assert provider["name"] == "aws", f"registry_provider={provider!r}"
    assert not provider.get("alias"), f"registry_provider_alias={provider.get('alias')!r}"
    region = provider["expressions"]["region"].get("constant_value")
    assert region == "us-east-2", f"registry_region={region!r}"
    if "region" in configured.get("expressions", {}):
        evaluated_region = resource["change"]["after"].get("region")
        assert evaluated_region == "us-east-2", f"registry_evaluated_region={evaluated_region!r}"
    assert resource["change"]["actions"] == ["create"], (
        f"registry_actions={resource['change']['actions']!r}"
    )


def _assert_untagged_policy(policy: str) -> None:
    rules = json.loads(policy)["rules"]
    assert len(rules) == 1, f"registry_lifecycle_rule_count={len(rules)}"
    rule = rules[0]
    priority = rule.get("rulePriority")
    assert type(priority) is int, f"registry_lifecycle_priority_type={type(priority)!r}"
    assert priority > 0, f"registry_lifecycle_priority={priority!r}"
    assert rule["action"] == {"type": "expire"}, f"registry_lifecycle_action={rule['action']!r}"
    expected = {
        "tagStatus": "untagged",
        "countType": "sinceImagePushed",
        "countUnit": "days",
        "countNumber": 7,
    }
    assert rule["selection"] == expected, f"registry_lifecycle_selection={rule['selection']!r}"


def test_ecr_scan_on_push_e_lifecycle_limitado(registry_plan: tuple[Node, Node]) -> None:
    plan, configuration = registry_plan
    repositories = _managed_resources(plan, "aws_ecr_repository")
    owned = [
        resource
        for resource in repositories
        if resource["change"]["after"]["name"] == PRODUCT_NAMESPACE
        or resource["change"]["after"]["name"].startswith(f"{PRODUCT_NAMESPACE}-")
    ]
    assert len(owned) == 1, (
        f"registry_owned_private_repository expected=1 actual={len(owned)} "
        f"names={[resource['change']['after']['name'] for resource in repositories]!r}"
    )
    repository = owned[0]
    values = repository["change"]["after"]
    _assert_product_provider(repository, configuration)
    assert values["image_tag_mutability"] == "IMMUTABLE", (
        f"registry_tag_mutability={values['image_tag_mutability']!r}"
    )
    assert values["image_scanning_configuration"] == [{"scan_on_push": True}], (
        f"registry_scanning={values['image_scanning_configuration']!r}"
    )
    assert values["force_delete"] is False, f"registry_force_delete={values['force_delete']!r}"
    policies = [
        resource
        for resource in _managed_resources(plan, "aws_ecr_lifecycle_policy")
        if resource["change"]["after"]["repository"] == values["name"]
    ]
    assert len(policies) == 1, f"registry_related_lifecycle expected=1 actual={len(policies)}"
    _assert_product_provider(policies[0], configuration)
    _assert_untagged_policy(policies[0]["change"]["after"]["policy"])


def _items(value: Any) -> list[Any]:
    return value if isinstance(value, list) else [value]


def _configured(resource: Node, configuration: Node) -> Node:
    resources = dict(_configuration_resources(configuration["root_module"]))
    return resources[_configuration_address(resource["address"])]


def _assert_role_output_interfaces(module: Node) -> None:
    found = module.get("outputs", {})
    expected = {"execution_role_arn", "execution_role_name", "task_role_arns", "task_role_names"}
    assert expected.issubset(found), f"iam_output_interfaces_missing={expected.difference(found)!r}"
    for output in expected:
        assert isinstance(found[output].get("expression"), dict), f"iam_output_expression={output}"


def _runtime_instances(plan: Node, configuration: Node, root: Path) -> list[Node]:
    calls = configuration["root_module"]["module_calls"]
    source = (root / calls["runtime_iam"]["source"]).resolve()
    return [
        module
        for module in plan["planned_values"]["root_module"].get("child_modules", [])
        if (
            root
            / calls[_configuration_address(module["address"]).removeprefix("module.")]["source"]
        ).resolve()
        == source
    ]


def _runtime_identities(plan: Node, configuration: Node, root: Path) -> tuple[Node, list[Node]]:
    roles = _managed_resources(plan, "aws_iam_role")
    prefix = "module.runtime_iam.aws_iam_role"
    names = {"execution": "processor-execution", "step_functions[0]": "step-functions"}
    names.update({f'task["{mode}"]': f"processor-{mode}" for mode in ("unit", "recovery", "audit")})
    expected = {f"{prefix}.{key}": f"{PRODUCT_NAMESPACE}-{name}" for key, name in names.items()}
    assert len(roles) == 5, f"iam_runtime_roles expected=5 actual={len(roles)}"
    assert {role["address"] for role in roles} == expected.keys(), "iam_runtime_role_addresses"
    instances = _runtime_instances(plan, configuration, root)
    assert len(instances) == 1, f"iam_runtime_instances={len(instances)}"
    module = configuration["root_module"]["module_calls"]["runtime_iam"]["module"]
    _assert_role_output_interfaces(module)
    account = plan["variables"]["shared_outputs"]["value"]["account_id"]
    for role in roles:
        values = role["change"]["after"]
        name = expected[role["address"]]
        assert values["name"] == name, f"iam_role_name={role['address']}"
        assert values["arn"] == f"arn:aws:iam::{account}:role/{name}", "iam_role_arn"
    sfn = next(role for role in roles if role["address"].endswith(".step_functions[0]"))
    _assert_product_provider(sfn, configuration)
    machine = f"arn:aws:states:us-east-2:{account}:stateMachine:{PRODUCT_NAMESPACE}"
    exact_step_functions_trust(sfn, account, machine)
    execution = next(role for role in roles if role["address"].endswith(".execution"))
    return execution, [role for role in roles if role["address"].startswith(f"{prefix}.task[")]


def _assert_trust_conditions(conditions: Node, account: str) -> None:
    allowed = {
        "StringEquals": {"aws:SourceAccount"},
        "ArnLike": {"aws:SourceArn"},
        "ArnEquals": {"aws:SourceArn"},
        "StringLike": {"aws:SourceArn"},
    }
    expected = {
        "aws:SourceAccount": {account},
        "aws:SourceArn": {f"arn:aws:ecs:us-east-2:{account}:*"},
    }
    assert isinstance(conditions, dict), f"iam_trust_condition_type={type(conditions)!r}"
    for operator, entries in conditions.items():
        assert operator in allowed, f"iam_trust_operator={operator!r}"
        assert isinstance(entries, dict), f"iam_trust_entries_type={type(entries)!r}"
        assert entries, f"iam_trust_empty_operator={operator!r}"
        assert entries.keys() <= allowed[operator], f"iam_trust_condition_keys={entries!r}"
        for key, values in entries.items():
            assert set(_items(values)) == expected[key], (
                f"iam_trust_value key={key} values={values!r}"
            )
    accounts = conditions.get("StringEquals", {}).get("aws:SourceAccount", [])
    assert set(_items(accounts)) == {account}, f"iam_trust_account={conditions!r}"


def _assert_ecs_trust(role: Node, account: str) -> None:
    statements = _items(json.loads(role["change"]["after"]["assume_role_policy"])["Statement"])
    assert statements, f"iam_trust_empty={role['address']}"
    for statement in statements:
        assert statement["Effect"] == "Allow", f"iam_trust_effect={statement!r}"
        assert set(_items(statement.get("Action"))) == {"sts:AssumeRole"}
        principal = statement.get("Principal", {})
        assert set(principal) == {"Service"}, f"iam_trust_principal={principal!r}"
        assert set(_items(principal["Service"])) == {"ecs-tasks.amazonaws.com"}
        assert not {"NotPrincipal", "NotAction", "Resource", "NotResource"} & statement.keys()
        _assert_trust_conditions(statement.get("Condition", {}), account)


def _assert_inline_adoption(binding: Node, plan: Node, identities: set[str]) -> None:
    known = {
        policy["change"]["after"]["name"]
        for policy in _managed_resources(plan, "aws_iam_role_policy")
        if policy["change"]["after"]["role"] in identities
    }
    assert set(binding["policy_names"]) <= known, f"iam_unresolved_inline={binding!r}"


def _assert_inline_policy_block(policy: Any) -> None:
    assert isinstance(policy, dict), f"iam_inline_policy_block={policy!r}"
    assert set(policy) == {"name", "policy"}, f"iam_inline_policy_block={policy!r}"
    assert all(
        isinstance(policy[field], str) and policy[field].strip() for field in ("name", "policy")
    ), f"iam_inline_policy_block={policy!r}"


def _role_policy_bindings(
    role: Node, plan: Node, configuration: Node
) -> tuple[list[str], set[str]]:
    values = role["change"]["after"]
    expressions = _configured(role, configuration).get("expressions", {})
    documents = []
    arns = set()
    if "inline_policy" in expressions:
        policies = values.get("inline_policy", [])
        assert isinstance(policies, list), f"iam_inline_policy_blocks={policies!r}"
        for policy in policies:
            if policy == {"name": None, "policy": None}:
                continue
            _assert_inline_policy_block(policy)
            documents.append(policy["policy"])
    if "managed_policy_arns" in expressions:
        arns.update(values.get("managed_policy_arns", []))
    identities = {values["name"], values["id"]}
    binding_role = {"change": {"after": {"name": values["name"], "id": values["id"]}}}
    for resource in attached_policy_resources(plan, binding_role):
        kind, after = resource["type"], resource["change"]["after"]
        if kind == "aws_iam_role_policy":
            documents.append(after["policy"])
        if kind in {"aws_iam_role_policy_attachment", "aws_iam_policy_attachment"}:
            arns.add(after["policy_arn"])
        if kind == "aws_iam_role_policy_attachments_exclusive":
            arns.update(after["policy_arns"])
        if kind == "aws_iam_role_policies_exclusive":
            _assert_inline_adoption(after, plan, identities)
    return documents, arns


def _managed_policy_documents(plan: Node, arns: set[str]) -> list[str]:
    policies = _managed_resources(plan, "aws_iam_policy")
    documents = []
    for arn in arns:
        related = [policy for policy in policies if policy["change"]["after"]["arn"] == arn]
        assert len(related) == 1, f"iam_external_or_unresolved_policy={arn!r}"
        documents.append(related[0]["change"]["after"]["policy"])
    return documents


def _assert_execution_grants(documents: list[str], repository: str, log_group: str) -> None:
    grants = set()
    streams = f"{log_group.removesuffix(':*')}:log-stream:*"
    for document in documents:
        for statement in _items(json.loads(document)["Statement"]):
            assert statement["Effect"] == "Allow", f"iam_permission_effect={statement!r}"
            assert not {"NotAction", "NotResource", "Principal", "Condition"} & statement.keys()
            actions, resources = (
                set(_items(statement["Action"])),
                set(_items(statement["Resource"])),
            )
            if "ecr:GetAuthorizationToken" in actions:
                assert actions == {"ecr:GetAuthorizationToken"}, f"iam_token_actions={statement!r}"
                assert resources == {"*"}, f"iam_token_resource={statement!r}"
            grants.update(
                (
                    action,
                    streams
                    if action.startswith("logs:")
                    and resource == f"{log_group.removesuffix(':*')}:*"
                    else resource,
                )
                for action in actions
                for resource in resources
            )
    expected = {
        ("ecr:BatchGetImage", repository),
        ("ecr:GetDownloadUrlForLayer", repository),
        ("ecr:BatchCheckLayerAvailability", repository),
        ("ecr:GetAuthorizationToken", "*"),
        ("logs:CreateLogStream", streams),
        ("logs:PutLogEvents", streams),
    }
    assert grants == expected, (
        f"iam_execution_grants missing={expected - grants!r} extra={grants - expected!r}"
    )


def test_execution_role_so_pull_e_logs_proprios(
    registry_plan: tuple[Node, Node], valid_shared_outputs: Node, prod_root: Path
) -> None:
    plan, configuration = registry_plan
    execution, tasks = _runtime_identities(plan, configuration, prod_root)
    for role in [execution, *tasks]:
        _assert_product_provider(role, configuration)
        _assert_ecs_trust(role, valid_shared_outputs["account_id"])
    repositories = [
        resource["change"]["after"]
        for resource in _managed_resources(plan, "aws_ecr_repository")
        if resource["change"]["after"]["name"] == PRODUCT_NAMESPACE
        or resource["change"]["after"]["name"].startswith(f"{PRODUCT_NAMESPACE}-")
    ]
    assert len(repositories) == 1, f"iam_own_repositories={repositories!r}"
    groups = [
        resource
        for resource in _managed_resources(plan, "aws_cloudwatch_log_group")
        if resource["change"]["after"]["name"] == "/ecs/cnesdata-prod-processor"
    ]
    assert len(groups) == 1, f"iam_own_log_groups={len(groups)}"
    group = groups[0]
    assert not group["address"].startswith("module."), f"iam_log_group_not_root={group['address']}"
    _assert_product_provider(group, configuration)
    assert group["change"]["after"]["retention_in_days"] == 7, f"iam_log_retention={group!r}"
    documents, arns = _role_policy_bindings(execution, plan, configuration)
    documents.extend(_managed_policy_documents(plan, arns))
    _assert_execution_grants(documents, repositories[0]["arn"], group["change"]["after"]["arn"])


def test_root_anexa_inventario_exato_de_policies_as_roles(
    registry_plan: tuple[Node, Node], prod_root: Path
) -> None:
    plan, configuration = registry_plan
    _runtime_identities(plan, configuration, prod_root)
    for role in _managed_resources(plan, "aws_iam_role"):
        after = role["change"]["after"]
        assert not after.get("inline_policy"), "iam_unexpected_role_inline"
        assert not after.get("managed_policy_arns"), "iam_unexpected_role_managed"
    variables = plan["variables"]
    assert variables["image_digest"]["value"] is None, "bootstrap_digest_expected_null"
    assert variables["processor_active_unit_task_definition_arn"]["value"] is None
    assert variables["processor_retained_unit_task_definition_arns"]["value"] == []
    address = "module.processing.aws_iam_role_policy.step_functions_ecs[0]"
    role = resource_at(plan, "module.runtime_iam.aws_iam_role.step_functions[0]")
    specification = {
        "address": address,
        "name": f"{PRODUCT_NAMESPACE}-step-functions-ecs",
        "allowed": [address, "module.runtime_iam.aws_iam_role_policy.step_functions[0]"],
    }
    expected = literal_grants(("ecs:DescribeTasks", "ecs:StopTask"), ("*",), {})
    assert_policy_grants(plan, role, specification, expected)
    assert_root_policy_inventory(plan)
