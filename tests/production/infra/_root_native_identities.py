"""Contrato puro das identidades computadas da raiz."""

import json
import re
from copy import deepcopy

import pytest

from tests.production.infra._processing_identities import (
    Node,
    _computed_fields,
    _configuration_address,
    _configuration_resources,
)

PROVIDER = "registry.opentofu.org/hashicorp/aws"
PROD_SOURCE = "infra/opentofu/env/prod/main.tf"
ROLE_SOURCE = "infra/opentofu/modules/runtime-iam/task-roles.tf"
ROOT_PREFIXES = (
    "module.control_plane.aws_dynamodb_table.",
    "module.data_buckets.aws_s3_bucket.",
    "module.runtime_iam.aws_iam_role.",
)
TARGETS = (
    ("module.control_plane.aws_dynamodb_table.control_plane", "control-plane",
     [PROD_SOURCE, "infra/opentofu/modules/control-plane/main.tf"]),
    ("module.data_buckets.aws_s3_bucket.data", "data",
     [PROD_SOURCE, "infra/opentofu/modules/data-buckets/main.tf"]),
    ("module.data_buckets.aws_s3_bucket.audit", "audit",
     [PROD_SOURCE, "infra/opentofu/modules/data-buckets/main.tf"]),
    ("module.runtime_iam.aws_iam_role.execution", "processor-execution", [ROLE_SOURCE]),
    ('module.runtime_iam.aws_iam_role.task["unit"]', "processor-unit", [ROLE_SOURCE]),
    ('module.runtime_iam.aws_iam_role.task["recovery"]', "processor-recovery", [ROLE_SOURCE]),
    ('module.runtime_iam.aws_iam_role.task["audit"]', "processor-audit", [ROLE_SOURCE]),
    ("module.runtime_iam.aws_iam_role.step_functions[0]", "step-functions",
     [PROD_SOURCE, "infra/opentofu/modules/runtime-iam/step-functions.tf"]),
)


def _assert_seed_provider(configuration: Node) -> None:
    provider = configuration["provider_config"]["aws"]
    if (
        provider.get("name") != "aws"
        or provider.get("full_name") != PROVIDER
        or provider.get("alias")
        or provider.get("expressions", {}).get("region", {}).get("constant_value") != "us-east-2"
    ):
        pytest.fail("setup_error reason=root_seed_provider")


def _assert_seed_scope(configuration: Node, fixture_contract: Node) -> None:
    _assert_seed_provider(configuration)
    root = configuration["root_module"]
    calls = root.get("module_calls", {})
    if (
        fixture_contract.get("region") != "us-east-2"
        or fixture_contract.get("name_prefix") != "cnesdata-prod"
        or not re.fullmatch(r"[0-9]{12}", fixture_contract.get("account_id", ""))
        or "shared_outputs" not in root.get("variables", {})
    ):
        pytest.fail("setup_error reason=root_seed_provider_contract")
    for module in ("control_plane", "data_buckets", "runtime_iam"):
        if calls.get(module, {}).get("source") != "../../modules/" + module.replace("_", "-"):
            pytest.fail(f"setup_error root_seed_module={module}")


def _seed_values(kind: str, name: str, contract: Node) -> dict[str, str]:
    if kind == "aws_dynamodb_table":
        return {
            "arn": f"arn:aws:dynamodb:{contract['region']}:{contract['account_id']}:table/{name}"
        }
    if kind == "aws_s3_bucket":
        return {"arn": f"arn:aws:s3:::{name}", "id": name}
    return {"arn": f"arn:aws:iam::{contract['account_id']}:role/{name}", "id": name}


def _seed_record(
    target: tuple[str, str, list[str]], configuration: Node, schema: Node, contract: Node
) -> Node:
    address, suffix, sources = target
    kind = address.split(".")[2]
    name = f"{contract['name_prefix']}-{suffix}"
    values = _seed_values(kind, name, contract)
    resource = {"address": address, "type": kind, "provider_name": PROVIDER}
    configured = _computed_fields(resource, configuration, schema, set(values))
    if (
        configured.get("mode") != "managed"
        or configured.get("type") != kind
        or configured.get("provider_config_key") != "aws"
        or configured.get("address") != _configuration_address(address).split(".", maxsplit=2)[2]
    ):
        pytest.fail(f"setup_error root_seed_producer={address}")
    resource_schema = schema["provider_schemas"][PROVIDER]["resource_schemas"][kind]
    attributes = resource_schema["block"]["attributes"]
    if any(attributes[field].get("type") != "string" for field in values):
        pytest.fail(f"setup_error root_seed_attribute={address}")
    fields = {
        field: {"value": value, "source": {
            "schema_attribute": deepcopy(attributes[field]), "provider_config_key": "aws",
            "fixture_contract": deepcopy(contract),
            "fixture_source": "tests/production/infra/conftest.py::VALID_SHARED_OUTPUTS",
            "producer_address": address, "producer_sources": list(sources),
            "name": name, "path": "/" if kind == "aws_iam_role" else None,
        }} for field, value in values.items()
    }
    return {"address": address, "type": kind, "provider": PROVIDER,
            "configured_expressions": deepcopy(configured.get("expressions", {})), "fields": fields}


def root_identity_seeds(configuration: Node, schema: Node, fixture_contract: Node) -> list[Node]:
    """Args: Configuração, schema já verificado e contrato explícito.
    Returns: Identidades computadas com proveniência pré-plan.
    Raises: pytest.fail.Exception em setup inválido.
    """
    try:
        _assert_seed_scope(configuration, fixture_contract)
        return [_seed_record(target, configuration, schema, fixture_contract) for target in TARGETS]
    except (KeyError, TypeError, AttributeError) as error:
        pytest.fail(f"setup_error root_seed_structure={error}")


def _merge_field(current: Node, incoming: Node, address: str, name: str) -> None:
    if current["value"] != incoming["value"]:
        pytest.fail(f"setup_error identity_conflict={address} field={name}")
    if current.get("source") == incoming.get("source"):
        return
    sources = current.get("source", [])
    sources = sources if isinstance(sources, list) else [sources]
    additions = incoming.get("source", [])
    additions = additions if isinstance(additions, list) else [additions]
    current["source"] = sources + [
        deepcopy(source) for source in additions if source not in sources
    ]


def _merge_record(current: Node, incoming: Node) -> None:
    address = incoming["address"]
    for name, field in incoming["fields"].items():
        if name in current["fields"]:
            _merge_field(current["fields"][name], field, address, name)
        else:
            current["fields"][name] = deepcopy(field)
    for key, value in incoming.items():
        if key == "fields":
            continue
        if key not in current:
            current[key] = deepcopy(value)
        elif current[key] != value:
            pytest.fail(f"setup_error identity_metadata_conflict={address} field={key}")


def merge_identity_records(*groups: list[Node]) -> list[Node]:
    """Args: Grupos de registros independentes.
    Returns: União por endereço/campo, preservando proveniência.
    Raises: pytest.fail.Exception em conflito de identidade.
    """
    records: dict[str, Node] = {}
    for group in groups:
        for record in group:
            address = record["address"]
            if address in records:
                _merge_record(records[address], record)
            else:
                records[address] = deepcopy(record)
    return list(records.values())


def _seed_origin(seed: Node) -> Node:
    source = seed["fields"]["arn"]["source"]
    sources = source if isinstance(source, list) else [source]
    return next(item for item in sources if "fixture_contract" in item)


def _producer_inventory(plan: Node, seeds: list[Node]) -> dict[str, Node]:
    expected = {target[0] for target in TARGETS}
    if {seed["address"] for seed in seeds} != expected or len(seeds) != len(expected):
        pytest.fail("setup_error reason=root_seed_targets")
    resources = [
        item for item in plan["resource_changes"] if item["address"].startswith(ROOT_PREFIXES)
    ]
    actual = {item["address"]: item for item in resources}
    if set(actual) != expected or len(resources) != len(expected):
        pytest.fail(f"setup_error root_producer_targets={sorted(actual)}")
    return actual


def _assert_role_path(values: Node, configured: Node, address: str) -> None:
    path = values.get("path")
    if path is None:
        if "path" in configured.get("expressions", {}):
            pytest.fail(f"setup_error root_producer_path_unknown={address}")
        path = "/"
    if path != "/":
        pytest.fail(f"setup_error root_producer_path={address}")


def _assert_table_indexes(values: Node, address: str) -> None:
    indexes = values.get("global_secondary_index", [])
    if len(indexes) != 6 or {item.get("name") for item in indexes} != {
        f"gsi{number}" for number in range(1, 7)
    }:
        pytest.fail(f"setup_error root_producer_indexes={address}")


def _assert_producer(resource: Node, seed: Node, configured: Node) -> None:
    address, kind = seed["address"], seed["type"]
    values = resource["change"]["after"]
    if (
        resource.get("mode") != "managed"
        or resource.get("type") != kind
        or resource.get("provider_name") != seed["provider"]
    ):
        pytest.fail(f"setup_error root_producer_binding={address}")
    name_field = "bucket" if kind == "aws_s3_bucket" else "name"
    if values.get(name_field) != _seed_origin(seed)["name"]:
        pytest.fail(f"setup_error root_producer_name={address}")
    for field, record in seed["fields"].items():
        if values.get(field) != record["value"]:
            pytest.fail(f"setup_error root_producer_value={address} field={field}")
    if kind == "aws_iam_role":
        _assert_role_path(values, configured, address)
    if kind == "aws_dynamodb_table":
        _assert_table_indexes(values, address)


def assert_root_identity_producers(plan: Node, seeds: list[Node]) -> None:
    """Args: Plano root avaliado e seeds correspondentes.
    Returns: None após conferir produtores e valores reais.
    Raises: pytest.fail.Exception em drift ou plano inválido.
    """
    try:
        resources = _producer_inventory(plan, seeds)
        contract = _seed_origin(seeds[0])["fixture_contract"]
        if plan["variables"]["shared_outputs"]["value"]["account_id"] != contract["account_id"]:
            pytest.fail("setup_error reason=root_producer_account")
        configured = dict(_configuration_resources(plan["configuration"]["root_module"]))
        for seed in seeds:
            address = seed["address"]
            _assert_producer(resources[address], seed, configured[_configuration_address(address)])
    except (KeyError, TypeError, AttributeError, StopIteration) as error:
        pytest.fail(f"setup_error root_producer_structure={error}")


def _override_hcl(identities: list[Node]) -> str:
    return "\n".join(
        f"override_resource {{\n  target = {item['address']}\n  values = "
        + json.dumps({field: data["value"] for field, data in item["fields"].items()})
        + "\n}\n"
        for item in identities
    )


def preserve_root_mock_defaults(
    records: list[Node], configuration: Node, schema: Node, default_context: Node
) -> list[Node]:
    """Args: Registros, configuração, schema e default explícito.
    Returns: Registros para preservação dos defaults computados.
    Raises: pytest.fail.Exception em setup inválido.
    """
    try:
        if not all(isinstance(default_context.get(key), str) and default_context[key]
                   for key in ("value", "source")):
            pytest.fail("setup_error reason=root_mock_default_context")
        targets = {
            "module.data_buckets.aws_s3_bucket.data", "module.data_buckets.aws_s3_bucket.audit",
        }
        buckets = [record for record in records if record["address"] in targets]
        if {record["address"] for record in buckets} != targets:
            pytest.fail("setup_error reason=root_mock_default_targets")
        additions = [
            _mock_default_record(record, configuration, schema, default_context)
            for record in buckets
        ]
        return merge_identity_records(records, additions)
    except (KeyError, TypeError, AttributeError) as error:
        pytest.fail(f"setup_error root_mock_default_structure={error}")


def _mock_default_record(record: Node, configuration: Node, schema: Node, context: Node) -> Node:
    name = "bucket_regional_domain_name"
    resource = {"address": record["address"], "type": record["type"],
                "provider_name": record["provider"]}
    configured = _computed_fields(resource, configuration, schema, {name})
    attributes = schema["provider_schemas"][PROVIDER]["resource_schemas"]["aws_s3_bucket"]
    attribute = attributes["block"]["attributes"][name]
    if attribute.get("type") != "string" or configured.get("mode") != "managed":
        pytest.fail(f"setup_error root_mock_default_attribute={record['address']}")
    fields = {} if name in record["fields"] else {name: {"value": context["value"], "source": {
        "mock_default_source": context["source"], "producer_address": record["address"],
        "provider_config_key": configured["provider_config_key"],
        "schema_attribute": deepcopy(attribute),
    }}}
    return dict(record, fields=fields)
