"""Identidades controladas exclusivamente em atributos computados dos produtores."""

from collections.abc import Iterator
from hashlib import sha256
from re import sub
from typing import Any

import pytest

type Node = dict[str, Any]


def _network_source(resource: Node, configuration: Node, schema: Node) -> tuple[Node, str]:
    configured = _computed_fields(resource, configuration, schema, {"id"})
    attribute = schema["provider_schemas"][resource["provider_name"]]["resource_schemas"][
        resource["type"]
    ]["block"]["attributes"]["id"]
    provider = configuration["provider_config"][configured["provider_config_key"]]
    region = provider.get("expressions", {}).get("region", {}).get("constant_value")
    expected = {
        "aws_security_group": "module.network.aws_security_group.tasks",
        "aws_subnet": "module.network.aws_subnet.public",
    }
    if (
        attribute.get("type") != "string"
        or attribute.get("optional") is not True
        or configured["provider_config_key"] != "aws"
        or resource["provider_name"] != "registry.opentofu.org/hashicorp/aws"
        or provider.get("name") != "aws"
        or provider.get("alias")
        or region != "us-east-2"
        or _configuration_address(resource["address"]) != expected.get(resource["type"])
        or configured.get("type") != resource["type"]
        or configured.get("mode") != "managed"
        or resource.get("mode") != "managed"
    ):
        pytest.fail(f"setup_error network_identity_schema_provider_address={resource['address']}")
    return configured, region


def _network_identity(resource: Node, configuration: Node, schema: Node, account: str) -> Node:
    configured, region = _network_source(resource, configuration, schema)
    original = resource["change"]["after"].get("id")
    if (
        not isinstance(original, str)
        or not original
        or len(account) != 12
        or not account.isdecimal()
    ):
        pytest.fail(f"setup_error network_identity_original_account={resource['address']}")
    prefix = "sg" if resource["type"] == "aws_security_group" else "subnet"
    seed = f"{account}:{region}:{resource['address']}"
    value = f"{prefix}-{sha256(seed.encode()).hexdigest()[:17]}"
    attribute = schema["provider_schemas"][resource["provider_name"]]["resource_schemas"][
        resource["type"]
    ]["block"]["attributes"]["id"]
    return {
        "address": resource["address"],
        "fields": {
            "id": {
                "value": value,
                "source": {
                    "account_id": account,
                    "region": region,
                    "producer_address": resource["address"],
                    "original_computed_value": original,
                    "schema_attribute": attribute,
                    "provider_config_key": configured["provider_config_key"],
                    "controlled_seed": seed,
                },
            }
        },
        "configured_expressions": configured.get("expressions", {}),
        "provider": resource["provider_name"],
        "type": resource["type"],
    }


def _configuration_address(address: str) -> str:
    return sub(r'\[(?:\d+|"(?:[^"\\]|\\.)*")\]', "", address)


def _configuration_resources(module: Node, prefix: str = "") -> Iterator[tuple[str, Node]]:
    for resource in module.get("resources", []):
        yield f"{prefix}{resource['address']}", resource
    for name, call in module.get("module_calls", {}).items():
        yield from _configuration_resources(call["module"], f"{prefix}module.{name}.")


def _computed_fields(resource: Node, configuration: Node, schema: Node, fields: set[str]) -> Node:
    configured = dict(_configuration_resources(configuration["root_module"]))[
        _configuration_address(resource["address"])
    ]
    attributes = schema["provider_schemas"][resource["provider_name"]]["resource_schemas"][
        resource["type"]
    ]["block"]["attributes"]
    if fields & configured.get("expressions", {}).keys():
        pytest.fail(f"setup_error computed_field_configured={resource['address']}")
    if not all(attributes[field].get("computed") is True for field in fields):
        pytest.fail(f"setup_error field_not_computed={resource['address']}")
    return configured


def _role_identity(resource: Node, configuration: Node, schema: Node, account: str) -> Node:
    configured = _computed_fields(resource, configuration, schema, {"arn", "id"})
    expressions, values = configured.get("expressions", {}), resource["change"]["after"]
    name, path = values.get("name"), values.get("path")
    if "name" not in expressions or not isinstance(name, str) or not name:
        pytest.fail(f"setup_error iam_identity_name={resource['address']}")
    if path is None and "path" not in expressions:
        path = "/"
    if not isinstance(path, str) or not path.startswith("/") or not path.endswith("/"):
        pytest.fail(f"setup_error iam_identity_path={resource['address']}")
    return {
        "address": resource["address"],
        "fields": {
            "arn": {
                "value": f"arn:aws:iam::{account}:role{path}{name}",
                "source": {"account_id": account, "name": name, "path": path},
            },
            "id": {"value": name, "source": {"name": name}},
        },
        "configured_expressions": expressions,
        "provider": resource["provider_name"],
        "type": resource["type"],
    }


def _repository_identity(resource: Node, configuration: Node, schema: Node, account: str) -> Node:
    configured = _computed_fields(resource, configuration, schema, {"repository_url"})
    provider = configuration["provider_config"][configured["provider_config_key"]]
    region = provider["expressions"]["region"].get("constant_value")
    name = resource["change"]["after"].get("name")
    if (
        provider["name"] != "aws"
        or provider.get("alias")
        or region != "us-east-2"
        or "name" not in configured["expressions"]
        or not isinstance(name, str)
        or not name
    ):
        pytest.fail(f"setup_error repository_identity={resource['address']}")
    return {
        "address": resource["address"],
        "fields": {
            "repository_url": {
                "value": f"{account}.dkr.ecr.{region}.amazonaws.com/{name}",
                "source": {"account_id": account, "region": region, "name": name},
            }
        },
        "configured_expressions": configured["expressions"],
        "provider": resource["provider_name"],
        "type": resource["type"],
    }


def _task_identity(
    resource: Node, configuration: Node, schema: Node, seed: tuple[str, int]
) -> Node:
    fields = {"arn", "arn_without_revision", "revision"}
    configured = _computed_fields(resource, configuration, schema, fields)
    attributes = schema["provider_schemas"][resource["provider_name"]]["resource_schemas"][
        resource["type"]
    ]["block"]["attributes"]
    if {field: attributes[field]["type"] for field in fields} != {
        "arn": "string",
        "arn_without_revision": "string",
        "revision": "number",
    }:
        pytest.fail("setup_error task_identity_schema_types")
    provider = configuration["provider_config"][configured["provider_config_key"]]
    region = provider["expressions"]["region"].get("constant_value")
    values = resource["change"]["after"]
    family = values.get("family")
    if (
        provider["name"] != "aws"
        or provider.get("alias")
        or region != "us-east-2"
        or "family" not in configured.get("expressions", {})
        or not isinstance(family, str)
        or not family
    ):
        pytest.fail(f"setup_error task_identity_family_provider={resource['address']}")
    account, revision = seed
    base = f"arn:aws:ecs:{region}:{account}:task-definition/{family}"
    observed = {"arn": f"{base}:{revision}", "arn_without_revision": base, "revision": revision}
    return {
        "address": resource["address"],
        "mode_index": resource.get("index"),
        "fields": {
            field: {
                "value": value,
                "source": {
                    "account_id": account,
                    "region": region,
                    "family": family,
                    "controlled_revision_seed": revision,
                    "original_computed_value": values.get(field),
                },
            }
            for field, value in observed.items()
        },
        "configured_expressions": configured.get("expressions", {}),
        "provider": resource["provider_name"],
        "type": resource["type"],
    }
