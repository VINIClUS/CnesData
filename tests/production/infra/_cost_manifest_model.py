"""Validação do subconjunto de JSON Schema usado pelo manifesto e recomputação de custo."""

import re
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

type Node = dict[str, Any]

SCENARIOS = ("min", "base", "max")
PRODUCT_LIMIT = Decimal("8")
AGGREGATE_LIMIT = Decimal("15")
CENTS = Decimal("0.01")
SUPPORTED_KEYWORDS = frozenset({
    "$schema", "$id", "$defs", "$ref", "title", "description", "type", "const", "enum",
    "required", "properties", "additionalProperties", "items", "minItems", "minLength",
    "pattern", "minimum",
})
TYPES = {
    "object": dict, "array": list, "string": str, "null": type(None), "boolean": bool,
}


def unsupported_keywords(schema: Any, path: str = "$") -> list[str]:
    if not isinstance(schema, dict):
        return []
    found = [f"{path}.{key}" for key in schema if key not in SUPPORTED_KEYWORDS]
    for key in ("properties", "$defs"):
        for name, child in schema.get(key, {}).items():
            found += unsupported_keywords(child, f"{path}.{key}.{name}")
    found += unsupported_keywords(schema.get("items"), f"{path}.items")
    return found


def _type_ok(value: Any, name: str) -> bool:
    if name == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if name == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    return isinstance(value, TYPES[name]) and not (name != "boolean" and isinstance(value, bool))


def _resolve(schema: Node, root: Node) -> Node:
    reference = schema.get("$ref")
    if reference is None:
        return schema
    assert reference.startswith("#/$defs/"), f"ref_unsupported ref={reference}"
    return root["$defs"][reference.removeprefix("#/$defs/")]


def validate(instance: Any, schema: Node, root: Node | None = None, path: str = "$") -> list[str]:
    root = root or schema
    schema = _resolve(schema, root)
    errors: list[str] = []
    names = schema.get("type")
    names = [names] if isinstance(names, str) else names
    if names is not None and not any(_type_ok(instance, name) for name in names):
        return [f"{path} type={names}"]
    if "const" in schema and instance != schema["const"]:
        errors.append(f"{path} const={schema['const']!r}")
    if "enum" in schema and instance not in schema["enum"]:
        errors.append(f"{path} enum={schema['enum']!r}")
    if isinstance(instance, str):
        errors += _string_errors(instance, schema, path)
    if isinstance(instance, (int, float)) and "minimum" in schema and instance < schema["minimum"]:
        errors.append(f"{path} minimum={schema['minimum']}")
    if isinstance(instance, dict):
        errors += _object_errors(instance, schema, root, path)
    if isinstance(instance, list):
        errors += _array_errors(instance, schema, root, path)
    return errors


def _string_errors(instance: str, schema: Node, path: str) -> list[str]:
    errors = []
    if len(instance) < schema.get("minLength", 0):
        errors.append(f"{path} minLength={schema['minLength']}")
    if "pattern" in schema and re.search(schema["pattern"], instance) is None:
        errors.append(f"{path} pattern={schema['pattern']}")
    return errors


def _object_errors(instance: Node, schema: Node, root: Node, path: str) -> list[str]:
    errors = [f"{path} required={key}" for key in schema.get("required", []) if key not in instance]
    properties = schema.get("properties", {})
    if schema.get("additionalProperties") is False:
        errors += [f"{path}.{key} additional" for key in instance if key not in properties]
    for key, child in properties.items():
        if key in instance:
            errors += validate(instance[key], child, root, f"{path}.{key}")
    return errors


def _array_errors(instance: list[Any], schema: Node, root: Node, path: str) -> list[str]:
    errors = []
    if len(instance) < schema.get("minItems", 0):
        errors.append(f"{path} minItems={schema['minItems']}")
    for index, item in enumerate(instance):
        errors += validate(item, schema.get("items", {}), root, f"{path}[{index}]")
    return errors


def _dec(value: float | int | None) -> Decimal:
    return Decimal(0) if value is None else Decimal(str(value))


def line_cost(line: Node, scenario: str) -> Decimal:
    price = _dec(line["unit_price_usd"])
    billable = _dec(line["quantity"][scenario]) - _dec(line["free_allowance"][scenario])
    return max(Decimal(0), billable) * price


def totals(manifest: Node) -> dict[str, dict[str, Decimal]]:
    result: dict[str, dict[str, Decimal]] = {"product": {}, "aggregate": {}}
    for scenario in SCENARIOS:
        product = sum(
            (line_cost(line, scenario) for line in manifest["lines"] if line["scope"] == "product"),
            Decimal(0),
        )
        everything = sum((line_cost(line, scenario) for line in manifest["lines"]), Decimal(0))
        result["product"][scenario] = product.quantize(CENTS, ROUND_HALF_UP)
        result["aggregate"][scenario] = everything.quantize(CENTS, ROUND_HALF_UP)
    return result


def violations(manifest: Node) -> list[str]:
    found = []
    computed = totals(manifest)
    for scope, limit in (("product", PRODUCT_LIMIT), ("aggregate", AGGREGATE_LIMIT)):
        for scenario in SCENARIOS:
            value = computed[scope][scenario]
            if value > limit:
                found.append(
                    f"cost_over_limit scope={scope} scenario={scenario} "
                    f"total_usd={value:.2f} limit_usd={limit:.2f}"
                )
    return found
