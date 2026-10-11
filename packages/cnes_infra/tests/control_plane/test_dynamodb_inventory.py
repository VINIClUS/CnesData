import ast
import re
from collections.abc import Iterator
from inspect import signature
from pathlib import Path
from typing import Any

import pytest

import cnes_infra
from cnes_domain.control_plane.entities import Job
from cnes_infra.auth.dynamodb_memberships import DynamoDBMembershipCandidates
from cnes_infra.billing.keys import (
    QUOTA_RESERVATION_DUE_INDEX,
    QUOTA_RESERVATION_LOCATOR_INDEX,
    STRIPE_RECOVERY_DUE_INDEX,
)
from cnes_infra.control_plane import dynamodb_inventory
from cnes_infra.control_plane.dynamodb_codec import CandidateQuery, bounded_candidates
from cnes_infra.control_plane.dynamodb_inventory import (
    CONTROL_PLANE_INVENTORY,
    GlobalSecondaryIndex,
)
from cnes_infra.control_plane.dynamodb_keys import item_key

_SOURCE_ROOT = Path(cnes_infra.__file__).parent
_INVENTORY_SOURCE = Path(dynamodb_inventory.__file__)
_INDEX_TOKEN = re.compile(r"\bgsi\d+[a-z_]*")
_INDEX_NAME = re.compile(r"gsi\d+")
_DOCUMENTED = (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)
_TTL_NUMBER_TYPE = "N"


class _CandidateClient:
    def __init__(self, candidate: dict[str, dict[str, str]]) -> None:
        self._candidate = candidate
        self.base_keys: list[object] = []

    def query(self, **_request: Any) -> dict[str, Any]:
        return {"Items": [self._candidate]}

    def get_item(self, **request: Any) -> dict[str, Any]:
        self.base_keys.append(request["Key"])
        return {}


def _adapter_trees() -> Iterator[ast.Module]:
    for path in sorted(_SOURCE_ROOT.rglob("*.py")):
        if path != _INVENTORY_SOURCE:
            yield ast.parse(path.read_text(encoding="utf-8"))


def _docstrings(tree: ast.Module) -> set[int]:
    return {
        id(node.body[0].value)
        for node in ast.walk(tree)
        if isinstance(node, _DOCUMENTED) and node.body and isinstance(node.body[0], ast.Expr)
    }


def _string_constants(tree: ast.Module) -> Iterator[str]:
    docstrings = _docstrings(tree)
    return (
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and id(node) not in docstrings
    )


def _adapter_index_tokens() -> set[str]:
    return {
        token
        for tree in _adapter_trees()
        for value in _string_constants(tree)
        for token in _INDEX_TOKEN.findall(value)
    }


def _written_values(node: ast.AST, attribute: str) -> Iterator[ast.expr]:
    if isinstance(node, ast.Dict):
        yield from (
            value
            for key, value in zip(node.keys, node.values, strict=True)
            if isinstance(key, ast.Constant) and key.value == attribute
        )
    if isinstance(node, ast.Assign):
        yield from (
            node.value
            for target in node.targets
            if isinstance(target, ast.Subscript)
            and isinstance(target.slice, ast.Constant)
            and target.slice.value == attribute
        )


def _written_type_tags(attribute: str) -> set[object]:
    return {
        key.value
        for tree in _adapter_trees()
        for node in ast.walk(tree)
        for value in _written_values(node, attribute)
        if isinstance(value, ast.Dict)
        for key in value.keys
        if isinstance(key, ast.Constant)
    }


def _index_name(token: str) -> str:
    match = _INDEX_NAME.match(token)
    assert match is not None, f"token_sem_indice token={token}"
    return match.group()


def test_inventario_canonico_bate_com_os_adapters() -> None:
    inventory = CONTROL_PLANE_INVENTORY
    names = {index.name for index in inventory.indexes}
    table_keys = (inventory.partition_key, inventory.sort_key)
    index_keys = set(inventory.key_attributes()) - set(table_keys)
    tokens = _adapter_index_tokens()

    assert item_key("p", "s") == {
        inventory.partition_key: {inventory.key_attribute_type: "p"},
        inventory.sort_key: {inventory.key_attribute_type: "s"},
    }
    assert {_index_name(token) for token in tokens} == names
    assert tokens - names == index_keys
    assert inventory.key_attributes()[:2] == table_keys
    assert len(inventory.key_attributes()) == len(set(inventory.key_attributes()))


def test_constantes_de_indice_dos_adapters_pertencem_ao_inventario() -> None:
    names = {index.name for index in CONTROL_PLANE_INVENTORY.indexes}
    memberships = signature(DynamoDBMembershipCandidates).parameters["index_name"].default

    assert {
        STRIPE_RECOVERY_DUE_INDEX,
        QUOTA_RESERVATION_DUE_INDEX,
        QUOTA_RESERVATION_LOCATOR_INDEX,
        memberships,
    } <= names


def test_ttl_do_inventario_e_gravado_como_numero_pelos_adapters() -> None:
    attribute = CONTROL_PLANE_INVENTORY.ttl_attribute

    assert attribute not in CONTROL_PLANE_INVENTORY.key_attributes()
    assert _written_type_tags(attribute) == {_TTL_NUMBER_TYPE}


@pytest.mark.parametrize(
    "index", CONTROL_PLANE_INVENTORY.indexes, ids=lambda index: index.name
)
def test_codec_le_candidatos_pelas_chaves_e_projecao_do_inventario(
    index: GlobalSecondaryIndex,
) -> None:
    inventory = CONTROL_PLANE_INVENTORY
    keys = (inventory.partition_key, inventory.sort_key, index.partition_key, index.sort_key)
    candidate = {name: {inventory.key_attribute_type: name} for name in keys}
    candidate |= {"base_pk": {"S": "base-pk"}, "base_sk": {"S": "base-sk"}}
    client = _CandidateClient(candidate)

    found = bounded_candidates(
        client,
        {"TableName": "control-plane", "IndexName": index.name},
        CandidateQuery(Job, lambda _job: True, 1),
    )

    assert found == ()
    assert client.base_keys == [item_key("base-pk", "base-sk")]
    assert index.projection == "ALL"
