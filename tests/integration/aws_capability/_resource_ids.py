"""Validação fechada da identidade de client Cognito."""

import json
from collections.abc import Mapping

from tests.integration.aws_capability._service_cognito import _is_regional_pool_id
from tests.integration.aws_capability.contracts import Resource, RunSpec, ServiceFailure

_FIELDS = {"v", "run", "service", "root_id", "parent_id", "kind", "child_id"}


def parse_cognito_client(resource: Resource, spec: RunSpec) -> tuple[Resource, str]:
    """Returns:
    Root e client ID após validação fechada do envelope.
    Raises:
    ServiceFailure: identidade inválida.
    """
    if resource.service != "cognito" or not isinstance(resource.name, str) or not resource.name:
        raise ServiceFailure("service=resource_invalid")
    payload = _decode(resource.resource_id)
    _validate_scope(payload, spec)
    root_id = payload["root_id"]
    if (
        not isinstance(root_id, str)
        or not _is_regional_pool_id(root_id, spec.region)
        or payload["parent_id"] != root_id
    ):
        raise ServiceFailure("service=resource_invalid")
    child = payload["child_id"]
    if not isinstance(child, Mapping) or set(child) != {"client_id"}:
        raise ServiceFailure("service=resource_invalid")
    client_id = child["client_id"]
    if not isinstance(client_id, str) or not client_id:
        raise ServiceFailure("service=resource_invalid")
    return Resource("cognito", root_id, resource.name), client_id


def _decode(value: str) -> Mapping[str, object]:
    try:
        payload = json.loads(value, object_pairs_hook=_object)
    except (TypeError, ValueError, RecursionError):
        raise ServiceFailure("service=resource_invalid") from None
    if not isinstance(payload, Mapping) or set(payload) != _FIELDS:
        raise ServiceFailure("service=resource_invalid")
    return payload


def _validate_scope(payload: Mapping[str, object], spec: RunSpec) -> None:
    if type(payload["v"]) is not int or payload["v"] != 1:
        raise ServiceFailure("service=resource_invalid")
    if payload["run"] != spec.run_id or payload["service"] != "cognito":
        raise ServiceFailure("service=resource_invalid")
    if payload["kind"] != "cognito.client":
        raise ServiceFailure("service=resource_invalid")


def _object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate")
        result[key] = value
    return result
