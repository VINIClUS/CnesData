"""Validação fechada das identidades de filhos Cognito."""

import json
from collections.abc import Mapping

from tests.integration.aws_capability._service_cognito import _is_regional_pool_id
from tests.integration.aws_capability.contracts import Resource, RunSpec, ServiceFailure

_FIELDS = {"v", "run", "service", "root_id", "parent_id", "kind", "child_id"}
_CHILD_FIELDS = {
    "cognito.client": {"client_id"},
    "cognito.domain": {"domain"},
    "cognito.server": {"identifier"},
    "cognito.user": {"username"},
    "cognito.branding": {"client_id", "branding_id"},
}


def parse_cognito_client(resource: Resource, spec: RunSpec) -> tuple[Resource, str]:
    """Returns:
    Root e client ID após validação fechada do envelope.
    Raises:
    ServiceFailure: identidade inválida.
    """
    root, kind, child = parse_cognito_child(resource, spec)
    if kind != "cognito.client":
        raise ServiceFailure("service=resource_invalid")
    return root, child["client_id"]


def parse_cognito_child(resource: Resource, spec: RunSpec) -> tuple[Resource, str, dict[str, str]]:
    """Returns:
    Root, tipo e identidade após validação fechada do filho e pai.
    Raises:
    ServiceFailure: identidade inválida.
    """
    if resource.service != "cognito" or not isinstance(resource.name, str) or not resource.name:
        raise ServiceFailure("service=resource_invalid")
    payload = _decode(resource.resource_id)
    _validate_scope(payload, spec)
    root_id = payload["root_id"]
    if not isinstance(root_id, str) or not _is_regional_pool_id(root_id, spec.region):
        raise ServiceFailure("service=resource_invalid")
    kind, child = _child_identity(payload["kind"], payload["child_id"])
    root = Resource("cognito", root_id, resource.name)
    parent_id = root_id
    if kind == "cognito.branding":
        parent_id = cognito_child(
            root, spec, "cognito.client", {"client_id": child["client_id"]}
        ).resource_id
    if payload["parent_id"] != parent_id:
        raise ServiceFailure("service=resource_invalid")
    return root, kind, child


def cognito_child(root: Resource, spec: RunSpec, kind: str, child: Mapping[str, str]) -> Resource:
    """Returns:
    Candidato Cognito com envelope canônico, sem prova de posse.
    """
    parent_id = root.resource_id
    if kind == "cognito.branding":
        parent_id = cognito_child(
            root, spec, "cognito.client", {"client_id": child["client_id"]}
        ).resource_id
    payload = {
        "v": 1,
        "run": spec.run_id,
        "service": "cognito",
        "root_id": root.resource_id,
        "parent_id": parent_id,
        "kind": kind,
        "child_id": dict(child),
    }
    resource = Resource(
        "cognito", json.dumps(payload, sort_keys=True, separators=(",", ":")), root.name
    )
    parse_cognito_child(resource, spec)
    return resource


def _child_identity(kind: object, raw: object) -> tuple[str, dict[str, str]]:
    if not isinstance(kind, str) or kind not in _CHILD_FIELDS:
        raise ServiceFailure("service=resource_invalid")
    if not isinstance(raw, Mapping) or set(raw) != _CHILD_FIELDS[kind]:
        raise ServiceFailure("service=resource_invalid")
    child: dict[str, str] = {}
    for key in _CHILD_FIELDS[kind]:
        value = raw[key]
        if not isinstance(value, str) or not value:
            raise ServiceFailure("service=resource_invalid")
        child[key] = value
    return kind, child


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


def _object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate")
        result[key] = value
    return result
