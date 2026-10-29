"""Criação e leitura exata do resource server Cognito canônico."""

from collections.abc import Mapping

from tests.integration.aws_capability._resource_ids import cognito_child, parse_cognito_child
from tests.integration.aws_capability._safety import owned, run_name
from tests.integration.aws_capability._service_cognito import _is_regional_pool_id, _read_tags
from tests.integration.aws_capability._service_commands import (
    call_command,
    is_remote_not_found,
    require_data,
)
from tests.integration.aws_capability._service_contracts import CommandPort, Reply
from tests.integration.aws_capability.cli import Request
from tests.integration.aws_capability.contracts import (
    Ownership,
    RequestContext,
    Resource,
    RunSpec,
    ServerLookup,
    ServiceFailure,
)

_IDENTIFIER = "https://api.cnesdata.com.br"


def create_server(
    commands: CommandPort, spec: RunSpec, root: Resource, context: RequestContext
) -> Resource:
    """Args: commands, spec, root e context autorizados.
    Returns: Envelope canônico confirmado por readback.
    Raises: ServiceFailure para entrada, posse ou resposta inválida.
    """
    _guard_root(spec, root, context)
    if context.lane != "work":
        raise ServiceFailure("service=cognito_creation_invalid")
    child = cognito_child(root, spec, "cognito.server", {"identifier": _IDENTIFIER})
    if _lookup_parent(commands, spec, root, context) != "exact_present":
        raise ServiceFailure("service=not_owned")
    reply = call_command(
        commands, Request("cognito-idp", "create-resource-server", _payload(root)), context
    )
    if _server_result(reply, root) != "exact_present":
        raise ServiceFailure("service=cognito_create_invalid")
    if _describe(commands, root, context) != "exact_present":
        raise ServiceFailure("service=cognito_create_invalid")
    return child


def lookup_server_exact(
    commands: CommandPort, spec: RunSpec, pending: Resource, context: RequestContext
) -> ServerLookup:
    """Args: commands, spec, pending persistido e context autorizados.
    Returns: Resultado fechado de posse e configuração fresca.
    Raises: ServiceFailure para entrada local inválida.
    """
    root, kind, child = parse_cognito_child(pending, spec)
    _guard_root(spec, root, context)
    if kind != "cognito.server" or child != {"identifier": _IDENTIFIER}:
        raise ServiceFailure("service=resource_invalid")
    if pending != cognito_child(root, spec, kind, child):
        raise ServiceFailure("service=resource_invalid")
    parent = _lookup_parent(commands, spec, root, context)
    if parent != "exact_present":
        return parent
    try:
        return _describe(commands, root, context)
    except ServiceFailure:
        return "transport_failure"


def _guard_root(spec: RunSpec, root: Resource, context: RequestContext) -> None:
    if (
        spec.probe != "cognito-resource-server"
        or not spec.allow_governance_bypass
        or context.lane not in {"work", "cleanup"}
        or root.service != "cognito"
        or root.name != run_name(spec)
        or not _is_regional_pool_id(root.resource_id, spec.region)
    ):
        raise ServiceFailure("service=resource_invalid")


def _lookup_parent(
    commands: CommandPort, spec: RunSpec, root: Resource, context: RequestContext
) -> ServerLookup:
    try:
        reply = call_command(
            commands,
            Request("cognito-idp", "describe-user-pool", {"UserPoolId": root.resource_id}),
            context,
        )
        if is_remote_not_found(reply):
            return "parent_missing"
        pool = require_data(reply).get("UserPool")
        if not isinstance(pool, Mapping):
            return "transport_failure"
        arn = f"arn:aws:cognito-idp:{spec.region}:{spec.account}:userpool/{root.resource_id}"
        if (pool.get("Id"), pool.get("Name"), pool.get("Arn")) != (
            root.resource_id,
            root.name,
            arn,
        ):
            return "parent_missing"
        owner = Ownership(spec.account, _read_tags(commands, spec, arn, context), spec.region)
        return "exact_present" if owned(root, owner, spec) else "parent_missing"
    except ServiceFailure:
        return "transport_failure"


def _payload(root: Resource) -> dict[str, object]:
    return {
        "UserPoolId": root.resource_id,
        "Identifier": _IDENTIFIER,
        "Name": f"{root.name}-api",
        "Scopes": [{"ScopeName": "api.access", "ScopeDescription": "Access to the CnesData API"}],
    }


def _describe(commands: CommandPort, root: Resource, context: RequestContext) -> ServerLookup:
    reply = call_command(
        commands,
        Request(
            "cognito-idp",
            "describe-resource-server",
            {"UserPoolId": root.resource_id, "Identifier": _IDENTIFIER},
        ),
        context,
    )
    return _server_result(reply, root)


def _server_result(reply: Reply, root: Resource) -> ServerLookup:
    if is_remote_not_found(reply):
        return "exact_absent"
    data = require_data(reply)
    server = data.get("ResourceServer")
    if set(data) != {"ResourceServer"} or not isinstance(server, Mapping):
        return "transport_failure"
    return "exact_present" if _valid_server(server, root) else "configuration_mismatch"


def _valid_server(server: Mapping[str, object], root: Resource) -> bool:
    if set(server) != {"UserPoolId", "Identifier", "Name", "Scopes"}:
        return False
    if (
        server["UserPoolId"] != root.resource_id
        or server["Identifier"] != _IDENTIFIER
        or server["Name"] != f"{root.name}-api"
    ):
        return False
    scopes = server["Scopes"]
    if not isinstance(scopes, list) or len(scopes) != 1:
        return False
    scope = scopes[0]
    return (
        isinstance(scope, Mapping)
        and set(scope) == {"ScopeName", "ScopeDescription"}
        and scope["ScopeName"] == "api.access"
        and scope["ScopeDescription"] == "Access to the CnesData API"
    )
