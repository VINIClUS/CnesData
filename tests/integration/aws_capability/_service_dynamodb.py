"""Leituras frescas de tabela, tags e folha DynamoDB."""

from collections.abc import Mapping

from tests.integration.aws_capability._service_commands import (
    call_command,
    is_remote_not_found,
    require_data,
    require_error,
)
from tests.integration.aws_capability._service_contracts import CommandPort
from tests.integration.aws_capability.cli import Request
from tests.integration.aws_capability.contracts import (
    Ownership,
    Page,
    RequestContext,
    Resource,
    RunSpec,
    ServiceFailure,
)


def lookup_table(
    commands: CommandPort, spec: RunSpec, resource: Resource, context: RequestContext
) -> Ownership | None:
    reply = call_command(
        commands,
        Request("dynamodb", "describe-table", {"TableName": resource.resource_id}),
        context,
    )
    if not reply.ok:
        if is_remote_not_found(reply):
            return None
        require_error(reply)
        raise ServiceFailure("service=describe_failed")
    arn = _table_arn(require_data(reply), spec, resource)
    tags = _read_tags(commands, spec, arn, context)
    return Ownership(spec.account, tags, spec.region)


def inspect_table(
    commands: CommandPort, spec: RunSpec, resource: Resource, context: RequestContext
) -> Ownership:
    owner = lookup_table(commands, spec, resource, context)
    if owner is None:
        raise ServiceFailure("service=not_found")
    return owner


def leaf_dependents(
    commands: CommandPort, spec: RunSpec, resource: Resource, context: RequestContext
) -> Page:
    owner = inspect_table(commands, spec, resource, context)
    required = {
        "Project": "CnesData",
        "Purpose": "aws-capability",
        "Run": spec.run_id,
        "ExpiresAt": str(int(spec.deadline)),
    }
    if any(owner.tags.get(key) != value for key, value in required.items()):
        raise ServiceFailure("service=not_owned")
    return Page()


def _table_arn(data: Mapping[str, object], spec: RunSpec, resource: Resource) -> str:
    table = data.get("Table")
    if not isinstance(table, Mapping):
        raise ServiceFailure("service=describe_invalid")
    name = table.get("TableName")
    arn = table.get("TableArn")
    expected = f"arn:aws:dynamodb:{spec.region}:{spec.account}:table/{resource.name}"
    if name != resource.name or resource.resource_id != resource.name or arn != expected:
        raise ServiceFailure("service=describe_invalid")
    return expected


def _read_tags(
    commands: CommandPort, spec: RunSpec, arn: str, context: RequestContext
) -> dict[str, str]:
    tags: dict[str, str] = {}
    token: str | None = None
    used_tokens: set[str] = set()
    pages = 0
    while True:
        payload: dict[str, object] = {"ResourceArn": arn}
        if token is not None:
            payload["NextToken"] = token
        reply = call_command(
            commands,
            Request("dynamodb", "list-tags-of-resource", payload),
            context,
        )
        data = require_data(reply)
        page_tags, token = _tag_page(data)
        for key, value in page_tags:
            if key in tags:
                raise ServiceFailure("service=tags_ambiguous")
            tags[key] = value
        pages += 1
        if token is None:
            return tags
        if token in used_tokens:
            raise ServiceFailure("service=tags_cycle")
        used_tokens.add(token)
        if pages >= context.budget.limits.pages:
            raise ServiceFailure("service=tags_pages")


def _tag_page(data: Mapping[str, object]) -> tuple[list[tuple[str, str]], str | None]:
    if set(data) - {"Tags", "NextToken"} or "Tags" not in data:
        raise ServiceFailure("service=tags_invalid")
    raw_tags = data["Tags"]
    if not isinstance(raw_tags, list):
        raise ServiceFailure("service=tags_invalid")
    parsed: list[tuple[str, str]] = []
    for item in raw_tags:
        if not isinstance(item, Mapping) or set(item) != {"Key", "Value"}:
            raise ServiceFailure("service=tags_invalid")
        key, value = item["Key"], item["Value"]
        if not isinstance(key, str) or not isinstance(value, str):
            raise ServiceFailure("service=tags_invalid")
        parsed.append((key, value))
    token: str | None = None
    if "NextToken" in data:
        candidate = data["NextToken"]
        if not isinstance(candidate, str) or not candidate:
            raise ServiceFailure("service=tags_invalid")
        token = candidate
    return parsed, token
