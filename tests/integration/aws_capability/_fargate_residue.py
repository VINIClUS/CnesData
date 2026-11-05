"""Revisões de task definition do run: leitura paginada, família exata e contagem final."""

from tests.integration.aws_capability._safety import run_name
from tests.integration.aws_capability._service_commands import require_data
from tests.integration.aws_capability._service_contracts import CommandPort
from tests.integration.aws_capability._service_fargate import MAX_PAGES, call, next_token
from tests.integration.aws_capability.contracts import RequestContext, RunSpec, ServiceFailure

STATUSES = ("ACTIVE", "INACTIVE")
BATCH = 10


def family_of(arn: str) -> str:
    return arn.rsplit("/", 1)[-1].rsplit(":", 1)[0]


def belongs_to_run(arn: str, spec: RunSpec) -> bool:
    family = family_of(arn)
    return family == run_name(spec) or family.startswith(run_name(spec) + "-")


def list_revisions(
    commands: CommandPort, spec: RunSpec, status: str, context: RequestContext
) -> list[str]:
    """Returns: ARNs das revisões do run no status; Raises: ServiceFailure sem fim de página."""
    payload: dict[str, object] = {
        "familyPrefix": run_name(spec),
        "status": status,
        "maxResults": 100,
    }
    found: list[str] = []
    for _ in range(MAX_PAGES):
        data = require_data(call(commands, "ecs.list-task-definitions", payload, context))
        arns = data.get("taskDefinitionArns")
        if not isinstance(arns, list) or not all(isinstance(arn, str) and arn for arn in arns):
            raise ServiceFailure("service=inventory_invalid")
        found.extend(arn for arn in arns if belongs_to_run(arn, spec))
        token = next_token(data, "nextToken")
        if token is None:
            return found
        payload["nextToken"] = token
    raise ServiceFailure("service=inventory_invalid")


def revision_counts(
    commands: CommandPort, spec: RunSpec, context: RequestContext
) -> dict[str, int]:
    """Returns: contagem de revisões ACTIVE e INACTIVE do run."""
    return {
        status: len(list_revisions(commands, spec, status, context)) for status in STATUSES
    }


def purge_revisions(commands: CommandPort, spec: RunSpec, context: RequestContext) -> None:
    """Raises: ServiceFailure se a AWS recusar; só a recontagem prova que as revisões saíram."""
    for arn in list_revisions(commands, spec, "ACTIVE", context):
        payload = {"taskDefinition": arn}
        require_data(call(commands, "ecs.deregister-task-definition", payload, context))
    inactive = list_revisions(commands, spec, "INACTIVE", context)
    for start in range(0, len(inactive), BATCH):
        payload = {"taskDefinitions": inactive[start : start + BATCH]}
        require_data(call(commands, "ecs.delete-task-definitions", payload, context))
