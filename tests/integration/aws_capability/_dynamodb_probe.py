"""Preparo DynamoDB com falha explícita dos perfis ainda incompletos."""

from collections.abc import Sequence

from tests.integration.aws_capability._dynamodb_cancel import verify_cancel
from tests.integration.aws_capability._dynamodb_cas import verify_cas
from tests.integration.aws_capability._dynamodb_gsi import verify_gsi
from tests.integration.aws_capability._dynamodb_token import verify_token
from tests.integration.aws_capability._dynamodb_transactions import verify_transactions
from tests.integration.aws_capability._dynamodb_ttl import verify_ttl
from tests.integration.aws_capability._safety import run_name
from tests.integration.aws_capability._service_contracts import ServicePorts
from tests.integration.aws_capability._service_dynamodb_prepare import prepare_table
from tests.integration.aws_capability.contracts import (
    RequestContext,
    Resource,
    RunSpec,
    ServiceFailure,
)


def probe_dynamodb(
    resources: Sequence[Resource], spec: RunSpec, ports: ServicePorts, context: RequestContext
) -> None:
    """Raises: ServiceFailure após preparo ou para entrada/preparo inválido."""
    name = run_name(spec)
    root = Resource("dynamodb", name, name)
    if context.lane != "work" or len(resources) != 1 or resources[0] != root:
        raise ServiceFailure("probe=ddb_prepare_invalid")
    prepare_table(ports.commands, spec, root, context)
    if spec.probe == "ddb-conditions":
        verify_cas(ports.commands, spec, root, context)
        verify_cancel(ports.commands, spec, root, context)
        verify_token(ports.commands, spec, root, context)
    if spec.probe == "ddb-transactions":
        verify_transactions(ports.commands, spec, root, context)
    if spec.probe == "ddb-ttl-gsi":
        verify_ttl(ports, spec, root, context)
        verify_gsi(ports.commands, spec, root, context)
    raise ServiceFailure("probe=not_implemented")
