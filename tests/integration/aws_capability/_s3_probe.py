"""Despacho dos probes S3 com validação fechada dos recursos criados."""

from collections.abc import Mapping, Sequence

from tests.integration.aws_capability._s3_actor import Actor
from tests.integration.aws_capability._s3_hold_probe import probe_hold
from tests.integration.aws_capability._s3_retention_probe import probe_retention
from tests.integration.aws_capability._safety import run_name
from tests.integration.aws_capability._service_contracts import ServicePorts
from tests.integration.aws_capability.contracts import (
    RequestContext,
    Resource,
    RunSpec,
    ServiceFailure,
)


def probe_s3(
    resources: Sequence[Resource], spec: RunSpec, ports: ServicePorts, context: RequestContext
) -> Mapping[str, object]:
    """Returns: observações não sensíveis; Raises: ServiceFailure para entrada inválida."""
    name = run_name(spec)
    bucket = Resource("s3", name, name)
    expected = [bucket] if spec.probe == "s3-hold" else [bucket, Resource("iam", name, name)]
    if context.lane != "work" or list(resources) != expected:
        raise ServiceFailure("probe=s3_invalid")
    actor = Actor(ports.commands, name, spec.account, context)
    if spec.probe == "s3-hold":
        return probe_hold(actor)
    if ports.session is None:
        raise ServiceFailure("probe=session_unavailable")
    return probe_retention(actor, ports, spec)
