"""Laboratório do cognito-login sobre a fronteira com estado."""

from dataclasses import replace
from pathlib import Path
from typing import Any

from tests.integration.aws_capability._probe_testing import ProbeLab, make_probe_lab
from tests.integration.aws_capability._resource_ids import cognito_child
from tests.integration.aws_capability._safety import run_name, tags
from tests.integration.aws_capability.contracts import Creation, RequestContext, Resource
from tests.integration.aws_capability.services import AwsServices

API = "https://api.cnesdata.com.br"


def make_login_lab(tmp_path: Path, **options: Any) -> ProbeLab:
    return make_probe_lab(tmp_path / "private", "cognito-login", seeded=False, **options)


def cleanup_context(lab: ProbeLab) -> RequestContext:
    return RequestContext(lab.budget, "cleanup")


def create_root(lab: ProbeLab) -> Resource:
    spec = lab.spec
    creation = Creation(spec, run_name(spec), tags(spec))
    return lab.services.create("cognito", creation, lab.context)


def with_ports(lab: ProbeLab, **ports: Any) -> AwsServices:
    services = AwsServices(lab.spec, replace(lab.services.ports, **ports))
    lab.services = services
    return services


def server_of(lab: ProbeLab, root: Resource) -> Resource:
    return cognito_child(root, lab.spec, "cognito.server", {"identifier": API})
