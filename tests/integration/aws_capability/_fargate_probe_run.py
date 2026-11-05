"""Plano do probe fargate-write: publica, concede, roda duas tasks e lê só o que foi fechado."""

import base64
import binascii
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace

from tests.integration.aws_capability._fargate_mirror import TaskSettings
from tests.integration.aws_capability._fargate_probe import (
    LOGICAL,
    MARKER,
    count_denials,
    parse_probe_line,
    probe_script,
)
from tests.integration.aws_capability._fargate_probe_task import (
    Finished,
    Runner,
    await_stop,
    read_stream,
    register,
    start,
)
from tests.integration.aws_capability._safety import root_name, tags
from tests.integration.aws_capability._service_contracts import (
    ImagePort,
    RegistryLogin,
    ServicePorts,
)
from tests.integration.aws_capability._service_fargate import rows_of, text_of
from tests.integration.aws_capability._service_fargate_create import default_vpc
from tests.integration.aws_capability.contracts import (
    RequestContext,
    Resource,
    RunSpec,
    ServiceFailure,
)

IAM_PAUSE = 10.0
POLICY_NAME = "cap-pull-and-logs"
TAG = "cap"
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
_GROUP = re.compile(r"sg-[a-z0-9]+")


@dataclass(frozen=True)
class Roots:
    name: str
    execution: str
    task: str
    group: str


@dataclass(frozen=True)
class Network:
    subnets: tuple[str, ...]
    ingress: int
    egress: int


def probe_fargate(
    resources: Sequence[Resource], spec: RunSpec, ports: ServicePorts, context: RequestContext
) -> Mapping[str, object]:
    """Returns: observação fechada; Raises: ServiceFailure com motivo probe= sem dados brutos."""
    roots = _roots(resources, spec, context)
    if ports.images is None or ports.image is None:
        raise ServiceFailure("probe=fargate_inputs_required")
    try:
        return _execute(roots, spec, ports, context)
    except ServiceFailure as error:
        message = str(error)
        if message.startswith("probe="):
            raise
        raise ServiceFailure("probe=" + message.partition("=")[2]) from None


def _roots(resources: Sequence[Resource], spec: RunSpec, context: RequestContext) -> Roots:
    name = root_name(spec, "")
    execution, task = root_name(spec, "exec"), root_name(spec, "task")
    expected = [("ecr", name), ("logs", name), ("iam", execution), ("iam", task)]
    expected += [("ec2", name), ("ecs", name)]
    found = [(ref.service, ref.name) for ref in resources]
    group = resources[4].resource_id if len(resources) == 6 else ""
    identifiers = [ref.resource_id for ref in resources]
    valid = (
        context.lane == "work"
        and found == expected
        and _GROUP.fullmatch(group) is not None
        and identifiers[:4] == [name, name, execution, task]
        and identifiers[5] == name
    )
    if not valid:
        raise ServiceFailure("probe=fargate_invalid")
    return Roots(name, execution, task, group)


def _execute(
    roots: Roots, spec: RunSpec, ports: ServicePorts, context: RequestContext
) -> Mapping[str, object]:
    assert ports.images is not None
    assert ports.image is not None
    runner = Runner(ports.commands, ports.pause, context, roots.name)
    image, digest = _publish(runner, roots, ports.images, ports.image)
    network = _network(runner, roots)
    _grant(runner, spec, roots, ports)
    settings = _settings(spec, roots, image, network)
    written, written_logs = _run(runner, settings, "write", ("python", "-c", probe_script()))
    recovered, recovered_logs = _run(runner, settings, "recovery", None)
    line = parse_probe_line("\n".join(written_logs))
    observed = _observation(digest, written, line)
    observed.update(
        recovery_exit_code=recovered.exit_code,
        recovery_disk_denials=count_denials(recovered_logs),
        network_ingress_rules=network.ingress,
        network_egress_rules=network.egress,
        network_subnets=len(network.subnets),
    )
    return observed


def _publish(
    runner: Runner, roots: Roots, images: ImagePort, local: str
) -> tuple[str, str]:
    repository = {"repositoryNames": [roots.name]}
    rows = rows_of(runner.data("ecr", "describe-repositories", repository), "repositories")
    if len(rows) != 1:
        raise ServiceFailure("probe=repository_invalid")
    uri = text_of(rows[0], "repositoryUri")
    if not uri.endswith("/" + roots.name):
        raise ServiceFailure("probe=repository_invalid")
    login = _login(runner, uri.split("/")[0])
    try:
        digest = images.push(local, f"{uri}:{TAG}", login, runner.context)
    except ServiceFailure:
        raise ServiceFailure("probe=push_failed") from None
    _confirm_digest(runner, roots, digest)
    return f"{uri}@{digest}", digest


def _login(runner: Runner, registry: str) -> RegistryLogin:
    data = runner.data("ecr", "get-authorization-token", {})
    rows = rows_of(data, "authorizationData")
    try:
        decoded = base64.b64decode(text_of(rows[0], "authorizationToken"), validate=True)
        username, _, password = decoded.decode().partition(":")
    except (binascii.Error, UnicodeDecodeError, IndexError):
        raise ServiceFailure("probe=token_invalid") from None
    if not username or not password:
        raise ServiceFailure("probe=token_invalid")
    return RegistryLogin(registry, username, password)


def _confirm_digest(runner: Runner, roots: Roots, digest: str) -> None:
    payload = {"repositoryName": roots.name, "imageIds": [{"imageTag": TAG}]}
    rows = rows_of(runner.data("ecr", "describe-images", payload), "imageDetails")
    published = rows[0].get("imageDigest") if len(rows) == 1 else None
    if _DIGEST.fullmatch(digest) is None or published != digest:
        raise ServiceFailure("probe=digest_mismatch")


def _network(runner: Runner, roots: Roots) -> Network:
    vpc = default_vpc(runner.commands, runner.context)
    filters = {"Filters": [{"Name": "vpc-id", "Values": [vpc]}]}
    rows = rows_of(runner.data("ec2", "describe-subnets", filters), "Subnets")
    subnets = tuple(sorted(text_of(row, "SubnetId") for row in rows))
    group = rows_of(
        runner.data("ec2", "describe-security-groups", {"GroupIds": [roots.group]}),
        "SecurityGroups",
    )
    if not subnets or len(group) != 1:
        raise ServiceFailure("probe=network_invalid")
    ingress, egress = group[0].get("IpPermissions"), group[0].get("IpPermissionsEgress")
    if not isinstance(ingress, list) or not isinstance(egress, list):
        raise ServiceFailure("probe=network_invalid")
    return Network(subnets, len(ingress), len(egress))


def _trust(spec: RunSpec) -> str:
    document = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "ecs-tasks.amazonaws.com"},
                "Action": "sts:AssumeRole",
                "Condition": {"StringEquals": {"aws:SourceAccount": spec.account}},
            }
        ],
    }
    return json.dumps(document, sort_keys=True, separators=(",", ":"))


def _pull_policy(spec: RunSpec, roots: Roots) -> str:
    repository = f"arn:aws:ecr:{spec.region}:{spec.account}:repository/{roots.name}"
    group = f"arn:aws:logs:{spec.region}:{spec.account}:log-group:{roots.name}"
    pulls = ["ecr:BatchGetImage", "ecr:GetDownloadUrlForLayer", "ecr:BatchCheckLayerAvailability"]
    document = {
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Action": pulls, "Resource": repository},
            {"Effect": "Allow", "Action": "ecr:GetAuthorizationToken", "Resource": "*"},
            {
                "Effect": "Allow",
                "Action": ["logs:CreateLogStream", "logs:PutLogEvents"],
                "Resource": f"{group}:log-stream:*",
            },
        ],
    }
    return json.dumps(document, sort_keys=True, separators=(",", ":"))


def _grant(runner: Runner, spec: RunSpec, roots: Roots, ports: ServicePorts) -> None:
    for role in (roots.execution, roots.task):
        payload = {"RoleName": role, "PolicyDocument": _trust(spec)}
        runner.data("iam", "update-assume-role-policy", payload)
    policy = {
        "RoleName": roots.execution,
        "PolicyName": POLICY_NAME,
        "PolicyDocument": _pull_policy(spec, roots),
    }
    runner.data("iam", "put-role-policy", policy)
    ports.pause(IAM_PAUSE)


def _settings(spec: RunSpec, roots: Roots, image: str, network: Network) -> TaskSettings:
    prefix = f"arn:aws:iam::{spec.account}:role/cnesdata-cap/"
    machine = f"arn:aws:states:{spec.region}:{spec.account}:stateMachine:{roots.name}"
    return TaskSettings(
        family=roots.name,
        image=image,
        execution_role_arn=prefix + roots.execution,
        task_role_arn=prefix + roots.task,
        log_group=roots.name,
        control_plane_table=roots.name,
        data_bucket=roots.name,
        state_machine_arn=machine,
        subnet_ids=network.subnets,
        security_group_id=roots.group,
        tags=tags(spec),
    )


def _run(
    runner: Runner, settings: TaskSettings, label: str, entry: tuple[str, ...] | None
) -> tuple[Finished, list[str]]:
    family = replace(settings, family=f"{settings.family}-{label}")
    definition = register(runner, family, entry)
    network = {
        "subnets": list(settings.subnet_ids),
        "securityGroups": [settings.security_group_id],
        "assignPublicIp": "ENABLED",
    }
    finished = await_stop(runner, start(runner, definition, network))
    wanted = _has_marker if entry else bool
    return finished, read_stream(runner, settings.log_group, finished.stream, wanted)


def _has_marker(messages: list[str]) -> bool:
    return any(line.startswith(MARKER) for message in messages for line in message.splitlines())


def _observation(digest: str, written: Finished, line: Mapping[str, object]) -> dict[str, object]:
    observed: dict[str, object] = {
        "image_digest": digest,
        "platform_version": written.platform,
        "user": line["user"],
        "uid": line["uid"],
        "root_write": line["root_write"],
        "tempfile": line["tempfile"],
        "write_exit_code": written.exit_code,
    }
    writes = []
    for logical in LOGICAL:
        entry = line[logical]
        assert isinstance(entry, Mapping)
        prefix = logical.lstrip("/")
        observed.update({f"{prefix}_{key}": entry[key] for key in ("uid", "gid", "mode", "write")})
        writes.append(entry["write"])
    observed["all_writable"] = all(w == "ok" for w in writes) and line["tempfile"] == "ok:/tmp"
    return observed
