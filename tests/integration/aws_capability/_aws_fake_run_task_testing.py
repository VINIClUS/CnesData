"""ecs run-task da fronteira com estado: pull, execução do container e logs com atraso."""

from collections.abc import Callable

from tests.integration.aws_capability._aws_fake_ecs_testing import (
    PULL_ACTIONS,
    SERVICE_PRINCIPAL,
    probe_line,
    recovery_lines,
)
from tests.integration.aws_capability._aws_fake_fargate_testing import (
    ACCOUNT,
    REGION,
    STOPPED_CANARY,
    TLS_EGRESS,
    FakeRevision,
    FakeTask,
    FargateState,
    repo_uri,
)
from tests.integration.aws_capability._aws_fake_types_testing import FakeRole, denied, good
from tests.integration.aws_capability._service_contracts import Reply


class RunTaskHandlers:
    fargate: FargateState
    roles: dict[str, FakeRole]
    counter: int
    clock: Callable[[], float]

    def _ecs_run_task(self, payload: dict, who: str) -> Reply:
        name = payload["cluster"].rsplit("/", 1)[-1]
        cluster = self.fargate.clusters.get(name)
        if cluster is None or cluster.status != "ACTIVE":
            return denied("ClusterNotFoundException")
        revision = self._by_arn(payload["taskDefinition"])  # type: ignore[attr-defined]
        if revision is None or revision.status != "ACTIVE":
            return denied("InvalidParameterException")
        network = payload["networkConfiguration"]["awsvpcConfiguration"]
        problem = self._pull_problem(revision, network)
        task = self._new_task(name, revision, network, problem)
        self.fargate.tasks[task.arn] = task
        row = {"taskArn": task.arn, "lastStatus": "PROVISIONING"}
        return good({"tasks": [row], "failures": []})

    def _new_task(
        self, cluster: str, revision: FakeRevision, network: dict, problem: str | None
    ) -> FakeTask:
        self.fargate.task_counter += 1
        identifier = f"{self.fargate.task_counter:032x}"
        arn = f"arn:aws:ecs:{REGION}:{ACCOUNT}:task/{cluster}/{identifier}"
        now = self.clock()
        group_id = network["securityGroups"][0]
        if problem is not None:
            return FakeTask(arn, cluster, group_id, now + 4, now + 8, False, None, problem)
        container = revision.definition["containerDefinitions"][0]
        probing = container.get("entryPoint", [None])[0] == "python"
        exit_code = self.fargate.probe_exit if probing else 1
        start = now + self.fargate.provision_seconds
        stop = start + self.fargate.task_seconds
        self._write_logs(container, identifier, stop, probing)
        return FakeTask(arn, cluster, group_id, start, stop, True, exit_code, None)

    def _write_logs(self, container: dict, identifier: str, stop: float, probing: bool) -> None:
        options = container["logConfiguration"]["options"]
        group = self.fargate.log_groups[options["awslogs-group"]]
        stream = f"{options['awslogs-stream-prefix']}/{container['name']}/{identifier}"
        if probing:
            override = self.fargate.probe_lines
            lines = override if override is not None else [probe_line(self.fargate.writable)]
        else:
            override = self.fargate.recovery_lines
            lines = override or recovery_lines(self.fargate.recovery_denials)
        if ("probe" if probing else "recovery") not in self.fargate.no_logs:
            group.streams[stream] = (stop + self.fargate.log_lag, list(lines))

    def _pull_problem(self, revision: FakeRevision, network: dict) -> str | None:
        definition = revision.definition
        container = definition["containerDefinitions"][0]
        reasons = (
            (self.fargate.force_pull_failure, "CannotPullContainerError"),
            (network.get("assignPublicIp") != "ENABLED", "CannotPullContainerError"),
            (not self._egress_ok(network["securityGroups"][0]), "CannotPullContainerError"),
            (not self._image_exists(container["image"]), "CannotPullContainerError"),
            (not self._log_group_exists(container), "ResourceInitializationError"),
            (not self._roles_ready(definition), "ResourceInitializationError"),
        )
        return next((f"{code}: {STOPPED_CANARY}" for failed, code in reasons if failed), None)

    def _egress_ok(self, group_id: str) -> bool:
        group = self.fargate.groups.get(group_id)
        return group is not None and any(
            rule["IpProtocol"] == "-1"
            or (rule["IpProtocol"] == "tcp" and rule.get("FromPort") == TLS_EGRESS["FromPort"])
            for rule in group.egress
        )

    def _image_exists(self, image: str) -> bool:
        uri, _, digest = image.partition("@")
        name = uri.rsplit("/", 1)[-1]
        repo = self.fargate.repos.get(name)
        return repo is not None and uri == repo_uri(name) and digest in repo.images.values()

    def _log_group_exists(self, container: dict) -> bool:
        options = container["logConfiguration"]["options"]
        return options["awslogs-group"] in self.fargate.log_groups

    def _roles_ready(self, definition: dict) -> bool:
        execution = self.roles.get(definition["executionRoleArn"].rsplit("/", 1)[-1])
        task = self.roles.get(definition["taskRoleArn"].rsplit("/", 1)[-1])
        if execution is None or task is None:
            return False
        actions = {a for acts, effective, _ in execution.policies.values()
                   if self.counter >= effective for a in acts}
        return (
            _trusts_ecs(execution, ACCOUNT)
            and _trusts_ecs(task, ACCOUNT)
            and actions >= PULL_ACTIONS
        )


def _trusts_ecs(role: FakeRole, account: str) -> bool:
    document = role.trust_at(10**9)
    statement = document["Statement"][0]  # type: ignore[index]
    condition = statement.get("Condition", {}).get("StringEquals", {})
    return (
        statement["Principal"] == SERVICE_PRINCIPAL
        and statement["Action"] == "sts:AssumeRole"
        and condition.get("aws:SourceAccount") == account
    )
