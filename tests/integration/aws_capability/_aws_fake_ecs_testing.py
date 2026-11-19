"""Operações ECS da fronteira com estado: cluster, famílias, revisões e tasks Fargate."""

# ruff: noqa: S108
import json
from collections.abc import Callable

from tests.integration.aws_capability._aws_fake_fargate_testing import (
    LOG_CANARY,
    STOPPED_CANARY,
    FakeCluster,
    FakeRevision,
    FakeTask,
    FargateState,
    cluster_arn,
    pairs,
)
from tests.integration.aws_capability._aws_fake_types_testing import FakeRole, denied, good
from tests.integration.aws_capability._service_contracts import Reply

SERVICE_PRINCIPAL = {"Service": "ecs-tasks.amazonaws.com"}
FARGATE_MEMORY = {"256": ("512", "1024", "2048"), "512": ("1024", "2048", "3072", "4096")}


def probe_line(writable: bool) -> str:
    owner = 999 if writable else 0
    mount = "ok" if writable else "EACCES"
    entry = {"mode": "0o755", "uid": owner, "gid": owner, "write": mount}
    body = {
        "uid": 999, "user": "app", "root_write": "EROFS",
        "/tmp": {**entry, "mode": "0o1777" if writable else "0o755", "uid": 0, "gid": 0},
        "/logs": dict(entry), "/data": dict(entry),
        "tempfile": "ok:/tmp" if writable else "FileNotFoundError",
    }
    return "CAPPROBE " + json.dumps(body, sort_keys=True)


def recovery_lines(denials: int) -> list[str]:
    start = {"event": "processor_started", "mode": "recovery", "note": LOG_CANARY}
    failed = {"event": "processor_entrypoint_failed", "error": "ProcessorExecutionUnavailable"}
    denial = "PermissionError: [Errno 13] Permission denied: '/tmp/x'"
    return [json.dumps(start), *[denial] * denials, json.dumps(failed)]


class EcsHandlers:
    fargate: FargateState
    roles: dict[str, FakeRole]
    counter: int
    clock: Callable[[], float]

    def _cluster_row(self, name: str) -> dict:
        cluster = self.fargate.clusters[name]
        return {"clusterName": name, "clusterArn": cluster_arn(name), "status": cluster.status}

    def _ecs_create_cluster(self, payload: dict, who: str) -> Reply:
        name = payload["clusterName"]
        active = name in self.fargate.clusters and self.fargate.clusters[name].status == "ACTIVE"
        if active:
            return denied("InvalidParameterException")
        tags = pairs(payload.get("tags", []), "key", "value")
        self.fargate.clusters[name] = FakeCluster(tags)
        return good({"cluster": self._cluster_row(name)})

    def _ecs_describe_clusters(self, payload: dict, who: str) -> Reply:
        found, failures = [], []
        for ref in payload["clusters"]:
            name = ref.rsplit("/", 1)[-1]
            if name not in self.fargate.clusters:
                failures.append({"arn": cluster_arn(name), "reason": "MISSING"})
                continue
            row = self._cluster_row(name)
            tags = self.fargate.clusters[name].tags
            if "TAGS" in payload.get("include", []):
                row["tags"] = [{"key": k, "value": v} for k, v in tags.items()]
            found.append(row)
        return good({"clusters": found, "failures": failures})

    def _ecs_list_clusters(self, payload: dict, who: str) -> Reply:
        arns = [
            cluster_arn(name)
            for name, cluster in self.fargate.clusters.items()
            if cluster.status == "ACTIVE"
        ]
        arns, token = self.fargate.paged(arns, payload.get("nextToken"))
        return good({"clusterArns": arns, **({"nextToken": token} if token else {})})

    def _ecs_list_tags_for_resource(self, payload: dict, who: str) -> Reply:
        name = payload["resourceArn"].rsplit("/", 1)[-1]
        cluster = self.fargate.clusters.get(name)
        if cluster is None:
            return denied("ClusterNotFoundException")
        return good({"tags": [{"key": k, "value": v} for k, v in cluster.tags.items()]})

    def _ecs_register_task_definition(self, payload: dict, who: str) -> Reply:
        problem = self._invalid_definition(payload)
        if problem is not None:
            return denied("ClientException", problem)
        family = payload["family"]
        number = 1 + sum(r.family == family for r in self.fargate.revisions)
        revision = FakeRevision(family, number, json.loads(json.dumps(payload)))
        self.fargate.revisions.append(revision)
        row = {"taskDefinitionArn": revision.arn, "family": family, "revision": number}
        return good({"taskDefinition": {**row, "status": "ACTIVE"}})

    def _invalid_definition(self, payload: dict) -> str | None:
        if "FARGATE" not in payload.get("requiresCompatibilities", []):
            return "requiresCompatibilities"
        if payload.get("networkMode") != "awsvpc":
            return "networkMode"
        if payload.get("memory") not in FARGATE_MEMORY.get(str(payload.get("cpu")), ()):
            return "cpu_memory"
        role = str(payload.get("executionRoleArn", "")).rsplit("/", 1)[-1]
        if role not in self.roles:
            return "executionRoleArn"
        return None

    def _by_arn(self, arn: str) -> FakeRevision | None:
        return next((r for r in self.fargate.revisions if r.arn == arn), None)

    def _ecs_list_task_definition_families(self, payload: dict, who: str) -> Reply:
        status = payload.get("status", "ACTIVE")
        wanted = {"ACTIVE": {"ACTIVE"}, "INACTIVE": {"INACTIVE", "DELETE_IN_PROGRESS"}}
        allowed = wanted.get(status, {"ACTIVE", "INACTIVE", "DELETE_IN_PROGRESS"})
        prefix = payload.get("familyPrefix", "")
        names = sorted({
            r.family for r in self.fargate.revisions
            if r.status in allowed and r.family.startswith(prefix)
        })
        return good({"families": names})

    def _ecs_list_task_definitions(self, payload: dict, who: str) -> Reply:
        status = payload.get("status", "ACTIVE")
        prefix = payload.get("familyPrefix", "")
        arns = [
            r.arn for r in self.fargate.revisions
            if r.status == status and r.family.startswith(prefix)
        ]
        return good({"taskDefinitionArns": arns})

    def _ecs_deregister_task_definition(self, payload: dict, who: str) -> Reply:
        revision = self._by_arn(payload["taskDefinition"])
        if self.fargate.deregister_denied:
            return denied("AccessDeniedException")
        if revision is None or revision.status == "DELETE_IN_PROGRESS":
            return denied("ClientException")
        revision.status = "INACTIVE"
        return good({"taskDefinition": {"taskDefinitionArn": revision.arn, "status": "INACTIVE"}})

    def _ecs_delete_task_definitions(self, payload: dict, who: str) -> Reply:
        if self.fargate.delete_definition_failures > 0:
            self.fargate.delete_definition_failures -= 1
            refused = [{"arn": arn, "reason": "SERVER"} for arn in payload["taskDefinitions"]]
            return good({"taskDefinitions": [], "failures": refused})
        done, failures = [], []
        for arn in payload["taskDefinitions"]:
            revision = self._by_arn(arn)
            if revision is None or revision.status == "ACTIVE":
                failures.append({"arn": arn, "reason": "TASK_DEFINITION_ACTIVE"})
                continue
            revision.status = "DELETE_IN_PROGRESS"
            done.append({"taskDefinitionArn": arn, "status": "DELETE_IN_PROGRESS"})
        return good({"taskDefinitions": done, "failures": failures})

    def _ecs_delete_cluster(self, payload: dict, who: str) -> Reply:
        name = payload["cluster"].rsplit("/", 1)[-1]
        cluster = self.fargate.clusters.get(name)
        if cluster is None or cluster.status != "ACTIVE":
            return denied("ClusterNotFoundException")
        if any(self._status(t) != "STOPPED" for t in self._tasks_of(name)):
            return denied("ClusterContainsTasksException")
        cluster.status = "INACTIVE"
        return good({"cluster": self._cluster_row(name)})

    def _tasks_of(self, name: str) -> list[FakeTask]:
        return [t for t in self.fargate.tasks.values() if t.cluster == name]

    def _status(self, task: FakeTask) -> str:
        now = self.clock()
        if now >= task.stop:
            return "STOPPED"
        return "RUNNING" if now >= task.start else "PROVISIONING"

    def _ecs_list_tasks(self, payload: dict, who: str) -> Reply:
        name = payload["cluster"].rsplit("/", 1)[-1]
        cluster = self.fargate.clusters.get(name)
        if cluster is None or cluster.status != "ACTIVE":
            return denied("ClusterNotFoundException")
        arns = [t.arn for t in self._tasks_of(name) if self._status(t) != "STOPPED"]
        return good({"taskArns": arns})

    def _ecs_describe_tasks(self, payload: dict, who: str) -> Reply:
        rows, failures = [], []
        if self.fargate.describe_missing > 0:
            self.fargate.describe_missing -= 1
            missing = [{"arn": arn, "reason": "MISSING"} for arn in payload["tasks"]]
            return good({"tasks": [], "failures": missing})
        for arn in payload["tasks"]:
            task = self.fargate.tasks.get(arn)
            if task is None:
                failures.append({"arn": arn, "reason": "MISSING"})
                continue
            rows.append(self._task_row(task))
        return good({"tasks": rows, "failures": failures})

    def _task_row(self, task: FakeTask) -> dict:
        status = self._status(task)
        container: dict = {"name": "processor"}
        row: dict = {
            "taskArn": task.arn, "lastStatus": status, "platformVersion": "1.4.0",
            "containers": [container],
        }
        if status == "STOPPED":
            row["stoppedReason"] = task.reason or "Essential container in task exited"
            if task.exit_code is not None:
                container["exitCode"] = task.exit_code
            else:
                container["reason"] = STOPPED_CANARY
        return row

    def _ecs_stop_task(self, payload: dict, who: str) -> Reply:
        task = self.fargate.tasks.get(payload["task"])
        if task is None:
            return denied("InvalidParameterException")
        if self._status(task) != "STOPPED" and not task.stopped_by_request:
            task.stop = self.clock() + self.fargate.stop_seconds
            task.stopped_by_request = True
            task.exit_code = None
        return good({"task": {"taskArn": task.arn, "lastStatus": self._status(task)}})
