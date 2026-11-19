"""Execução de tasks do probe: registro, run-task, espera limitada e leitura de logs."""

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass

from tests.integration.aws_capability._fargate_mirror import TaskSettings, task_definition_payload
from tests.integration.aws_capability._service_commands import require_data
from tests.integration.aws_capability._service_contracts import CommandPort
from tests.integration.aws_capability._service_fargate import call, failed_with, next_token
from tests.integration.aws_capability.contracts import RequestContext, ServiceFailure

RUN_ATTEMPTS = 4
RUN_PAUSE = 10.0
POLLS = 24
POLL_PAUSE = 5.0
LOG_TRIES = 12
LOG_PAUSE = 5.0
LOG_PAGES = 40
_PLATFORM = re.compile(r"[0-9]+\.[0-9]+\.[0-9]+")


@dataclass(frozen=True)
class Runner:
    commands: CommandPort
    pause: Callable[[float], None]
    context: RequestContext
    cluster: str

    def data(self, service: str, operation: str, payload: Mapping[str, object]) -> Mapping:
        return require_data(call(self.commands, f"{service}.{operation}", payload, self.context))


@dataclass(frozen=True)
class Finished:
    exit_code: int
    platform: str
    stream: str


def register(runner: Runner, settings: TaskSettings, entry: tuple[str, ...] | None) -> str:
    """Returns: ARN da revisão registrada com o payload espelhado."""
    payload = task_definition_payload(settings, entry)
    definition = runner.data("ecs", "register-task-definition", payload).get("taskDefinition")
    arn = definition.get("taskDefinitionArn") if isinstance(definition, Mapping) else None
    if not isinstance(arn, str) or not arn:
        raise ServiceFailure("probe=register_invalid")
    return arn


def start(runner: Runner, definition: str, network: Mapping[str, object]) -> str:
    """Returns: ARN da task; repete failures do run-task até o limite sem ecoar o motivo."""
    payload = {
        "cluster": runner.cluster,
        "launchType": "FARGATE",
        "taskDefinition": definition,
        "count": 1,
        "networkConfiguration": {"awsvpcConfiguration": dict(network)},
    }
    for attempt in range(RUN_ATTEMPTS):
        data = runner.data("ecs", "run-task", payload)
        tasks = data.get("tasks")
        if isinstance(tasks, list) and len(tasks) == 1 and isinstance(tasks[0], Mapping):
            arn = tasks[0].get("taskArn")
            if isinstance(arn, str) and arn:
                return arn
        if attempt < RUN_ATTEMPTS - 1:
            runner.pause(RUN_PAUSE)
    raise ServiceFailure("probe=run_task_failed")


def await_stop(runner: Runner, task: str) -> Finished:
    """Returns: saída da task parada; Raises: ServiceFailure se não parar ou não iniciar."""
    payload = {"cluster": runner.cluster, "tasks": [task]}
    for _ in range(POLLS):
        rows = runner.data("ecs", "describe-tasks", payload).get("tasks")
        row = rows[0] if isinstance(rows, list) and rows else None
        if isinstance(row, Mapping) and row.get("lastStatus") == "STOPPED":
            return _finished(row, task)
        runner.pause(POLL_PAUSE)
    stop = {"cluster": runner.cluster, "task": task, "reason": "cnesdata capability timeout"}
    runner.data("ecs", "stop-task", stop)
    raise ServiceFailure("probe=task_timeout")


def _finished(row: Mapping[str, object], task: str) -> Finished:
    containers = row.get("containers")
    container = containers[0] if isinstance(containers, list) and containers else None
    code = container.get("exitCode") if isinstance(container, Mapping) else None
    platform = row.get("platformVersion")
    if type(code) is not int:
        raise ServiceFailure("probe=task_not_started")
    if not isinstance(platform, str) or _PLATFORM.fullmatch(platform) is None:
        raise ServiceFailure("probe=task_invalid")
    return Finished(code, platform, f"recovery/processor/{task.rsplit('/', 1)[-1]}")


def read_stream(
    runner: Runner, group: str, stream: str, wanted: Callable[[list[str]], bool] = bool
) -> list[str]:
    """Returns: mensagens do stream; espera o atraso dos logs e segue o token até repetir."""
    messages: list[str] = []
    for attempt in range(LOG_TRIES):
        messages = _read_pages(runner, group, stream)
        if messages and wanted(messages):
            return messages
        if attempt < LOG_TRIES - 1:
            runner.pause(LOG_PAUSE)
    if messages:
        return messages
    raise ServiceFailure("probe=log_missing")


def _read_pages(runner: Runner, group: str, stream: str) -> list[str]:
    payload: dict[str, object] = {
        "logGroupName": group,
        "logStreamName": stream,
        "startFromHead": True,
    }
    messages: list[str] = []
    token: str | None = None
    for _ in range(LOG_PAGES):
        reply = call(runner.commands, "logs.get-log-events", payload, runner.context)
        if failed_with(reply, "ResourceNotFoundException"):
            return []
        data = require_data(reply)
        events = data.get("events")
        if not isinstance(events, list):
            raise ServiceFailure("probe=log_invalid")
        messages.extend(str(event.get("message", "")) for event in events if _is_event(event))
        following = next_token(data, "nextForwardToken")
        if following is None or following == token:
            return messages
        token = following
        payload["nextToken"] = following
    raise ServiceFailure("probe=log_invalid")


def _is_event(event: object) -> bool:
    return isinstance(event, dict)
