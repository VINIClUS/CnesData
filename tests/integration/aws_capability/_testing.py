"""Fronteiras sintéticas independentes para os testes de lifecycle."""

import json
import os
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path

from tests.integration.aws_capability._oracle import Trace
from tests.integration.aws_capability.contracts import (
    Creation,
    Ownership,
    Page,
    Policy,
    Ports,
    RequestBudget,
    RequestContext,
    Resource,
    RunSpec,
    ServiceFailure,
)

ACCOUNT = "111111111111"
OTHER_ACCOUNT = "222222222222"
RUN_ID = "a1b2c3d4"
NAME = "cnesdata-cap-a1b2c3d4"
SERVICES = ("cognito", "dynamodb", "s3", "iam")
SECRETS = (
    "dummy-password-do-not-log",
    "dummy-refresh-do-not-log",
    "dummy-code-do-not-log",
    "dummy-session-do-not-log",
    "<html>dummy-private-session</html>",
    "https://example.invalid/?X-Amz-Signature=dummy-private-signature",
)


def owned_tags() -> dict[str, str]:
    return {
        "Project": "CnesData",
        "Purpose": "aws-capability",
        "Run": "a1b2c3d4",
        "ExpiresAt": "1060",
    }


@dataclass
class Remote:
    events: list[tuple[object, ...]]
    resources: dict[str, Resource] = field(default_factory=dict)
    ownership: dict[str, Ownership] = field(default_factory=dict)
    children: dict[str, list[Resource]] = field(default_factory=dict)
    pages: dict[str, list[Page | Exception]] = field(default_factory=dict)
    child_pages: dict[str, list[Page | Exception]] = field(default_factory=dict)
    inspect_errors: set[str] = field(default_factory=set)
    delete_errors: dict[str, int] = field(default_factory=dict)
    delete_noop: set[str] = field(default_factory=set)
    identity_account: str = ACCOUNT
    create_error: BaseException | None = None
    fail_service: str = "cognito"
    probe_error: BaseException | None = None
    remote_created_on_error: bool = False
    after_create: Callable[[], None] = lambda: None
    at_create: Callable[[RunSpec], None] = lambda spec: None
    at_delete: Callable[[], None] = lambda: None
    at_probe: Callable[[], None] = lambda: None
    page_size: int = 1
    calls: int = 0
    probe_requests: int = 1
    reply_name: str = NAME
    reply_owner: Ownership | None = None
    request_lanes: list[str] = field(default_factory=list)

    def touch(self, *event: object) -> None:
        self.events.append(event)

    def request(self, context: RequestContext) -> None:
        context.budget.acquire(context.lane)
        self.calls += 1
        assert self.calls <= 200, "oraculo=limite_externo_de_chamadas"
        self.request_lanes.append(context.lane)

    def add(self, resource: Resource, owner: Ownership | None = None) -> None:
        self.resources[resource.resource_id] = resource
        self.ownership[resource.resource_id] = owner or Ownership(ACCOUNT, owned_tags())

    def identity(self, context: RequestContext) -> str:
        self.request(context)
        self.touch("identity")
        return self.identity_account

    def inventory(self, service: str, cursor: str | None, context: RequestContext) -> Page:
        self.request(context)
        self.touch("inventory", service, cursor)
        try:
            page = self.inventory_page(service, cursor)
        except Exception:
            self.touch("inventory_error", service, cursor)
            raise
        self.touch("inventory_result", service, cursor, page)
        return page

    def inventory_page(self, service: str, cursor: str | None) -> Page:
        if self.pages.get(service):
            return self.scripted(self.pages[service])
        children = {child.resource_id for group in self.children.values() for child in group}
        rows = [r for r in self.resources.values() if r.service == service]
        return self.paginate([r for r in rows if r.resource_id not in children], cursor)

    def inspect(self, resource: Resource, context: RequestContext) -> Ownership:
        self.request(context)
        self.touch("inspect", resource.resource_id)
        if resource.resource_id in self.inspect_errors:
            raise ServiceFailure("ownership=unavailable")
        return self.ownership[resource.resource_id]

    def create(self, service: str, creation: Creation, context: RequestContext) -> Resource:
        self.request(context)
        self.touch("create", service, creation)
        self.at_create(creation.spec)
        resource = Resource(service, f"synthetic-{service}", self.reply_name)
        failing = self.create_error is not None and service == self.fail_service
        if not failing or self.remote_created_on_error:
            self.add(resource, self.reply_owner)
        self.after_create()
        if failing:
            assert self.create_error is not None
            raise self.create_error
        self.touch("created", resource)
        return resource

    def probe(self, resources: Sequence[Resource], spec: RunSpec, context: RequestContext) -> None:
        self.touch("probe", tuple(resources), spec)
        self.at_probe()
        for _ in range(self.probe_requests):
            self.request(context)
        if self.probe_error:
            raise self.probe_error

    def dependents(self, resource: Resource, cursor: str | None, context: RequestContext) -> Page:
        self.request(context)
        self.touch("dependents", resource.resource_id, cursor)
        if self.child_pages.get(resource.resource_id):
            return self.scripted(self.child_pages[resource.resource_id])
        rows = self.children.get(resource.resource_id, [])
        return self.paginate([r for r in rows if r.resource_id in self.resources], cursor)

    def delete(self, resource: Resource, bypass: bool, context: RequestContext) -> None:
        self.request(context)
        self.touch("delete", resource.resource_id, bypass)
        self.at_delete()
        if self.delete_errors.get(resource.resource_id, 0):
            self.delete_errors[resource.resource_id] -= 1
            raise ServiceFailure("delete=denied " + " ".join(SECRETS))
        if resource.resource_id not in self.delete_noop:
            self.resources.pop(resource.resource_id, None)

    def paginate(self, rows: list[Resource], cursor: str | None) -> Page:
        offset = int(cursor or "0")
        end = offset + self.page_size
        return Page(tuple(rows[offset:end]), str(end) if end < len(rows) else None)

    @staticmethod
    def scripted(pages: list[Page | Exception]) -> Page:
        item = pages.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


@dataclass
class ProcessBoundary:
    events: list[tuple[object, ...]]
    available: bool = True
    startup_error: bool = False
    ack: dict[str, object] | None = None
    workers: dict[int, bool] = field(default_factory=dict)

    def start(self, argv: Sequence[str], options: Mapping[str, object]) -> int:
        self.events.append(("start", tuple(argv), dict(options)))
        if self.startup_error:
            raise OSError("supervisor=unavailable")
        return 9876

    def alive(self, pid: int) -> bool:
        self.events.append(("alive", pid))
        return self.workers.get(pid, self.available)

    def receive(self, pid: int) -> Mapping[str, object] | None:
        self.events.append(("receive", pid, self.ack))
        return self.ack

    def send(self, pid: int, message: Mapping[str, object]) -> None:
        self.events.append(("send", pid, dict(message)))


@dataclass
class Lab:
    spec: RunSpec
    remote: Remote
    processes: ProcessBoundary
    events: Trace
    time: float = 1000

    def ports(self) -> Ports:
        self.events.begin()
        self.remote.calls = 0
        self.remote.request_lanes.clear()
        policy = Policy(ACCOUNT, frozenset({"us-east-2"}))
        budget = RequestBudget(policy.limits, self.spec.deadline, lambda: self.time)
        return Ports(self.remote, self.processes, lambda: self.time, policy, budget)

    def journal(self) -> Path:
        return self.spec.journal_dir / f"{self.spec.run_id}.json"


def make_lab(path: Path) -> Lab:
    events = Trace()
    spec = RunSpec(ACCOUNT, "us-east-2", RUN_ID, 1060, path, allow_governance_bypass=True)
    processes = ProcessBoundary(events)
    processes.ack = {
        "state": "ready",
        "account": ACCOUNT,
        "region": "us-east-2",
        "run_id": "a1b2c3d4",
        "journal": str(path / "a1b2c3d4.json"),
        "worker_pid": os.getpid(),
    }
    return Lab(spec, Remote(events), processes, events)


def seed_journal(lab: Lab, created: Sequence[Resource] = ()) -> None:
    lab.spec.journal_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    data = {
        "schema_version": 1,
        "account": "111111111111",
        "region": "us-east-2",
        "run_id": "a1b2c3d4",
        "deadline": 1060,
        "phase": "creating",
        "baseline": [],
        "pending_service": None if created else "cognito",
        "operation_state": "confirmed" if created else "unknown",
        "created": [ref_json(resource) for resource in created],
    }
    lab.journal().write_text(json.dumps(data), encoding="utf-8")
    lab.journal().chmod(0o600)


def ref_json(resource: Resource) -> dict[str, str]:
    return {
        "service": resource.service,
        "resource_id": resource.resource_id,
        "name": resource.name,
    }
