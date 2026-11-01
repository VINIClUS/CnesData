"""Contratos de fronteira e orçamento compartilhado do ensaio efêmero."""

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal, Protocol


@dataclass(frozen=True)
class RunSpec:
    account: str
    region: str
    run_id: str
    deadline: float
    journal_dir: Path
    retention: str = "GOVERNANCE"
    allow_governance_bypass: bool = False
    probe: str = "all"


@dataclass(frozen=True)
class Limits:
    calls: int = 80
    pages: int = 8
    attempts: int = 2
    cleanup_seconds: float = 30
    cleanup_reserve: int = 40


class BudgetExhausted(Exception):
    pass


@dataclass
class RequestBudget:
    limits: Limits
    deadline: float
    now: Callable[[], float]
    used: int = field(default=0, init=False)
    work_used: int = field(default=0, init=False)
    cleanup_deadline: float | None = field(default=None, init=False)

    def remaining_seconds(self, lane: str) -> float:
        if lane == "work":
            return self.deadline - self.now()
        if lane != "cleanup":
            raise BudgetExhausted("budget=invalid_lane")
        if self.cleanup_deadline is None:
            self.cleanup_deadline = self.now() + self.limits.cleanup_seconds
        return self.cleanup_deadline - self.now()

    def check(self, lane: str) -> None:
        if self.remaining_seconds(lane) <= 0:
            raise BudgetExhausted("budget=deadline")
        if self.used >= self.limits.calls:
            raise BudgetExhausted("budget=requests")
        if lane == "work" and self.work_used >= self.limits.calls - self.limits.cleanup_reserve:
            raise BudgetExhausted("budget=requests")

    def acquire(self, lane: str) -> None:
        self.check(lane)
        self.used += 1
        if lane == "work":
            self.work_used += 1


@dataclass(frozen=True)
class RequestContext:
    budget: RequestBudget
    lane: str


@dataclass(frozen=True)
class Policy:
    account: str
    regions: frozenset[str]
    limits: Limits = field(default_factory=Limits)


@dataclass(frozen=True)
class Resource:
    service: str
    resource_id: str
    name: str


@dataclass(frozen=True)
class Ownership:
    account: str
    tags: Mapping[str, str]
    region: str


@dataclass(frozen=True)
class Creation:
    spec: RunSpec
    name: str
    tags: Mapping[str, str]


@dataclass(frozen=True)
class Page:
    resources: tuple[Resource, ...] = ()
    cursor: str | None = None


@dataclass(frozen=True)
class Outcome:
    status: str = "not_implemented"
    reason: str = "not_implemented"
    remaining: tuple[Resource, ...] | None = None
    evidence: Mapping[str, object] = field(default_factory=dict)


class ServiceFailure(Exception):
    pass


ServerLookup = Literal[
    "exact_present", "exact_absent", "parent_missing", "transport_failure", "configuration_mismatch"
]


class Services(Protocol):
    def identity(self, context: RequestContext) -> str: ...
    def inventory(self, service: str, cursor: str | None, context: RequestContext) -> Page: ...
    def inspect(self, resource: Resource, context: RequestContext) -> Ownership: ...
    def lookup(self, resource: Resource, context: RequestContext) -> Ownership | None: ...
    def create(self, service: str, creation: Creation, context: RequestContext) -> Resource: ...
    def create_child(self, parent: Resource, kind: str, context: RequestContext) -> Resource: ...
    def lookup_server_exact(self, pending: Resource, context: RequestContext) -> ServerLookup: ...
    def probe(
        self, resources: Sequence[Resource], spec: RunSpec, context: RequestContext
    ) -> Mapping[str, object]: ...
    def dependents(
        self, resource: Resource, cursor: str | None, context: RequestContext
    ) -> Page: ...
    def delete(self, resource: Resource, bypass: bool, context: RequestContext) -> None: ...


class Processes(Protocol):
    def start(self, argv: Sequence[str], options: Mapping[str, object]) -> int: ...
    def alive(self, pid: int) -> bool: ...
    def receive(self, pid: int) -> Mapping[str, object] | None: ...
    def send(self, pid: int, message: Mapping[str, object]) -> None: ...


@dataclass(frozen=True)
class Ports:
    services: Services
    processes: Processes
    now: Callable[[], float]
    policy: Policy
    budget: RequestBudget
