"""Intenções duráveis e prontidão do supervisor antes de mutações."""

import os
import sys
from collections.abc import Mapping
from dataclasses import dataclass, field

from tests.integration.aws_capability._journal import Journal
from tests.integration.aws_capability._profiles import PROFILES
from tests.integration.aws_capability._resource_ids import cognito_child
from tests.integration.aws_capability._safety import SafetyFailure, run_name, tags
from tests.integration.aws_capability.contracts import (
    BudgetExhausted,
    Creation,
    Ports,
    RequestContext,
    Resource,
    RunSpec,
    ServiceFailure,
)


def acknowledgement(spec: RunSpec, worker_pid: int) -> dict[str, object]:
    return {
        "state": "ready",
        "account": spec.account,
        "region": spec.region,
        "run_id": spec.run_id,
        "journal": str(Journal(spec).path),
        "worker_pid": worker_pid,
    }


def supervisor_arguments(spec: RunSpec) -> list[str]:
    return [
        sys.executable,
        "-m",
        "tests.integration.aws_capability.lifecycle",
        "monitor",
        "--account",
        spec.account,
        "--region",
        spec.region,
        "--run-id",
        spec.run_id,
        "--deadline",
        str(spec.deadline),
        "--journal-dir",
        str(spec.journal_dir),
        "--worker-pid",
        str(os.getpid()),
        "--retention",
        spec.retention,
        "--allow-governance-bypass",
        "--probe",
        spec.probe,
    ]


def start_supervisor(spec: RunSpec, ports: Ports) -> int:
    try:
        pid = ports.processes.start(
            supervisor_arguments(spec),
            {"start_new_session": True, "close_fds": True, "shell": False},
        )
        if not ports.processes.alive(pid):
            raise SafetyFailure("supervisor_unavailable")
        if ports.processes.receive(pid) != acknowledgement(spec, os.getpid()):
            raise SafetyFailure("supervisor_not_ready")
        return pid
    except OSError:
        raise SafetyFailure("supervisor_unavailable") from None


@dataclass
class Worker:
    journal: Journal
    ports: Ports
    supervisor_pid: int
    mutation_attempted: bool = field(default=False, init=False)
    observations: Mapping[str, object] = field(default_factory=dict, init=False)

    def guard(self) -> None:
        if self.ports.now() >= self.journal.spec.deadline:
            raise SafetyFailure("timeout")
        if not self.ports.processes.alive(self.supervisor_pid):
            raise SafetyFailure("supervisor_unavailable")
        self.ports.budget.check("work")

    def create(self, service: str) -> None:
        self.guard()
        self.journal.phase = "creating"
        self.journal.pending_service = service
        self.journal.operation_state = "unknown"
        self.journal.save()
        spec = self.journal.spec
        creation = Creation(spec, run_name(spec), tags(spec))
        self.mutation_attempted = True
        resource = self.ports.services.create(
            service,
            creation,
            RequestContext(self.ports.budget, "work"),
        )
        self.journal.created.append(resource)
        self.journal.pending_service = None
        self.journal.operation_state = "confirmed"
        self.journal.save()

    def execute(self) -> str | None:
        try:
            for service in PROFILES[self.journal.spec.probe].services:
                self.create(service)
            for kind in PROFILES[self.journal.spec.probe].children:
                self.create_child(self.journal.created[-1], kind)
            self.guard()
            self.journal.phase = "probing"
            self.journal.save()
            self.observations = dict(
                self.ports.services.probe(
                    self.journal.created,
                    self.journal.spec,
                    RequestContext(self.ports.budget, "work"),
                )
            )
            self.guard()
        except SafetyFailure as error:
            return error.reason
        except BudgetExhausted:
            return "request_budget_exhausted"
        except TimeoutError:
            return "timeout"
        except KeyboardInterrupt:
            return "interrupted"
        except OSError:
            raise
        except Exception:
            return "operation_failed"
        return None

    def create_child(self, parent: Resource, kind: str) -> None:
        self.guard()
        child = cognito_child(
            parent, self.journal.spec, kind, {"identifier": "https://api.cnesdata.com.br"}
        )
        self.journal.pending_service = "cognito"
        self.journal.pending_resource = child
        self.journal.operation_state = "unknown"
        self.journal.save()
        self.mutation_attempted = True
        result = self.ports.services.create_child(
            parent, kind, RequestContext(self.ports.budget, "work")
        )
        if result != child:
            raise ServiceFailure("service=cognito_create_invalid")
        self.journal.created.append(child)
        self.journal.pending_service = None
        self.journal.pending_resource = None
        self.journal.operation_state = "confirmed"
        self.journal.save()
