"""Intenções duráveis e prontidão do supervisor antes de mutações."""

import os
import sys
from dataclasses import dataclass, field

from tests.integration.aws_capability._journal import Journal
from tests.integration.aws_capability._safety import SERVICES, SafetyFailure, run_name, tags
from tests.integration.aws_capability.contracts import (
    BudgetExhausted,
    Creation,
    Ports,
    RequestContext,
    RunSpec,
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
            for service in SERVICES:
                self.create(service)
            self.guard()
            self.journal.phase = "probing"
            self.journal.save()
            self.ports.services.probe(
                self.journal.created,
                self.journal.spec,
                RequestContext(self.ports.budget, "work"),
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
