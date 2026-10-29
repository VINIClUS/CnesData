"""Entradas explícitas do lifecycle com serviços e processos injetados."""

import argparse
import logging
from collections.abc import Sequence
from pathlib import Path
from typing import NoReturn

from tests.integration.aws_capability._cleanup import Cleanup
from tests.integration.aws_capability._inventory import inventory
from tests.integration.aws_capability._journal import Journal, lease, prepare_directory
from tests.integration.aws_capability._safety import SafetyFailure, matches_name, validate
from tests.integration.aws_capability._worker import Worker, acknowledgement, start_supervisor
from tests.integration.aws_capability.contracts import (
    BudgetExhausted,
    Outcome,
    Ports,
    RequestContext,
    RunSpec,
    ServiceFailure,
)


def refusal(reason: str) -> Outcome:
    return Outcome("refused", reason, None, {"inventory_complete": False})


def confirm_identity(spec: RunSpec, ports: Ports, lane: str) -> None:
    if ports.services.identity(RequestContext(ports.budget, lane)) != spec.account:
        raise SafetyFailure("identity_mismatch")


def run_locked(journal: Journal, ports: Ports) -> Outcome:
    if journal.path.exists() or journal.path.is_symlink():
        return refusal("journal_exists")
    confirm_identity(journal.spec, ports, "work")
    baseline = inventory(ports, RequestContext(ports.budget, "work"))
    if any(matches_name(ref, journal.spec) for ref, _ in baseline):
        return refusal("run_collision")
    journal.baseline = [ref for ref, _ in baseline]
    journal.save()
    supervisor_pid = start_supervisor(journal.spec, ports)
    worker = Worker(journal, ports, supervisor_pid)
    try:
        reason = worker.execute()
        journal.phase = "cleanup_requested"
        journal.save()
    except OSError:
        status = "incomplete" if worker.mutation_attempted else "refused"
        return Outcome(status, "journal_unavailable", None, {"inventory_complete": False})
    outcome = Cleanup(journal, ports).execute()
    if reason and outcome.status == "succeeded":
        return Outcome("failed", reason, outcome.remaining, outcome.evidence)
    return outcome


def run(spec: RunSpec, ports: Ports) -> Outcome:
    try:
        validate(spec, ports)
        prepare_directory(spec.journal_dir)
        with lease(spec):
            return run_locked(Journal(spec), ports)
    except BlockingIOError:
        return Outcome("waiting", "cleanup_owned")
    except SafetyFailure as error:
        return refusal(error.reason)
    except OSError:
        return refusal("journal_unavailable")
    except BudgetExhausted:
        return refusal("request_budget_exhausted")
    except ServiceFailure:
        return refusal("operation_failed")


def janitor(spec: RunSpec, ports: Ports) -> Outcome:
    try:
        validate(spec, ports, recovery=True)
        journal = Journal(spec)
        journal.load()
        prepare_directory(spec.journal_dir)
        with lease(spec):
            journal.load()
            confirm_identity(spec, ports, "cleanup")
            return Cleanup(journal, ports).execute()
    except BlockingIOError:
        return Outcome("waiting", "cleanup_owned")
    except SafetyFailure as error:
        return refusal(error.reason)
    except OSError:
        return refusal("journal_unavailable")
    except (BudgetExhausted, ServiceFailure):
        return refusal("identity_unavailable")


def monitor(spec: RunSpec, ports: Ports, worker_pid: int) -> Outcome:
    try:
        validate(spec, ports, recovery=True)
        journal = Journal(spec)
        journal.load()
        if journal.phase == "complete":
            return Outcome("stopped", "worker_complete")
        active = ports.processes.alive(worker_pid) and ports.now() < spec.deadline
        if active and journal.phase != "cleanup_requested":
            ports.processes.send(worker_pid, acknowledgement(spec, worker_pid))
            return Outcome("waiting", "worker_active")
        return janitor(spec, ports)
    except SafetyFailure as error:
        return refusal(error.reason)
    except OSError:
        return refusal("supervisor_unavailable")


class Parser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        raise SafetyFailure("missing_argument")


def parse(argv: Sequence[str]) -> tuple[str, RunSpec, int | None]:
    if not argv or argv[0] not in {"run", "janitor", "monitor"}:
        raise SafetyFailure("explicit_command_required")
    parser = Parser(add_help=False, allow_abbrev=False)
    parser.add_argument("command", choices=("run", "janitor", "monitor"))
    for flag in ("account", "region", "run-id", "journal-dir", "retention"):
        parser.add_argument("--" + flag, required=True)
    parser.add_argument("--deadline", required=True, type=float)
    parser.add_argument("--worker-pid", type=int, required=argv[0] == "monitor")
    parser.add_argument("--allow-governance-bypass", action="store_true")
    parser.add_argument("--probe", default="all")
    args = parser.parse_args(argv)
    spec = RunSpec(
        args.account,
        args.region,
        args.run_id,
        args.deadline,
        Path(args.journal_dir),
        args.retention,
        args.allow_governance_bypass,
        args.probe,
    )
    return args.command, spec, args.worker_pid


def main(argv: Sequence[str], ports: Ports) -> Outcome:
    try:
        command, spec, worker_pid = parse(argv)
        if command == "monitor":
            if worker_pid is None or worker_pid <= 0:
                return refusal("invalid_worker_pid")
            return monitor(spec, ports, worker_pid)
        return run(spec, ports) if command == "run" else janitor(spec, ports)
    except SafetyFailure as error:
        return refusal(error.reason)


if __name__ == "__main__":
    logging.error("lifecycle=service_adapters_unavailable cycle=3a")
    raise SystemExit(2)
