"""Prontidão vinculada ao run e transferência exclusiva para cleanup."""

import fcntl
import json
import os

import pytest

from tests.integration.aws_capability import lifecycle
from tests.integration.aws_capability._durability import DurabilityTrace, assert_durable
from tests.integration.aws_capability._oracle import assert_complete, assert_no_mutation
from tests.integration.aws_capability._testing import NAME, Lab, seed_journal
from tests.integration.aws_capability.contracts import Resource


@pytest.mark.parametrize(
    "field", ["missing", "state", "account", "region", "run_id", "journal", "worker_pid"]
)
def test_processo_vivo_sem_ack_correto_nao_autoriza_criacao(lab: Lab, field: str) -> None:
    if field == "missing":
        lab.processes.ack = None
    else:
        assert lab.processes.ack is not None
        lab.processes.ack[field] = "foreign"
    outcome = lifecycle.run(lab.spec, lab.ports())
    assert outcome.status == "refused"
    assert outcome.reason == "supervisor_not_ready"
    assert_no_mutation(lab.events)


def test_monitor_aguarda_worker_vivo_sem_limpar(lab: Lab) -> None:
    resource = Resource("cognito", "pool", NAME)
    lab.remote.add(resource)
    seed_journal(lab, [resource])
    lab.processes.workers[4321] = True
    outcome = lifecycle.monitor(lab.spec, lab.ports(), 4321)
    assert outcome.status == "waiting"
    assert outcome.reason == "worker_active"
    assert_no_mutation(lab.events)
    assert set(lab.remote.resources) == {"pool"}
    acknowledgements = [event[2] for event in lab.events if event[0] == "send"]
    assert {
        "state": "ready",
        "account": "111111111111",
        "region": "us-east-2",
        "run_id": "a1b2c3d4",
        "journal": str(lab.journal()),
        "worker_pid": 4321,
    } in acknowledgements


@pytest.mark.parametrize("trigger", ["dead", "deadline", "handoff"])
def test_monitor_so_remove_apos_trigger_e_lock_exclusivo(lab: Lab, trigger: str) -> None:
    resource = Resource("cognito", "pool", NAME)
    lab.remote.add(resource)
    seed_journal(lab, [resource])
    lab.processes.workers[4321] = trigger != "dead"
    if trigger == "deadline":
        lab.time = 1061
    if trigger == "handoff":
        data = json.loads(lab.journal().read_text())
        data["phase"] = "cleanup_requested"
        lab.journal().write_text(json.dumps(data))
    lock = lab.spec.journal_dir / "a1b2c3d4.lock"
    with lock.open("a+") as worker_lease:
        fcntl.flock(worker_lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
        blocked = lifecycle.monitor(lab.spec, lab.ports(), 4321)
        assert blocked.status == "waiting"
        assert blocked.reason == "cleanup_owned"
        assert_no_mutation(lab.events)
    outcome = lifecycle.monitor(lab.spec, lab.ports(), 4321)
    assert_complete(outcome, lab.events)
    assert lab.remote.resources == {}


def test_worker_mantem_lock_durante_criacao_e_probe(lab: Lab) -> None:
    observed: list[str] = []

    def check_lease(spec: object) -> None:
        lock = lab.spec.journal_dir / "a1b2c3d4.lock"
        assert lock.exists()
        with lock.open("r+") as contender:
            with pytest.raises(BlockingIOError):
                fcntl.flock(contender, fcntl.LOCK_EX | fcntl.LOCK_NB)
        observed.append("exclusive")

    lab.remote.at_create = check_lease
    lab.remote.at_probe = lambda: check_lease(lab.spec)
    lab.remote.at_delete = lambda: check_lease(lab.spec)
    outcome = lifecycle.run(lab.spec, lab.ports())
    assert_complete(outcome, lab.events)
    assert observed == ["exclusive"] * 9


def test_monitor_concluido_nao_reabre_cleanup(lab: Lab) -> None:
    seed_journal(lab)
    data = json.loads(lab.journal().read_text())
    data.update(phase="complete", pending_service=None, operation_state="confirmed")
    lab.journal().write_text(json.dumps(data))
    outcome = lifecycle.monitor(lab.spec, lab.ports(), os.getpid())
    assert outcome.status == "stopped"
    assert outcome.reason == "worker_complete"
    assert_no_mutation(lab.events)


def test_monitor_nao_confirma_prontidao_com_journal_corrompido(lab: Lab) -> None:
    seed_journal(lab)
    lab.journal().write_text("truncated")
    outcome = lifecycle.monitor(lab.spec, lab.ports(), 4321)
    assert outcome.status == "refused"
    assert outcome.reason == "journal_corrupt"
    assert not any(event[0] == "send" for event in lab.events)
    assert_no_mutation(lab.events)


def test_janitor_manual_nao_concorre_com_outro_dono_do_cleanup(lab: Lab) -> None:
    resource = Resource("cognito", "pool", NAME)
    lab.remote.add(resource)
    seed_journal(lab, [resource])
    lock = lab.spec.journal_dir / "a1b2c3d4.lock"
    with lock.open("a+") as lease:
        fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
        outcome = lifecycle.janitor(lab.spec, lab.ports())
        assert outcome.status == "waiting"
        assert outcome.reason == "cleanup_owned"
        assert_no_mutation(lab.events)


def test_conclusao_normal_duravel_desarma_monitor_sem_novo_cleanup(
    lab: Lab, monkeypatch: pytest.MonkeyPatch
) -> None:
    trace = DurabilityTrace()
    trace.install(monkeypatch)
    outcome = lifecycle.run(lab.spec, lab.ports())
    assert_complete(outcome, lab.events)
    assert_durable(trace, lab.journal())
    data = json.loads(lab.journal().read_text())
    assert data["phase"] == "complete"
    assert data["pending_service"] is None
    stopped = lifecycle.monitor(lab.spec, lab.ports(), os.getpid())
    assert stopped.status == "stopped"
    assert stopped.reason == "worker_complete"
    assert_no_mutation(lab.events)
