"""Janitor real reconcilia checkpoints duráveis sem adotar colisões."""

import json
from contextlib import nullcontext

import pytest

from tests.integration.aws_capability._cleanup import Cleanup
from tests.integration.aws_capability._journal import Journal
from tests.integration.aws_capability._safety import SafetyFailure
from tests.integration.aws_capability._service_cognito_root_testing import (
    ROOT,
    RootLab,
    assert_journal,
    assert_private_channels,
    assert_trace,
    install_fault,
    load_requests,
    load_trace,
    observe_durability,
    queue_trace,
    root_lab,
)
from tests.integration.aws_capability._service_read_assertions import assert_rejected
from tests.integration.aws_capability._worker import Worker
from tests.integration.aws_capability.contracts import ServiceFailure
from tests.integration.aws_capability.lifecycle import janitor, run

__all__ = ["root_lab"]


@pytest.mark.parametrize(
    "name", ["recover_timeout", "recover_invalid"], ids=["timeout", "resposta_invalida"]
)
def test_create_perdido_recupera(root_lab: RootLab, name: str) -> None:
    trace = load_trace(name)
    journal = Journal(root_lab.services.spec)
    journal.save()
    queue_trace(root_lab, trace["steps"])
    with pytest.raises(ServiceFailure) as error:
        Worker(journal, root_lab.ports, 9876).create("cognito")
    unknown = assert_journal(root_lab, "j_unknown")
    assert_private_channels(root_lab, error.value)
    assert_rejected(error.value)
    fresh = Journal(journal.spec)
    fresh.load()
    assert fresh is not journal
    assert fresh.path.read_bytes() == unknown

    def before_mutation(argv: list[str], options: object) -> None:
        if argv[2] in {"update-user-pool", "delete-user-pool"}:
            observe_durability(root_lab, "j_confirmed")

    root_lab.at_request = before_mutation
    outcome = janitor(fresh.spec, root_lab.ports)
    assert [outcome.status, outcome.reason] == trace["result"]
    assert_journal(root_lab, trace["final"])
    assert_trace(root_lab, trace["steps"], ["work"] + ["cleanup"] * 56)
    assert root_lab.context.budget.work_used == 1
    assert [argv[2] for argv, _ in root_lab.process.calls].count("create-user-pool") == 1


@pytest.mark.parametrize(
    "name",
    [
        "collision_baseline",
        "collision_foreign",
        "collision_suffix",
        "collision_two_owned",
        "collision_owned_foreign",
    ],
    ids=["baseline", "foreign", "sufixo", "dois_owned", "owned_foreign"],
)
def test_owner_collision_preserva(root_lab: RootLab, name: str) -> None:
    trace = load_trace(name)
    journal = Journal(root_lab.services.spec)
    journal.path.write_text(json.dumps(load_trace(trace["initial"])))
    journal.path.chmod(0o600)
    before = assert_journal(root_lab, trace["initial"])
    queue_trace(root_lab, trace["steps"])
    outcome = janitor(journal.spec, root_lab.ports)
    assert [outcome.status, outcome.reason] == trace["result"]
    assert_journal(root_lab, trace["final"])
    assert journal.path.read_bytes() == before
    assert_trace(root_lab, trace["steps"], ["cleanup"] * trace["cleanup"])
    assert not any(
        argv[2].startswith(("update-", "delete-", "create-")) for argv, _ in root_lab.process.calls
    )


def test_estrangeiro_irrelevante_nao_bloqueia_recuperacao(root_lab: RootLab) -> None:
    trace = load_trace("recover_unrelated_foreign")
    journal = Journal(
        root_lab.services.spec,
        phase="creating",
        pending_service="cognito",
        operation_state="unknown",
    )
    journal.save()
    assert_journal(root_lab, "j_unknown")
    queue_trace(root_lab, trace["steps"])
    outcome = janitor(journal.spec, root_lab.ports)
    assert [outcome.status, outcome.reason] == trace["result"]
    assert_journal(root_lab, "j_complete")
    assert_trace(root_lab, trace["steps"], ["cleanup"] * 60)
    mutations = [
        (argv[2], json.loads(options["input"]))
        for argv, options in root_lab.process.calls
        if argv[2].startswith(("update-", "delete-"))
    ]
    assert mutations == [
        ("update-user-pool", load_requests()["U"]["stdin"]),
        ("delete-user-pool", {"UserPoolId": "us-east-2_PoolRoot123"}),
    ]
    assert all(payload["UserPoolId"] == "us-east-2_PoolRoot123" for _, payload in mutations)
    assert trace["steps"][-6:] == [
        ["L", "q"],
        ["D_OTHER", "f"],
        ["T_OTHER", "t"],
        ["LDDB", "d"],
        ["LS3", "b"],
        ["LIAM", "r"],
    ]


def test_lista_omissa_mantem_unknown(root_lab: RootLab) -> None:
    trace = load_trace("unknown_omitted")
    journal = Journal(
        root_lab.services.spec,
        phase="creating",
        pending_service="cognito",
        operation_state="unknown",
    )
    journal.save()
    before = assert_journal(root_lab, "j_unknown")
    queue_trace(root_lab, trace["steps"])
    outcome = janitor(journal.spec, root_lab.ports)
    assert [outcome.status, outcome.reason] == trace["result"]
    assert journal.path.read_bytes() == before
    assert_journal(root_lab, "j_unknown")
    assert_trace(root_lab, trace["steps"], ["cleanup"] * 33)


@pytest.mark.parametrize("name", ["eighth_plain", "eighth_retry"], ids=["sem_retry", "com_retry"])
def test_candidato_na_oitava_varredura(root_lab: RootLab, name: str) -> None:
    trace = load_trace(name)
    journal = Journal(
        root_lab.services.spec,
        phase="creating",
        pending_service="cognito",
        operation_state="unknown",
    )
    journal.save()
    queue_trace(root_lab, trace["steps"])
    outcome = janitor(journal.spec, root_lab.ports)
    assert [outcome.status, outcome.reason] == trace["result"]
    assert_journal(root_lab, "j_complete")
    if name == "eighth_retry":
        budget = root_lab.context.budget
        assert budget.used == 104
        assert budget.work_used == 0
        deadline = budget.cleanup_deadline
        before = assert_journal(root_lab, "j_complete")
        fresh = Journal(journal.spec)
        fresh.load()
        second = janitor(fresh.spec, root_lab.ports)
        assert (second.status, second.reason) == ("succeeded", "complete")
        assert root_lab.context.budget is budget
        assert root_lab.ports.budget is budget
        assert budget.used == 113
        assert budget.cleanup_deadline == deadline
        assert journal.path.read_bytes() == before
        assert len(root_lab.process.calls[104:]) == 9
        assert not any(
            argv[2].startswith(("update-", "delete-")) for argv, _ in root_lab.process.calls[104:]
        )
    assert_trace(root_lab, trace["steps"], ["cleanup"] * trace["cleanup"])
    assert root_lab.context.budget.used == trace["calls"]
    assert root_lab.context.budget.used <= 128
    deletes = [argv[2] for argv, _ in root_lab.process.calls].count("delete-user-pool")
    assert deletes == (2 if name == "eighth_retry" else 1)


@pytest.mark.parametrize("case", ["r3", "r4", "r5", "r6", "r7"], ids=["r3", "r4", "r5", "r6", "r7"])
def test_restart_apos_checkpoint(root_lab: RootLab, case: str) -> None:
    trace = load_trace("restart_" + case)
    journal = Journal(root_lab.services.spec)
    journal.path.write_text(json.dumps(load_trace(trace["initial"])))
    journal.path.chmod(0o600)
    journal.load()
    journal.save()
    queue_trace(root_lab, trace["steps"])
    if case in {"r3", "r4"}:
        if case == "r3":
            install_fault(root_lab, "replace", 2, True)
        expected = pytest.raises(KeyboardInterrupt) if case == "r3" else nullcontext()
        try:
            with expected:
                Worker(journal, root_lab.ports, 9876).create("cognito")
        except ServiceFailure as error:
            assert_private_channels(root_lab, error)
            assert_rejected(error)
            raise
    elif case in {"r5", "r6"}:
        install_fault(root_lab, "process", trace["fault"]["index"], True)
        try:
            with pytest.raises(KeyboardInterrupt):
                Cleanup(journal, root_lab.ports).delete(ROOT)
        except (ServiceFailure, SafetyFailure):
            assert len(root_lab.process.calls) == trace["fault"]["index"]
            raise
    else:
        first = Cleanup(journal, root_lab.ports).execute()
        assert (first.status, first.reason) == ("succeeded", "complete")
    checkpoint = "j_unknown" if case == "r3" else "j_confirmed"
    checkpoint = "j_complete" if case == "r7" else checkpoint
    before = assert_journal(root_lab, checkpoint)
    fresh = Journal(journal.spec)
    fresh.load()
    assert fresh is not journal
    assert fresh.path.read_bytes() == before
    outcome = janitor(fresh.spec, root_lab.ports)
    assert [outcome.status, outcome.reason] == trace["result"]
    assert_journal(root_lab, "j_complete")
    assert_trace(
        root_lab, trace["steps"], ["work"] * trace["work"] + ["cleanup"] * trace["cleanup"]
    )


@pytest.mark.parametrize(
    ("fault", "name"),
    [
        ("arquivo_fsync", "confirm_file_fsync"),
        ("replace", "confirm_replace"),
        ("diretorio_fsync", "confirm_directory_fsync"),
    ],
    ids=["arquivo_fsync", "replace", "diretorio_fsync"],
)
def test_save_falhou_preserva(root_lab: RootLab, fault: str, name: str) -> None:
    trace = load_trace(name)
    journal = Journal(
        root_lab.services.spec,
        phase="creating",
        pending_service="cognito",
        operation_state="unknown",
    )
    journal.save()
    before = assert_journal(root_lab, "j_unknown")
    install_fault(root_lab, fault)
    queue_trace(root_lab, trace["steps"])
    outcome = janitor(journal.spec, root_lab.ports)
    assert [outcome.status, outcome.reason] == trace["result"]
    assert_journal(root_lab, trace["final"])
    if fault != "diretorio_fsync":
        assert journal.path.read_bytes() == before
    assert_trace(root_lab, trace["steps"], ["cleanup"] * 7)
    assert not list(root_lab.directory.glob(".journal-*"))


@pytest.mark.parametrize(
    "name",
    ["proof_local_rnf", "proof_lookup_denied", "proof_final_visible", "proof_final_denied"],
    ids=["rnf_local", "lookup_denied", "final_visivel", "final_denied"],
)
def test_lookup_e_final_obrigatorios(root_lab: RootLab, name: str) -> None:
    trace = load_trace(name)
    journal = Journal(root_lab.services.spec, phase="creating", created=[ROOT])
    journal.save()
    before = assert_journal(root_lab, "j_confirmed")
    queue_trace(root_lab, trace["steps"])
    outcome = janitor(journal.spec, root_lab.ports)
    assert [outcome.status, outcome.reason] == trace["result"]
    assert_journal(root_lab, "j_confirmed")
    assert journal.path.read_bytes() == before
    assert_trace(root_lab, trace["steps"], ["cleanup"] * trace["cleanup"])


def test_segundo_janitor(root_lab: RootLab) -> None:
    trace = load_trace("second_janitor")
    journal = Journal(
        root_lab.services.spec,
        phase="creating",
        pending_service="cognito",
        operation_state="unknown",
    )
    journal.save()
    queue_trace(root_lab, trace["steps"])
    first = janitor(journal.spec, root_lab.ports)
    assert (first.status, first.reason) == ("succeeded", "complete")
    before = assert_journal(root_lab, "j_complete")
    fresh = Journal(journal.spec)
    fresh.load()
    budget_before = root_lab.context.budget.used
    outcome = janitor(fresh.spec, root_lab.ports)
    assert [outcome.status, outcome.reason] == trace["result"]
    assert root_lab.context.budget.used - budget_before == 9
    assert_journal(root_lab, "j_complete")
    assert journal.path.read_bytes() == before
    assert_trace(root_lab, trace["steps"], ["cleanup"] * 65)


@pytest.mark.parametrize(
    "name",
    ["budget_work", "budget_cleanup", "budget_deadline"],
    ids=["work_esgotado", "cleanup_esgotado", "deadline"],
)
def test_reserva_deadline(root_lab: RootLab, name: str) -> None:
    trace = load_trace(name)
    journal = Journal(root_lab.services.spec)
    journal.path.write_text(json.dumps(load_trace(trace["initial"])))
    journal.path.chmod(0o600)
    journal.load()
    before = assert_journal(root_lab, trace["initial"])
    budget = root_lab.context.budget
    for _ in range(trace["seed_work"]):
        budget.acquire("work")
    for _ in range(trace["seed_cleanup"]):
        budget.acquire("cleanup")
    if name == "budget_work":
        reason = Worker(journal, root_lab.ports, 9876).execute()
        assert reason == "request_budget_exhausted"
    elif name == "budget_cleanup":
        outcome = Cleanup(journal, root_lab.ports).execute()
        assert (outcome.status, outcome.reason) == ("incomplete", "request_budget_exhausted")
    else:
        root_lab.clock[0] = 1061
        budget.cleanup_deadline = 1060
        with pytest.raises(SafetyFailure, match="lifecycle=timeout"):
            Worker(journal, root_lab.ports, 9876).create("cognito")
        outcome = Cleanup(journal, root_lab.ports).execute()
        assert (outcome.status, outcome.reason) == ("incomplete", "cleanup_timeout")
    assert root_lab.process.calls == []
    assert root_lab.process.observed_debits == []
    assert root_lab.lanes == []
    assert budget.used == trace["seed_work"] + trace["seed_cleanup"]
    assert journal.path.read_bytes() == before
    assert_journal(root_lab, trace["final"])


def test_lifecycle_root_limpo_mantem_falha_terminal(root_lab: RootLab) -> None:
    trace = load_trace("run_terminal")
    queue_trace(root_lab, trace["steps"])
    outcome = run(root_lab.services.spec, root_lab.ports)
    assert [outcome.status, outcome.reason] == trace["result"]
    assert outcome.remaining == ()
    assert outcome.evidence == {
        "inventory_complete": True,
        "counts": {"cognito": 0, "dynamodb": 0, "s3": 0, "iam": 0},
    }
    assert_journal(root_lab, "j_complete")
    assert_trace(root_lab, trace["steps"], ["work"] * 8 + ["cleanup"] * 55)
    assert root_lab.context.budget.work_used == 8


def test_id_confirmado_ausente_exige_lookup(root_lab: RootLab) -> None:
    trace = load_trace("confirmed_absent")
    journal = Journal(root_lab.services.spec, phase="creating", created=[ROOT])
    journal.save()
    assert_journal(root_lab, "j_confirmed")
    queue_trace(root_lab, trace["steps"])
    outcome = janitor(journal.spec, root_lab.ports)
    assert [outcome.status, outcome.reason] == trace["result"]
    assert_journal(root_lab, "j_complete")
    assert_trace(root_lab, trace["steps"], ["cleanup"] * 10)
    assert root_lab.context.budget.work_used == 0
