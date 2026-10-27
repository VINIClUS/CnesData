"""Recibo causal durável precede ausência e aposentadoria DynamoDB após restart."""

import json
import os
import stat
import subprocess
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from tests.integration.aws_capability._cleanup import Cleanup
from tests.integration.aws_capability._durability import DurabilityTrace, assert_durable
from tests.integration.aws_capability._inventory import inventory
from tests.integration.aws_capability._journal import Journal, prepare_directory
from tests.integration.aws_capability._safety import SafetyFailure
from tests.integration.aws_capability._service_read_testing import ReadLab, make_read_lab, respond
from tests.integration.aws_capability._testing import ProcessBoundary
from tests.integration.aws_capability.contracts import (
    Ownership,
    Policy,
    Ports,
    RequestContext,
    Resource,
    ServiceFailure,
)
from tests.integration.aws_capability.lifecycle import janitor
from tests.integration.aws_capability.test_service_ddb_delete import (
    ROOT,
    fixture,
    queue,
    trace,
)

OWNER = Ownership(
    "111111111111",
    {"Project": "CnesData", "Purpose": "aws-capability", "Run": "a1b2c3d4", "ExpiresAt": "1060"},
    "us-east-2",
)


@pytest.fixture
def recovery_lab(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> ReadLab:
    directory = tmp_path / "private"
    prepare_directory(directory)
    lab = make_read_lab(monkeypatch, directory)
    lab.context = RequestContext(lab.context.budget, "cleanup")
    lab.services = replace(
        lab.services, spec=replace(lab.services.spec, allow_governance_bypass=True)
    )
    return lab


def cleanup(lab: ReadLab, accepted: bool = False) -> Cleanup:
    journal = Journal(lab.services.spec, phase="cleanup_requested", created=[ROOT])
    journal.save()
    if accepted:
        seed(journal, receipt())
        journal.load()
    ports = Ports(
        lab.services,
        ProcessBoundary([]),
        lab.context.budget.now,
        Policy("111111111111", frozenset({"us-east-2"})),
        lab.context.budget,
    )
    return Cleanup(journal, ports)


def receipt() -> dict[str, Any]:
    data = fixture()["journal"]
    data["dynamodb_delete_accepted"] = [fixture()["root"]]
    return data


def seed(journal: Journal, data: dict[str, Any]) -> None:
    journal.path.write_text(json.dumps(data))
    journal.path.chmod(0o600)


def durable(journal: Journal, accepted: bool = True) -> None:
    assert json.loads(journal.path.read_bytes()) == (
        receipt() if accepted else fixture()["journal"]
    )
    assert journal.path.stat().st_mode & 0o777 == 0o600
    assert journal.spec.journal_dir.stat().st_mode & 0o777 == 0o700


def finish(lab: ReadLab, protected: bool = True) -> list[str]:
    data = fixture()
    steps = (
        data["initial_visible"]
        + data["removal_reads"]
        + data["adapter_protected" if protected else "adapter_unprotected"]
        + [["D", "rnf"]]
        + data["empty_inventory"]
    )
    queue(lab, steps)
    return [name for name, _ in steps]


@pytest.mark.parametrize(("protected", "calls"), [(True, 23), (False, 20)])
def test_cleanup_completo_exige_aceitacao_e_inventario_final(
    recovery_lab: ReadLab, protected: bool, calls: int
) -> None:
    engine = cleanup(recovery_lab)
    names = finish(recovery_lab, protected)
    outcome = engine.execute()
    assert (outcome.status, outcome.reason, outcome.remaining) == ("succeeded", "complete", ())
    assert outcome.evidence == {
        "inventory_complete": True,
        "counts": {"cognito": 0, "dynamodb": 0, "s3": 0, "iam": 0},
    }
    trace(recovery_lab, names)
    assert recovery_lab.context.budget.used == calls
    assert engine.journal.created == []
    expected = {**fixture()["journal"], "phase": "complete", "created": []}
    assert json.loads(engine.journal.path.read_bytes()) == expected
    assert "dynamodb_delete_accepted" not in engine.journal.content().decode()


class AcceptedInterruption(BaseException):
    pass


def test_recibo_e_fsync_antecedem_primeira_ausencia_e_janitor_recarrega(
    recovery_lab: ReadLab, monkeypatch: pytest.MonkeyPatch
) -> None:
    engine = cleanup(recovery_lab)
    names = finish(recovery_lab)
    durability = DurabilityTrace()
    durability.install(monkeypatch)
    original = recovery_lab.process.run

    def stopped(argv: list[str], **options: Any) -> Any:
        if len(recovery_lab.process.calls) == 18:
            recovery_lab.process.calls.append((list(argv), options))
            recovery_lab.process.observed_debits.append(recovery_lab.context.budget.used)
            durable(engine.journal)
            assert_durable(durability, engine.journal.path)
            raise AcceptedInterruption
        return original(argv, **options)

    monkeypatch.setattr(subprocess, "run", stopped)
    with pytest.raises(AcceptedInterruption):
        engine.execute()
    durable(engine.journal)
    assert_durable(durability, engine.journal.path)
    trace(recovery_lab, names[:19], 5)
    recovery_lab.process.outcomes.clear()
    loaded = Journal(engine.journal.spec)
    loaded.load()
    assert loaded is not engine.journal
    assert json.loads(loaded.content()) == receipt()
    assert Cleanup(loaded, engine.ports).absent == set()
    monkeypatch.setattr(subprocess, "run", original)
    budget = recovery_lab.context.budget
    before = budget.used
    steps = [["A", "identity"]] + fixture()["empty_inventory"]
    steps += [["D", "rnf"]] + fixture()["empty_inventory"]
    queue(recovery_lab, steps)
    outcome = janitor(loaded.spec, engine.ports)
    assert (outcome.status, outcome.reason) == ("succeeded", "complete")
    assert budget.used - before == 10
    assert budget.cleanup_deadline == 1030
    trace(recovery_lab, names[:19] + [name for name, _ in steps])
    assert [call[0][2] for call in recovery_lab.process.calls].count("delete-table") == 1
    assert [call[0][2] for call in recovery_lab.process.calls].count("update-table") == 1
    assert json.loads(loaded.path.read_bytes())["created"] == []


def test_recibo_recarregado_precisa_lookup_exato_e_nove_requests(recovery_lab: ReadLab) -> None:
    engine = cleanup(recovery_lab, True)
    steps = fixture()["empty_inventory"] + [["D", "rnf"]] + fixture()["empty_inventory"]
    queue(recovery_lab, steps)
    outcome = engine.execute()
    assert (outcome.status, outcome.reason) == ("succeeded", "complete")
    trace(recovery_lab, [name for name, _ in steps])
    assert recovery_lab.context.budget.used == 9
    assert json.loads(engine.journal.path.read_bytes())["created"] == []


@pytest.mark.parametrize("failure", ["rnf", "in_use", "timeout", "object"])
def test_delete_sem_aceitacao_nao_faz_lookup_nem_aposenta(
    recovery_lab: ReadLab, failure: str
) -> None:
    engine = cleanup(recovery_lab)
    before = engine.journal.path.read_bytes()
    queue(recovery_lab, [["D", "unprotected"], ["T", "owner"]])
    if failure == "timeout":
        recovery_lab.process.outcomes.append(subprocess.TimeoutExpired("aws", 7))
    else:
        queue(recovery_lab, [["X", failure]])
    queue(recovery_lab, [["D", "rnf"], *fixture()["adapter_protected"]])
    with pytest.raises((SafetyFailure, ServiceFailure)) as error:
        engine.delete(ROOT)
    assert error.value.args in (
        ("lifecycle=cleanup_failed",),
        ("service=request_failed",),
        ("service=ddb_delete_state",),
    )
    trace(recovery_lab, ["D", "T", "X"], 7)
    engine.retire_confirmed([])
    assert engine.journal.path.read_bytes() == before
    assert engine.journal.created == [ROOT]
    assert engine.absent == set()
    assert "dynamodb_delete_accepted" not in engine.journal.content().decode()


@pytest.mark.parametrize("restart", [False, True])
def test_rnf_sem_recibo_apos_active_preserva_bytes(recovery_lab: ReadLab, restart: bool) -> None:
    engine = cleanup(recovery_lab)
    before = engine.journal.path.read_bytes()
    queue(recovery_lab, [["D", "protected"], ["T", "owner"]])
    assert recovery_lab.services.lookup(ROOT, recovery_lab.context) == OWNER
    if restart:
        loaded = Journal(engine.journal.spec)
        loaded.load()
        engine = Cleanup(loaded, engine.ports)
    queue(recovery_lab, [["D", "rnf"], ["D", "rnf"]])
    assert engine.remove_omitted([]) == "creation_uncertain"
    assert engine.remove_omitted([]) == "creation_uncertain"
    engine.retire_confirmed([])
    trace(recovery_lab, ["D", "T", "D", "D"])
    assert engine.journal.path.read_bytes() == before
    assert engine.journal.created == [ROOT]
    assert engine.absent == set()


@pytest.mark.parametrize("case", ["present", "foreign", "final_visible", "final_failed"])
def test_recibo_nao_substitui_ausencia_e_inventario_final(recovery_lab: ReadLab, case: str) -> None:
    engine = cleanup(recovery_lab, True)
    before = engine.journal.path.read_bytes()
    steps = fixture()["empty_inventory"]
    queue(recovery_lab, steps)
    if case in {"present", "foreign"}:
        queue(recovery_lab, [["D", "unprotected"]])
        owner = fixture()["replies"]["owner"]
        if case == "foreign":
            owner["Tags"][2]["Value"] = "foreign-run"
        respond(recovery_lab, owner)
        lookup = ["D", "T"]
    else:
        queue(recovery_lab, [["D", "rnf"]])
        lookup = ["D"]
    final = (
        fixture()["initial_visible"] if case == "final_visible" else fixture()["empty_inventory"]
    )
    if case == "final_failed":
        queue(recovery_lab, [["C", "denied"]])
        expected = "inventory_incomplete"
        names = [name for name, _ in steps] + lookup + ["C"]
    else:
        queue(recovery_lab, final)
        expected = "ownership_mismatch" if case == "foreign" else "cleanup_failed"
        names = [name for name, _ in steps] + lookup + [name for name, _ in final]
    outcome = engine.execute()
    assert (outcome.status, outcome.reason) == ("incomplete", expected)
    trace(recovery_lab, names)
    assert engine.journal.path.read_bytes() == before
    assert engine.journal.created == [ROOT]
    durable(engine.journal)


@pytest.mark.parametrize("fault", ["file_sync", "replace", "directory_sync"])
def test_save_falho_nao_deixa_recibo_em_memoria_nem_autoriza_lookup(
    recovery_lab: ReadLab, monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    engine = cleanup(recovery_lab)
    before = engine.journal.path.read_bytes()
    original_sync, original_replace = os.fsync, os.replace

    def synced(fd: int) -> None:
        directory = stat.S_ISDIR(os.fstat(fd).st_mode)
        if (fault == "directory_sync" and directory) or (fault == "file_sync" and not directory):
            raise OSError("native=receipt_fault")
        original_sync(fd)

    def replaced(source: str, target: str) -> None:
        if fault == "replace":
            raise OSError("native=receipt_fault")
        original_replace(source, target)

    monkeypatch.setattr(os, "fsync", synced)
    monkeypatch.setattr(os, "replace", replaced)
    queue(recovery_lab, fixture()["adapter_unprotected"] + [["D", "rnf"]])
    with pytest.raises(OSError):
        engine.delete(ROOT)
    trace(recovery_lab, ["D", "T", "X"], 1)
    assert json.loads(engine.journal.content()) == fixture()["journal"]
    assert engine.absent == set()
    expected = receipt() if fault == "directory_sync" else fixture()["journal"]
    assert json.loads(engine.journal.path.read_bytes()) == expected
    monkeypatch.setattr(os, "fsync", original_sync)
    monkeypatch.setattr(os, "replace", original_replace)
    recovery_lab.process.outcomes.clear()
    queue(recovery_lab, [["D", "rnf"]])
    assert engine.remove_omitted([]) == "creation_uncertain"
    assert engine.absent == set()
    assert engine.journal.created == [ROOT]
    assert recovery_lab.context.budget.used == 4
    fresh = Journal(engine.journal.spec)
    fresh.load()
    assert json.loads(fresh.content()) == expected
    if fault != "directory_sync":
        assert engine.journal.path.read_bytes() == before


def test_aposentadoria_salva_root_e_recibo_juntos_e_reverte_memoria_na_falha(
    recovery_lab: ReadLab, monkeypatch: pytest.MonkeyPatch
) -> None:
    engine = cleanup(recovery_lab, True)
    before = engine.journal.path.read_bytes()
    engine.absent.add(ROOT)
    original = os.replace

    def failed(source: str, target: str) -> None:
        raise OSError("native=retirement_fault")

    monkeypatch.setattr(os, "replace", failed)
    with pytest.raises(OSError):
        engine.retire_confirmed([])
    assert engine.journal.created == [ROOT]
    assert json.loads(engine.journal.content()) == receipt()
    assert engine.journal.path.read_bytes() == before
    monkeypatch.setattr(os, "replace", original)
    durability = DurabilityTrace()
    durability.install(monkeypatch)
    engine.retire_confirmed([])
    assert engine.journal.created == []
    assert json.loads(engine.journal.path.read_bytes()) == {**fixture()["journal"], "created": []}
    assert_durable(durability, engine.journal.path)


def test_final_visivel_por_identidade_impede_aposentadoria_mesmo_com_outro_nome(
    recovery_lab: ReadLab,
) -> None:
    engine = cleanup(recovery_lab, True)
    before = engine.journal.path.read_bytes()
    engine.absent.add(ROOT)
    engine.retire_confirmed([(Resource("dynamodb", "cnesdata-cap-a1b2c3d4", "other"), OWNER)])
    assert engine.journal.created == [ROOT]
    assert engine.journal.path.read_bytes() == before
    durable(engine.journal)


def test_esgotamento_antes_do_inventario_final_nao_reinicia_budget(recovery_lab: ReadLab) -> None:
    engine = cleanup(recovery_lab, True)
    budget = recovery_lab.context.budget
    budget.limits = replace(budget.limits, calls=5, cleanup_reserve=1)
    budget.deadline = 999
    budget.cleanup_deadline = 1017
    steps = fixture()["empty_inventory"] + [["D", "rnf"]] + fixture()["empty_inventory"]
    queue(recovery_lab, steps)
    outcome = engine.execute()
    assert (outcome.status, outcome.reason) == ("incomplete", "request_budget_exhausted")
    trace(recovery_lab, ["C", "L", "S", "I", "D"], 4)
    assert budget.cleanup_deadline == 1017
    assert budget.deadline == 999
    durable(engine.journal)


@pytest.mark.parametrize("case", ["missing", "baseline", "unknown", "memory_only"])
def test_mutacao_exige_root_confirmado_duravel_e_fora_do_baseline(
    recovery_lab: ReadLab, case: str
) -> None:
    engine = cleanup(recovery_lab)
    if case in {"missing", "memory_only"}:
        engine.journal.created = []
    elif case == "baseline":
        engine.journal.baseline = [replace(ROOT, name="baseline-alias")]
    else:
        engine.journal.operation_state = "unknown"
        engine.journal.pending_service = "dynamodb"
    engine.journal.save()
    before = engine.journal.path.read_bytes()
    if case == "memory_only":
        engine.journal.created = [ROOT]
    queue(recovery_lab, fixture()["adapter_protected"])
    with pytest.raises((SafetyFailure, ServiceFailure)) as error:
        engine.delete(ROOT)
    assert error.value.args in {
        ("lifecycle=cleanup_failed",),
        ("lifecycle=ownership_mismatch",),
        ("lifecycle=creation_uncertain",),
    }
    trace(recovery_lab, [], 6)
    assert engine.journal.path.read_bytes() == before
    assert engine.absent == set()


def test_stub_falho_mais_rnf_nao_fabrica_sucesso_causal(recovery_lab: ReadLab) -> None:
    engine = cleanup(recovery_lab)
    before = engine.journal.path.read_bytes()
    queue(
        recovery_lab, fixture()["removal_reads"] + [["D", "rnf"]] + fixture()["adapter_protected"]
    )
    reason = engine.remove([(ROOT, OWNER)])
    assert reason == "cleanup_failed"
    assert engine.absent == set()
    engine.retire_confirmed([])
    assert engine.journal.path.read_bytes() == before
    assert engine.journal.created == [ROOT]
    trace(recovery_lab, ["D", "T", "D", "T", "D", "T", "D"], 6)


def test_controle_nativo_dos_inventarios_e_tres_leituras_de_remocao(
    recovery_lab: ReadLab,
) -> None:
    engine = cleanup(recovery_lab)
    steps = fixture()["initial_visible"] + fixture()["removal_reads"]
    steps += fixture()["empty_inventory"]
    queue(recovery_lab, steps)
    assert inventory(engine.ports, engine.context) == [(ROOT, OWNER)]
    assert engine.removal_order(ROOT) == [ROOT]
    assert engine.ports.services.dependents(ROOT, None, engine.context).resources == ()
    assert inventory(engine.ports, engine.context) == []
    trace(recovery_lab, [name for name, _ in steps])
    durable(engine.journal, False)


def test_delete_aceito_com_presenca_posterior_preserva_recibo_sem_repetir(
    recovery_lab: ReadLab,
) -> None:
    engine = cleanup(recovery_lab)
    queue(recovery_lab, fixture()["adapter_unprotected"] + [["D", "unprotected"], ["T", "owner"]])
    with pytest.raises(SafetyFailure) as error:
        engine.delete(ROOT)
    assert error.value.args == ("lifecycle=cleanup_failed",)
    trace(recovery_lab, ["D", "T", "X", "D", "T"])
    assert engine.absent == set()
    assert engine.journal.created == [ROOT]
    durable(engine.journal)


def test_save_incerto_apos_replace_mantem_invocacao_incompleta(
    recovery_lab: ReadLab, monkeypatch: pytest.MonkeyPatch
) -> None:
    engine = cleanup(recovery_lab)
    original = os.fsync

    def failed(fd: int) -> None:
        if stat.S_ISDIR(os.fstat(fd).st_mode):
            raise OSError("native=directory_fault")
        original(fd)

    monkeypatch.setattr(os, "fsync", failed)
    names = finish(recovery_lab)
    outcome = engine.execute()
    assert (outcome.status, outcome.reason) == ("incomplete", "journal_unavailable")
    trace(recovery_lab, names[:18], 5)
    assert json.loads(engine.journal.content()) == fixture()["journal"]
    durable(engine.journal)
