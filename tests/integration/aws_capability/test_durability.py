"""Durabilidade por inode/conteúdo e recuperação de falhas reais do filesystem."""

import json
import os
import stat
from pathlib import Path

import pytest

from tests.integration.aws_capability import lifecycle
from tests.integration.aws_capability._durability import DurabilityTrace, assert_durable
from tests.integration.aws_capability._oracle import assert_complete, assert_no_mutation
from tests.integration.aws_capability._testing import Lab, seed_journal
from tests.integration.aws_capability.contracts import RunSpec


def test_intencao_atual_persistida_no_inode_correto_antes_de_cada_create(
    lab: Lab, monkeypatch: pytest.MonkeyPatch
) -> None:
    trace = DurabilityTrace()
    trace.install(monkeypatch)
    observed: list[str] = []

    def inspect_journal(spec: RunSpec) -> None:
        assert_durable(trace, lab.journal())
        assert stat.S_IMODE(lab.journal().stat().st_mode) == 0o600
        assert stat.S_IMODE(spec.journal_dir.stat().st_mode) == 0o700
        data = json.loads(lab.journal().read_text())
        assert data["phase"] == "creating"
        assert data["pending_service"] == lab.events[-1][1]
        assert data["operation_state"] == "unknown"
        observed.append(data["pending_service"])
        trace.events.clear()

    lab.remote.at_create = inspect_journal
    outcome = lifecycle.run(lab.spec, lab.ports())
    assert_complete(outcome, lab.events)
    assert set(observed) == {"cognito", "dynamodb", "s3", "iam"}


@pytest.mark.parametrize("failure", ["write", "fsync", "replace"])
def test_falha_de_persistencia_inicial_impede_mutacao_remota(
    lab: Lab, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    original = getattr(os, failure)
    observed: list[str] = []

    def broken(*args: object, **kwargs: object) -> object:
        path = (
            Path(str(args[1]))
            if failure == "replace"
            else Path(os.readlink(f"/proc/self/fd/{args[0]}"))
        )
        if lab.spec.journal_dir in (path, path.parent):
            observed.append(failure)
            raise OSError("journal=storage_failure")
        return original(*args, **kwargs)

    monkeypatch.setattr(os, failure, broken)
    outcome = lifecycle.run(lab.spec, lab.ports())
    assert outcome.status == "refused"
    assert outcome.reason == "journal_unavailable"
    assert observed
    assert_no_mutation(lab.events)


def test_replace_falho_preserva_ultimo_journal_integro_para_novo_janitor(
    lab: Lab, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = os.replace
    previous: list[bytes] = []

    def fail_update(source: str | Path, target: str | Path, **kwargs: int | None) -> None:
        if Path(target) == lab.journal():
            data = json.loads(Path(source).read_text())
            if data["phase"] == "creating":
                previous.append(lab.journal().read_bytes())
                raise OSError("replace=failed_before_commit")
        original(source, target, **kwargs)

    monkeypatch.setattr(os, "replace", fail_update)
    outcome = lifecycle.run(lab.spec, lab.ports())
    assert outcome.reason == "journal_unavailable"
    assert previous
    assert lab.journal().read_bytes() == previous[-1]
    assert json.loads(previous[-1])["phase"] == "prepared"
    assert_no_mutation(lab.events)
    monkeypatch.setattr(os, "replace", original)
    recovered = lifecycle.janitor(lab.spec, lab.ports())
    assert_complete(recovered, lab.events)


def test_journal_truncado_nao_autoriza_cleanup(lab: Lab) -> None:
    seed_journal(lab)
    lab.journal().write_text('{"run_id": "a1b2c3d4",')
    outcome = lifecycle.janitor(lab.spec, lab.ports())
    assert outcome.status == "refused"
    assert outcome.reason == "journal_corrupt"
    assert_no_mutation(lab.events)


@pytest.mark.parametrize("mutation", ["empty", "inode", "path", "order", "parent"])
def test_oraculo_duravel_rejeita_fsync_sem_provar_journal_atual(
    tmp_path: Path, mutation: str
) -> None:
    journal = tmp_path / "run.json"
    journal.write_bytes(b'{"phase":"creating"}')
    inode = journal.stat().st_ino
    temporary = tmp_path / "pending.tmp"
    events = [
        ("file", inode, temporary, journal.read_bytes()),
        ("replace", inode, temporary, journal),
        ("directory", tmp_path.stat().st_ino, tmp_path),
    ]
    if mutation == "empty":
        events[0] = ("file", inode, temporary, b"")
    elif mutation == "inode":
        events[0] = ("file", inode + 1, temporary, journal.read_bytes())
    elif mutation == "path":
        events[0] = ("file", inode, tmp_path / "unrelated", journal.read_bytes())
    elif mutation == "order":
        events = [events[2], events[0], events[1]]
    else:
        events[2] = ("directory", tmp_path.parent.stat().st_ino, tmp_path.parent)
    with pytest.raises(AssertionError):
        assert_durable(DurabilityTrace(events), journal)


def test_controle_duravel_positivo_usa_syscalls_reais(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    trace = DurabilityTrace()
    trace.install(monkeypatch)
    temporary = tmp_path / "pending.tmp"
    journal = tmp_path / "run.json"
    with temporary.open("wb") as handle:
        handle.write(b'{"phase":"creating"}')
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, journal)
    descriptor = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    assert_durable(trace, journal)
