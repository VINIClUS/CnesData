"""O handoff com a senha some em toda limpeza, mesmo após crash com journal incerto."""

from dataclasses import replace
from pathlib import Path

import pytest

from tests.integration.aws_capability import lifecycle
from tests.integration.aws_capability._cleanup import Cleanup
from tests.integration.aws_capability._journal import Journal, lease
from tests.integration.aws_capability._login_handoff import handoff_path, write_handoff
from tests.integration.aws_capability._login_lab_testing import (
    LoginWorld,
    create_root,
    make_login_world,
)
from tests.integration.aws_capability._probe_testing import janitor_ports


def stale(world: LoginWorld) -> Path:
    write_handoff(world.lab.spec, "url", "user", "PASSWORD-CANARY")
    return handoff_path(world.lab.spec)


def journal_with_root(world: LoginWorld) -> Journal:
    root = create_root(world.lab)
    journal = Journal(world.lab.spec)
    journal.created = [root]
    journal.phase = "cleanup_requested"
    journal.save()
    return journal


def test_janitor_remove_handoff_deixado_por_crash(tmp_path: Path) -> None:
    world = make_login_world(tmp_path)
    journal_with_root(world)
    path = stale(world)
    outcome = lifecycle.janitor(world.lab.spec, janitor_ports(world.lab))
    assert outcome.status == "succeeded"
    assert not path.exists()


def test_janitor_remove_handoff_mesmo_com_journal_corrompido(tmp_path: Path) -> None:
    world = make_login_world(tmp_path)
    journal = journal_with_root(world)
    journal.path.write_text("{corrompido")
    path = stale(world)
    outcome = lifecycle.janitor(world.lab.spec, janitor_ports(world.lab))
    assert outcome.status == "refused"
    assert not path.exists()


def test_cleanup_remove_handoff_antes_de_qualquer_chamada_remota(tmp_path: Path) -> None:
    world = make_login_world(tmp_path)
    journal = journal_with_root(world)
    path = stale(world)
    calls_before = len(world.lab.aws.calls)
    cleanup = Cleanup(journal, janitor_ports(world.lab))
    cleanup.execute()
    assert not path.exists()
    assert len(world.lab.aws.calls) > calls_before


def test_cleanup_remove_handoff_mesmo_se_o_inventario_falha(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    world = make_login_world(tmp_path)
    journal = journal_with_root(world)
    path = stale(world)
    cleanup = Cleanup(journal, janitor_ports(world.lab))

    def broken() -> None:
        raise RuntimeError("boom")

    monkeypatch.setattr(cleanup, "reconcile", broken)
    with pytest.raises(RuntimeError):
        cleanup.execute()
    assert not path.exists()


def test_run_completo_com_crash_no_meio_da_cadeia_nao_deixa_handoff(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    world = make_login_world(tmp_path)

    def explode(timeout: float) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(world.listener, "wait", explode)
    outcome = lifecycle.run(world.lab.spec, janitor_ports(world.lab, supervised=True))
    assert outcome.status == "failed"
    assert not handoff_path(world.lab.spec).exists()
    assert world.lab.aws.pools == {}
    assert world.listener.closed is True


def temp_of(world: LoginWorld) -> Path:
    path = world.lab.spec.journal_dir / f".handoff-{world.lab.spec.run_id}-abc"
    path.write_text("PASSWORD-CANARY")
    return path


def test_janitor_com_lease_do_worker_preserva_o_handoff_ativo(tmp_path: Path) -> None:
    world = make_login_world(tmp_path)
    journal_with_root(world)
    path, temp = stale(world), temp_of(world)
    with lease(world.lab.spec):
        outcome = lifecycle.janitor(world.lab.spec, janitor_ports(world.lab))
    assert (outcome.status, outcome.reason) == ("waiting", "cleanup_owned")
    assert path.exists()
    assert temp.exists()


def test_janitor_com_journal_corrompido_e_lease_do_worker_preserva_o_handoff(
    tmp_path: Path,
) -> None:
    world = make_login_world(tmp_path)
    journal = journal_with_root(world)
    journal.path.write_text("{corrompido")
    path = stale(world)
    with lease(world.lab.spec):
        outcome = lifecycle.janitor(world.lab.spec, janitor_ports(world.lab))
    assert outcome.status == "refused"
    assert path.exists()


def test_janitor_com_run_id_invalido_nao_cria_lock_fora_do_formato(tmp_path: Path) -> None:
    world = make_login_world(tmp_path)
    spec = replace(world.lab.spec, run_id="../fora")
    outcome = lifecycle.janitor(spec, janitor_ports(world.lab))
    assert outcome.status == "refused"
    assert not (tmp_path / "fora.lock").exists()
    assert not any(p.name.endswith(".lock") for p in tmp_path.rglob("*fora*"))
