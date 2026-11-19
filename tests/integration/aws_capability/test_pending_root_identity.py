"""Raiz pendente: o nome exato vai ao journal antes do create e só ele confirma a criação."""

import json
from pathlib import Path
from typing import cast

import pytest

from tests.integration.aws_capability import lifecycle
from tests.integration.aws_capability._aws_fake_types_testing import FakeRole, good
from tests.integration.aws_capability._b1_testing import NAME, TRUST, TRUST2
from tests.integration.aws_capability._cleanup import Cleanup
from tests.integration.aws_capability._journal import Journal, prepare_directory
from tests.integration.aws_capability._probe_testing import (
    ROLE,
    ROLE2,
    ProbeLab,
    janitor_ports,
    make_probe_lab,
)
from tests.integration.aws_capability._safety import SafetyFailure, tags

JOURNAL = "a1b2c3d4.json"


def seed_crashed_r2(lab: ProbeLab, *, pending_name: str | None) -> None:
    prepare_directory(lab.spec.journal_dir)
    lab.aws.roles[NAME] = FakeRole(tags(lab.spec), TRUST)
    lab.aws.roles[ROLE2.name] = FakeRole(tags(lab.spec), TRUST2)
    journal = Journal(lab.spec)
    journal.created = [ROLE]
    journal.phase = "creating"
    journal.pending_service = "iam"
    journal.pending_name = pending_name
    journal.operation_state = "unknown"
    journal.save()


def hide_r2_from_listing(lab: ProbeLab, passes: int | None) -> None:
    original = lab.aws._iam_list_roles
    served = [0]

    def listing(payload: dict, who: str):
        reply = original(payload, who)
        served[0] += 1
        if passes is not None and served[0] > passes:
            return reply
        rows = cast("list[dict]", reply.data["Roles"])
        return good({"Roles": [row for row in rows if row["RoleName"] != ROLE2.name]})

    lab.aws._iam_list_roles = listing  # type: ignore[method-assign]


def saved(lab: ProbeLab) -> dict:
    return json.loads(lab.spec.journal_dir.joinpath(JOURNAL).read_bytes())


def test_listagem_sem_r2_nao_confirma_a_criacao_pendente_com_a_raiz_anterior(
    tmp_path: Path,
) -> None:
    lab = make_probe_lab(tmp_path / "private", "iam-trust", seeded=False)
    seed_crashed_r2(lab, pending_name=ROLE2.name)
    hide_r2_from_listing(lab, passes=None)
    outcome = lifecycle.janitor(lab.spec, janitor_ports(lab))
    assert (outcome.status, outcome.reason) == ("incomplete", "creation_uncertain")
    assert sorted(lab.aws.roles) == [ROLE2.name]
    journal = saved(lab)
    assert (journal["pending_service"], journal["pending_name"]) == ("iam", ROLE2.name)
    assert journal["operation_state"] == "unknown"


def test_r2_visivel_na_segunda_passada_resolve_a_pendencia_e_remove_as_duas_raizes(
    tmp_path: Path,
) -> None:
    lab = make_probe_lab(tmp_path / "private", "iam-trust", seeded=False)
    seed_crashed_r2(lab, pending_name=ROLE2.name)
    hide_r2_from_listing(lab, passes=1)
    outcome = lifecycle.janitor(lab.spec, janitor_ports(lab))
    assert (outcome.status, outcome.reason) == ("succeeded", "complete")
    assert lab.aws.roles == {}
    deleted = [str(r.payload["RoleName"]) for r in lab.aws.requests if r.operation == "delete-role"]
    assert sorted(deleted) == sorted([NAME, ROLE2.name])
    assert saved(lab)["created"] == []


def test_r2_visivel_resolve_a_pendencia_para_r2_e_registra_no_journal(tmp_path: Path) -> None:
    lab = make_probe_lab(tmp_path / "private", "iam-trust", seeded=False)
    seed_crashed_r2(lab, pending_name=ROLE2.name)
    journal = Journal(lab.spec)
    journal.load()
    cleanup = Cleanup(journal, janitor_ports(lab))
    cleanup.confirm_pending(cleanup.inventory())
    assert journal.created == [ROLE, ROLE2]
    assert (journal.pending_service, journal.pending_name) == (None, None)
    assert journal.operation_state == "confirmed"


def test_worker_grava_o_nome_da_raiz_antes_do_create_de_r2(tmp_path: Path) -> None:
    lab = make_probe_lab(tmp_path / "private", "iam-trust", seeded=False)
    original = lab.aws._iam_create_role
    seen: list[dict] = []

    def interrupting(payload: dict, who: str):
        if payload["RoleName"] == ROLE2.name:
            seen.append(saved(lab))
            raise KeyboardInterrupt
        return original(payload, who)

    lab.aws._iam_create_role = interrupting  # type: ignore[method-assign]
    outcome = lifecycle.run(lab.spec, janitor_ports(lab, supervised=True))
    assert (outcome.status, outcome.reason) == ("incomplete", "creation_uncertain")
    assert seen[0]["pending_service"] == "iam"
    assert seen[0]["pending_name"] == ROLE2.name
    assert [row["name"] for row in seen[0]["created"]] == [NAME]


def test_worker_grava_o_nome_da_primeira_raiz_e_limpa_ao_confirmar(tmp_path: Path) -> None:
    lab = make_probe_lab(tmp_path / "private", "iam-trust", seeded=False)
    original = lab.aws._iam_create_role
    seen: list[dict] = []

    def recording(payload: dict, who: str):
        seen.append(saved(lab))
        return original(payload, who)

    lab.aws._iam_create_role = recording  # type: ignore[method-assign]
    lifecycle.run(lab.spec, janitor_ports(lab, supervised=True))
    assert [row["pending_name"] for row in seen] == [NAME, ROLE2.name]
    assert "pending_name" not in saved(lab)


@pytest.mark.parametrize("pending_name", [f"{NAME}-zz", NAME + "x", "outro", "", None], ids=repr)
def test_journal_rejeita_nome_pendente_fora_das_raizes_planejadas_ou_ausente(
    tmp_path: Path, pending_name: str | None
) -> None:
    lab = make_probe_lab(tmp_path / "private", "iam-trust", seeded=False)
    seed_crashed_r2(lab, pending_name=pending_name)
    with pytest.raises(SafetyFailure) as error:
        Journal(lab.spec).load()
    assert error.value.reason == "journal_corrupt"


def test_journal_de_raiz_unica_sem_nome_pendente_continua_carregando(tmp_path: Path) -> None:
    lab = make_probe_lab(tmp_path / "private", "s3-hold", seeded=False)
    prepare_directory(lab.spec.journal_dir)
    journal = Journal(lab.spec)
    journal.phase = "creating"
    journal.pending_service = "s3"
    journal.operation_state = "unknown"
    journal.save()
    assert "pending_name" not in saved(lab)
    loaded = Journal(lab.spec)
    loaded.load()
    assert (loaded.pending_service, loaded.pending_name) == ("s3", None)
