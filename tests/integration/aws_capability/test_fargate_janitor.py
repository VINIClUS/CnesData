"""fargate-write: janitor após queda, posse estrita, raiz recusada e segredos fora das saídas."""

import json
from pathlib import Path

from tests.integration.aws_capability import lifecycle
from tests.integration.aws_capability._aws_fake_fargate_testing import (
    ACCOUNT,
    TOKEN_CANARY,
    FakeRevision,
    FakeTask,
)
from tests.integration.aws_capability._fargate_testing import (
    CLUSTER,
    ECR,
    EXEC,
    GROUP,
    LOGS,
    OTHER_RUN,
    SG_ID,
    FargateLab,
    empty_state,
    make_fargate_lab,
    operations,
    seed_fargate,
    seed_foreign,
)
from tests.integration.aws_capability._journal import Journal, prepare_directory
from tests.integration.aws_capability._probe_testing import janitor_ports, payload_text
from tests.integration.aws_capability._production import render
from tests.integration.aws_capability._safety import FARGATE_SERVICES

NAME = "cnesdata-cap-a1b2c3d4"
FAR_FUTURE = 10.0**9


def crashed_lab(tmp_path: Path) -> FargateLab:
    lab = make_fargate_lab(tmp_path / "private", seeded=False, image=None)
    seed_fargate(lab.aws, lab.spec)
    seed_foreign(lab.aws, lab.spec)
    state = lab.state
    state.revisions += [
        FakeRevision(f"{NAME}-probe", 1, {}), FakeRevision(f"{NAME}-recovery", 1, {}),
        FakeRevision(f"{OTHER_RUN}-probe", 1, {}),
        FakeRevision("cnesdata-prod-processor-recovery", 1, {}),
    ]
    task = FakeTask(
        f"arn:aws:ecs:us-east-2:{ACCOUNT}:task/{NAME}/t1", NAME, SG_ID, 0.0, FAR_FUTURE,
        True, 0, None,
    )
    state.tasks[task.arn] = task
    return lab


def write_journal(
    lab: FargateLab, created: list, pending: tuple[str, str] | None = None
) -> None:
    prepare_directory(lab.spec.journal_dir)
    journal = Journal(lab.spec)
    journal.created = created
    journal.phase = "creating"
    if pending is not None:
        journal.pending_service, journal.pending_name = pending
        journal.operation_state = "unknown"
    journal.save()


def foreign_intact(lab: FargateLab) -> bool:
    state = lab.state
    revisions = {r.family: r.status for r in state.revisions}
    return (
        OTHER_RUN in state.repos
        and OTHER_RUN in state.log_groups
        and state.clusters[OTHER_RUN].status == "ACTIVE"
        and "sg-0foreign0000001" in state.groups
        and f"{OTHER_RUN}-exec" in lab.aws.roles
        and revisions[f"{OTHER_RUN}-probe"] == "ACTIVE"
        and revisions["cnesdata-prod-processor-recovery"] == "ACTIVE"
    )


def own_gone(lab: FargateLab) -> bool:
    state = lab.state
    return (
        NAME not in state.repos
        and NAME not in state.log_groups
        and state.clusters[NAME].status == "INACTIVE"
        and SG_ID not in state.groups
        and not any(name.startswith(NAME) for name in lab.aws.roles)
        and all(r.status == "DELETE_IN_PROGRESS" for r in state.revisions
                if r.family.startswith(NAME))
    )


def test_janitor_apos_queda_com_task_rodando_remove_so_o_que_e_do_run_sem_a_imagem(
    tmp_path: Path,
) -> None:
    lab = crashed_lab(tmp_path)
    write_journal(lab, [ECR, LOGS, EXEC], pending=("iam", f"{NAME}-task"))
    outcome = lifecycle.janitor(lab.spec, janitor_ports(lab))
    assert (outcome.status, outcome.reason) == ("succeeded", "complete")
    assert outcome.evidence["task_definition_revisions"] == {"ACTIVE": 0, "INACTIVE": 0}
    assert own_gone(lab)
    assert foreign_intact(lab)
    assert lab.state.tasks[f"arn:aws:ecs:us-east-2:{ACCOUNT}:task/{NAME}/t1"].stopped_by_request
    text = " ".join(
        str(r.payload) for r in lab.aws.requests
        if r.operation.startswith(("delete", "deregister", "stop"))
    )
    assert OTHER_RUN not in text
    assert "cnesdata-prod" not in text


def test_janitor_com_journal_minimo_remove_recursos_achados_so_pelo_nome_do_run(
    tmp_path: Path,
) -> None:
    """Vazio vale para o run: final_counts_all_cap conta também recursos de outros runs."""
    lab = crashed_lab(tmp_path)
    write_journal(lab, [])
    outcome = lifecycle.janitor(lab.spec, janitor_ports(lab))
    assert (outcome.status, outcome.reason) == ("succeeded", "complete")
    assert outcome.evidence["task_definition_revisions"] == {"ACTIVE": 0, "INACTIVE": 0}
    assert own_gone(lab)
    assert foreign_intact(lab)
    assert outcome.evidence["counts"] == dict.fromkeys(FARGATE_SERVICES, 0)
    assert outcome.evidence["final_counts_all_cap"] == {
        **dict.fromkeys(FARGATE_SERVICES, 0),
        "ecr": 1, "ecs": 1, "logs": 1, "ec2": 1, "iam": 1,
    }


def test_janitor_acha_e_remove_recurso_do_run_que_esta_em_pagina_posterior(
    tmp_path: Path,
) -> None:
    lab = crashed_lab(tmp_path)
    for index in range(3):
        seed_foreign(lab.aws, lab.spec, f"cnesdata-cap-0000000{index}")
        lab.state.groups[f"sg-0crowd{index:011d}"] = lab.state.groups["sg-0foreign0000001"]
    lab.state.page_size = 1
    write_journal(lab, [])
    outcome = lifecycle.janitor(lab.spec, janitor_ports(lab))
    assert (outcome.status, outcome.reason) == ("succeeded", "complete")
    assert own_gone(lab)
    assert foreign_intact(lab)


def test_janitor_nao_apaga_recurso_com_o_nome_do_run_e_tag_divergente_e_reporta(
    tmp_path: Path,
) -> None:
    lab = crashed_lab(tmp_path)
    lab.state.log_groups[NAME].tags["Purpose"] = "other"
    write_journal(lab, [ECR, LOGS])
    outcome = lifecycle.janitor(lab.spec, janitor_ports(lab))
    assert (outcome.status, outcome.reason) == ("incomplete", "ownership_mismatch")
    assert NAME in lab.state.log_groups
    assert "logs.delete-log-group" not in operations(lab)
    assert NAME not in lab.state.repos
    assert SG_ID not in lab.state.groups
    assert foreign_intact(lab)
    assert outcome.remaining is not None
    assert [r.service for r in outcome.remaining] == ["logs"]


def test_janitor_com_sg_travado_pela_eni_termina_incomplete_e_preserva_o_resto_removido(
    tmp_path: Path,
) -> None:
    lab = crashed_lab(tmp_path)
    lab.state.sg_violations = 10**6
    write_journal(lab, [ECR, LOGS, EXEC, GROUP, CLUSTER])
    outcome = lifecycle.janitor(lab.spec, janitor_ports(lab))
    assert outcome.status == "incomplete"
    assert SG_ID in lab.state.groups
    assert NAME not in lab.state.repos
    assert foreign_intact(lab)


def test_janitor_recarrega_journal_com_servicos_fargate_via_save(tmp_path: Path) -> None:
    lab = crashed_lab(tmp_path)
    write_journal(lab, [ECR, LOGS, GROUP, CLUSTER])
    reloaded = Journal(lab.spec)
    reloaded.load()
    assert reloaded.created == [ECR, LOGS, GROUP, CLUSTER]


def test_run_com_chamador_root_para_na_identidade_sem_criar_nem_ler_nada(
    tmp_path: Path,
) -> None:
    good = make_fargate_lab(tmp_path / "ok", seeded=False)
    lifecycle.run(good.spec, janitor_ports(good, supervised=True))
    assert "ecr.create-repository" in operations(good)
    root = make_fargate_lab(
        tmp_path / "root", seeded=False, caller_arn=f"arn:aws:iam::{ACCOUNT}:root"
    )
    outcome = lifecycle.run(root.spec, janitor_ports(root, supervised=True))
    assert (outcome.status, outcome.reason) == ("refused", "operator_mismatch")
    assert root.aws.calls == [("sts", "get-caller-identity", "operator")]
    assert empty_state(root) == dict.fromkeys(empty_state(root), 0)


def test_token_do_ecr_nao_aparece_em_journal_saida_repr_nem_requests(tmp_path: Path) -> None:
    lab = make_fargate_lab(tmp_path / "private", seeded=False)
    outcome = lifecycle.run(lab.spec, janitor_ports(lab, supervised=True))
    journal = lab.spec.journal_dir.joinpath("a1b2c3d4.json").read_text()
    assert [login.token for login in lab.images.logins] == [TOKEN_CANARY]
    haystack = journal + render(outcome) + repr(outcome) + payload_text(lab)
    haystack += repr(lab.images) + repr(lab.services.ports) + repr(lab.services)
    assert TOKEN_CANARY not in haystack
    assert json.loads(journal)["phase"] == "complete"
