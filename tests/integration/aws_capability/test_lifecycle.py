"""Contrato comportamental de entrada, pré-condições e execução efêmera."""

import importlib
import os
import sys
from dataclasses import replace
from pathlib import Path

import pytest

from tests.integration.aws_capability import lifecycle
from tests.integration.aws_capability._oracle import (
    assert_complete,
    assert_no_mutation,
    assert_probe_order,
)
from tests.integration.aws_capability._testing import OTHER_ACCOUNT, SERVICES, Lab
from tests.integration.aws_capability.contracts import Creation, Ownership


def test_importacao_normal_nao_cria_journal_nem_inicia_aws(lab: Lab) -> None:
    importlib.reload(lifecycle)
    assert not lab.spec.journal_dir.exists()
    assert lab.events == []


@pytest.mark.parametrize(
    ("changes", "reason"),
    [
        ({"account": OTHER_ACCOUNT}, "account_not_authorized"),
        ({"region": "eu-west-1"}, "region_not_authorized"),
        ({"run_id": "../outside"}, "invalid_run_id"),
        ({"run_id": "a1b2c3d4-extra"}, "invalid_run_id"),
        ({"run_id": "A1B2C3D4"}, "invalid_run_id"),
        ({"run_id": "zzzzzzzz"}, "invalid_run_id"),
        ({"deadline": 999}, "deadline_expired"),
        ({"deadline": float("inf")}, "invalid_deadline"),
        ({"deadline": float("nan")}, "invalid_deadline"),
        ({"retention": "COMPLIANCE"}, "retention_forbidden"),
        ({"retention": "governance"}, "retention_forbidden"),
        ({"allow_governance_bypass": False}, "cleanup_bypass_required"),
    ],
)
def test_rejeita_entrada_insegura_sem_mutacao(lab: Lab, changes: dict, reason: str) -> None:
    outcome = lifecycle.run(replace(lab.spec, **changes), lab.ports())
    assert outcome.status == "refused"
    assert outcome.reason == reason
    assert_no_mutation(lab.events)


def test_identidade_estrangeira_nao_cria_recursos(lab: Lab) -> None:
    lab.remote.identity_account = OTHER_ACCOUNT
    outcome = lifecycle.run(lab.spec, lab.ports())
    assert outcome.reason == "identity_mismatch"
    assert ("identity",) in lab.events
    assert_no_mutation(lab.events)


@pytest.mark.parametrize("failure", ["spawn", "dead"])
def test_janitor_indisponivel_impede_criacao(lab: Lab, failure: str) -> None:
    lab.processes.startup_error = failure == "spawn"
    lab.processes.available = failure != "dead"
    outcome = lifecycle.run(lab.spec, lab.ports())
    assert outcome.status == "refused"
    assert outcome.reason == "supervisor_unavailable"
    assert_no_mutation(lab.events)


def test_run_valido_cria_testa_limpa_e_prova_inventario_final(lab: Lab) -> None:
    outcome = lifecycle.run(lab.spec, lab.ports())
    assert_complete(outcome, lab.events)
    assert lab.remote.resources == {}
    assert {event[1] for event in lab.events if event[0] == "create"} == set(SERVICES)
    assert_probe_order(lab.events, lab.spec)
    assert len([event for event in lab.events if event[0] == "delete"]) == 4


def test_criacao_recebe_nome_tags_e_expiracao_derivados_do_run_validado(lab: Lab) -> None:
    lifecycle.run(lab.spec, lab.ports())
    creations = [event[2] for event in lab.events if event[0] == "create"]
    assert len(creations) == 4
    for creation in creations:
        assert isinstance(creation, Creation)
        assert creation.name == "cnesdata-cap-a1b2c3d4"
        assert creation.tags == {
            "Project": "CnesData",
            "Purpose": "aws-capability",
            "Run": "a1b2c3d4",
            "ExpiresAt": "1060",
        }
        assert creation.spec.retention == "GOVERNANCE"


def test_segundo_run_valido_deriva_nome_tags_journal_e_deadline_proprios(lab: Lab) -> None:
    lab.spec = replace(lab.spec, run_id="deafbeef", deadline=1130)
    lab.remote.reply_name = "cnesdata-cap-deafbeef"
    tags = {
        "Project": "CnesData",
        "Purpose": "aws-capability",
        "Run": "deafbeef",
        "ExpiresAt": "1130",
    }
    lab.remote.reply_owners = {
        "cognito": Ownership("111111111111", tags, "us-east-2"),
        "dynamodb": Ownership("111111111111", tags, "us-east-2"),
        "s3": Ownership("111111111111", tags, "us-east-2"),
        "iam": Ownership("111111111111", tags, "aws-global"),
    }
    lab.events.expected_name = "cnesdata-cap-deafbeef"
    assert lab.processes.ack is not None
    lab.processes.ack.update(run_id="deafbeef", journal=str(lab.spec.journal_dir / "deafbeef.json"))
    outcome = lifecycle.run(lab.spec, lab.ports())
    assert_complete(outcome, lab.events)
    assert_probe_order(lab.events, lab.spec)
    creations = [event[2] for event in lab.events if event[0] == "create"]
    assert len(creations) == 4
    for creation in creations:
        assert isinstance(creation, Creation)
        assert creation.name == "cnesdata-cap-deafbeef"
        assert creation.tags == tags
        assert creation.spec.deadline == 1130
    assert (lab.spec.journal_dir / "deafbeef.json").exists()
    assert not (lab.spec.journal_dir / "a1b2c3d4.json").exists()


def test_identidade_inventario_e_supervisor_precedem_primeira_criacao(lab: Lab) -> None:
    lifecycle.run(lab.spec, lab.ports())
    kinds = [event[0] for event in lab.events]
    assert "create" in kinds
    initial = lab.events[: kinds.index("create")]
    assert initial[0] == ("identity",)
    assert {event[1] for event in initial if event[0] == "inventory"} == set(SERVICES)
    assert ("alive", 9876) in initial
    start = next(event for event in initial if event[0] == "start")
    argv, options = start[1:]
    assert isinstance(argv, tuple)
    assert isinstance(options, dict)
    assert argv[:3] == (sys.executable, "-m", "tests.integration.aws_capability.lifecycle")
    assert "monitor" in argv
    assert "janitor" not in argv
    assert "--deadline" in argv
    assert "1060" in argv or "1060.0" in argv
    assert "--run-id" in argv
    assert "a1b2c3d4" in argv
    assert options.get("start_new_session") is True
    assert options.get("close_fds") is True
    assert options.get("shell", False) is False
    expected = {
        "--account": "111111111111",
        "--region": "us-east-2",
        "--run-id": "a1b2c3d4",
        "--journal-dir": str(lab.spec.journal_dir),
        "--worker-pid": str(os.getpid()),
        "--retention": "GOVERNANCE",
    }
    for flag, value in expected.items():
        assert argv[argv.index(flag) + 1] == value
    assert "--allow-governance-bypass" in argv
    assert any(event[0] == "receive" for event in initial)


def test_journal_existente_nao_e_sobrescrito_por_novo_run(lab: Lab) -> None:
    lab.spec.journal_dir.mkdir(mode=0o700)
    lab.journal().write_text("preservar-evidencia")
    outcome = lifecycle.run(lab.spec, lab.ports())
    assert outcome.reason == "journal_exists"
    assert lab.journal().read_text() == "preservar-evidencia"
    assert_no_mutation(lab.events)


def test_journal_symlink_nao_escreve_fora_do_diretorio(lab: Lab, tmp_path: Path) -> None:
    target = tmp_path / "outside.json"
    target.write_text("intocado")
    lab.spec.journal_dir.mkdir(mode=0o700)
    lab.journal().symlink_to(target)
    outcome = lifecycle.run(lab.spec, lab.ports())
    assert outcome.status == "refused"
    assert target.read_text() == "intocado"
    assert_no_mutation(lab.events)


def test_cli_exige_opt_in_e_campos_explicitos(lab: Lab) -> None:
    outcome = lifecycle.main([], lab.ports())
    assert outcome.status == "refused"
    assert outcome.reason == "explicit_command_required"
    assert_no_mutation(lab.events)


def test_cli_run_atravessa_orquestrador_real(lab: Lab) -> None:
    args = [
        "run",
        "--account",
        "111111111111",
        "--region",
        "us-east-2",
        "--run-id",
        "a1b2c3d4",
        "--deadline",
        "1060",
        "--journal-dir",
        str(lab.spec.journal_dir),
        "--retention",
        "GOVERNANCE",
        "--allow-governance-bypass",
    ]
    outcome = lifecycle.main(args, lab.ports())
    assert_complete(outcome, lab.events)
    assert any(event[0] == "create" for event in lab.events)


@pytest.mark.parametrize("missing", ["--account", "--region", "--run-id", "--deadline"])
def test_cli_nao_infere_identidade_regiao_ou_prazo(lab: Lab, missing: str) -> None:
    values = {"--account": "111111111111", "--region": "us-east-2"}
    values.update({"--run-id": "a1b2c3d4", "--deadline": "1060"})
    args = ["run"] + [
        item for key, value in values.items() if key != missing for item in (key, value)
    ]
    outcome = lifecycle.main(args, lab.ports())
    assert outcome.status == "refused"
    assert outcome.reason == "missing_argument"
    assert_no_mutation(lab.events)
