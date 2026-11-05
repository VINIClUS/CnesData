"""Ciclo completo do fargate-write sobre a fronteira com estado, com orçamento medido."""

import json
from pathlib import Path

import pytest

from tests.integration.aws_capability import lifecycle
from tests.integration.aws_capability._aws_fake_fargate_testing import ACCOUNT
from tests.integration.aws_capability._aws_fake_types_testing import good
from tests.integration.aws_capability._fargate_testing import (
    FargateLab,
    empty_state,
    first,
    last,
    make_fargate_lab,
    operations,
)
from tests.integration.aws_capability._probe_testing import (
    cleanup_calls,
    cleanup_seconds,
    janitor_ports,
    work_calls,
    work_seconds,
)
from tests.integration.aws_capability._production import render
from tests.integration.aws_capability._profiles import PROFILES
from tests.integration.aws_capability._safety import FARGATE_SERVICES
from tests.integration.aws_capability.contracts import Outcome

PROFILE = PROFILES["fargate-write"]
ZERO = dict.fromkeys(FARGATE_SERVICES, 0)
MEASURED_PUSH, MEASURED_PROBE, MEASURED_RECOVERY, MEASURED_CYCLE = 88, 52, 73, 288
AVERAGE_TASK = (MEASURED_PROBE + MEASURED_RECOVERY) / 2
SCENARIOS = {
    "tipico": {
        "push": float(MEASURED_PUSH),
        "state": {"task_seconds": AVERAGE_TASK - 6, "provision_seconds": 6.0},
    },
    "lento": {
        "push": 1.2 * MEASURED_PUSH,
        "state": {
            "task_seconds": 1.1 * AVERAGE_TASK, "provision_seconds": 8.0, "log_lag": 12.0,
            "stop_seconds": 10.0, "sg_violations": 8,
        },
    },
    "pior_caso_limitado": {
        "push": 1.4 * MEASURED_PUSH,
        "state": {
            "task_seconds": 1.2 * AVERAGE_TASK, "provision_seconds": 10.0, "log_lag": 20.0,
            "stop_seconds": 12.0, "sg_violations": 12, "log_page_size": 5,
            "recovery_denials": 7,
        },
    },
}
CREATES = [
    "ecr.create-repository", "logs.create-log-group", "iam.create-role", "iam.create-role",
    "ec2.create-security-group", "ecs.create-cluster",
]


def cycle(tmp_path: Path, scenario: str = "tipico", **state: object) -> FargateLab:
    lab = make_fargate_lab(tmp_path / "private", seeded=False)
    spec = SCENARIOS[scenario]
    lab.images.seconds = spec["push"]  # type: ignore[assignment]
    for key, value in {**spec["state"], **state}.items():  # type: ignore[arg-type]
        setattr(lab.state, key, value)
    return lab


def execute(lab: FargateLab) -> Outcome:
    return lifecycle.run(lab.spec, janitor_ports(lab, supervised=True))


def probe_of(outcome: Outcome) -> dict:
    value = outcome.evidence["probe"]
    assert isinstance(value, dict)
    return value


@pytest.mark.parametrize("writable", [False, True], ids=["vermelho", "verde"])
def test_run_completo_termina_succeeded_com_observacao_e_inventario_vazio(
    tmp_path: Path, writable: bool
) -> None:
    lab = cycle(tmp_path, writable=writable)
    outcome = execute(lab)
    assert (outcome.status, outcome.reason) == ("succeeded", "complete")
    assert probe_of(outcome)["all_writable"] is writable
    assert set(empty_state(lab).values()) == {0}
    assert outcome.evidence["counts"] == ZERO
    assert outcome.evidence["final_counts_all_cap"] == ZERO
    assert outcome.remaining == ()


def test_vermelho_nao_e_falha_do_perfil_e_o_journal_termina_completo_e_vazio(
    tmp_path: Path,
) -> None:
    lab = cycle(tmp_path, writable=False)
    outcome = execute(lab)
    journal = json.loads(lab.spec.journal_dir.joinpath("a1b2c3d4.json").read_bytes())
    assert outcome.status == "succeeded"
    assert probe_of(outcome)["tmp_write"] == "EACCES"
    assert (journal["phase"], journal["created"]) == ("complete", [])


def _read_services(ops: list[str]) -> set[str]:
    return {op.split(".")[0] for op in ops if op.split(".")[1].startswith(("list-", "describe-"))}


def test_baseline_e_inventario_final_leem_os_oito_servicos_do_perfil(tmp_path: Path) -> None:
    lab = cycle(tmp_path)
    assert execute(lab).status == "succeeded"
    wanted = {"ecr", "ecs", "logs", "ec2", "iam", "cognito-idp", "dynamodb", "s3api"}
    work = operations(lab, "work")
    assert wanted <= _read_services(work[: work.index("ecr.create-repository")])
    cleanup = operations(lab, "cleanup")
    final = cleanup[len(cleanup) - 1 - cleanup[::-1].index("ec2.delete-security-group") :]
    assert wanted <= _read_services(final)


def test_cria_as_seis_raizes_na_ordem_do_perfil(tmp_path: Path) -> None:
    lab = cycle(tmp_path)
    execute(lab)
    created = [op for op in operations(lab, "work") if op in set(CREATES)]
    assert created == CREATES


def test_journal_registra_a_intencao_antes_de_cada_raiz(tmp_path: Path) -> None:
    lab = cycle(tmp_path)
    seen: list[dict] = []
    names = (
        "_ecr_create_repository", "_logs_create_log_group", "_iam_create_role",
        "_ec2_create_security_group", "_ecs_create_cluster",
    )
    for name in names:
        original = getattr(lab.aws, name)

        def recording(payload: dict, who: str, original=original):
            row = json.loads(lab.spec.journal_dir.joinpath("a1b2c3d4.json").read_bytes())
            seen.append(row)
            return original(payload, who)

        setattr(lab.aws, name, recording)
    execute(lab)
    assert [(r["pending_service"], r["operation_state"]) for r in seen] == [
        ("ecr", "unknown"), ("logs", "unknown"), ("iam", "unknown"), ("iam", "unknown"),
        ("ec2", "unknown"), ("ecs", "unknown"),
    ]
    assert [len(r["created"]) for r in seen] == [0, 1, 2, 3, 4, 5]
    assert [r.get("pending_name") for r in seen][2:4] == [
        "cnesdata-cap-a1b2c3d4-exec", "cnesdata-cap-a1b2c3d4-task",
    ]


def test_cleanup_para_task_antes_do_cluster_e_so_solta_o_security_group_depois(
    tmp_path: Path,
) -> None:
    lab = cycle(tmp_path)
    outcome = execute(lab)
    assert outcome.status == "succeeded"
    cluster = first(lab, "ecs.delete-cluster", "cleanup")
    assert last(lab, "ecs.delete-task-definitions", "cleanup") < cluster
    assert cluster < last(lab, "ec2.delete-security-group", "cleanup")
    for later in ("logs.delete-log-group", "ecr.delete-repository", "iam.delete-role"):
        assert cluster < first(lab, later, "cleanup")


def test_prova_final_le_revisoes_do_run_ativas_e_inativas_e_tolera_nomes_de_familia_inertes(
    tmp_path: Path,
) -> None:
    lab = cycle(tmp_path)
    outcome = execute(lab)
    assert outcome.status == "succeeded"
    deleted = [r for r in lab.state.revisions if r.status == "DELETE_IN_PROGRESS"]
    assert len(deleted) == 2
    inert = lab.aws._ecs_list_task_definition_families(
        {"familyPrefix": "cnesdata-cap-a1b2c3d4", "status": "INACTIVE"}, "operator"
    )
    families = inert.data["families"]
    assert isinstance(families, list)
    assert len(families) == 2
    cleanup = operations(lab, "cleanup")
    last_delete = max(i for i, op in enumerate(cleanup) if op == "ecs.delete-task-definitions")
    cleanup_requests = [
        r for r, lane in zip(lab.aws.requests, lab.aws.lanes, strict=True) if lane == "cleanup"
    ]
    final = [
        dict(r.payload) for r in cleanup_requests[last_delete + 1 :]
        if (r.service, r.operation) == ("ecs", "list-task-definitions")
    ]
    assert {p["status"] for p in final} >= {"ACTIVE", "INACTIVE"}
    assert all(str(p["familyPrefix"]).startswith("cnesdata-cap-a1b2c3d4") for p in final)
    assert outcome.evidence["task_definition_revisions"] == {"ACTIVE": 0, "INACTIVE": 0}


def test_revisao_inativa_que_nunca_foi_apagada_nao_conta_como_vazio(tmp_path: Path) -> None:
    lab = cycle(tmp_path)
    lab.aws._ecs_delete_task_definitions = (  # type: ignore[method-assign]
        lambda payload, who: good({"taskDefinitions": [], "failures": []})
    )
    outcome = execute(lab)
    assert outcome.status == "incomplete"
    assert any(r.status == "INACTIVE" for r in lab.state.revisions)


def test_task_rodando_quando_o_probe_desiste_e_parada_antes_de_apagar_o_cluster(
    tmp_path: Path,
) -> None:
    lab = cycle(tmp_path, task_seconds=10.0**6)
    outcome = execute(lab)
    assert (outcome.status, outcome.reason) == ("failed", "operation_failed")
    assert probe_of(outcome) == {}
    assert set(empty_state(lab).values()) == {0}
    assert last(lab, "ecs.stop-task") < first(lab, "ecs.delete-cluster")
    assert cleanup_seconds(lab) * 1.2 <= PROFILE.limits.cleanup_seconds


def test_dependency_violation_do_security_group_e_repetido_ate_a_eni_sair(
    tmp_path: Path,
) -> None:
    lab = cycle(tmp_path, sg_violations=9)
    outcome = execute(lab)
    assert outcome.status == "succeeded"
    assert operations(lab).count("ec2.delete-security-group") >= 10
    assert empty_state(lab)["groups"] == 0


def test_estouro_do_limite_de_espera_pela_eni_vira_incomplete_com_o_sg_remanescente(
    tmp_path: Path,
) -> None:
    lab = cycle(tmp_path, sg_violations=10**6)
    outcome = execute(lab)
    assert outcome.status == "incomplete"
    assert outcome.status != "succeeded"
    assert outcome.remaining is not None
    assert [r.service for r in outcome.remaining] == ["ec2"]
    assert empty_state(lab)["groups"] == 1
    assert {k: v for k, v in empty_state(lab).items() if k != "groups"} == dict.fromkeys(
        ("repos", "log_groups", "clusters", "revisions", "roles"), 0
    )


def test_revogacao_negada_do_egress_aborta_antes_de_rodar_tasks_e_a_limpeza_conclui(
    tmp_path: Path,
) -> None:
    lab = cycle(tmp_path, revoke_error="UnauthorizedOperation")
    outcome = execute(lab)
    assert (outcome.status, outcome.reason) == ("failed", "operation_failed")
    assert "ecs.run-task" not in operations(lab)
    assert set(empty_state(lab).values()) == {0}


@pytest.mark.parametrize("scenario", list(SCENARIOS))
@pytest.mark.parametrize("writable", [False, True], ids=["vermelho", "verde"])
def test_cada_cenario_cabe_nos_limites_e_na_janela_com_margem_de_20_por_cento(
    tmp_path: Path, scenario: str, writable: bool
) -> None:
    lab = cycle(tmp_path, scenario, writable=writable)
    outcome = execute(lab)
    limits = PROFILE.limits
    assert (outcome.status, outcome.reason) == ("succeeded", "complete")
    assert work_calls(lab) <= limits.calls - limits.cleanup_reserve
    assert cleanup_calls(lab) <= limits.cleanup_reserve
    assert work_seconds(lab) * 1.2 <= PROFILE.window
    assert cleanup_seconds(lab) * 1.2 <= limits.cleanup_seconds


def test_limites_ficam_abaixo_de_tetos_absolutos_derivados_dos_ensaios_reais(
    tmp_path: Path,
) -> None:
    limits = PROFILE.limits
    assert PROFILE.window <= 2 * MEASURED_CYCLE
    assert limits.cleanup_seconds <= MEASURED_CYCLE + 60
    assert limits.calls <= 2 * MEASURED_CYCLE / 1.3
    assert limits.cleanup_reserve <= limits.calls / 2


def test_evidencia_do_ciclo_nao_carrega_conta_arn_nem_ids(tmp_path: Path) -> None:
    lab = cycle(tmp_path)
    outcome = execute(lab)
    text = render(outcome) + repr(outcome)
    assert outcome.status == "succeeded"
    assert ACCOUNT not in text
    assert "arn:" not in text
    for canary in ("sg-0", "subnet-0", "vpc-0", "STOPPED_REASON_CANARY", "LOG_LINE_CANARY"):
        assert canary not in text
    assert "dkr.ecr" not in text
