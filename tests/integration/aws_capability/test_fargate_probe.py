"""Probe fargate-write: escrita do user=app nos mounts e entrypoint real, com logs fechados."""

# ruff: noqa: S108
import hashlib
import json
import re
from dataclasses import replace
from pathlib import Path

import pytest

from tests.integration.aws_capability._aws_fake_ecs_testing import probe_line
from tests.integration.aws_capability._aws_fake_fargate_testing import (
    ACCOUNT,
    LOG_CANARY,
    STOPPED_CANARY,
    SUBNETS,
    TOKEN_CANARY,
    repo_arn,
    repo_uri,
)
from tests.integration.aws_capability._b1_testing import NAME
from tests.integration.aws_capability._fargate_mirror import TaskSettings, task_definition_payload
from tests.integration.aws_capability._fargate_testing import (
    EXEC,
    IMAGE,
    SG_ID,
    TASK,
    FargateLab,
    make_fargate_lab,
    operations,
    requests_of,
)
from tests.integration.aws_capability._probe_testing import payload_text
from tests.integration.aws_capability.contracts import RequestContext, Resource, ServiceFailure

DIGEST = "sha256:" + hashlib.sha256(IMAGE.encode()).hexdigest()
KEYS = {
    "image_digest", "platform_version", "user", "uid", "root_write", "tempfile", "all_writable",
    "tmp_uid", "tmp_gid", "tmp_mode", "tmp_write", "logs_uid", "logs_gid", "logs_mode",
    "logs_write", "data_uid", "data_gid", "data_mode", "data_write", "write_exit_code",
    "recovery_exit_code", "recovery_permission_denials", "network_ingress_rules",
    "network_egress_rules", "network_subnets",
}
COMMON = {
    "image_digest": DIGEST, "platform_version": "1.4.0", "user": "app", "uid": 999,
    "root_write": "EROFS", "write_exit_code": 0, "recovery_exit_code": 1,
    "recovery_permission_denials": 0, "network_ingress_rules": 0, "network_egress_rules": 1,
    "network_subnets": 3,
}
RED = {
    **COMMON, "all_writable": False, "tempfile": "FileNotFoundError",
    **{f"{p}_{k}": v for p in ("tmp", "logs", "data")
       for k, v in (("uid", 0), ("gid", 0), ("mode", "0o755"), ("write", "EACCES"))},
}
GREEN = {
    **COMMON, "all_writable": True, "tempfile": "ok:/tmp",
    "tmp_uid": 0, "tmp_gid": 0, "tmp_mode": "0o1777", "tmp_write": "ok",
    **{f"{p}_{k}": v for p in ("logs", "data")
       for k, v in (("uid", 999), ("gid", 999), ("mode", "0o755"), ("write", "ok"))},
}


@pytest.fixture
def lab(tmp_path: Path) -> FargateLab:
    return make_fargate_lab(tmp_path / "j")


def run(lab: FargateLab) -> dict:
    return dict(lab.services.probe(lab.resources, lab.spec, lab.context))


def fails(lab: FargateLab) -> ServiceFailure:
    with pytest.raises(ServiceFailure) as error:
        run(lab)
    return error.value


def test_imagem_sem_volume_e_observada_como_vermelho_com_todos_os_mounts_de_root(
    lab: FargateLab,
) -> None:
    assert run(lab) == RED


def test_imagem_com_volume_e_observada_como_verde_com_tmp_de_root_1777_gravavel(
    lab: FargateLab,
) -> None:
    lab.state.writable = True
    assert run(lab) == GREEN


@pytest.mark.parametrize("writable", [False, True])
def test_esquema_da_evidencia_e_fechado_em_chaves_e_formatos(
    lab: FargateLab, writable: bool
) -> None:
    lab.state.writable = writable
    observed = run(lab)
    assert set(observed) == KEYS
    assert isinstance(observed["all_writable"], bool)
    assert re.fullmatch(r"sha256:[0-9a-f]{64}", str(observed["image_digest"]))
    for path in ("tmp", "logs", "data"):
        assert re.fullmatch(r"0o[0-7]{3,4}", str(observed[f"{path}_mode"]))
        assert re.fullmatch(r"ok|E[A-Z0-9]+", str(observed[f"{path}_write"]))
        assert type(observed[f"{path}_uid"]) is int
        assert type(observed[f"{path}_gid"]) is int
    assert re.fullmatch(r"ok:/tmp|[A-Za-z]+Error", str(observed["tempfile"]))
    for key in ("write_exit_code", "recovery_exit_code", "recovery_permission_denials"):
        assert type(observed[key]) is int


def test_conta_linhas_com_negacao_de_disco_sem_contar_duas_vezes_a_mesma_linha(
    lab: FargateLab,
) -> None:
    lab.state.recovery_lines = [
        '{"event": "processor_entrypoint_failed", "exception_type": "Unavailable"}',
        "PermissionError: [Errno 13] Permission denied: '/tmp/x'",
        "OSError: [Errno 13] Permission denied",
        "OSError: [Errno 30] Read-only file system: '/data/y'",
        "FileNotFoundError: No usable temporary directory found in ['/tmp']",
        "PermissionError and Errno 13 na mesma linha",
        "linha comum sem relacao com disco",
        f"nota sem erro {LOG_CANARY}",
    ]
    assert run(lab)["recovery_permission_denials"] == 5


def test_le_todas_as_paginas_de_log_antes_de_contar_as_negacoes(lab: FargateLab) -> None:
    lab.state.recovery_denials = 7
    lab.state.log_page_size = 2
    assert run(lab)["recovery_permission_denials"] == 7


def test_saida_do_probe_de_escrita_nao_zero_e_registrada(lab: FargateLab) -> None:
    lab.state.probe_exit = 3
    assert run(lab)["write_exit_code"] == 3


def test_espera_o_atraso_dos_logs_depois_de_stopped(lab: FargateLab) -> None:
    lab.state.log_lag = 25.0
    assert run(lab) == RED


def _line(**changes: object) -> str:
    body = json.loads(probe_line(False).removeprefix("CAPPROBE "))
    for key, value in changes.items():
        if value is None:
            body.pop(key, None)
        else:
            body[key] = value
    return "CAPPROBE " + json.dumps(body)


def _nested(path: str, **changes: object) -> str:
    body = json.loads(probe_line(False).removeprefix("CAPPROBE "))
    body[path].update(changes)
    return "CAPPROBE " + json.dumps(body)


MALFORMED = {
    "sem_linha_capprobe": ["linha qualquer", "outra"],
    "sem_eventos": [],
    "json_invalido": ["CAPPROBE {nao e json"],
    "json_nao_objeto": ["CAPPROBE [1, 2]"],
    "linha_duplicada": [_line(), _line()],
    "chave_ausente": [_line(tempfile=None)],
    "chave_extra": [_line(cap_eff=f"0000 {LOG_CANARY}")],
    "caminho_ausente": [_line(**{"/data": None})],
    "uid_como_texto": [_line(uid="999")],
    "uid_booleano": [_line(uid=True)],
    "write_minusculo": [_nested("/tmp", write="eacces")],
    "write_numerico": [_nested("/logs", write=13)],
    "mode_invalido": [_nested("/data", mode="rwxr-xr-x")],
    "chave_extra_no_caminho": [_nested("/tmp", extra=LOG_CANARY)],
    "tempfile_livre": [_line(tempfile=f"linha livre {LOG_CANARY}")],
    "user_invalido": [_line(user="a b")],
    "capprobe_no_meio_do_texto": [f"prefixo {_line()}"],
}


@pytest.mark.parametrize("case", sorted(MALFORMED))
def test_log_malformado_ou_ausente_reprova_o_probe_sem_virar_observacao(
    lab: FargateLab, case: str
) -> None:
    lab.state.probe_lines = MALFORMED[case]
    error = fails(lab)
    assert str(error).startswith("probe=")
    assert LOG_CANARY not in str(error)


def test_stream_do_probe_inexistente_reprova_com_leituras_limitadas(lab: FargateLab) -> None:
    lab.state.no_logs = {"probe"}
    error = fails(lab)
    assert str(error).startswith("probe=")
    assert operations(lab).count("logs.get-log-events") <= 40


def test_recovery_sem_logs_reprova_o_probe(lab: FargateLab) -> None:
    lab.state.no_logs = {"recovery"}
    assert str(fails(lab)).startswith("probe=")


def test_task_que_nao_inicia_o_container_reprova_sem_vazar_o_motivo_cru(
    lab: FargateLab,
) -> None:
    lab.state.force_pull_failure = True
    error = fails(lab)
    assert str(error).startswith("probe=")
    assert STOPPED_CANARY not in str(error)
    assert "CannotPull" not in str(error)


def test_task_que_nunca_para_reprova_com_polling_limitado(lab: FargateLab) -> None:
    lab.state.task_seconds = 10.0**6
    error = fails(lab)
    assert str(error).startswith("probe=")
    assert operations(lab).count("ecs.describe-tasks") <= 120
    assert lab.clock[0] - lab.spec.deadline < 0


def test_digest_diferente_do_ecr_reprova_antes_de_registrar_qualquer_task(
    lab: FargateLab,
) -> None:
    lab.images.digest_override = "sha256:" + "0" * 64
    error = fails(lab)
    assert str(error).startswith("probe=")
    assert "ecs.register-task-definition" not in operations(lab)
    assert "ecs.run-task" not in operations(lab)
    assert "ecr.describe-images" in operations(lab)


def test_sequencia_autentica_publica_confere_ajusta_trust_e_so_depois_roda(
    lab: FargateLab,
) -> None:
    run(lab)
    ops = operations(lab)
    assert ops.index("ecr.get-authorization-token") < ops.index("ecr.describe-images")
    assert ops.index("ecr.describe-images") < ops.index("ecs.register-task-definition")
    assert len(lab.images.pushes) == 1
    image, target = lab.images.pushes[0]
    assert image == IMAGE
    assert target.startswith(repo_uri(NAME) + ":")
    first_run = ops.index("ecs.run-task")
    assert ops.index("iam.update-assume-role-policy") < first_run
    assert ops.count("iam.update-assume-role-policy") == 2
    assert ops.index("iam.put-role-policy") < first_run
    assert ops.count("ecs.run-task") == 2
    assert ops.count("iam.put-role-policy") == 1


def test_trust_dos_dois_roles_e_ecs_tasks_com_source_account(lab: FargateLab) -> None:
    run(lab)
    updates = requests_of(lab, "iam.update-assume-role-policy")
    assert {u["RoleName"] for u in updates} == {EXEC.name, TASK.name}
    expected = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow", "Principal": {"Service": "ecs-tasks.amazonaws.com"},
            "Action": "sts:AssumeRole",
            "Condition": {"StringEquals": {"aws:SourceAccount": ACCOUNT}},
        }],
    }
    assert all(json.loads(u["PolicyDocument"]) == expected for u in updates)


def test_politica_de_execucao_espelha_pull_do_repo_e_logs_do_grupo_e_a_task_role_fica_vazia(
    lab: FargateLab,
) -> None:
    run(lab)
    puts = requests_of(lab, "iam.put-role-policy")
    assert [p["RoleName"] for p in puts] == [EXEC.name]
    document = json.loads(puts[0]["PolicyDocument"])
    group = f"arn:aws:logs:us-east-2:{ACCOUNT}:log-group:{NAME}:log-stream:*"
    assert document["Statement"] == [
        {"Effect": "Allow",
         "Action": ["ecr:BatchGetImage", "ecr:GetDownloadUrlForLayer",
                    "ecr:BatchCheckLayerAvailability"],
         "Resource": repo_arn(NAME)},
        {"Effect": "Allow", "Action": "ecr:GetAuthorizationToken", "Resource": "*"},
        {"Effect": "Allow", "Action": ["logs:CreateLogStream", "logs:PutLogEvents"],
         "Resource": group},
    ]


def test_duas_tasks_rodam_na_rede_default_com_ip_publico_e_so_o_probe_troca_o_entrypoint(
    lab: FargateLab,
) -> None:
    run(lab)
    runs = requests_of(lab, "ecs.run-task")
    assert len(runs) == 2
    for request in runs:
        network = request["networkConfiguration"]["awsvpcConfiguration"]
        assert network["assignPublicIp"] == "ENABLED"
        assert sorted(network["subnets"]) == sorted(SUBNETS)
        assert network["securityGroups"] == [SG_ID]
        assert request["launchType"] == "FARGATE"
        assert request["count"] == 1
        assert request["cluster"] == NAME
    containers = [
        r["containerDefinitions"][0] for r in requests_of(lab, "ecs.register-task-definition")
    ]
    assert [c.get("entryPoint", [None])[:2] for c in containers] == [["python", "-c"], [None]]


def _settings_from(payload: dict) -> tuple[TaskSettings, tuple[str, ...] | None]:
    container = payload["containerDefinitions"][0]
    env = {item["name"]: item["value"] for item in container["environment"]}
    settings = TaskSettings(
        family=payload["family"],
        image=container["image"],
        execution_role_arn=payload["executionRoleArn"],
        task_role_arn=payload["taskRoleArn"],
        log_group=container["logConfiguration"]["options"]["awslogs-group"],
        control_plane_table=env["AWS_CONTROL_PLANE_TABLE"],
        data_bucket=env["AWS_DATA_BUCKET"],
        state_machine_arn=env["AWS_STATE_MACHINE_ARN"],
        subnet_ids=tuple(env["AWS_PUBLIC_SUBNET_IDS"].split(",")),
        security_group_id=env["AWS_SECURITY_GROUP_ID"],
        tags={item["key"]: item["value"] for item in payload["tags"]},
    )
    entry = container.get("entryPoint")
    return settings, None if entry is None else tuple(entry)


def test_task_definitions_registradas_sao_exatamente_o_payload_do_espelho(
    lab: FargateLab,
) -> None:
    run(lab)
    registered = requests_of(lab, "ecs.register-task-definition")
    assert len(registered) == 2
    for payload in registered:
        settings, entry = _settings_from(payload)
        assert task_definition_payload(settings, entry) == payload
        assert settings.log_group == NAME
        assert settings.image == f"{repo_uri(NAME)}@{DIGEST}"
        assert settings.execution_role_arn.endswith(f"role/cnesdata-cap/{EXEC.name}")
        assert settings.task_role_arn.endswith(f"role/cnesdata-cap/{TASK.name}")
        assert settings.family.startswith(NAME)
        assert settings.tags["Run"] == "a1b2c3d4"
    assert len({p["family"] for p in registered}) == 2


def test_evidencia_nao_carrega_conta_arn_ids_de_rede_log_cru_motivo_nem_token(
    lab: FargateLab,
) -> None:
    text = json.dumps(run(lab))
    for forbidden in (
        ACCOUNT, "arn:", "sg-", "subnet-", "vpc-", ".amazonaws.com", NAME, LOG_CANARY,
        STOPPED_CANARY, TOKEN_CANARY, "CAPPROBE",
    ):
        assert forbidden not in text
    assert not re.search(r"[0-9a-f]{32}", text.replace(DIGEST, ""))


def test_token_do_ecr_chega_so_a_porta_de_imagem_e_nunca_a_comandos_nem_a_repr(
    lab: FargateLab,
) -> None:
    observed = run(lab)
    assert [login.token for login in lab.images.logins] == [TOKEN_CANARY]
    assert operations(lab).count("ecr.get-authorization-token") == 1
    haystack = payload_text(lab) + repr(lab.images) + repr(lab.services.ports) + str(observed)
    assert TOKEN_CANARY not in haystack
    assert TOKEN_CANARY not in "".join(repr(r) for r in lab.aws.requests)


def test_falha_do_probe_nao_vaza_o_token_na_excecao(lab: FargateLab) -> None:
    lab.state.probe_lines = ["CAPPROBE {"]
    error = fails(lab)
    assert [login.token for login in lab.images.logins] == [TOKEN_CANARY]
    assert TOKEN_CANARY not in str(error) + repr(error)


@pytest.mark.parametrize("missing", ["images", "image"])
def test_sem_porta_ou_sem_imagem_o_probe_falha_antes_de_publicar(
    lab: FargateLab, missing: str
) -> None:
    ports = replace(lab.services.ports, **{missing: None})
    services = lab.services.__class__(lab.spec, ports)
    with pytest.raises(ServiceFailure) as error:
        services.probe(lab.resources, lab.spec, lab.context)
    assert error.value.args == ("probe=fargate_inputs_required",)
    assert lab.images.pushes == []
    assert "ecr.get-authorization-token" not in operations(lab)


def _invalid(lab: FargateLab, case: str):
    spec = replace(lab.spec, run_id="ffffffff")
    cases = {
        "faltando": (lab.resources[:-1], lab.spec, lab.context),
        "extra": ([*lab.resources, lab.resources[0]], lab.spec, lab.context),
        "ordem": (list(reversed(lab.resources)), lab.spec, lab.context),
        "outro_nome": ([*lab.resources[:-1], Resource("ecs", "x", "x")],
                       lab.spec, lab.context),
        "spec": (lab.resources, spec, lab.context),
        "lane": (lab.resources, lab.spec, RequestContext(lab.budget, "cleanup")),
    }
    return cases[case]


@pytest.mark.parametrize("case", ["faltando", "extra", "ordem", "outro_nome", "spec", "lane"])
def test_recusa_entrada_invalida_sem_nenhuma_chamada(lab: FargateLab, case: str) -> None:
    resources, spec, context = _invalid(lab, case)
    with pytest.raises(ServiceFailure) as error:
        lab.services.probe(resources, spec, context)
    assert error.value.args == ("probe=fargate_invalid",)
    assert lab.aws.calls == []
    assert lab.images.pushes == []
