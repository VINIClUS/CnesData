"""Integridade da fronteira falsa do fargate-write contra os fatos medidos nos ensaios reais."""

import json
from pathlib import Path

import pytest

from tests.integration.aws_capability._aws_fake_fargate_testing import (
    DEFAULT_VPC,
    SUBNETS,
    group_arn,
    repo_arn,
    repo_uri,
)
from tests.integration.aws_capability._b1_testing import ACCOUNT, NAME
from tests.integration.aws_capability._fargate_testing import (
    SG_ID,
    FargateLab,
    make_fargate_lab,
    seed_fargate,
)
from tests.integration.aws_capability._service_contracts import RegistryLogin, Reply
from tests.integration.aws_capability.cli import Request

ECS_TRUST = {
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow", "Principal": {"Service": "ecs-tasks.amazonaws.com"},
        "Action": "sts:AssumeRole",
        "Condition": {"StringEquals": {"aws:SourceAccount": ACCOUNT}},
    }],
}
def pull_policy(stream: str = "recovery/processor/*") -> dict:
    repo, group = repo_arn(NAME), group_arn(NAME)
    return {
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Action": [
                "ecr:BatchGetImage", "ecr:GetDownloadUrlForLayer",
                "ecr:BatchCheckLayerAvailability"], "Resource": repo},
            {"Effect": "Allow", "Action": "ecr:GetAuthorizationToken", "Resource": "*"},
            {"Effect": "Allow", "Action": ["logs:CreateLogStream", "logs:PutLogEvents"],
             "Resource": f"{group}:log-stream:{stream}"},
        ],
    }


PULL = pull_policy()


def call(lab: FargateLab, service: str, operation: str, **payload: object) -> Reply:
    return lab.aws.call(Request(service, operation, payload), lab.context)


def ok(lab: FargateLab, service: str, operation: str, **payload: object) -> dict:
    reply = call(lab, service, operation, **payload)
    assert reply.ok, (service, operation, reply.error)
    return dict(reply.data)


def trust_roles(lab: FargateLab) -> None:
    for suffix in ("exec", "task"):
        ok(lab, "iam", "update-assume-role-policy", RoleName=f"{NAME}-{suffix}",
           PolicyDocument=json.dumps(ECS_TRUST))
    ok(lab, "iam", "put-role-policy", RoleName=f"{NAME}-exec", PolicyName="cap",
       PolicyDocument=json.dumps(PULL))


def push(lab: FargateLab) -> str:
    uri = repo_uri(NAME)
    login = RegistryLogin(uri.split("/")[0], "AWS", "ECR_TOKEN_CANARY_do_not_log")
    digest = lab.images.push("local:img", f"{uri}:cap", login, lab.context)
    return f"{uri}@{digest}"


def definition(lab: FargateLab, image: str, entry: list[str] | None = None) -> str:
    container: dict = {
        "name": "processor", "image": image,
        "logConfiguration": {"logDriver": "awslogs", "options": {
            "awslogs-group": NAME, "awslogs-region": "us-east-2",
            "awslogs-stream-prefix": "recovery"}},
    }
    if entry:
        container["entryPoint"] = entry
    reply = ok(lab, "ecs", "register-task-definition", family=f"{NAME}-recovery",
               containerDefinitions=[container], cpu="256", memory="512",
               networkMode="awsvpc", requiresCompatibilities=["FARGATE"],
               executionRoleArn=f"arn:aws:iam::{ACCOUNT}:role/cnesdata-cap/{NAME}-exec",
               taskRoleArn=f"arn:aws:iam::{ACCOUNT}:role/cnesdata-cap/{NAME}-task")
    return str(reply["taskDefinition"]["taskDefinitionArn"])


def run(lab: FargateLab, arn: str, public: str = "ENABLED") -> str:
    network = {"awsvpcConfiguration": {
        "subnets": list(SUBNETS), "securityGroups": [SG_ID], "assignPublicIp": public}}
    reply = ok(lab, "ecs", "run-task", cluster=NAME, launchType="FARGATE", taskDefinition=arn,
               count=1, networkConfiguration=network)
    return str(reply["tasks"][0]["taskArn"])


def wait_stopped(lab: FargateLab, task: str) -> dict:
    for _ in range(200):
        row = ok(lab, "ecs", "describe-tasks", cluster=NAME, tasks=[task])["tasks"][0]
        if row["lastStatus"] == "STOPPED":
            return dict(row)
        lab.clock[0] += 5
    raise AssertionError("fake=task_never_stopped")


def events(lab: FargateLab, task: str, prefix: str = "recovery") -> list[str]:
    stream = f"{prefix}/processor/{task.rsplit('/', 1)[1]}"
    lab.clock[0] += 60
    data = ok(lab, "logs", "get-log-events", logGroupName=NAME, logStreamName=stream,
              startFromHead=True)
    return [str(event["message"]) for event in data["events"]]


@pytest.fixture
def lab(tmp_path: Path) -> FargateLab:
    return make_fargate_lab(tmp_path)


def test_task_sem_ip_publico_nao_inicia_container_e_expoe_motivo_sem_exit_code(
    lab: FargateLab,
) -> None:
    trust_roles(lab)
    task = run(lab, definition(lab, push(lab)), public="DISABLED")
    row = wait_stopped(lab, task)
    assert "exitCode" not in row["containers"][0]
    assert "CannotPullContainerError" in row["stoppedReason"]


def test_task_sem_trust_do_ecs_nao_inicia_container(lab: FargateLab) -> None:
    task = run(lab, definition(lab, push(lab)))
    row = wait_stopped(lab, task)
    assert "exitCode" not in row["containers"][0]
    assert "ResourceInitializationError" in row["stoppedReason"]


def test_logs_so_aparecem_depois_do_atraso_e_o_token_de_paginacao_se_repete_no_fim(
    lab: FargateLab,
) -> None:
    trust_roles(lab)
    task = run(lab, definition(lab, push(lab)))
    wait_stopped(lab, task)
    stream = f"recovery/processor/{task.rsplit('/', 1)[1]}"
    early = call(lab, "logs", "get-log-events", logGroupName=NAME, logStreamName=stream)
    assert getattr(early.error, "code", None) == "ResourceNotFoundException"
    lab.clock[0] += 60
    first = ok(lab, "logs", "get-log-events", logGroupName=NAME, logStreamName=stream,
               startFromHead=True)
    again = ok(lab, "logs", "get-log-events", logGroupName=NAME, logStreamName=stream,
               nextToken=first["nextForwardToken"])
    assert again["events"] == []
    assert again["nextForwardToken"] == first["nextForwardToken"]


def test_security_group_nasce_com_egress_total_e_so_sai_depois_da_task_e_da_eni(
    lab: FargateLab,
) -> None:
    created = ok(lab, "ec2", "create-security-group", GroupName="cnesdata-cap-x",
                 Description="d", VpcId=DEFAULT_VPC)
    row = ok(lab, "ec2", "describe-security-groups", GroupIds=[created["GroupId"]])
    group = row["SecurityGroups"][0]
    assert group["IpPermissions"] == []
    assert group["IpPermissionsEgress"][0]["IpProtocol"] == "-1"
    trust_roles(lab)
    task = run(lab, definition(lab, push(lab)))
    busy = call(lab, "ec2", "delete-security-group", GroupId=SG_ID)
    assert getattr(busy.error, "code", None) == "DependencyViolation"
    wait_stopped(lab, task)
    lab.state.sg_violations = 2
    codes = [
        getattr(call(lab, "ec2", "delete-security-group", GroupId=SG_ID).error, "code", "ok")
        for _ in range(3)
    ]
    assert codes == ["DependencyViolation", "DependencyViolation", "ok"]


def test_cluster_com_task_viva_nao_e_apagado_e_inativo_some_da_listagem(
    lab: FargateLab,
) -> None:
    trust_roles(lab)
    task = run(lab, definition(lab, push(lab)))
    denied = call(lab, "ecs", "delete-cluster", cluster=NAME)
    assert getattr(denied.error, "code", None) == "ClusterContainsTasksException"
    ok(lab, "ecs", "stop-task", cluster=NAME, task=task, reason="cap")
    wait_stopped(lab, task)
    ok(lab, "ecs", "delete-cluster", cluster=NAME)
    assert ok(lab, "ecs", "list-clusters")["clusterArns"] == []
    described = ok(lab, "ecs", "describe-clusters", clusters=[NAME], include=["TAGS"])
    assert described["clusters"][0]["status"] == "INACTIVE"


def test_apos_delete_task_definitions_familia_inativa_persiste_mas_nao_ha_revisao(
    lab: FargateLab,
) -> None:
    arn = definition(lab, push(lab))
    refused = ok(lab, "ecs", "delete-task-definitions", taskDefinitions=[arn])
    assert refused["failures"]
    ok(lab, "ecs", "deregister-task-definition", taskDefinition=arn)
    inactive = ok(lab, "ecs", "list-task-definitions", familyPrefix=NAME, status="INACTIVE")
    assert inactive["taskDefinitionArns"] == [arn]
    ok(lab, "ecs", "delete-task-definitions", taskDefinitions=[arn])
    for status in ("ACTIVE", "INACTIVE"):
        listed = ok(lab, "ecs", "list-task-definitions", familyPrefix=NAME, status=status)
        assert listed["taskDefinitionArns"] == []
    families = ok(lab, "ecs", "list-task-definition-families", familyPrefix=NAME,
                  status="INACTIVE")
    assert families["families"] == [f"{NAME}-recovery"]
    active = ok(lab, "ecs", "list-task-definition-families", familyPrefix=NAME, status="ACTIVE")
    assert active["families"] == []


def test_describe_clusters_so_devolve_tags_quando_pedidas(lab: FargateLab) -> None:
    ok(lab, "ecs", "create-cluster", clusterName="tagged", tags=[{"key": "Run", "value": "x"}])
    bare = ok(lab, "ecs", "describe-clusters", clusters=["tagged"])["clusters"][0]
    tagged = ok(lab, "ecs", "describe-clusters", clusters=["tagged"], include=["TAGS"])
    tagged = tagged["clusters"][0]
    assert "tags" not in bare
    assert tagged["tags"] == [{"key": "Run", "value": "x"}]


def _without(action: str) -> dict:
    policy = pull_policy()
    for statement in policy["Statement"]:
        listed = statement["Action"]
        kept = [a for a in ([listed] if isinstance(listed, str) else listed) if a != action]
        statement["Action"] = kept or ["sts:GetCallerIdentity"]
    return policy


@pytest.mark.parametrize(
    "action",
    [
        "ecr:BatchGetImage", "ecr:GetDownloadUrlForLayer", "ecr:BatchCheckLayerAvailability",
        "ecr:GetAuthorizationToken", "logs:CreateLogStream", "logs:PutLogEvents",
    ],
)
def test_politica_de_execucao_sem_qualquer_acao_obrigatoria_impede_o_container(
    lab: FargateLab, action: str
) -> None:
    trust_roles(lab)
    ok(lab, "iam", "put-role-policy", RoleName=f"{NAME}-exec", PolicyName="cap",
       PolicyDocument=json.dumps(_without(action)))
    row = wait_stopped(lab, run(lab, definition(lab, push(lab))))
    assert "exitCode" not in row["containers"][0]
    assert "ResourceInitializationError" in row["stoppedReason"]


@pytest.mark.parametrize("stream", ["other/processor/*", "recovery/*"])
def test_politica_com_recurso_de_log_errado_so_vale_quando_cobre_o_stream(
    lab: FargateLab, stream: str
) -> None:
    trust_roles(lab)
    ok(lab, "iam", "put-role-policy", RoleName=f"{NAME}-exec", PolicyName="cap",
       PolicyDocument=json.dumps(pull_policy(stream)))
    row = wait_stopped(lab, run(lab, definition(lab, push(lab))))
    assert ("exitCode" in row["containers"][0]) == (stream == "recovery/*")


def test_create_security_group_exige_descricao_e_tipo_de_recurso_da_tag(lab: FargateLab) -> None:
    base = {"GroupName": "cnesdata-cap-y", "VpcId": DEFAULT_VPC}
    assert not call(lab, "ec2", "create-security-group", **base).ok
    spec = [{"ResourceType": "instance", "Tags": [{"Key": "a", "Value": "b"}]}]
    extra = {"Description": "d", **base}
    assert not call(lab, "ec2", "create-security-group", TagSpecifications=spec, **extra).ok
    spec[0]["ResourceType"] = "security-group"
    assert call(lab, "ec2", "create-security-group", TagSpecifications=spec, **extra).ok


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("cpu", "999"),
        ("memory", "9999"),
        ("networkMode", "bridge"),
        ("requiresCompatibilities", ["EC2"]),
        ("executionRoleArn", f"arn:aws:iam::{ACCOUNT}:role/cnesdata-cap/ausente"),
    ],
)
def test_register_task_definition_recusa_definicao_que_o_fargate_recusaria(
    lab: FargateLab, field: str, value: object
) -> None:
    options = {"awslogs-group": NAME, "awslogs-stream-prefix": "recovery"}
    container = {
        "name": "processor", "image": "x",
        "logConfiguration": {"logDriver": "awslogs", "options": options},
    }
    payload: dict = {
        "family": f"{NAME}-recovery", "containerDefinitions": [container], "cpu": "256",
        "memory": "512", "networkMode": "awsvpc", "requiresCompatibilities": ["FARGATE"],
        "executionRoleArn": f"arn:aws:iam::{ACCOUNT}:role/cnesdata-cap/{NAME}-exec",
        "taskRoleArn": f"arn:aws:iam::{ACCOUNT}:role/cnesdata-cap/{NAME}-task",
    }
    assert call(lab, "ecs", "register-task-definition", **payload).ok
    assert not call(lab, "ecs", "register-task-definition", **{**payload, field: value}).ok


def _stopped_with_lines(lab: FargateLab, lines: list[str], size: int) -> str:
    lab.state.recovery_lines = lines
    lab.state.log_page_size = size
    trust_roles(lab)
    task = run(lab, definition(lab, push(lab)))
    wait_stopped(lab, task)
    lab.clock[0] += 60
    return f"recovery/processor/{task.rsplit('/', 1)[1]}"


def test_get_log_events_sem_start_from_head_devolve_a_cauda(lab: FargateLab) -> None:
    lines = [f"l{i}" for i in range(7)]
    stream = _stopped_with_lines(lab, lines, 3)
    tail = ok(lab, "logs", "get-log-events", logGroupName=NAME, logStreamName=stream)
    assert [e["message"] for e in tail["events"]] == lines[-3:]
    head = ok(lab, "logs", "get-log-events", logGroupName=NAME, logStreamName=stream,
              startFromHead=True)
    assert [e["message"] for e in head["events"]] == lines[:3]


def test_get_log_events_entrega_paginas_parciais_ate_o_token_se_repetir(lab: FargateLab) -> None:
    lines = [f"l{i}" for i in range(7)]
    stream = _stopped_with_lines(lab, lines, 3)
    seen: list[str] = []
    token = None
    for _ in range(10):
        extra = {} if token is None else {"nextToken": token}
        page = ok(lab, "logs", "get-log-events", logGroupName=NAME, logStreamName=stream,
                  startFromHead=True, **extra)
        seen += [e["message"] for e in page["events"]]
        if page["nextForwardToken"] == token:
            break
        token = page["nextForwardToken"]
    assert seen == lines


def test_primeira_pagina_pode_vir_vazia_mesmo_com_eventos_adiante(lab: FargateLab) -> None:
    stream = _stopped_with_lines(lab, ["a", "b"], 5)
    lab.state.empty_first_log_page = True
    first = ok(lab, "logs", "get-log-events", logGroupName=NAME, logStreamName=stream,
               startFromHead=True)
    assert first["events"] == []
    later = ok(lab, "logs", "get-log-events", logGroupName=NAME, logStreamName=stream,
               startFromHead=True, nextToken=first["nextForwardToken"])
    assert [e["message"] for e in later["events"]] == ["a", "b"]


def test_describe_tasks_responde_missing_nas_primeiras_chamadas(lab: FargateLab) -> None:
    trust_roles(lab)
    task = run(lab, definition(lab, push(lab)))
    lab.state.describe_missing = 2
    replies = [ok(lab, "ecs", "describe-tasks", cluster=NAME, tasks=[task]) for _ in range(3)]
    assert [(r["tasks"] == [], [f["reason"] for f in r["failures"]]) for r in replies] == [
        (True, ["MISSING"]), (True, ["MISSING"]), (False, [])]


def test_run_task_pode_voltar_ok_com_failures_e_sem_tasks(lab: FargateLab) -> None:
    trust_roles(lab)
    arn = definition(lab, push(lab))
    lab.state.run_failures = 1
    network = {"awsvpcConfiguration": {
        "subnets": list(SUBNETS), "securityGroups": [SG_ID], "assignPublicIp": "ENABLED"}}
    payload = {"cluster": NAME, "launchType": "FARGATE", "taskDefinition": arn, "count": 1,
               "networkConfiguration": network}
    refused = ok(lab, "ecs", "run-task", **payload)
    assert refused["tasks"] == []
    assert refused["failures"]
    assert ok(lab, "ecs", "run-task", **payload)["tasks"]


@pytest.mark.parametrize(
    ("service", "operation", "names"),
    [
        ("ecr", "describe-repositories", ("repositories", "nextToken")),
        ("logs", "describe-log-groups", ("logGroups", "nextToken")),
        ("ecs", "list-clusters", ("clusterArns", "nextToken")),
        ("ec2", "describe-security-groups", ("SecurityGroups", "NextToken")),
    ],
)
def test_listagens_paginam_com_cursor_encadeado(
    lab: FargateLab, service: str, operation: str, names: tuple[str, str]
) -> None:
    key, token = names
    seed_fargate(lab.aws, lab.spec)
    for index in range(4):
        seed_fargate(lab.aws, lab.spec, f"{NAME}-{index}")
        lab.state.groups[f"sg-0extra{index:010d}"] = lab.state.groups[SG_ID]
    lab.state.page_size = 2
    total, cursor, pages = 0, None, 0
    while pages < 20:
        extra = {} if cursor is None else {token: cursor}
        page = ok(lab, service, operation, **extra)
        total += len(page[key])
        pages += 1
        cursor = page.get(token)
        if cursor is None:
            break
    assert pages > 1
    assert total >= 5
