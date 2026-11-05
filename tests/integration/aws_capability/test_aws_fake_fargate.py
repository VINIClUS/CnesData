"""Integridade da fronteira falsa do fargate-write contra os fatos medidos nos ensaios reais."""

# ruff: noqa: S108
import json
from pathlib import Path

import pytest

from tests.integration.aws_capability._aws_fake_fargate_testing import (
    DEFAULT_VPC,
    SUBNETS,
    repo_uri,
)
from tests.integration.aws_capability._b1_testing import ACCOUNT, NAME
from tests.integration.aws_capability._fargate_testing import (
    SG_ID,
    FargateLab,
    make_fargate_lab,
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
PULL = {
    "Version": "2012-10-17",
    "Statement": [{"Effect": "Allow", "Action": [
        "ecr:BatchGetImage", "ecr:GetAuthorizationToken", "logs:PutLogEvents"], "Resource": "*"}],
}


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
               containerDefinitions=[container],
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


@pytest.mark.parametrize("writable", [False, True])
def test_fluxo_de_referencia_do_prototipo_produz_o_resultado_medido(
    lab: FargateLab, writable: bool
) -> None:
    lab.state.writable = writable
    image = push(lab)
    trust_roles(lab)
    task = run(lab, definition(lab, image, ["python", "-c", "probe"]))
    stopped = wait_stopped(lab, task)
    assert stopped["containers"][0]["exitCode"] == 0
    assert stopped["platformVersion"] == "1.4.0"
    line = events(lab, task)[0]
    observed = json.loads(line.removeprefix("CAPPROBE "))
    assert (observed["uid"], observed["user"], observed["root_write"]) == (999, "app", "EROFS")
    assert observed["/logs"]["write"] == ("ok" if writable else "EACCES")
    assert observed["/tmp"]["mode"] == ("0o1777" if writable else "0o755")
    assert observed["tempfile"] == ("ok:/tmp" if writable else "FileNotFoundError")


def test_recovery_sai_1_e_termina_igual_nas_duas_imagens(lab: FargateLab) -> None:
    results = []
    image = push(lab)
    trust_roles(lab)
    for writable in (False, True):
        lab.state.writable = writable
        task = run(lab, definition(lab, image))
        row = wait_stopped(lab, task)
        results.append((row["containers"][0]["exitCode"], events(lab, task)))
    assert results[0] == results[1]
    assert results[0][0] == 1
    assert "ProcessorExecutionUnavailable" in results[0][1][-1]


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
