"""Autentica activation e executa a revisão audit com isolamento canary."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from queue import Empty, Queue
from threading import Thread
from typing import TYPE_CHECKING, Any

import boto3
from botocore.config import Config

from release.verify_activation import (
    activation_fields,
    admit_ext2,
    canonical,
    sha,
    timestamp,
    verify_activation,
)
from release.verify_candidate import require
from scripts.promotion._canonical import object_hash
from scripts.promotion.verify_task_revisions import SystemClock, revision_arn

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

type Node = dict[str, Any]

_RUN_AUTHORITY = object()


@dataclass(frozen=True)
class RunObservation(Mapping[str, Any]):
    """Observação opaca criada exclusivamente na fronteira nativa RunTask."""

    payload: bytes
    authority: object

    def __getitem__(self, key: str) -> Any:
        return json.loads(self.payload)[key]

    def __iter__(self) -> Iterator[str]:
        return iter(json.loads(self.payload))

    def __len__(self) -> int:
        return len(json.loads(self.payload))


def validate_run(run: Any) -> None:
    """Raises: ValueError para dados de operação autodeclarados pelo chamador."""
    require(type(run) is RunObservation and run.authority is _RUN_AUTHORITY, "canary=run_authority")


def _network(governed: Node, proof: Node) -> tuple[Node, str]:
    account, region = governed["account_id"], governed["region"]
    network = governed["network"]
    cluster = f"arn:aws:ecs:{region}:{account}:cluster/cnesdata-prod-processor"
    require(
        object_hash(network) == proof["network_sha256"] and network["cluster"] == cluster,
        "canary=network_binding",
    )
    require(
        set(network) == {"cluster", "subnets", "security_group", "public_ip", "platform"}
        and network["public_ip"] == "ENABLED"
        and network["platform"] == "1.4.0"
        and 0 < len(network["subnets"]) <= 16
        and network["security_group"],
        "canary=network_shape",
    )
    return network, cluster


def _event(fields: Node, proof: Node) -> Node:
    return {
        "tenant_id": "_GLOBAL",
        "event_id": "canary-" + fields["canary_id"],
        "event_type": "production.release.canary",
        "aggregate_id": fields["release_id"],
        "payload": fields,
        "created_at": proof["created_at"],
        "delivered_at": None,
    }


def _contract(admitted: Node, governed: Node) -> Node:
    candidate = json.loads(admitted["candidate_bytes"])
    proof = json.loads(admitted["phase_a_bytes"])
    fields = activation_fields(
        candidate, proof, (admitted["candidate_bytes"], admitted["phase_a_bytes"])
    )
    require(fields == admitted["activation_fields"], "canary=activation_binding")
    account, region = governed["account_id"], governed["region"]
    require(
        account.isdecimal() and len(account) == 12 and region == "us-east-2",
        "canary=account_region",
    )
    for mode, arn in fields["revisions"].items():
        revision_arn(arn, governed, mode)
    repository = f"{account}.dkr.ecr.{region}.amazonaws.com/cnesdata-prod-processor@sha256:"
    digest = fields["ecr_uri"].removeprefix(repository)
    require(
        fields["ecr_uri"].startswith(repository)
        and len(digest) == 64
        and all(c in "0123456789abcdef" for c in digest),
        "canary=ecr_identity",
    )
    network, cluster = _network(governed, proof)
    roles = {
        name + "_role": f"arn:aws:iam::{account}:role/cnesdata-prod-canary-{name}"
        for name in ("task", "execution", "seeder", "verifier")
    }
    event = _event(fields, proof)
    return (
        fields
        | roles
        | {
            "table": "cnesdata-prod-canary",
            "index": "gsi6",
            "bucket": "cnesdata-prod-canary",
            "cluster": cluster,
            "network": network,
            "event": event,
            "account_id": account,
            "region": region,
            "_admitted": admitted,
            "_governed": governed,
        }
    )


def validate_contract(contract: Node) -> None:
    """Raises: ValueError para mistura de material, recurso ou papel autenticado."""
    require(
        contract == _contract(contract["_admitted"], contract["_governed"]), "canary=contract_drift"
    )


def authenticate_canary(reference: Any, services: Node, governed: Node) -> Node:
    """Returns: contrato somente após carrier, assinatura e admissão aprovados."""
    now, governance = governed["now"], governed["activation"]
    verified = verify_activation(
        github=services["github"],
        s3=services["s3"],
        governance=governance,
        now=now,
        ext2_post_upload_receipt_v1=reference,
        historical=True,
    )
    request = {
        "request_version": 1,
        "mode": "historical",
        "inputs": None,
        "source_sha": governance["provenance"]["source_sha"],
        "ext2_post_upload_receipt_v1": reference,
    }
    admitted = admit_ext2(
        verified.payload_bytes,
        request=request,
        github=services["github"],
        governance=governance,
        now=now,
        historical=True,
    )
    return _contract(admitted, governed)


def bounded(action: Callable[[], Any], clock: Any, deadline: float) -> Any:
    """Returns: resultado dentro do prazo; chamada ambígua expira sem emitir sucesso."""
    remaining = deadline - clock.monotonic()
    require(remaining > 0, "canary=timeout")
    result: Queue[tuple[bool, Any]] = Queue(maxsize=1)

    def invoke() -> None:
        try:
            result.put((True, action()))
        except Exception as error:
            result.put((False, error))

    Thread(target=invoke, daemon=True).start()
    try:
        success, value = result.get(timeout=remaining)
    except Empty:
        raise ValueError("canary=timeout") from None
    require(clock.monotonic() < deadline, "canary=timeout")
    if not success:
        raise value
    return value


def call(session: Any, clock: Any, deadline: float, request: tuple[str, str, Node]) -> Node:
    """Returns: resposta nativa com retries desabilitados e orçamento compartilhado."""
    remaining = deadline - clock.monotonic()
    require(remaining > 0, "canary=timeout")
    service, operation, parameters = request
    timeout = min(1.0, remaining / 2)
    config = Config(
        connect_timeout=timeout, read_timeout=timeout, retries={"total_max_attempts": 1}
    )

    def invoke() -> Node:
        client = session.client(service, region_name="us-east-2", config=config)
        return getattr(client, operation)(**parameters)

    return bounded(invoke, clock, deadline)


def _run_request(contract: Node) -> Node:
    environment = {
        "AWS_CONTROL_PLANE_TABLE": contract["table"],
        "AWS_AUDIT_BUCKET": contract["bucket"],
        "AWS_AUDIT_RETENTION_DAYS": "1",
    }
    network = contract["network"]
    return {
        "cluster": contract["cluster"],
        "taskDefinition": contract["revisions"]["audit"],
        "launchType": "FARGATE",
        "count": 1,
        "platformVersion": network["platform"],
        "clientToken": contract["binding_sha256"],
        "startedBy": contract["canary_id"],
        "networkConfiguration": {
            "awsvpcConfiguration": {
                "subnets": network["subnets"],
                "securityGroups": [network["security_group"]],
                "assignPublicIp": network["public_ip"],
            }
        },
        "overrides": {
            "taskRoleArn": contract["task_role"],
            "executionRoleArn": contract["execution_role"],
            "containerOverrides": [
                {
                    "name": "processor",
                    "environment": [{"name": k, "value": v} for k, v in environment.items()],
                }
            ],
        },
    }


def run_canary(contract: Node, session: Any, clock: Any) -> RunObservation:
    """Returns: tentativa vinculada; falha parcial não gera prova de execução."""
    validate_contract(contract)
    started = clock.utcnow()
    require(timestamp(contract["event"]["created_at"]) <= started, "canary=event_future")
    response = call(
        session, clock, clock.monotonic() + 60, ("ecs", "run_task", _run_request(contract))
    )
    tasks = response.get("tasks", [])
    require(not response.get("failures") and len(tasks) == 1, "canary=run_partial")
    task = tasks[0]
    created = task["createdAt"]
    require(
        timestamp(contract["event"]["created_at"]) <= created <= clock.utcnow(),
        "canary=task_origin",
    )
    require(
        task["taskDefinitionArn"] == contract["revisions"]["audit"]
        and task["clusterArn"] == contract["cluster"]
        and task["taskArn"].startswith(contract["cluster"].replace(":cluster/", ":task/") + "/"),
        "canary=run_identity",
    )
    facts = {
        "binding_sha256": contract["binding_sha256"],
        "event_sha256": sha(canonical(contract["event"])),
        "task_arn": task["taskArn"],
        "started_at": created.isoformat().replace("+00:00", "Z"),
    }
    return RunObservation(canonical(facts), _RUN_AUTHORITY)


def assume_canary(contract: Node, sts: Any, actor: str) -> Any:
    """Returns: sessão efêmera do principal canary exato, sem persistir credenciais."""
    require(actor in {"seeder", "verifier"}, "canary=actor")
    validate_contract(contract)
    response = sts.assume_role(
        RoleArn=contract[actor + "_role"],
        RoleSessionName=f"canary-{actor}-{contract['canary_id']}",
        DurationSeconds=900,
    )
    credentials = response["Credentials"]
    return boto3.Session(
        aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"],
        aws_session_token=credentials["SessionToken"],
        region_name=contract["region"],
    )


def execute_canary(reference: Node, services: Node, governed: Node) -> Node:
    """Returns: veredito somente após activation assinada, seed, run e retenção reais."""
    from scripts.promotion.seed_canary import seed_canary
    from scripts.promotion.verify_canary import verify_canary

    clock = SystemClock()
    contract = authenticate_canary(reference, services, governed | {"now": clock.utcnow()})
    seed_session = assume_canary(contract, services["sts"], "seeder")
    seed_canary(contract, seed_session, clock)
    verifier_session = assume_canary(contract, services["sts"], "verifier")
    run = run_canary(contract, verifier_session, clock)
    return verify_canary(contract, verifier_session, clock, run)
