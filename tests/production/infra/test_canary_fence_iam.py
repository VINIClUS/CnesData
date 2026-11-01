"""Canary persistente, roles por release e fence de promoção (PROD-INFRA-010)."""

import json
import os
import shutil
from pathlib import Path

import pytest

from cnes_infra.control_plane.dynamodb_inventory import CONTROL_PLANE_INVENTORY
from tests.production.infra._aws_mock_defaults import MOCK_AWS_PROVIDERS
from tests.production.infra._processing_native import _native_result
from tests.production.infra._step_functions_iam_contracts import (
    Grants,
    Node,
    assert_policy_grants,
    event_for,
    literal_grants,
    managed_resources,
    native_events,
    policy_grants,
    resource_at,
)

ACCOUNT, PREFIX = "123456789012", "cnesdata-prod"
RELEASE, OTHER = "r20261101a", "r20261101b"
PRINCIPAL = f"arn:aws:iam::{ACCOUNT}:role/promotion-workflow"
CP_TABLE = f"arn:aws:dynamodb:us-east-2:{ACCOUNT}:table/{PREFIX}-control-plane"
AUDIT = f"arn:aws:s3:::{PREFIX}-audit"
DATA = f"arn:aws:s3:::{PREFIX}-data"
TABLE = f"arn:aws:dynamodb:us-east-2:{ACCOUNT}:table/{PREFIX}-canary"
INDEX = f"{TABLE}/index/gsi6"
BUCKET = f"arn:aws:s3:::{PREFIX}-canary"
CLUSTER = f"arn:aws:ecs:us-east-2:{ACCOUNT}:cluster/{PREFIX}-processor"
AUDIT_TASK = f"arn:aws:ecs:us-east-2:{ACCOUNT}:task-definition/{PREFIX}-processor-audit:7"
SUBJECT = "module.subject"
AUDIT_POLICY = f'{SUBJECT}.aws_iam_role_policy.runtime["audit"]'
FORBIDDEN_ACTION_PARTS = ("*", "Scan", "Delete", "List", "BypassGovernanceRetention", "PutItem")
ROLES = ("canary_task", "canary_execution", "canary_seeder", "canary_verifier",
         "promotion_fence_operator")
POLICY_NAMES = {
    "canary_task": "canary-task-runtime", "canary_execution": "canary-pull-logs",
    "canary_seeder": "canary-seeder-put", "canary_verifier": "canary-verifier-read",
    "promotion_fence_operator": "promotion-fence-operator-gate",
}
ROLE_NAMES = {role: f"{PREFIX}-{role.replace('_', '-')}" for role in ROLES}
INVALID_RELEASES = {
    "estrela": "r1*x", "estrela_final": "r1*", "interrogacao": "r1?x", "barra": "r1/x",
    "cerquilha": "r1#x",
    "contrabarra": "r1\\x", "ponto": "r1.x", "dois_pontos": "..", "espaco": "r1 x",
    "maiuscula": "R1", "hifen_inicial": "-r1", "hifen_final": "r1-", "vazio": "", "longo": "r" * 33,
}


def _release(release: str = RELEASE, **changes: str) -> Node:
    base = {"release_id": release, "trusted_principal_arn": PRINCIPAL, "table_arn": TABLE,
            "index_arn": INDEX, "bucket_arn": BUCKET}
    return base | changes


def _grants_input() -> Node:
    indexes = {name: f"{CP_TABLE}/index/{name}" for name in ("gsi1", "gsi4", "gsi6")}
    return {"table_arn": CP_TABLE, "index_arns": indexes, "data_bucket_arn": DATA,
            "audit_bucket_arn": AUDIT,
            "api_role_arn": f"arn:aws:iam::{ACCOUNT}:role/{PREFIX}-api"}


RUNTIME_BASE: Node = {
    "name_prefix": PREFIX, "account_id": ACCOUNT,
    "repository_arn": f"arn:aws:ecr:us-east-2:{ACCOUNT}:repository/{PREFIX}",
    "log_group_arn": f"arn:aws:logs:us-east-2:{ACCOUNT}:log-group:/ecs/{PREFIX}-processor:*",
    "state_machine_arn": f"arn:aws:states:us-east-2:{ACCOUNT}:stateMachine:{PREFIX}",
    "runtime_grants": _grants_input(), "canary_release": _release(),
}
PROCESSING_BASE: Node = {
    "image_digest": "sha256:" + "a" * 64, "name_prefix": PREFIX,
    "repository_uri": f"{ACCOUNT}.dkr.ecr.us-east-2.amazonaws.com/{PREFIX}",
    "task_role_arns": {m: f"arn:aws:iam::{ACCOUNT}:role/{PREFIX}-{m}"
                       for m in ("unit", "recovery", "audit")},
    "execution_role_arn": f"arn:aws:iam::{ACCOUNT}:role/{PREFIX}-execution",
    "log_group_name": f"/ecs/{PREFIX}-processor", "control_plane_table": f"{PREFIX}-control-plane",
    "data_bucket": f"{PREFIX}-data", "audit_bucket": f"{PREFIX}-audit", "audit_retention_days": 365,
    "state_machine_arn": f"arn:aws:states:us-east-2:{ACCOUNT}:stateMachine:{PREFIX}",
    "public_subnet_ids": ["subnet-production"], "security_group_id": "sg-production",
    "canary_storage_enabled": True, "canary_verifier_role_id": ROLE_NAMES["canary_verifier"],
}


def _override(target: str, values: Node) -> str:
    return f"override_resource {{\n  target = {target}\n  values = {json.dumps(values)}\n}}\n"


def _role_overrides() -> str:
    roles = {f"{SUBJECT}.aws_iam_role.{role}[0]": ROLE_NAMES[role] for role in ROLES}
    roles |= {f'{SUBJECT}.aws_iam_role.task["{m}"]': f"{PREFIX}-processor-{m}"
              for m in ("unit", "recovery", "audit")}
    return "".join(
        _override(target, {"id": name, "arn": f"arn:aws:iam::{ACCOUNT}:role/{name}"})
        for target, name in roles.items()
    )


def _processing_overrides() -> str:
    return (
        _override(f"{SUBJECT}.aws_dynamodb_table.canary[0]", {"arn": TABLE})
        + _override(f"{SUBJECT}.aws_s3_bucket.canary[0]", {"arn": BUCKET})
        + _override(f"{SUBJECT}.aws_ecs_cluster.processor", {"arn": CLUSTER})
        + _override(f'{SUBJECT}.aws_ecs_task_definition.processor["audit"]', {"arn": AUDIT_TASK})
    )


def _run_block(label: str, variables: Node) -> str:
    literals = "\n".join(f"    {name} = {json.dumps(value)}" for name, value in variables.items())
    return f'\nrun "{label}" {{\n  command = plan\n  variables {{\n{literals}\n  }}\n}}\n'


def _execute(tofu_context: tuple[str, dict[str, str]],
             request: tuple[Path, Path, tuple[str, str], str],
             base: Node, runs: dict[str, Node]) -> list[Node]:
    root, prod_root, (source, label), overrides = request
    (root / "tests").mkdir(parents=True)
    module_path = os.path.relpath((prod_root / source).resolve(), root)
    declarations = "".join(f'variable "{name}" {{\n  type = any\n}}\n' for name in base)
    assignments = "".join(f"  {name} = var.{name}\n" for name in base)
    (root / "main.tf").write_text(
        'terraform {\n  required_providers {\n    aws = {\n      source = "hashicorp/aws"\n'
        '      version = "6.54.0"\n    }\n  }\n}\nprovider "aws" {\n  region = "us-east-2"\n}\n'
        f'{declarations}module "subject" {{\n  source = "{module_path}"\n'
        f"{assignments}}}\n", encoding="utf-8")
    shutil.copyfile(prod_root / ".terraform.lock.hcl", root / ".terraform.lock.hcl")
    for name, changes in runs.items():
        file = name if name.startswith("invalido_") else "valido"
        with (root / "tests" / f"{file}.tftest.hcl").open("a", encoding="utf-8") as handle:
            if handle.tell() == 0:
                handle.write(MOCK_AWS_PROVIDERS + overrides)
            handle.write(_run_block(name, base | changes))
    tofu = (tofu_context[0], tofu_context[1] | {"TF_DATA_DIR": str(root / ".tofu")})
    init = _native_result(tofu, root, ("init", "-backend=false", "-input=false"), f"{label}-init")
    assert init.returncode == 0, f"setup_error init_{label}={init.stderr!r}"
    result = _native_result(
        tofu, root, ("test", "-no-color", "-json", "-verbose", "-test-directory=tests"), label)
    events = native_events(result.stdout)
    assert events, f"setup_error events_missing_{label}={result.stderr!r}"
    return events


@pytest.fixture(scope="module")
def runtime_events(tmp_path_factory: pytest.TempPathFactory, prod_root: Path,
                   tofu_context: tuple[str, dict[str, str]]) -> list[Node]:
    invalid: dict[str, Node] = {
        f"invalido_{name}": {"canary_release": _release(value)}
        for name, value in INVALID_RELEASES.items()}
    other = {
        "outra_conta": _release(trusted_principal_arn="arn:aws:iam::999999999999:role/x"),
        "principal_curinga": _release(trusted_principal_arn=f"arn:aws:iam::{ACCOUNT}:role/*"),
        "tabela_alheia": _release(table_arn=TABLE.replace("canary", "control-plane")),
        "indice_alheio": _release(index_arn=INDEX.replace("gsi6", "gsi1")),
        "bucket_alheio": _release(bucket_arn=AUDIT),
    }
    invalid |= {f"invalido_{name}": {"canary_release": value} for name, value in other.items()}
    invalid["invalido_sem_runtime_grants"] = {"runtime_grants": None}
    runs = {"ativo": {}, "ativo_b": {"canary_release": _release(OTHER)},
            "desligado": {"canary_release": None}} | invalid
    request = (tmp_path_factory.mktemp("canary-runtime"), prod_root,
               ("../../modules/runtime-iam", "canary-runtime"), _role_overrides())
    return _execute(tofu_context, request, RUNTIME_BASE, runs)


@pytest.fixture(scope="module")
def processing_events(tmp_path_factory: pytest.TempPathFactory, prod_root: Path,
                      tofu_context: tuple[str, dict[str, str]]) -> list[Node]:
    runs = {
        "ativo": {},
        "desligado": {"canary_storage_enabled": False, "canary_verifier_role_id": None},
        "invalido_sem_digest": {"image_digest": None},
        "invalido_verifier_alheio": {"canary_verifier_role_id": f"{PREFIX}-api"},
    }
    request = (tmp_path_factory.mktemp("canary-processing"), prod_root,
               ("../../modules/processing", "canary-processing"), _processing_overrides())
    return _execute(tofu_context, request, PROCESSING_BASE, runs)


def _plan(events: list[Node], label: str) -> Node:
    assert event_for(events, "test_run", label)["test_run"]["status"] == "pass", f"run={label}"
    return event_for(events, "test_plan", label)["test_plan"]


def _output(plan: Node, name: str) -> object:
    return plan["planned_values"].get("outputs", {}).get(name, {}).get("value")


def _defaults(plan: Node, names: tuple[str, ...]) -> Node:
    module = plan["configuration"]["root_module"]["module_calls"]["subject"]["module"]
    return {name: module["variables"][name]["default"] for name in names}


def _diagnostic_text(events: list[Node], label: str) -> str:
    return " ".join(
        f"{e['diagnostic'].get('summary', '')} {e['diagnostic'].get('detail', '')}"
        for e in events
        if e.get("type") == "diagnostic" and e.get("@testrun") == label
        and e["diagnostic"].get("severity") == "error")


def _lk(values: tuple[str, ...]) -> Node:
    return {"ForAllValues:StringEquals": {"dynamodb:LeadingKeys": list(values)},
            "Null": {"dynamodb:LeadingKeys": "false"}}


def _object_grants(actions: tuple[str, ...], resources: tuple[str, ...]) -> Grants:
    return literal_grants(actions, resources, {})


def _audit_matrix(table: str, index: str, bucket: str) -> Grants:
    return (
        literal_grants(("dynamodb:Query",), (index,), _lk(("OUTBOX#PENDING",)))
        | literal_grants(("dynamodb:GetItem", "dynamodb:UpdateItem"), (table,),
                         _lk(("TENANT#_GLOBAL", "OUTBOX#CURSOR")))
        | _object_grants(("s3:GetBucketObjectLockConfiguration",), (bucket,))
        | _object_grants(("s3:PutObject", "s3:PutObjectRetention", "s3:GetObject",
                          "s3:GetObjectRetention"), (f"{bucket}/audit/*",))
    )


def _objects(release: str) -> tuple[str, str]:
    event = f"canary-{release}"
    return (f"{BUCKET}/audit/.event-id/{event}.json", f"{BUCKET}/audit/*/*/*/*/{event}.json")


def _expected(release: str) -> dict[str, Grants]:
    task, execution = (f"arn:aws:iam::{ACCOUNT}:role/{ROLE_NAMES[r]}"
                       for r in ("canary_task", "canary_execution"))
    ecr = ("ecr:BatchGetImage", "ecr:GetDownloadUrlForLayer", "ecr:BatchCheckLayerAvailability")
    return {
        "canary_task": _audit_matrix(TABLE, INDEX, BUCKET),
        "canary_execution": _object_grants(ecr, (f"arn:aws:ecr:us-east-2:{ACCOUNT}:repository/"
                                                  f"{PREFIX}",))
        | _object_grants(("ecr:GetAuthorizationToken",), ("*",))
        | _object_grants(("logs:CreateLogStream", "logs:PutLogEvents"),
                         (f"arn:aws:logs:us-east-2:{ACCOUNT}:log-group:/ecs/{PREFIX}-processor"
                          ":log-stream:*",)),
        "canary_seeder": literal_grants(("dynamodb:PutItem",), (TABLE,), _lk(("TENANT#_GLOBAL",))),
        "canary_verifier": literal_grants(("dynamodb:Query",), (INDEX,), _lk(("OUTBOX#PENDING",)))
        | literal_grants(("dynamodb:GetItem",), (TABLE,), _lk(("TENANT#_GLOBAL",)))
        | _object_grants(("s3:GetObject", "s3:GetObjectRetention"), _objects(release))
        | literal_grants(("iam:PassRole",), (task, execution),
                         {"StringEquals": {"iam:PassedToService": "ecs-tasks.amazonaws.com"}}),
        "promotion_fence_operator":
            literal_grants(("dynamodb:GetItem",), (CP_TABLE,),
                           _lk(("ENV#prod#FENCE", "ENV#prod#SEMAPHORE")))
            | literal_grants(("dynamodb:UpdateItem",), (CP_TABLE,), _lk(("ENV#prod#FENCE",))),
    }


def _actor(plan: Node, role: str) -> tuple[Node, Node]:
    address = f"{SUBJECT}.aws_iam_role_policy.{role}[0]"
    spec = {"address": address, "name": f"{PREFIX}-{POLICY_NAMES[role]}", "allowed": [address]}
    return resource_at(plan, f"{SUBJECT}.aws_iam_role.{role}[0]"), spec


def _observed(plan: Node, role: str) -> Grants:
    policy = resource_at(plan, f"{SUBJECT}.aws_iam_role_policy.{role}[0]")
    return policy_grants(policy["change"]["after"]["policy"])


def _canary_resources(plan: Node) -> list[str]:
    return [r["address"] for r in plan["resource_changes"]
            if any(part in r["address"] for part in ("canary", "promotion"))]


@pytest.mark.parametrize("role", ROLES)
def test_cada_role_recebe_a_matriz_exata_da_release(runtime_events: list[Node], role: str) -> None:
    plan = _plan(runtime_events, "ativo")
    actor, spec = _actor(plan, role)
    assert_policy_grants(plan, actor, spec, _expected(RELEASE)[role])


def test_seeder_so_put_na_tabela_de_canary(runtime_events: list[Node]) -> None:
    observed = _observed(_plan(runtime_events, "ativo"), "canary_seeder")
    assert {(a, r) for a, r, _ in observed} == {("dynamodb:PutItem", TABLE)}
    assert all(INDEX not in resource and CP_TABLE not in resource for _, resource, _ in observed)


def test_verifier_nega_outro_release_id(runtime_events: list[Node]) -> None:
    ativo, outra = _plan(runtime_events, "ativo"), _plan(runtime_events, "ativo_b")
    own, other = _observed(ativo, "canary_verifier"), _observed(outra, "canary_verifier")
    assert own == _expected(RELEASE)["canary_verifier"]
    assert other == _expected(OTHER)["canary_verifier"]
    own_objects = {r for a, r, _ in own if a.startswith("s3:")}
    other_objects = {r for a, r, _ in other if a.startswith("s3:")}
    assert own_objects == set(_objects(RELEASE))
    assert not own_objects & other_objects
    assert all(OTHER not in resource for _, resource, _ in own)
    assert all(f"canary-{RELEASE}.json" not in resource for _, resource, _ in other)


def test_operador_muta_so_o_fence(runtime_events: list[Node]) -> None:
    observed = _observed(_plan(runtime_events, "ativo"), "promotion_fence_operator")
    writes = {(r, c) for a, r, c in observed if a == "dynamodb:UpdateItem"}
    assert writes == {(CP_TABLE, next(iter(
        c for a, _, c in _expected(RELEASE)["promotion_fence_operator"]
        if a == "dynamodb:UpdateItem")))}
    assert all("SEMAPHORE" not in c for a, _, c in observed if a == "dynamodb:UpdateItem")
    assert {a for a, _, _ in observed} == {"dynamodb:GetItem", "dynamodb:UpdateItem"}


def test_dispatch_de_audit_so_leading_keys_de_outbox(runtime_events: list[Node]) -> None:
    plan = _plan(runtime_events, "ativo")
    audit = resource_at(plan, f'{SUBJECT}.aws_iam_role.task["audit"]')
    spec = {"address": AUDIT_POLICY, "name": f"{PREFIX}-processor-audit-runtime",
            "allowed": [AUDIT_POLICY]}
    expected = _audit_matrix(CP_TABLE, f"{CP_TABLE}/index/gsi6", AUDIT)
    assert_policy_grants(plan, audit, spec, expected)
    assert_policy_grants(plan, *_actor(plan, "canary_task"), _expected(RELEASE)["canary_task"])


@pytest.mark.parametrize("which", ["audit", "canary_task"])
def test_dispatch_de_audit_sem_put_scan_ou_bypass(runtime_events: list[Node], which: str) -> None:
    plan = _plan(runtime_events, "ativo")
    observed = (policy_grants(resource_at(plan, AUDIT_POLICY)["change"]["after"]["policy"])
                if which == "audit" else _observed(plan, which))
    allowed_keys = {"OUTBOX#PENDING", "TENANT#_GLOBAL", "OUTBOX#CURSOR"}
    for action, resource, condition in observed:
        assert not any(part in action for part in FORBIDDEN_ACTION_PARTS), f"action={action}"
        if action.startswith("dynamodb:"):
            keys = json.loads(condition)["ForAllValues:StringEquals"]["dynamodb:LeadingKeys"]
            assert set(keys) <= allowed_keys, f"leading_keys={keys}"
            assert "ENV#" not in condition, f"condition={condition}"
            assert "RUN#" not in condition, f"condition={condition}"
        assert "*" not in resource or resource.endswith("/audit/*"), f"resource={resource}"


def test_sessao_maxima_limitada(runtime_events: list[Node]) -> None:
    plan = _plan(runtime_events, "ativo")
    audit = resource_at(plan, f'{SUBJECT}.aws_iam_role.task["audit"]')["change"]["after"]
    assert audit["max_session_duration"] in (None, 3600), "audit_session=above_aws_minimum"
    for role in ROLES:
        values = resource_at(plan, f"{SUBJECT}.aws_iam_role.{role}[0]")["change"]["after"]
        assert values["max_session_duration"] == 3600, f"role={role}"
        assert values["name"] == ROLE_NAMES[role], f"role={role}"
        assert len(values["name"]) <= 64, f"role={role}"
        assert values["tags"] == {"ReleaseId": RELEASE}, f"role={role}"


def test_roles_novas_sem_curinga_scan_delete_list_ou_bypass(runtime_events: list[Node]) -> None:
    plan = _plan(runtime_events, "ativo")
    for role in ROLES:
        actions = {action for action, _, _ in _observed(plan, role)}
        bad = {a for a in actions if any(p in a for p in FORBIDDEN_ACTION_PARTS[:-1])}
        assert not bad, f"role={role} actions={sorted(bad)}"
        assert ("dynamodb:PutItem" in actions) == (role == "canary_seeder"), f"role={role}"


def test_confianca_exata_das_roles(runtime_events: list[Node]) -> None:
    plan = _plan(runtime_events, "ativo")
    ecs = {"Version": "2012-10-17", "Statement": [{
        "Effect": "Allow", "Principal": {"Service": "ecs-tasks.amazonaws.com"},
        "Action": "sts:AssumeRole",
        "Condition": {"StringEquals": {"aws:SourceAccount": ACCOUNT}}}]}
    sessions = {"canary_seeder": f"canary-seeder-{RELEASE}",
                "canary_verifier": f"canary-verifier-{RELEASE}",
                "promotion_fence_operator": f"promotion-fence-{RELEASE}"}
    for role in ROLES:
        document = json.loads(resource_at(plan, f"{SUBJECT}.aws_iam_role.{role}[0]")[
            "change"]["after"]["assume_role_policy"])
        expected = ecs if role not in sessions else {"Version": "2012-10-17", "Statement": [{
            "Effect": "Allow", "Principal": {"AWS": PRINCIPAL}, "Action": "sts:AssumeRole",
            "Condition": {"StringEquals": {"sts:RoleSessionName": sessions[role]}}}]}
        assert document == expected, f"role={role}"


def test_padrao_desligado_nao_cria_recursos_de_canary(runtime_events: list[Node]) -> None:
    plan = _plan(runtime_events, "desligado")
    assert _defaults(plan, ("canary_release",)) == {"canary_release": None}
    assert _canary_resources(plan) == []
    for name in ("canary_task_role_arn", "canary_seeder_role_arn", "canary_event_id",
                 "promotion_fence_operator_role_arn", "canary_object_arns"):
        assert _output(plan, name) is None, f"output={name}"


def test_saidas_de_ativacao_da_release(runtime_events: list[Node]) -> None:
    plan = _plan(runtime_events, "ativo")
    assert _output(plan, "canary_event_id") == f"canary-{RELEASE}"
    assert _output(plan, "canary_object_arns") == list(_objects(RELEASE))
    assert _output(plan, "canary_max_session_duration") == 3600
    assert _output(plan, "canary_verifier_role_id") == ROLE_NAMES["canary_verifier"]


INVALID_CODES = {
    **{f"invalido_{name}": "canary_release_id=" for name in INVALID_RELEASES},
    "invalido_outra_conta": "canary_principal_arn_identity=",
    "invalido_principal_curinga": "canary_principal_arn_identity=",
    "invalido_tabela_alheia": "canary_release_table=",
    "invalido_indice_alheio": "canary_release_table=",
    "invalido_bucket_alheio": "canary_release_bucket=",
    "invalido_sem_runtime_grants": "canary_release_runtime_grants=",
}


@pytest.mark.parametrize("label", sorted(INVALID_CODES))
def test_ativacao_invalida_falha_na_validacao_nativa(
    runtime_events: list[Node], label: str,
) -> None:
    assert event_for(runtime_events, "test_run", label)["test_run"]["status"] == "error"
    assert INVALID_CODES[label] in _diagnostic_text(runtime_events, label)


def _by_type(plan: Node, kind: str) -> Node:
    found = managed_resources(plan, kind)
    assert len(found) == 1, f"type={kind} count={len(found)}"
    return found[0]["change"]["after"]


def test_tabela_de_canary_tem_o_schema_do_inventario(processing_events: list[Node]) -> None:
    plan = _plan(processing_events, "ativo")
    table = _by_type(plan, "aws_dynamodb_table")
    inventory = CONTROL_PLANE_INVENTORY
    assert table["name"] == f"{PREFIX}-canary"
    assert (table["hash_key"], table["range_key"]) == (inventory.partition_key, inventory.sort_key)
    assert {(a["name"], a["type"]) for a in table["attribute"]} == {
        (name, inventory.key_attribute_type) for name in inventory.key_attributes()}
    indexes = {i["name"]: ([(k["attribute_name"], k["key_type"]) for k in i["key_schema"]],
                           i["projection_type"]) for i in table["global_secondary_index"]}
    assert indexes == {i.name: ([(i.partition_key, "HASH"), (i.sort_key, "RANGE")], i.projection)
                       for i in inventory.indexes}
    assert [(t["attribute_name"], t["enabled"]) for t in table["ttl"]] == [
        (inventory.ttl_attribute, True)]
    assert table["deletion_protection_enabled"] is True
    assert [(p["enabled"], p["recovery_period_in_days"])
            for p in table["point_in_time_recovery"]] == [(True, 35)]
    caps = [{"max_read_request_units": 100, "max_write_request_units": 25}]
    assert table["on_demand_throughput"] == caps
    assert all(i["on_demand_throughput"] == caps for i in table["global_secondary_index"])


def test_bucket_de_canary_tem_object_lock_compliance_tls_e_bloqueio_publico(
    processing_events: list[Node],
) -> None:
    plan = _plan(processing_events, "ativo")
    assert _by_type(plan, "aws_s3_bucket")["object_lock_enabled"] is True
    lock = _by_type(plan, "aws_s3_bucket_object_lock_configuration")
    assert lock["rule"][0]["default_retention"][0] == {"mode": "COMPLIANCE", "days": 1,
                                                       "years": None}
    block = _by_type(plan, "aws_s3_bucket_public_access_block")
    assert all(block[k] is True for k in ("block_public_acls", "block_public_policy",
                                          "ignore_public_acls", "restrict_public_buckets"))
    assert _by_type(plan, "aws_s3_bucket_versioning")["versioning_configuration"][0][
        "status"] == "Enabled"
    assert _by_type(plan, "aws_s3_bucket_ownership_controls")["rule"][0][
        "object_ownership"] == "BucketOwnerEnforced"
    sse = _by_type(plan, "aws_s3_bucket_server_side_encryption_configuration")
    assert sse["rule"][0]["apply_server_side_encryption_by_default"][0]["sse_algorithm"] == "AES256"
    policy = json.loads(_by_type(plan, "aws_s3_bucket_policy")["policy"])
    assert policy["Statement"] == [{
        "Sid": "DenyInsecureTransport", "Effect": "Deny", "Principal": "*", "Action": "s3:*",
        "Resource": [BUCKET, f"{BUCKET}/*"],
        "Condition": {"Bool": {"aws:SecureTransport": "false"}}}]
    rule = _by_type(plan, "aws_s3_bucket_lifecycle_configuration")["rule"][0]
    assert rule["abort_incomplete_multipart_upload"][0]["days_after_initiation"] == 1


def test_run_task_so_na_revisao_exata_de_audit_no_cluster(processing_events: list[Node]) -> None:
    plan = _plan(processing_events, "ativo")
    address = f"{SUBJECT}.aws_iam_role_policy.canary_verifier_run_task[0]"
    role = {"change": {"after": {"name": ROLE_NAMES["canary_verifier"]}}}
    spec = {"address": address, "name": f"{PREFIX}-canary-verifier-run-task", "allowed": [address]}
    condition = {"ArnEquals": {"ecs:cluster": CLUSTER}}
    expected = literal_grants(("ecs:RunTask",), (AUDIT_TASK,), condition)
    assert_policy_grants(plan, role, spec, expected)


def test_saidas_de_armazenamento_do_canary(processing_events: list[Node]) -> None:
    plan = _plan(processing_events, "ativo")
    assert _output(plan, "canary_table_arn") == TABLE
    assert _output(plan, "canary_gsi6_arn") == INDEX
    assert _output(plan, "canary_bucket_arn") == BUCKET
    assert _output(plan, "canary_bucket_name") == f"{PREFIX}-canary"
    assert _output(plan, "canary_retention_days") == 1


def test_armazenamento_desligado_por_padrao_nao_cria_recurso(processing_events: list[Node]) -> None:
    plan = _plan(processing_events, "desligado")
    assert _defaults(plan, ("canary_storage_enabled", "canary_verifier_role_id")) == {
        "canary_storage_enabled": False, "canary_verifier_role_id": None}
    assert _canary_resources(plan) == []
    for name in ("canary_table_arn", "canary_gsi6_arn", "canary_bucket_arn",
                 "canary_retention_days"):
        assert _output(plan, name) is None, f"output={name}"


@pytest.mark.parametrize(("label", "code"), [
    ("invalido_sem_digest", "canary_run_task=requires_image_digest"),
    ("invalido_verifier_alheio", "canary_verifier_role_id=expected_own_role_name"),
])
def test_run_task_exige_digest_e_verifier_proprio(
    processing_events: list[Node], label: str, code: str,
) -> None:
    assert event_for(processing_events, "test_run", label)["test_run"]["status"] == "error"
    assert code in _diagnostic_text(processing_events, label)
