"""Contratos sintéticos de promoção, sem comprovante operacional fabricado."""

from __future__ import annotations

import copy
import json
from typing import Any

import pytest

from tests.production.delivery import test_candidate_manifest as candidate_fixtures
from tests.production.delivery._phase_a_fixtures import (
    ACCOUNT,
    ECR,
    NOW,
    REGION,
    SHA,
    RegistryBoundary,
    _bytes,
    _candidate_reference,
    _creator_pin,
    _digest,
    _iam_context,
    _promotion,
)

github = candidate_fixtures.github


@pytest.mark.parametrize("mutation", ["repository", "digest"])
def test_recusa_repositorio_errado_e_digest_divergente(mutation):
    from scripts.promotion.promote_ecr import promote_image
    registry = RegistryBoundary()
    context = _promotion(registry)
    if mutation == "repository":
        registry.repository["name"] = "outro"
    else:
        context["image"] = context["image"].split("@")[0] + "@sha256:" + "f" * 64
    with pytest.raises(ValueError):
        promote_image(context, registry)
    assert registry.copied == []


@pytest.mark.parametrize("mutation", ["mutable", "media"])
def test_recusa_destino_mutavel_e_media_type_errado(mutation):
    from scripts.promotion.promote_ecr import promote_image
    registry = RegistryBoundary()
    if mutation == "mutable":
        registry.repository["image_tag_mutability"] = "MUTABLE"
    else:
        manifest = json.loads(registry.manifest)
        manifest["mediaType"] = "application/vnd.oci.image.index.v1+json"
        registry.manifest = _bytes(manifest)
    with pytest.raises(ValueError):
        promote_image(_promotion(registry), registry)
    assert registry.copied == []


CLUSTER = f"arn:aws:ecs:{REGION}:{ACCOUNT}:cluster/cnesdata-prod-processor"


def test_delta_da_phase_a_so_tem_revisoes_grants_e_canary():
    from scripts.promotion.phase_a import verify_delta
    plan = {
        "resource_changes": [
            {
                "mode": "managed",
                "address": "module.processing.aws_sfn_state_machine.processor[0]",
                "change": {
                    "actions": ["update"],
                    "before": {"definition": "A"},
                    "after": {"definition": "B"},
                    "after_unknown": {},
                },
            }
        ]
    }
    with pytest.raises(ValueError, match="delta"):
        verify_delta(plan, {})


def test_retry_nao_registra_outra_revisao():
    from scripts.promotion.phase_a import retry_action

    identity = {
        "release_id": "release",
        "candidate_sha256": "a" * 64,
        "image_digest": "sha256:" + "b" * 64,
        "plan_binding_sha256": "c" * 64,
    }
    assert retry_action(identity, {"identity": identity, "status": "applied"}) == "read-only"
    with pytest.raises(ValueError, match="recovery_required"):
        retry_action(identity, {"identity": identity, "status": "ambiguous"})


def test_evidencia_so_depois_da_propagacao_do_iam():
    from scripts.promotion.verify_task_revisions import wait_for_iam

    class Clock:
        elapsed = 0.0

        def monotonic(self):
            return self.elapsed

        def sleep(self, delay):
            self.elapsed += delay

        def jitter(self):
            return 0.0

    clock = Clock()
    observed = []

    def readback(deadline=None):
        observed.append(clock.elapsed)
        return {
            "policies": {"exact": "a" * 64},
            "trust": {"exact": "b" * 64},
            "roles": {"exact": "c" * 64},
            "network": {"cluster": "exact"},
            "evaluations": [
                {"case_id": "allowed", "expected": True, "allowed": True},
                {"case_id": "denied", "expected": False, "allowed": False},
            ],
        }

    proof = wait_for_iam(readback, clock, readback())
    assert proof["proof_kind"] == "iam-readback-and-evaluation"
    assert proof["second_readback_seconds"] - proof["first_readback_seconds"] >= 10
    assert observed[-1] <= 300


def test_promove_os_mesmos_bytes_de_manifest_config_e_layer():
    from scripts.promotion.promote_ecr import promote_image

    registry = RegistryBoundary()
    result = promote_image(_promotion(registry), registry)
    assert result["ecr_image"] == ECR + "@" + _digest(registry.manifest)
    assert registry.copied == [
        (_digest(registry.config), registry.config),
        (_digest(registry.layer), registry.layer),
        (_digest(registry.manifest), registry.manifest),
    ]


@pytest.mark.parametrize("mutation", ["architecture", "config", "blob", "account", "scan"])
def test_recusa_plataforma_blob_conta_ou_scan_incorreto(mutation):
    from scripts.promotion.promote_ecr import promote_image

    registry = RegistryBoundary()
    context = _promotion(registry)
    if mutation == "architecture":
        registry.config = _bytes({"architecture": "arm64", "os": "linux"})
    elif mutation == "config":
        context["config_digest"] = "sha256:" + "e" * 64
    elif mutation == "blob":
        registry.layer += b"changed"
    elif mutation == "account":
        registry.repository["account_id"] = "999999999999"
    else:
        context["scan"]["vulnerabilities"] = [{"Severity": "CRITICAL"}]
    with pytest.raises(ValueError):
        promote_image(context, registry)
    assert registry.copied == []


def test_deriva_canary_id_do_release_completo_sem_newline():
    from scripts.promotion.promote_ecr import canary_binding

    candidate = {
        "repository": "VINIClUS/CnesData",
        "release_id": "candidate-db4ebd7883de3fcd352bb01c6ade21304cabc5a1-42416377192-1",
        "source_sha": "db4ebd7883de3fcd352bb01c6ade21304cabc5a1",
    }
    result = canary_binding(
        candidate, "9a509eb987f01ba56d2f32066f41161ff295e459a71ca56c5c0d5e2327bb43c7"
    )
    assert result["binding_sha256"] == (
        "d155e7af91747d96917453791d3c21251f96edfd545d7c61bf49577d8e2d3b44"
    )
    assert result["canary_id"] == "d155e7af91747d96917453791d3c2125"
    assert result["release_id"] == candidate["release_id"]


def test_autentica_r2_de_head_posterior_preservando_todos_os_bytes_r1(github):
    from scripts.promotion.promote_ecr import authenticate_candidate

    reference = _candidate_reference(github)
    result = authenticate_candidate(reference, github, _creator_pin(), NOW)
    assert result["candidate"]["source_sha"] == SHA
    assert result["provenance"]["run"]["head_sha"] != SHA
    assert result["provenance"]["verification"]["id"] == 42
    assert len(result["provenance"]["artifacts"]) == 6


@pytest.mark.parametrize("mutation", ["attempt", "digest", "workflow", "expired", "r1"])
def test_recusa_mistura_de_r1_r2_e_identidades_de_artifact(github, mutation):
    from scripts.promotion.promote_ecr import authenticate_candidate

    reference = _candidate_reference(github)
    if mutation == "attempt":
        reference["run_attempt"] = 2
    elif mutation == "digest":
        reference["artifact_digest"] = "sha256:" + "e" * 64
    elif mutation == "workflow":
        github.routes["actions/runs/93"]["path"] = ".github/workflows/other.yml"
    elif mutation == "expired":
        github.metadata[-1]["expired"] = True
    else:
        github.routes["actions/runs/42"]["run_attempt"] = 1
    with pytest.raises(ValueError):
        authenticate_candidate(reference, github, _creator_pin(), NOW)


def test_preserva_active_e_retained_no_bootstrap_e_promocao():
    from scripts.promotion.verify_task_revisions import retained_inputs

    prefix = f"arn:aws:ecs:{REGION}:{ACCOUNT}:task-definition/cnesdata-prod-processor-"
    revisions = {mode: prefix + mode + ":2" for mode in ("unit", "recovery", "audit")}
    context = {
        "region": REGION,
        "account_id": ACCOUNT,
        "processor_active_unit_task_definition_arn": None,
        "processor_retained_unit_task_definition_arns": [],
    }
    assert retained_inputs(context, revisions) == {
        "active_before": None,
        "retained_before": [],
        "retained_after": [revisions["unit"]],
    }
    old = prefix + "unit:1"
    context.update(
        processor_active_unit_task_definition_arn=old,
        processor_retained_unit_task_definition_arns=[old],
    )
    assert retained_inputs(context, revisions) == {
        "active_before": old,
        "retained_before": [old],
        "retained_after": [old, revisions["unit"]],
    }


def test_matriz_exige_principais_roles_cluster_e_negativos_exatos():
    from scripts.promotion.verify_task_revisions import authorization_matrix

    context: dict[str, Any] = _iam_context() | {"cluster_arn": CLUSTER}
    cases = {item["case_id"]: item for item in authorization_matrix(context)}
    assert cases["sfn-candidate-unit"]["resource"] == "candidate-unit"
    assert cases["sfn-retained-0"]["resource"] == "old-unit"
    assert cases["verifier-audit"]["context"] == {"ecs:cluster": context["cluster_arn"]}
    assert cases["verifier-other-cluster"]["expected"] is False
    assert cases["verifier-describe"]["expected"] is True
    assert cases["seeder-describe"]["expected"] is False
    assert cases["verifier-pass-role-0"]["context"] == {
        "iam:PassedToService": "ecs-tasks.amazonaws.com"
    }
    assert cases["scheduler-recovery-candidate"]["expected"] is False
    assert cases["seeder-production-table"]["expected"] is False
    assert cases["fence-semaphore-update"]["expected"] is False


def test_iam_timeout_nao_aceita_relogio_de_parede_ou_apenas_positivos():
    from scripts.promotion.verify_task_revisions import IamNotReady, wait_for_iam

    class Clock:
        elapsed = 0.0

        def monotonic(self):
            return self.elapsed

        def sleep(self, delay):
            self.elapsed += delay

        def jitter(self):
            return 0.25

    clock = Clock()

    def unavailable(deadline):
        raise IamNotReady("readback=pending")

    with pytest.raises(ValueError, match="timeout"):
        wait_for_iam(unavailable, clock, {})
    assert clock.elapsed == 300
    snapshot = {
        "policies": {"x": "a"},
        "trust": {"x": "b"},
        "roles": {"x": "c"},
        "network": {"cluster": "exact"},
        "evaluations": [{"case_id": "only-positive", "expected": True, "allowed": True}],
    }
    with pytest.raises(ValueError, match="positive_negative"):
        wait_for_iam(lambda deadline: snapshot, Clock(), snapshot)


@pytest.mark.parametrize("mutation", [None, "memory", "command", "skip_destroy", "track_latest"])
def test_delta_aceita_so_digest_e_preserva_campos_funcionais(mutation):
    from scripts.promotion.phase_a import verify_delta

    address = 'module.processing.aws_ecs_task_definition.processor["unit"]'
    old = {
        "container_definitions": json.dumps(
            [{"name": "processor", "image": ECR + "@old", "command": []}]
        ),
        "memory": "512",
        "skip_destroy": True,
        "track_latest": False,
    }
    new = copy.deepcopy(old)
    new["container_definitions"] = json.dumps(
        [{"name": "processor", "image": ECR + "@new", "command": []}]
    )
    if mutation == "memory":
        new["memory"] = "1024"
    elif mutation == "command":
        new["container_definitions"] = json.dumps(
            [{"name": "processor", "image": ECR + "@new", "command": ["arbitrary"]}]
        )
    elif mutation == "skip_destroy":
        new["skip_destroy"] = False
    elif mutation == "track_latest":
        new["track_latest"] = True
    context = {"resources": {address: new}, "ecr_image": ECR + "@new"}
    plan = {
        "resource_changes": [
            {
                "mode": "managed",
                "address": address,
                "change": {
                    "actions": ["delete", "create"],
                    "before": old,
                    "after": new,
                    "after_unknown": {},
                },
            }
        ]
    }
    if mutation is None:
        verify_delta(plan, context)
    else:
        with pytest.raises(ValueError, match="delta"):
            verify_delta(plan, context)


def test_promocao_completa_exige_scan_e_job_autenticados_do_mesmo_r1(github):
    from scripts.promotion.promote_ecr import promote_candidate

    registry = RegistryBoundary()
    reference = _candidate_reference(github, registry)
    result = promote_candidate(
        reference,
        github,
        registry,
        {"account_id": ACCOUNT, "region": REGION, "now": NOW, "candidate_creator": _creator_pin()},
    )
    assert result["ecr_image"] == ECR + "@" + _digest(registry.manifest)
    assert result["scan"]["run_id"] == 42
    assert result["scan"]["run_attempt"] == 2
    assert (
        result["scan"]["artifact_sha256"]
        == result["provenance"]["artifacts"]["production-oci-data_processor"]["sha256"]
    )
    assert result["scan"]["job_id"] > 0


def test_nao_aceita_threshold_de_scan_sem_fonte_verificada(github):
    from scripts.promotion.promote_ecr import promote_candidate

    registry = RegistryBoundary()
    reference = _candidate_reference(github, registry)
    github.routes["contents/.github/workflows/verify-production.yml"]["content"] = "eA=="
    with pytest.raises(ValueError, match="threshold_contract"):
        promote_candidate(
            reference,
            github,
            registry,
            {
                "account_id": ACCOUNT,
                "region": REGION,
                "now": NOW,
                "candidate_creator": _creator_pin(),
            },
        )
    assert registry.copied == []


@pytest.mark.parametrize("mutation", [None, "role", "extra"])
def test_readback_de_revisoes_confere_todos_os_campos_funcionais(mutation):
    from scripts.promotion.verify_task_revisions import verify_revisions

    prefix = f"arn:aws:ecs:{REGION}:{ACCOUNT}:task-definition/cnesdata-prod-processor-"
    revisions = {mode: prefix + mode + ":3" for mode in ("unit", "recovery", "audit")}
    expected = {
        mode: {
            "family": "cnesdata-prod-processor-" + mode,
            "networkMode": "awsvpc",
            "requiresCompatibilities": ["FARGATE"],
            "cpu": "256",
            "memory": "512",
            "executionRoleArn": "execution",
            "taskRoleArn": mode,
            "runtimePlatform": {"cpuArchitecture": "X86_64", "operatingSystemFamily": "LINUX"},
            "volumes": [],
            "containerDefinitions": [{"name": "processor", "image": ECR + "@digest"}],
        }
        for mode in revisions
    }
    context = {
        "region": REGION,
        "account_id": ACCOUNT,
        "ecr_image": ECR + "@digest",
        "processor_active_unit_task_definition_arn": None,
        "processor_retained_unit_task_definition_arns": [],
        "task_definitions": expected,
    }

    class TaskBoundary:
        def describe_task_definition(self, arn):
            mode = next(mode for mode, value in revisions.items() if value == arn)
            result = copy.deepcopy(expected[mode]) | {"taskDefinitionArn": arn, "status": "ACTIVE"}
            if mutation == "role":
                result["taskRoleArn"] = "foreign"
            elif mutation == "extra":
                result["ephemeralStorage"] = {"sizeInGiB": 200}
            return result

    if mutation is None:
        result = verify_revisions(context, revisions, TaskBoundary())
        assert result["processor_task_definition_arns"] == revisions
        assert result["retained_after"] == [revisions["unit"]]
    else:
        with pytest.raises(ValueError, match="revision"):
            verify_revisions(context, revisions, TaskBoundary())


def test_matriz_bootstrap_preserva_ausencia_dos_schedulers():
    from scripts.promotion.verify_task_revisions import authorization_matrix

    context: dict[str, Any] = _iam_context() | {"cluster_arn": CLUSTER}
    context["scheduler_previous"] = {"recovery": None, "audit": None}
    context["principals"].update(scheduler_recovery=None, scheduler_audit=None)
    cases = authorization_matrix(context)
    assert not any(case["case_id"].startswith("scheduler-") for case in cases)
    assert all(case["principal"] is not None for case in cases)


def test_recusa_codigo_creator_r2_nao_aprovado_antes_de_copiar(github):
    from scripts.promotion.promote_ecr import promote_candidate

    registry = RegistryBoundary()
    reference = _candidate_reference(github, registry)
    governed = {
        "account_id": ACCOUNT,
        "region": REGION,
        "now": NOW,
        "candidate_creator": {
            "workflow_path": ".github/workflows/release-candidate.yml",
            "workflow_sha": "c" * 40,
        },
    }
    with pytest.raises(ValueError, match="creator"):
        promote_candidate(reference, github, registry, governed)
    assert registry.copied == []


@pytest.mark.parametrize(
    "address",
    [
        'module.processing.aws_s3_bucket.canary["tenant]PRIVATE_TOKEN"]',
        'module.root["private]suffix"].aws_example.test["private\\"quote]suffix"]',
    ],
)
def test_redacao_consume_indice_json_com_colchetes_e_aspas(address):
    from scripts.promotion._phase_a_graph import redacted_plan

    plan = {
        "resource_changes": [
            {"address": address, "type": "aws_s3_bucket", "change": {"actions": ["create"]}}
        ]
    }
    view = redacted_plan(plan, SHA, "b" * 64)
    shape = view["changes"][0]["address_shape"]
    assert shape in {
        "module.processing.aws_s3_bucket.canary[redacted]",
        "module.root[redacted].aws_example.test[redacted]",
    }
    assert view["changes"][0]["address_sha256"] == _digest(address.encode())[7:]


def test_gramatica_iam_recusa_funcoes_externas_e_limita_operacao_inteira():
    from scripts.promotion._iam_expression import Budget, parse

    for source in ('file("x")', "toset([])", "var.x[1]", "{ [ = 1 }"):
        with pytest.raises(ValueError):
            parse(source, Budget())
    budget = Budget()
    for _ in range(4096):
        parse("0", budget)
    with pytest.raises(ValueError, match="iam=budget_nodes"):
        parse("0", budget)
