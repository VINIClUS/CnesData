"""Vetores e contratos A3 sintéticos, sem comprovação de operações de produção."""

import json
from typing import Any

import pytest

from tests.production.delivery import test_candidate_manifest as candidate_fixtures
from tests.production.delivery._phase_a_application_fixtures import (
    _application,
    _rebind_application,
)
from tests.production.delivery._phase_a_fixtures import (
    ACCOUNT,
    NOW,
    REGION,
    SHA,
    FixtureClock,
    RegistryBoundary,
    _bytes,
    _candidate_reference,
    _creator_pin,
    _digest,
)
from tests.production.delivery._phase_a_runtime_fixtures import (
    RuntimeBoundary,
    _finalization_fixture,
)

github = candidate_fixtures.github


@pytest.mark.parametrize(
    ("raw", "expected", "digest"),
    [
        (b"{}", b"{}\n", "ca3d163bab055381827226140568f3bef7eaac187cebd76878e0b63e9e442356"),
        (b"-0", b"0\n", "9a271f2a916b0b6ee6cecb2426f0b3206ef074578be55d9bc94f6f3fe3ab86aa"),
        (b"1.2300", b"1.23\n", "e2902df5bb55e3039376c56f7b2775eeca583678532a2efe0e15f6c4ffdc229f"),
        (b"1e-3", b"0.001\n", "e531f93fa6db59aeec9b074895514574fffab4c9320399149d4935a7545a4b0d"),
        (
            b'{"a":[-0e+2,1e-3,1.23000e2]}',
            b'{"a":[0,0.001,123]}\n',
            "3b6e829fb7c6c1623c1bb9496af262a8990c7059c566db21880b9ca8dbee0068",
        ),
        (
            '{"z":-0.00,"a":[1e+3,0.0100,12345678901234567890.123400],"u":"á"}'.encode(),
            '{"a":[1000,0.01,12345678901234567890.1234],"u":"á","z":0}\n'.encode(),
            "2e3d7fec3a75a8350272355da617cf525ac7b57e8304dc47624c8333e19b6282",
        ),
    ],
)
def test_hash_do_state_canonico_preserva_decimal_exato(raw, expected, digest):
    from scripts.promotion._plan_contract import canonical_state

    result = canonical_state(raw)
    assert result == expected
    assert _digest(result)[7:] == digest


@pytest.mark.parametrize(
    "raw",
    [
        b'{"a":1,"a":1}',
        b"NaN",
        b"Infinity",
        b"1e999999999",
        b"-0e999999999",
        b'"\\ud800"',
        b"\xef\xbb\xbf{}",
        b"{}{}",
    ],
)
def test_state_recusa_json_ambiguo_e_expansao_ilimitada(raw):
    from scripts.promotion._plan_contract import canonical_state

    with pytest.raises(ValueError):
        canonical_state(raw)


@pytest.mark.parametrize(
    "mutation",
    [
        None,
        "extra",
        "binding",
        "package",
        "state",
        "outputs",
        "lease",
        "cost",
        "backend",
        "purpose",
    ],
)
def test_consumidor_recusa_envelope_concorrente_e_provas_inconsistentes(mutation):
    from scripts.promotion._plan_contract import verify_application

    bundle, context = _application()
    applied = json.loads(bundle["applied"])
    _mutate_application(bundle, applied, mutation)
    if mutation == "binding":
        bundle["applied"] = _bytes(applied) + b"\n"
    else:
        _rebind_application(bundle, applied)
    if mutation is None:
        result = verify_application(bundle, context, NOW)
        assert result["outputs"] == applied["apply"]["outputs"]
        assert result["plan_binding_sha256"] == applied["plan_binding_sha256"]
    else:
        with pytest.raises(ValueError):
            verify_application(bundle, context, NOW)


def test_schema_da_evidencia_fecha_tipo_iam_e_recusa_override():
    from pathlib import Path

    from scripts.promotion.phase_a import validate_evidence

    schema = json.loads(
        (Path(__file__).resolve().parents[3] / "release/phase-a-evidence.schema.json").read_bytes()
    )
    assert schema["additionalProperties"] is False
    assert schema["$defs"]["iam"]["properties"]["proof_kind"]["const"] == (
        "iam-readback-and-evaluation"
    )
    with pytest.raises(ValueError):
        validate_evidence({"schema_version": 1, "proof_kind": "simulation"}, NOW)


@pytest.mark.parametrize("mutation", [None, "pre", "pending", "expired", "mixed", "ledger", "paid"])
def test_phase_a_exige_custo_active_sem_selfhash_ou_fallback(mutation):
    from scripts.promotion._plan_contract import verify_application

    bundle, context = _application()
    applied = json.loads(bundle["applied"])
    cost = applied["plan"]["cost"]
    if mutation == "pre":
        cost.update(
            scope="pre-activation",
            cloudfront={"plan": None, "status": None},
            activation="blocked",
            dns="blocked",
            workloads="blocked",
            unbound_monthly_usd="1.00",
        )
    elif mutation == "pending":
        cost["cloudfront"]["status"] = "PENDING"
    elif mutation == "expired":
        cost["cloudfront"]["expires_at"] = "2026-11-07T20:59:59Z"
    elif mutation == "mixed":
        cost["cloudfront"]["waf_binding_sha256"] = "f" * 64
    elif mutation == "ledger":
        cost["expires_at"] = "2026-11-07T20:59:59Z"
    elif mutation == "paid":
        cost["paid_actions_limit_usd"] = "0.01"
    _rebind_application(bundle, applied)
    if mutation is None:
        result = verify_application(bundle, context, NOW)
        report = json.loads(bundle["reports"][result["plan"]["cost"]["evidence_sha256"]])
        assert "evidence_sha256" not in report["measurement"]["cost"]
        assert result["plan"]["cost"]["scope"] == "active"
    else:
        with pytest.raises(ValueError):
            verify_application(bundle, context, NOW)


@pytest.mark.parametrize("role_only", [False, True])
@pytest.mark.parametrize(
    "mutation", [None, "expression", "producer", "before", "source", "role_name"]
)
def test_unknown_iam_exige_grafo_e_fonte_independentes(role_only, mutation):
    from scripts.promotion.phase_a import review_phase_a_plan
    from tests.production.delivery._phase_a_symbolic_fixtures import symbolic_phase_a

    plan, context, _, _ = symbolic_phase_a("verifier" if role_only else "seeder")
    approved, files = context["approved_graph"], context["source_files"]
    if mutation == "expression":
        module = "processing" if role_only else "runtime_iam"
        plan["configuration"]["root_module"]["module_calls"][module]["module"]["resources"][0][
            "expressions"
        ]["policy"]["references"] = ["var.arbitrary_role"]
    elif mutation == "producer":
        plan["resource_changes"][0]["change"]["actions"] = ["no-op"]
    elif mutation == "before":
        plan["resource_changes"][1]["change"]["before"] = {"policy": '{"Statement":[]}'}
    elif mutation == "source":
        files[next(iter(files))] += b"changed"
    elif mutation == "role_name":
        plan["resource_changes"][0]["change"]["after_unknown"]["name"] = True
    if mutation is None:
        proof = review_phase_a_plan(plan, context)
        assert proof["governance_sha256"] == _digest(_bytes(approved) + b"\n")[7:]
    else:
        with pytest.raises(ValueError):
            review_phase_a_plan(plan, context)


def test_codec_interrompe_orcamento_utf8_antes_de_expandir_strings():
    from scripts.promotion._canonical import canonical_bytes

    with pytest.raises(ValueError, match="output_size"):
        canonical_bytes({"s": "\0" * 5}, limit=20)


@pytest.mark.parametrize("mutation", [None, "iam", "state", "artifact"])
def test_finalizacao_autenticada_guarda_uma_evidencia_e_retry_so_le(github, mutation):
    from scripts.promotion.phase_a import finalize_phase_a
    from scripts.promotion.promote_ecr import promote_candidate

    registry = RegistryBoundary()
    reference = _candidate_reference(github, registry)
    promotion = promote_candidate(
        reference,
        github,
        registry,
        {"account_id": ACCOUNT, "region": REGION, "now": NOW, "candidate_creator": _creator_pin()},
    )
    prepared: dict[str, Any] = _finalization_fixture(github, promotion, FixtureClock()) | {
        "registry": registry
    }
    api = RuntimeBoundary(github, prepared)
    references = prepared["bundle"]["references"]
    api.mutation = mutation
    if mutation == "artifact":
        github.routes[f"actions/artifacts/{references['applied']['artifact_id']}"]["digest"] = (
            "sha256:" + "f" * 64
        )
    if mutation is not None:
        with pytest.raises(ValueError):
            finalize_phase_a(promotion, references, api, prepared["governed"])
        assert api.writes == 0
    else:
        before = list(registry.copied)
        first = finalize_phase_a(promotion, references, api, prepared["governed"])
        again = finalize_phase_a(promotion, references, api, prepared["governed"])
        assert first == again
        assert first["iam"]["proof_kind"] == "iam-readback-and-evaluation"
        assert api.writes == 1
        assert registry.copied == before
    assert api.registrations == 0


def test_prepare_recusa_colisao_sem_copiar_imagem_ou_trocar_id(github):
    from scripts.promotion.phase_a import prepare_phase_a

    registry = RegistryBoundary()
    reference = _candidate_reference(github, registry)
    github.reserve_canary_binding = lambda binding: binding | {"binding_sha256": "f" * 64}
    governed = {
        "account_id": ACCOUNT,
        "region": REGION,
        "now": NOW,
        "candidate_creator": _creator_pin(),
    }
    with pytest.raises(ValueError, match="recovery_required"):
        prepare_phase_a(reference, github, registry, governed)
    assert registry.copied == []


def _mutate_application(bundle, applied, mutation):
    if mutation == "extra":
        applied["override"] = True
    elif mutation == "binding":
        applied["plan_binding_sha256"] = "0" * 64
    elif mutation == "package":
        bundle["files"]["plan.bin"] += b"changed"
    elif mutation == "state":
        bundle["state_after"] = bundle["state_after"].replace(b'"serial":1', b'"serial":2')
    elif mutation == "outputs":
        applied["apply"]["outputs"]["outputs_sha256"] = "0" * 64
    elif mutation == "lease":
        applied["apply"]["lease"]["terminal_status"] = "held"
    elif mutation == "cost":
        applied["plan"]["cost"]["aggregate_monthly_usd"] = "15.00"
    elif mutation == "backend":
        applied["plan"]["backend"]["configuration"]["workspace"] = "other"
    elif mutation == "purpose":
        applied["plan"]["purpose"] = "bootstrap"


@pytest.mark.parametrize("late", [False, True])
def test_recibo_exige_timestamp_de_publicacao_confirmado_pela_api(github, late):
    from scripts.promotion.phase_a import finalize_phase_a
    from scripts.promotion.promote_ecr import promote_candidate

    registry = RegistryBoundary()
    reference = _candidate_reference(github, registry)
    promotion = promote_candidate(
        reference,
        github,
        registry,
        {"account_id": ACCOUNT, "region": REGION, "now": NOW, "candidate_creator": _creator_pin()},
    )
    prepared: dict[str, Any] = _finalization_fixture(github, promotion, FixtureClock()) | {
        "registry": registry
    }
    if late:
        artifact_id = prepared["bundle"]["references"]["publication"]["artifact_id"]
        github.routes[f"actions/artifacts/{artifact_id}"]["created_at"] = "2026-11-07T20:51:46Z"
    boundary = RuntimeBoundary(github, prepared)
    if late:
        with pytest.raises(ValueError, match="publication=artifact_time"):
            finalize_phase_a(
                promotion, prepared["bundle"]["references"], boundary, prepared["governed"]
            )
    else:
        assert (
            finalize_phase_a(
                promotion, prepared["bundle"]["references"], boundary, prepared["governed"]
            )["schema_version"]
            == 1
        )


def test_artifact_generico_respeita_branch_governada_do_produtor(github):
    from scripts.promotion._artifact_origin import authenticate_artifacts
    from tests.production.delivery._phase_a_fixtures import _public_artifact

    reference = _public_artifact(github, "baseline", {"proof.json": b"{}\n"}, (201, "baseline"))
    github.routes["actions/runs/201"]["head_branch"] = "main"
    github.routes[f"actions/artifacts/{reference['artifact_id']}"]["workflow_run"][
        "head_branch"
    ] = "main"
    profile = {
        "repository": "VINIClUS/CnesData",
        "source_sha": SHA,
        "branch": "main",
        "event": "workflow_dispatch",
        "callers": [{"workflow_path": "baseline", "workflow_sha": SHA}],
        "reusables": {"baseline": []},
    }
    assert authenticate_artifacts({"baseline": reference}, github, profile, NOW)["baseline"][
        "members"
    ]


def test_artifact_r2_preserva_excecao_so_no_creator_governado(github):
    from scripts.promotion._artifact_origin import authenticate_r2_artifact

    reference = _candidate_reference(github)
    reference |= {"source_sha": SHA, **_creator_pin()}
    result = authenticate_r2_artifact(reference, github, _creator_pin(), NOW)
    assert sorted(result["members"]) == ["candidate.json", "candidate.json.sha256"]
    with pytest.raises(ValueError):
        authenticate_r2_artifact(reference | {"workflow_sha": SHA}, github, _creator_pin(), NOW)


def test_fingerprint_normaliza_json_e_recusa_chaves_duplicadas():
    from scripts.promotion.phase_a import resource_fingerprint

    assert resource_fingerprint("role", {"policy": '{"a": 1}'}) == resource_fingerprint(
        "role", {"policy": '{ "a":1 }'}
    )
    with pytest.raises(ValueError):
        resource_fingerprint("role", {"policy": '{"a":1,"a":1}'})


def _expected_symbolic_policy(kind):
    cluster = f"arn:aws:ecs:{REGION}:{ACCOUNT}:cluster/cnesdata-prod-processor"
    if kind == "seeder":
        resource = f"arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/cnesdata-prod-canary"
        condition = {
            "ForAllValues:StringEquals": {"dynamodb:LeadingKeys": ["TENANT#_GLOBAL"]},
            "Null": {"dynamodb:LeadingKeys": "false"},
        }
    else:
        resource = f"arn:aws:ecs:{REGION}:{ACCOUNT}:task-definition/cnesdata-prod-processor-audit:2"
        condition = {"ArnEquals": {"ecs:cluster": cluster}}
    statements = [
        {
            "Effect": "Allow",
            "Action": "dynamodb:PutItem" if kind == "seeder" else "ecs:RunTask",
            "Resource": resource,
            "Condition": condition,
        }
    ]
    if kind == "verifier":
        statements.append(
            {
                "Effect": "Allow",
                "Action": "ecs:DescribeTasks",
                "Resource": cluster.replace(":cluster/", ":task/") + "/*",
                "Condition": condition,
            }
        )
    return {"Version": "2012-10-17", "Statement": statements}


def _alter_symbolic_policy(policy, mutation):
    part, operation = mutation.split("-")
    statement = policy["Statement"][0]
    if part == "statement":
        policy["Statement"] = (
            []
            if operation == "drop"
            else policy["Statement"]
            + [{"Effect": "Allow", "Action": "iam:CreateUser", "Resource": "*"}]
        )
    elif part in {"action", "resource"}:
        field = part.title()
        statement[field] = [] if operation == "drop" else [statement[field], "*"]
    elif operation == "drop":
        statement.pop("Condition")
    else:
        statement["Condition"]["StringLike"] = {"aws:PrincipalArn": "*"}


@pytest.mark.parametrize("kind", ["seeder", "verifier"])
@pytest.mark.parametrize(
    "mutation",
    [
        None,
        "statement-add",
        "statement-drop",
        "action-add",
        "action-drop",
        "resource-add",
        "resource-drop",
        "condition-add",
        "condition-drop",
        "state-add",
    ],
)
def test_unknown_policy_resolve_grants_integrais_da_fonte_sem_widening(kind, mutation):
    from scripts.promotion.phase_a import review_phase_a_plan, verify_delta
    from tests.production.delivery._phase_a_symbolic_fixtures import (
        symbolic_phase_a,
        symbolic_state,
    )

    plan, context, observed, address = symbolic_phase_a(kind)
    policy = _expected_symbolic_policy(kind)
    proof = review_phase_a_plan(plan, context)
    if mutation and mutation != "state-add":
        _alter_symbolic_policy(policy, mutation)
    observed[address]["policy"] = json.dumps(policy)
    after = context | {"graph_proof": proof, "readback_resources": observed}
    if mutation is None or mutation == "state-add":
        verify_delta(plan, after)
        state = symbolic_state(observed)
        if mutation == "state-add":
            row = next(row for row in state["resources"] if row["type"] == "aws_iam_role_policy")
            changed = json.loads(row["instances"][0]["attributes"]["policy"])
            _alter_symbolic_policy(changed, "statement-add")
            row["instances"][0]["attributes"]["policy"] = json.dumps(changed)
            with pytest.raises(ValueError, match="iam=resolved_policy"):
                context["iam_resolver"].verify_state(state)
        else:
            context["iam_resolver"].verify_state(state)
    else:
        with pytest.raises(ValueError):
            verify_delta(plan, after)


@pytest.mark.parametrize("binding", ["input", "output", "producer"])
def test_resolver_exige_binding_real_e_readback_do_producer_canonico(binding):
    from scripts.promotion._iam_source import SourceResolver
    from scripts.promotion.phase_a import review_phase_a_plan, verify_delta
    from tests.production.delivery._phase_a_symbolic_fixtures import symbolic_phase_a

    plan, context, observed, address = symbolic_phase_a("verifier")
    observed[address]["policy"] = json.dumps(_expected_symbolic_policy("verifier"))
    if binding == "producer":
        proof = review_phase_a_plan(plan, context)
        observed.pop("module.runtime_iam.aws_iam_role.canary_verifier[0]")
        with pytest.raises(ValueError):
            verify_delta(plan, context | {"graph_proof": proof, "readback_resources": observed})
    else:
        calls = plan["configuration"]["root_module"]["module_calls"]
        node = (
            calls["processing"]["expressions"]["canary_verifier_role_id"]
            if binding == "input"
            else calls["runtime_iam"]["module"]["outputs"]["canary_verifier_role_id"]["expression"]
        )
        node["references"] = ["aws_iam_role.canary_task"]
        with pytest.raises(ValueError, match="iam=source_configuration_expression"):
            SourceResolver(plan, context["source_files"], context).precheck(address)


def test_loader_aceita_state_integral_maior_que_documento_normal(github):
    from scripts.promotion import phase_a
    from scripts.promotion._canonical import DOCUMENT_LIMIT, STATE_LIMIT, parse_document
    from tests.production.delivery._phase_a_symbolic_fixtures import (
        symbolic_phase_a,
        symbolic_readback_boundary,
    )

    plan, context, observed, address = symbolic_phase_a()
    observed[address]["policy"] = json.dumps(_expected_symbolic_policy("seeder"))
    proof = phase_a.review_phase_a_plan(plan, context)
    result, bundle, governed = symbolic_readback_boundary(github, (plan, context, observed), proof)
    assert DOCUMENT_LIMIT < len(bundle["state_after"]) < 128 * 1024**2
    with pytest.raises(ValueError, match="json=size"):
        parse_document(bundle["state_after"])
    phase_a._verify_resource_readbacks(result, bundle, github, governed)
    with pytest.raises(ValueError, match="state=readback_size"):
        phase_a._state_readback_size(STATE_LIMIT + 1)
