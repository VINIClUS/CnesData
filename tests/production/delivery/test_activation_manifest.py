"""Contrato sintético de activation; nenhuma fixture constitui aceite AWS."""

from __future__ import annotations

import copy
import hashlib
import importlib.util
import json
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

from release.build_activation import _r1_payload, prepare_ext2, render_inputs
from release.verify_activation import admit_ext2, canonical, verify_activation
from scripts.promotion._phase_a_evidence import validate_evidence
from scripts.promotion.phase_a import finalize_phase_a
from scripts.promotion.promote_ecr import promote_candidate
from tests.production.delivery import test_candidate_manifest as candidates
from tests.production.delivery._phase_a_fixtures import (
    ACCOUNT,
    FixtureClock,
    RegistryBoundary,
    _candidate_reference,
    _creator_pin,
    _public_artifact,
)
from tests.production.delivery._phase_a_fixtures import (
    NOW as BASE_NOW,
)
from tests.production.delivery._phase_a_runtime_fixtures import (
    RuntimeBoundary,
    _finalization_fixture,
)

github = candidates.github
ROOT = Path(__file__).resolve().parents[3]
NOW = BASE_NOW + timedelta(seconds=20)
EXT2_SHA = "d6c2fe101f5a5c2287aabc83ed73dd9f9b95f41f"


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


class Boundary:
    def __init__(self, api):
        self.api = api
        self.calls = []

    def read(self, path, *, raw=False, maximum=8 * 1024**2):
        self.calls.append(path)
        route = path.removeprefix("repos/VINIClUS/CnesData/")
        if raw:
            artifact_id = int(route.split("/")[-2])
            artifact = self.api.routes[f"actions/artifacts/{artifact_id}"]
            data = candidates._zip(self.api.payloads[artifact["name"]])
            assert len(data) <= maximum
            return data
        return self.api.get(route.split("/attempts/")[0])


@pytest.fixture
def activation(github):
    registry = RegistryBoundary()
    ref = _candidate_reference(github, registry)
    for item in github.metadata:
        item.setdefault("created_at", "2026-11-07T20:00:00Z")
    promotion = promote_candidate(
        ref,
        github,
        registry,
        {
            "account_id": ACCOUNT,
            "region": "us-east-2",
            "now": NOW,
            "candidate_creator": _creator_pin(),
        },
    )
    prepared: Any = _finalization_fixture(github, promotion, FixtureClock()) | {
        "registry": registry
    }
    runtime = RuntimeBoundary(github, prepared)
    proof = finalize_phase_a(
        promotion, prepared["bundle"]["references"], runtime, prepared["governed"]
    )
    phase_ref = _public_artifact(
        github,
        "synthetic-phase-a",
        {"phase-a-evidence.json": canonical(proof)},
        (103, "synthetic-phase-a"),
    )
    phase_run = github.routes["actions/runs/103"]
    phase_run.update(status="completed", conclusion="success")
    phase_ref["workflow_path"] = phase_run["path"] = ".github/workflows/synthetic-phase-a.yml"
    request = _request(proof, phase_ref)
    governance = _governance(proof)
    github.routes["actions/runs/42"]["repository"] = {"full_name": "VINIClUS/CnesData"}
    return {
        "github": Boundary(github),
        "request": request,
        "governance": governance,
        "proof": proof,
        "registry": registry,
    }


def _request(proof, phase_ref):
    candidate_ref = proof["references"]["candidate"]
    inputs = {
        "candidate_reference": candidate_ref,
        "phase_a_reference": phase_ref,
        "retained_unit_task_definition_arns": proof["retained_after"],
    }
    inputs = copy.deepcopy(inputs)
    for reference in (inputs["candidate_reference"], inputs["phase_a_reference"]):
        for key in ("run_id", "run_attempt", "artifact_id"):
            reference[key] = str(reference[key])
    request = {
        "request_version": 1,
        "mode": "activation",
        "source_sha": proof["source_sha"],
        "inputs": inputs,
        "ext2_post_upload_receipt_v1": None,
    }
    return request


def _governance(proof):
    governance = {
        "provenance": {"source_sha": proof["source_sha"], "workflow_sha": EXT2_SHA},
        "storage": {"account_id": ACCOUNT, "region": "us-east-2"},
    }
    governance["consumer"] = {
        "repository": "VINIClUS/CnesData",
        "source_sha": proof["source_sha"],
        "files": [
            {"path": "release/" + name, "sha256": digest((ROOT / "release" / name).read_bytes())}
            for name in ("activation.schema.json", "build_activation.py", "verify_activation.py")
        ],
    }
    return governance


def prepare(case):
    return prepare_ext2(
        case["request"], github=case["github"], governance=case["governance"], now=NOW
    )


def admit(case, raw, **kwargs):
    return admit_ext2(
        raw,
        request=case["request"],
        github=case["github"],
        governance=case["governance"],
        now=NOW,
        **kwargs,
    )


def test_prepara_bytes_canonicos_e_admite_a_mesma_phase_a(activation):
    copied = list(activation["registry"].copied)
    raw = prepare(activation)
    value = json.loads(raw)
    admitted = admit(activation, raw)
    assert len(value) == 14
    assert set(admitted) == {"candidate_bytes", "phase_a_bytes", "activation_fields"}
    assert value["phase_a_evidence_digest"] == digest(admitted["phase_a_bytes"])
    assert admitted["activation_fields"] == {k: v for k, v in value.items() if k != "inputs"}
    assert raw == canonical(value)
    assert activation["registry"].copied == copied


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("release_id", "candidate-" + "f" * 40 + "-42-2"),
        ("candidate_sha256", "f" * 64),
        ("source_sha", "f" * 40),
        ("binding_sha256", "f" * 64),
        ("canary_id", "f" * 32),
        ("phase_a_evidence_digest", "f" * 64),
        ("ecr_uri", "mutable:latest"),
        ("environment", "develop"),
    ],
)
def test_recusa_artefato_de_outra_release(activation, field, value):
    payload = json.loads(prepare(activation))
    payload[field] = value
    with pytest.raises(ValueError):
        admit(activation, canonical(payload))


def test_recusa_revisao_registrada_de_novo(activation):
    payload = json.loads(prepare(activation))
    payload["revisions"]["unit"] = payload["revisions"]["unit"].rsplit(":", 1)[0] + ":999"
    with pytest.raises(ValueError, match="binding"):
        admit(activation, canonical(payload))


def test_recusa_contexto_de_assinatura_ausente(activation):
    payload = json.loads(prepare(activation))
    del payload["project"]
    with pytest.raises(ValueError, match="fields"):
        admit(activation, canonical(payload))


def test_renderer_poe_recovery_e_audit_fora_de_retained(activation):
    fields = admit(activation, prepare(activation))["activation_fields"]
    unit, recovery, audit = (fields["revisions"][k] for k in ("unit", "recovery", "audit"))
    rendered = render_inputs(fields, [unit, unit])
    assert rendered == {
        "processor_active_unit_task_definition_arn": unit,
        "processor_retained_unit_task_definition_arns": [unit],
        "scheduler_recovery_task_definition_arn": recovery,
        "scheduler_audit_task_definition_arn": audit,
    }
    with pytest.raises(ValueError):
        render_inputs(fields, [unit, recovery])


def test_renderer_exige_active_contido_em_retained(activation):
    fields = admit(activation, prepare(activation))["activation_fields"]
    with pytest.raises(ValueError, match="active_must_be_retained"):
        render_inputs(fields, [])


@pytest.mark.parametrize("mutation", ["digest", "expired", "run", "attempt", "missing"])
def test_recusa_artifact_adulterado_expirado_ou_misturado(activation, mutation):
    reference = activation["request"]["inputs"]["phase_a_reference"]
    api = activation["github"].api
    artifact = api.routes[f"actions/artifacts/{reference['artifact_id']}"]
    if mutation == "digest":
        api.payloads[artifact["name"]]["phase-a-evidence.json"] += b" "
    elif mutation == "expired":
        artifact["expires_at"] = "2026-01-01T00:00:00Z"
    elif mutation == "run":
        artifact["workflow_run"]["id"] = 999
    elif mutation == "attempt":
        api.routes[f"actions/runs/{reference['run_id']}"]["run_attempt"] = 999
    else:
        del api.routes[f"actions/artifacts/{reference['artifact_id']}"]
    with pytest.raises(ValueError):
        prepare(activation)


@pytest.mark.parametrize(
    "field",
    [
        "state_sha256",
        "outputs_sha256",
        "plan_binding_sha256",
        "route_fingerprints",
        "processor_task_definition_arns",
        "iam",
        "promotion",
    ],
)
def test_recusa_bytes_phase_a_adulterados(activation, field):
    reference = activation["request"]["inputs"]["phase_a_reference"]
    api = activation["github"].api
    artifact = api.routes[f"actions/artifacts/{reference['artifact_id']}"]
    payload = json.loads(api.payloads[artifact["name"]]["phase-a-evidence.json"])
    payload[field] = None
    api.payloads[artifact["name"]]["phase-a-evidence.json"] = canonical(payload)
    with pytest.raises(ValueError):
        prepare(activation)


@pytest.mark.parametrize("kind", ["duplicate", "float", "depth", "oversize", "noncanonical"])
def test_json_falha_fechado(activation, kind):
    raw = prepare(activation)
    invalid = {
        "duplicate": b'{"a":1,"a":2}\n',
        "float": b'{"a":NaN}\n',
        "depth": b"[" * 30 + b"0" + b"]" * 30 + b"\n",
        "oversize": b" " * 262145,
        "noncanonical": raw + b" ",
    }
    with pytest.raises(ValueError):
        admit(activation, invalid[kind])


def test_historico_reconstroi_refs_assinadas_sem_estado_oculto(activation):
    raw = prepare(activation)
    expected = admit(activation, raw)
    activation["request"]["inputs"] = None
    activation["request"]["mode"] = "historical"
    assert admit(activation, raw, historical=True) == expected
    with pytest.raises(ValueError, match="expiry"):
        admit_ext2(
            raw,
            request=activation["request"],
            github=activation["github"],
            governance=activation["governance"],
            now=NOW + timedelta(days=60),
            historical=True,
        )


def test_slot_receipt_obrigatorio_antes_da_verificacao(activation):
    with pytest.raises(ValueError, match="receipt"):
        verify_activation(
            github=activation["github"],
            s3=None,
            governance=activation["governance"],
            now=NOW,
            ext2_post_upload_receipt_v1=None,
        )


def test_builder_carrega_apenas_verificador_autenticado(tmp_path, activation):
    source = ROOT / "release/build_activation.py"
    target = tmp_path / source.name
    target.write_bytes(source.read_bytes())
    spec = importlib.util.spec_from_file_location("isolated_builder", target)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    with pytest.raises(ValueError, match="closure"):
        module.prepare_ext2(
            activation["request"],
            github=activation["github"],
            governance=activation["governance"],
            now=NOW,
        )


def test_recusa_dependencia_previa_ausente(activation):
    api = activation["github"].api
    ref = activation["proof"]["references"]["planned"]
    artifact = api.routes[f"actions/artifacts/{ref['artifact_id']}"]
    index = json.loads(api.payloads[artifact["name"]]["index.json"])
    dependency = index["admission_proof"]["artifacts"][0]["reference"]
    del api.routes[f"actions/artifacts/{dependency['artifact_id']}"]
    with pytest.raises(ValueError, match="artifact_unavailable"):
        prepare(activation)


def test_recusa_admissao_a3_de_outro_candidato_com_zip_autentico(activation):
    api = activation["github"].api
    proof = activation["proof"]
    reference = proof["references"]["planned"]
    artifact = api.routes[f"actions/artifacts/{reference['artifact_id']}"]
    members = api.payloads[artifact["name"]]
    index = json.loads(members["index.json"])
    index["admission_proof"]["candidate_sha256"] = "f" * 64
    members["index.json"] = canonical(index)
    api.refresh(artifact["name"])
    reference["artifact_digest"] = artifact["digest"]
    phase_ref = activation["request"]["inputs"]["phase_a_reference"]
    phase = api.routes[f"actions/artifacts/{phase_ref['artifact_id']}"]
    api.payloads[phase["name"]]["phase-a-evidence.json"] = canonical(proof)
    api.refresh(phase["name"])
    phase_ref["artifact_digest"] = phase["digest"]
    with pytest.raises(ValueError, match="admission_binding"):
        prepare(activation)


@pytest.mark.parametrize(
    "mutation", ["iam", "intervalo", "promocao", "expiry", "fingerprint", "schema"]
)
def test_recusa_evidencia_semantica_invalida_mesmo_com_zip_autentico(activation, mutation):
    proof = activation["proof"]
    if mutation == "iam":
        proof["iam"]["proof_kind"] = "simulation"
    elif mutation == "intervalo":
        proof["iam"]["second_readback_seconds"] = "1.000000"
    elif mutation == "promocao":
        proof["promotion"]["manifest_sha256"] = "f" * 64
    elif mutation == "expiry":
        proof["expires_at"] = "2026-11-01T00:00:00Z"
    elif mutation == "schema":
        proof["schema_version"] = True
    else:
        proof["route_fingerprints"] = [{"address": "route", "fingerprint_sha256": "mutable"}]
    api = activation["github"].api
    reference = activation["request"]["inputs"]["phase_a_reference"]
    artifact = api.routes[f"actions/artifacts/{reference['artifact_id']}"]
    api.payloads[artifact["name"]]["phase-a-evidence.json"] = canonical(proof)
    api.refresh(artifact["name"])
    reference["artifact_digest"] = artifact["digest"]
    with pytest.raises(ValueError):
        prepare(activation)


def test_aceita_json_publicado_por_jq_sem_alterar_os_bytes(activation):
    admitted = admit(activation, prepare(activation))
    candidate = json.loads(admitted["candidate_bytes"])
    original = activation["github"].api.payloads["production-published-data_processor"]
    raw = (json.dumps(json.loads(original["manifest.json"]), indent=2) + "\n").encode()
    _r1_payload(candidate, "production-published-data_processor", {"manifest.json": raw})


def _reseal_phase(case, payload):
    reference = case["request"]["inputs"]["phase_a_reference"]
    api = case["github"].api
    artifact = api.routes[f"actions/artifacts/{reference['artifact_id']}"]
    raw = canonical(case["proof"])
    api.payloads[artifact["name"]]["phase-a-evidence.json"] = raw
    api.refresh(artifact["name"])
    reference["artifact_digest"] = artifact["digest"]
    payload["inputs"] = copy.deepcopy(case["request"]["inputs"])
    payload["phase_a_evidence_digest"] = digest(raw)
    assert reference["artifact_digest"] == "sha256:" + digest(
        candidates._zip(api.payloads[artifact["name"]])
    )
    return canonical(payload)


@pytest.mark.parametrize("mutation", ["path", "event", "creator", "provenance"])
def test_revalida_procedencia_r2_com_transporte_autentico(activation, mutation):
    payload = json.loads(prepare(activation))
    reference = activation["request"]["inputs"]["candidate_reference"]
    api = activation["github"].api
    run = api.routes[f"actions/runs/{reference['run_id']}"]
    if mutation == "path":
        run["path"] = reference["workflow_path"] = ".github/workflows/unrelated-success.yml"
        activation["proof"]["references"]["candidate"]["workflow_path"] = run["path"]
    elif mutation == "event":
        run["event"] = "push"
    elif mutation == "creator":
        run["head_sha"] = "f" * 40
    else:
        activation["proof"]["source_provenance_sha256"] = "f" * 64
    raw = _reseal_phase(activation, payload)
    for callback in (lambda: prepare(activation), lambda: admit(activation, raw)):
        with pytest.raises(ValueError):
            callback()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("scan_artifact_id", 999999),
        ("scan_artifact_sha256", "f" * 64),
        ("scan_sha256", "f" * 64),
        ("scan_job_id", 999999),
        ("scan_workflow_sha256", "f" * 64),
    ],
)
def test_revalida_scan_r1_com_transporte_autentico(activation, field, value):
    payload = json.loads(prepare(activation))
    activation["proof"]["promotion"][field] = value
    raw = _reseal_phase(activation, payload)
    for callback in (lambda: prepare(activation), lambda: admit(activation, raw)):
        with pytest.raises(ValueError, match="scan_binding"):
            callback()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("blob_digests", []),
        ("blob_digests", ["mutable"]),
        ("blob_digests", ["sha256:" + "f" * 64] * 130),
        ("scan_artifact_id", True),
        ("scan_job_id", 9007199254740992),
        ("extra", "unapproved"),
    ],
)
def test_valida_schema_consumido_aninhado_com_transporte_autentico(activation, field, value):
    payload = json.loads(prepare(activation))
    activation["proof"]["promotion"][field] = value
    raw = _reseal_phase(activation, payload)
    with pytest.raises(ValueError):
        validate_evidence(activation["proof"], NOW)
    for callback in (lambda: prepare(activation), lambda: admit(activation, raw)):
        with pytest.raises(ValueError):
            callback()


def test_schema_consumido_preserva_o_documento_governante():
    annotation = json.loads((ROOT / "release/activation.schema.json").read_bytes())
    source = json.loads((ROOT / "release/phase-a-evidence.schema.json").read_bytes())
    assert annotation["x-consumed-schemas"]["phase-a"]["schema"] == source
