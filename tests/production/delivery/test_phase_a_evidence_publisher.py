"""Publicador da evidência Phase A: campos autenticados contra o envelope e a promoção."""

from __future__ import annotations

import hashlib
import json
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
import yaml

from release.verify_activation import canonical
from release.verify_candidate import canonical as promotion_canonical
from scripts.promotion import publish_phase_a_evidence as publisher
from scripts.promotion._artifact_origin import artifact_content_hash
from scripts.promotion.phase_a import finalize_phase_a
from scripts.promotion.promote_ecr import promote_candidate
from tests.production.delivery import _phase_a_runtime_fixtures as runtime_fixtures
from tests.production.delivery import test_candidate_manifest as candidates
from tests.production.delivery._phase_a_fixtures import (
    ACCOUNT,
    FixtureClock,
    RegistryBoundary,
    _candidate_reference,
    _creator_pin,
    _public_artifact,
)
from tests.production.delivery._phase_a_fixtures import NOW as BASE_NOW
from tests.production.delivery._phase_a_runtime_fixtures import (
    RuntimeBoundary,
    _finalization_fixture,
)
from tests.production.delivery.test_activation_manifest import Boundary

github = candidates.github
ROOT = Path(__file__).resolve().parents[3]
WORKFLOW = ROOT / ".github/workflows/phase-a-evidence.yml"
NOW = BASE_NOW + timedelta(seconds=20)
INPUTS = {
    "expected_source_sha",
    "evidence",
    "evidence_sha256",
    "envelope",
    "envelope_sha256",
    "promotion",
    "promotion_sha256",
}


def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _com_manifesto(promotion_bytes: bytes) -> Any:
    original = runtime_fixtures._runtime_common

    def common(api: Any, governed: Any) -> Any:
        items, profiles = original(api, governed)
        manifest = {
            "objects": [
                {"name": "promotion.json", "sha256": sha(promotion_bytes),
                 "size": len(promotion_bytes)}
            ],
            "run_attempt": 1,
            "run_id": 207,
            "source_sha": governed["source_sha"],
        }
        members = {"phase-a-public.json": canonical(manifest)}
        reference = _public_artifact(
            api, "synthetic-phase-a-prepare", members, (207, "synthetic-source")
        )
        for item in items:
            if item["purpose"] == "phase-inputs":
                item.update(reference=reference, content_sha256=artifact_content_hash(members))
        return items, profiles

    return common


@pytest.fixture
def caso(github: Any, monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    registry = RegistryBoundary()
    ref = _candidate_reference(github, registry)
    for item in github.metadata:
        item.setdefault("created_at", "2026-11-07T20:00:00Z")
    governed = {
        "account_id": ACCOUNT,
        "region": "us-east-2",
        "now": NOW,
        "candidate_creator": _creator_pin(),
    }
    promotion = promote_candidate(ref, github, registry, governed)
    promotion_bytes = promotion_canonical(promotion)
    monkeypatch.setattr(runtime_fixtures, "_runtime_common", _com_manifesto(promotion_bytes))
    prepared: Any = _finalization_fixture(github, promotion, FixtureClock()) | {
        "registry": registry
    }
    references = prepared["bundle"]["references"]
    proof = finalize_phase_a(
        promotion, references, RuntimeBoundary(github, prepared), prepared["governed"]
    )
    receipt = json.loads(prepared["bundle"]["publication"])
    return {
        "api": Boundary(github),
        "evidence": proof | {"created_at": receipt["verified_at"]},
        "envelope": prepared["bundle"]["applied"],
        "promotion": promotion_bytes,
    }


def documentos(caso: dict[str, Any], **trocas: bytes) -> dict[str, bytes]:
    brutos = {
        "evidence": canonical(caso["evidence"]),
        "envelope": caso["envelope"],
        "promotion": caso["promotion"],
    }
    return brutos | trocas


def publicar(caso: dict[str, Any], brutos: dict[str, bytes]) -> bytes:
    return publisher.publish(brutos, caso["api"], NOW)


def evidencia_com(caso: dict[str, Any], mudar: Any) -> dict[str, bytes]:
    evidence = json.loads(canonical(caso["evidence"]))
    mudar(evidence)
    return documentos(caso, evidence=canonical(evidence))


def _retida_extra(evidence: dict[str, Any]) -> None:
    extra = evidence["processor_task_definition_arns"]["unit"].rsplit(":", 1)[0] + ":424242"
    for key in ("retained_before", "retained_after"):
        evidence[key] = sorted({*evidence[key], extra})


CAMPOS_DO_ENVELOPE = {
    "state_sha256": lambda e: e.update(state_sha256="f" * 64),
    "outputs_sha256": lambda e: e.update(outputs_sha256="f" * 64),
    "plan_binding_sha256": lambda e: e.update(plan_binding_sha256="f" * 64),
    "revisao_recovery": lambda e: e["processor_task_definition_arns"].update(
        recovery=e["processor_task_definition_arns"]["recovery"].rsplit(":", 1)[0] + ":999"
    ),
    "retidas": _retida_extra,
    "fingerprint": lambda e: e["resource_fingerprints"][0].update(fingerprint_sha256="f" * 64),
    "expires_at": lambda e: e.update(expires_at="2026-11-07T21:20:00Z"),
    "custo": lambda e: e["cost"].update(product_monthly_usd="0.01"),
}

CAMPOS_DA_PROMOCAO = {
    "blob_digests": lambda e: e["promotion"].update(blob_digests=["sha256:" + "f" * 64]),
    "ecr_image": lambda e: e.update(ecr_image=e["ecr_image"].replace("/", "/outro-", 1)),
    "source_provenance_sha256": lambda e: e.update(source_provenance_sha256="f" * 64),
    "scan_job_id": lambda e: e["promotion"].update(scan_job_id=e["promotion"]["scan_job_id"] + 1),
    "r1_verification": lambda e: e["r1_verification"].update(
        check_suite_id=e["r1_verification"]["check_suite_id"] + 1
    ),
}


def test_publica_os_bytes_exatos_da_evidencia_autenticada(caso):
    brutos = documentos(caso)
    assert publicar(caso, brutos) == brutos["evidence"]


def test_recusa_evidencia_nao_canonica(caso):
    brutos = documentos(caso, evidence=json.dumps(caso["evidence"], indent=1).encode())
    with pytest.raises(ValueError):
        publicar(caso, brutos)


def test_decodifica_exatamente_os_bytes_codificados(caso):
    for raw in documentos(caso).values():
        assert publisher.decode_document(publisher.encode_document(raw), sha(raw)) == raw


@pytest.mark.parametrize("nome", ["evidence", "envelope", "promotion"])
def test_recusa_documento_cujo_sha256_diverge(caso, nome):
    raw = documentos(caso)[nome]
    with pytest.raises(ValueError, match="sha256"):
        publisher.decode_document(publisher.encode_document(raw), "0" * 64)


def test_recusa_payload_acima_do_limite():
    raw = b" " * (publisher.LIMIT + 1)
    with pytest.raises(ValueError, match="size"):
        publisher.decode_document(publisher.encode_document(raw), sha(raw))


def test_recusa_envelope_que_nao_e_o_do_indice_aplicado(caso):
    envelope = json.loads(caso["envelope"])
    envelope["apply"]["lease"]["renewal_count"] += 1
    with pytest.raises(ValueError, match="envelope"):
        publicar(caso, documentos(caso, envelope=canonical(envelope)))


def test_recusa_promotion_que_nao_e_a_do_manifesto_phase_a_prepare(caso):
    promotion = json.loads(caso["promotion"])
    promotion["expires_at"] = "2026-11-08T00:00:00Z"
    with pytest.raises(ValueError, match="promotion"):
        publicar(caso, documentos(caso, promotion=promotion_canonical(promotion)))


@pytest.mark.parametrize("campo", sorted(CAMPOS_DO_ENVELOPE))
def test_recusa_campo_que_diverge_do_envelope(caso, campo):
    with pytest.raises(ValueError, match="envelope_field"):
        publicar(caso, evidencia_com(caso, CAMPOS_DO_ENVELOPE[campo]))


@pytest.mark.parametrize("campo", sorted(CAMPOS_DA_PROMOCAO))
def test_recusa_campo_que_diverge_da_promocao(caso, campo):
    with pytest.raises(ValueError, match="promotion_field"):
        publicar(caso, evidencia_com(caso, CAMPOS_DA_PROMOCAO[campo]))


def test_recusa_created_at_diferente_do_verified_at_do_recibo(caso):
    brutos = evidencia_com(caso, lambda e: e.update(created_at="2026-11-07T20:51:09Z"))
    with pytest.raises(ValueError, match="created_at"):
        publicar(caso, brutos)


def test_recusa_recibo_posterior_ao_relogio(caso):
    for artifact in caso["api"].api.metadata:
        artifact["created_at"] = "2026-11-07T20:00:00Z"
    with pytest.raises(ValueError, match="anchor"):
        publisher.publish(documentos(caso), caso["api"], BASE_NOW - timedelta(minutes=30))


def test_main_le_o_ambiente_e_grava_somente_a_evidencia(caso, tmp_path, monkeypatch):
    brutos = documentos(caso)
    for nome, raw in brutos.items():
        monkeypatch.setenv(nome.upper(), publisher.encode_document(raw))
        monkeypatch.setenv(nome.upper() + "_SHA256", sha(raw))
    monkeypatch.setenv("GITHUB_TOKEN", "sintetico")
    monkeypatch.setattr(publisher, "GitHubReader", lambda token: caso["api"])
    monkeypatch.setattr(publisher, "_now", lambda: NOW)
    saida = tmp_path / "phase-a-evidence.json"
    publisher.main(["--output", str(saida)])
    assert saida.read_bytes() == brutos["evidence"]


def test_workflow_publica_um_unico_arquivo_por_dispatch_sob_environment_production():
    workflow = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    gatilhos = workflow["on"] if "on" in workflow else workflow[True]
    assert list(gatilhos) == ["workflow_dispatch"]
    assert set(gatilhos["workflow_dispatch"]["inputs"]) == INPUTS
    assert workflow["permissions"] == {}
    (job,) = workflow["jobs"].values()
    assert job["environment"] == "production"
    assert job["permissions"] == {"contents": "read", "actions": "read", "checks": "read"}
    passos = job["steps"]
    uploads = [p for p in passos if str(p.get("uses", "")).startswith("actions/upload-artifact@")]
    assert len(uploads) == 1
    assert uploads[0]["with"]["path"].endswith("/phase-a-evidence.json")
    assert any("scripts.promotion.publish_phase_a_evidence" in p.get("run", "") for p in passos)
