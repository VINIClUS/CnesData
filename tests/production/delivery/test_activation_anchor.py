"""Frescor da Phase A ancorado no verified_at autenticado do receipt de publicação."""

from __future__ import annotations

import json
from datetime import timedelta

import pytest

from release.build_activation import prepare_ext2
from release.verify_activation import admit_ext2, canonical
from tests.production.delivery import test_activation_manifest as manifest

activation = manifest.activation
github = manifest.github
NOW = manifest.NOW
LATE = NOW - timedelta(seconds=20) + timedelta(minutes=90)
REPO = "repos/VINIClUS/CnesData/"


def _prepare(case, now):
    return prepare_ext2(
        case["request"], github=case["github"], governance=case["governance"], now=now
    )


def _admit(case, raw, now, *, historical=False):
    return admit_ext2(
        raw,
        request=case["request"],
        github=case["github"],
        governance=case["governance"],
        now=now,
        historical=historical,
    )


def _artifact(case, name):
    ref = case["proof"]["references"][name]
    return case["github"].api.routes[f"actions/artifacts/{ref['artifact_id']}"], ref


def _rebind(case, name):
    artifact, ref = _artifact(case, name)
    case["github"].api.refresh(artifact["name"])
    ref["artifact_digest"] = artifact["digest"]


def _publish_proof(case):
    api = case["github"].api
    phase_ref = case["request"]["inputs"]["phase_a_reference"]
    phase = api.routes[f"actions/artifacts/{phase_ref['artifact_id']}"]
    api.payloads[phase["name"]]["phase-a-evidence.json"] = canonical(case["proof"])
    api.refresh(phase["name"])
    phase_ref["artifact_digest"] = phase["digest"]


def _receipt(case, **fields):
    artifact, _ = _artifact(case, "publication")
    members = case["github"].api.payloads[artifact["name"]]
    members["receipt.json"] = canonical(json.loads(members["receipt.json"]) | fields)
    _rebind(case, "publication")
    _publish_proof(case)


def _dependency_expiry(case, purpose, expires_at):
    artifact, ref = _artifact(case, "applied")
    members = case["github"].api.payloads[artifact["name"]]
    index = json.loads(members["index.json"])
    for item in index["admission_proof"]["artifacts"]:
        if item["purpose"] == purpose:
            item["expires_at"] = expires_at
    members["index.json"] = canonical(index)
    _rebind(case, "applied")
    _receipt(case, index_artifact_digest=ref["artifact_digest"])


def test_activation_admite_phase_a_com_plano_expirado_se_receipt_na_janela(activation):
    assert activation["proof"]["expires_at"] == "2026-11-07T21:30:00Z"
    raw = _prepare(activation, LATE)
    assert raw == _prepare(activation, NOW)
    assert _admit(activation, raw, LATE) == _admit(activation, raw, NOW)


def test_historico_admite_payload_da_janela_apos_expirar_o_plano(activation):
    raw = _prepare(activation, NOW)
    expected = _admit(activation, raw, NOW)
    activation["request"]["inputs"] = None
    activation["request"]["mode"] = "historical"
    assert _admit(activation, raw, LATE, historical=True) == expected


def test_reescrever_o_receipt_sem_mudar_valores_preserva_a_admissao(activation):
    _receipt(activation, verified_at="2026-11-07T20:52:00Z")
    assert json.loads(_prepare(activation, NOW))


def test_admite_evidencia_montada_depois_de_expirar_o_plano(activation):
    activation["proof"]["created_at"] = "2026-11-07T22:00:00Z"
    _publish_proof(activation)
    assert json.loads(_prepare(activation, LATE))


def test_recusa_receipt_verificado_depois_do_now(activation):
    _receipt(activation, verified_at="2026-11-07T21:00:30Z")
    with pytest.raises(ValueError, match="phase_a_checkpoint"):
        _prepare(activation, NOW)


def test_recusa_receipt_verificado_apos_expirar_a_prova(activation):
    activation["proof"]["created_at"] = "2026-11-07T22:00:00Z"
    _receipt(activation, verified_at="2026-11-07T21:45:00Z")
    with pytest.raises(ValueError, match="expiry"):
        _prepare(activation, LATE)


def test_recusa_receipt_verificado_depois_da_emissao_da_evidencia(activation):
    _receipt(activation, verified_at="2026-11-07T21:00:16Z")
    with pytest.raises(ValueError, match="expiry"):
        _prepare(activation, NOW)


def test_recusa_receipt_verificado_antes_das_janelas_de_admissao(activation):
    _receipt(activation, verified_at="2026-11-07T20:29:00Z")
    with pytest.raises(ValueError, match="admission_report"):
        _prepare(activation, NOW)


def test_dependencia_vale_no_anchor_mesmo_expirada_no_now(activation):
    _dependency_expiry(activation, "backend", "2026-11-07T22:00:00Z")
    assert json.loads(_prepare(activation, LATE))


def test_recusa_dependencia_expirada_antes_do_anchor(activation):
    _dependency_expiry(activation, "backend", "2026-11-07T20:50:00Z")
    with pytest.raises(ValueError, match="expiry"):
        _prepare(activation, NOW)


@pytest.mark.parametrize("name", ["phase_a", "planned", "applied", "publication", "dependency"])
def test_recusa_artifact_github_expirado_no_now_mesmo_com_anchor_valido(activation, name):
    api = activation["github"].api
    if name == "phase_a":
        artifact_id = activation["request"]["inputs"]["phase_a_reference"]["artifact_id"]
    elif name == "dependency":
        artifact_id = _artifact(activation, "planned")[1]["artifact_id"]
        artifact_id = json.loads(
            api.payloads[api.routes[f"actions/artifacts/{artifact_id}"]["name"]]["index.json"]
        )["admission_proof"]["artifacts"][0]["reference"]["artifact_id"]
    else:
        artifact_id = _artifact(activation, name)[1]["artifact_id"]
    api.routes[f"actions/artifacts/{artifact_id}"]["expires_at"] = "2026-11-07T22:00:00Z"
    with pytest.raises(ValueError, match="expiry"):
        _prepare(activation, LATE)
    calls = activation["github"].calls
    assert f"{REPO}actions/artifacts/{artifact_id}" in calls
    assert f"{REPO}actions/artifacts/{artifact_id}/zip" not in calls
