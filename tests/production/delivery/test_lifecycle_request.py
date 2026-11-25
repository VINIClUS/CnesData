"""Descriptor e binding do caller de lifecycle contra o consumidor do infra-ansible (P25)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from scripts.promotion.lifecycle_request import (
    CallerClaim,
    caller_binding,
    caller_binding_sha256,
    descriptor_sha256,
    request_descriptor,
)
from tests.production.delivery import _ia_lifecycle_consumer as ia

VECTORS = Path(__file__).parent / "vectors"
IA_VECTOR = VECTORS / "ia_lifecycle_request_names_vector.json"
FULL_VECTOR = VECTORS / "lifecycle_request_names_vector.json"
SOURCE = "a" * 40
PRODUCER = {"workflow_sha": "b" * 40, "source_closure_sha256": "c" * 64}
REFERENCE = {"step": "inputs"}
STEPS = {
    "application-inventory": {"role": "application", "purpose": "application-prepare",
                              "input_jobs": {}},
    "plan-operational": {"role": "plan", "purpose": "application-prepare",
                         "input_jobs": {"inputs": {"name": "inputs", "step": "inputs"}}},
}
PURPOSES = ("promotion", "mode-readiness-drill")


def _claim(purpose: str = "promotion", **override: Any) -> CallerClaim:
    values: dict[str, Any] = {
        "application_purpose": purpose, "source_sha": SOURCE, "run_id": 42, "run_attempt": 1,
        "producer_workflow_sha": PRODUCER["workflow_sha"],
        "producer_source_closure_sha256": PRODUCER["source_closure_sha256"],
    }
    return CallerClaim(**(values | override))


def _governance(step: str) -> dict[str, Any]:
    return {"source_sha": SOURCE, "lifecycle_steps": STEPS, "role": STEPS[step]["role"],
            "purpose": STEPS[step]["purpose"]}


def _admit(raw: bytes, claim: CallerClaim, step: str, raw_sha: str | None = None) -> Any:
    caller = ia.authentic_caller(caller_binding(claim), PRODUCER)
    return ia.admit_request(raw, raw_sha or ia.sha(raw), _governance(step), caller)


def _descriptor(claim: CallerClaim, step: str, payloads: dict[str, Any]) -> bytes:
    return request_descriptor(claim, step, payloads, frozenset(STEPS[step]["input_jobs"]))


def _rewrite(raw: bytes, **changes: Any) -> bytes:
    value = json.loads(raw)
    value.update(changes)
    return ia.canonical(value)


@pytest.mark.parametrize("purpose", PURPOSES)
def test_binding_gerado_e_o_que_o_consumidor_deriva(purpose: str) -> None:
    claim = _claim(purpose)
    assert caller_binding(claim) == ia.authentic_caller(caller_binding(claim), PRODUCER)
    assert set(caller_binding(claim)) == set(ia.CALLER_BINDING_FIELDS.split())


def test_hash_do_binding_e_sha256_dos_bytes_canonicos_com_lf() -> None:
    claim = _claim()
    expected = ia.sha(ia.canonical(caller_binding(claim)))
    assert caller_binding_sha256(claim) == expected


@pytest.mark.parametrize("purpose", PURPOSES)
def test_consumidor_admite_o_descriptor_do_application_sem_payloads(purpose: str) -> None:
    claim = _claim(purpose)
    raw = _descriptor(claim, "application-inventory", {})
    request = _admit(raw, claim, "application-inventory")
    assert request["payloads"] == {}
    assert request["caller_binding_sha256"] == caller_binding_sha256(claim)


def test_consumidor_admite_o_descriptor_do_plan_com_payload_completo() -> None:
    claim = _claim()
    raw = _descriptor(claim, "plan-operational", {"inputs": REFERENCE})
    assert _admit(raw, claim, "plan-operational")["payloads"] == {"inputs": REFERENCE}


def test_descriptor_sai_em_bytes_canonicos_do_consumidor() -> None:
    raw = _descriptor(_claim(), "application-inventory", {})
    assert raw == ia.canonical(json.loads(raw))
    assert raw.endswith(b"\n")


def test_consumidor_recusa_campo_extra_no_descriptor() -> None:
    claim = _claim()
    raw = _rewrite(_descriptor(claim, "application-inventory", {}), extra=1)
    with pytest.raises(ia.Refused, match="closed object fields"):
        _admit(raw, claim, "application-inventory")


def test_consumidor_recusa_payloads_nao_vazio_no_application() -> None:
    claim = _claim()
    raw = _rewrite(_descriptor(claim, "application-inventory", {}),
                   payloads={"inputs": REFERENCE})
    with pytest.raises(ia.Refused, match="complete payload inventory"):
        _admit(raw, claim, "application-inventory")


def test_gerador_recusa_payloads_nao_vazio_no_application() -> None:
    with pytest.raises(ValueError, match="payloads=complete_inventory"):
        _descriptor(_claim(), "application-inventory", {"inputs": REFERENCE})


def test_consumidor_recusa_binding_sem_run_id() -> None:
    binding = caller_binding(_claim())
    del binding["run_id"]
    with pytest.raises(ia.Refused, match="closed object fields"):
        ia.authentic_caller(binding, PRODUCER)


@pytest.mark.parametrize("field", ["run_id", "run_attempt"])
@pytest.mark.parametrize("value", [None, 0, -1, True, "7"])
def test_gerador_recusa_run_id_e_attempt_invalidos(field: str, value: Any) -> None:
    with pytest.raises(ValueError, match=f"{field}="):
        caller_binding(_claim(**{field: value}))


@pytest.mark.parametrize("purpose", ["", "other", None])
def test_gerador_recusa_proposito_fora_do_conjunto_fechado(purpose: Any) -> None:
    with pytest.raises(ValueError, match="application_purpose="):
        caller_binding(_claim(purpose))


@pytest.mark.parametrize("field", ["source_sha", "producer_workflow_sha"])
def test_gerador_recusa_sha_de_commit_fora_de_40_hex(field: str) -> None:
    with pytest.raises(ValueError, match=f"{field}="):
        caller_binding(_claim(**{field: "A" * 40}))


def test_gerador_recusa_closure_fora_de_64_hex() -> None:
    with pytest.raises(ValueError, match="producer_source_closure_sha256="):
        caller_binding(_claim(producer_source_closure_sha256="c" * 63))


@pytest.mark.parametrize("field", ia.REQUEST_FIELDS.split())
def test_consumidor_recusa_um_byte_alterado_em_cada_campo_do_descriptor(field: str) -> None:
    claim = _claim()
    raw = _descriptor(claim, "application-inventory", {})
    value = json.loads(raw)
    value[field] = {
        "schema_version": 2, "repository": "VINIClUS/Other", "source_sha": "e" * 40,
        "step": "plan-operational", "payloads": {"inputs": REFERENCE},
        "application_purpose": "mode-readiness-drill", "caller_binding_sha256": "0" * 64,
    }[field]
    with pytest.raises(ia.Refused):
        _admit(ia.canonical(value), claim, "application-inventory")


def test_consumidor_recusa_sha_do_descriptor_diferente_dos_bytes() -> None:
    claim = _claim()
    raw = _descriptor(claim, "application-inventory", {})
    with pytest.raises(ia.Refused, match="request bytes"):
        _admit(raw, claim, "application-inventory", raw_sha="0" * 64)


def test_consumidor_recusa_descriptor_nao_canonico() -> None:
    claim = _claim()
    raw = json.dumps(json.loads(_descriptor(claim, "application-inventory", {})),
                     indent=1).encode()
    with pytest.raises(ia.Refused, match="noncanonical JSON bytes"):
        _admit(raw, claim, "application-inventory")


@pytest.mark.parametrize("field", ia.CALLER_BINDING_FIELDS.split())
def test_consumidor_recusa_um_byte_alterado_em_cada_campo_do_binding(field: str) -> None:
    claim = _claim()
    forged = caller_binding(claim)
    forged[field] = "forjado" if type(forged[field]) is str else forged[field] + 1
    forged_sha = ia.sha(ia.canonical(forged))
    raw = _rewrite(_descriptor(claim, "application-inventory", {}),
                   caller_binding_sha256=forged_sha)
    with pytest.raises(ia.Refused, match="original requested application caller"):
        _admit(raw, claim, "application-inventory")


def test_consumidor_recusa_binding_de_outro_run_attempt() -> None:
    other = _claim(run_attempt=2)
    raw = _descriptor(other, "application-inventory", {})
    with pytest.raises(ia.Refused, match="original requested application caller"):
        _admit(raw, _claim(run_attempt=1), "application-inventory")


@pytest.mark.parametrize("payload", [
    {"artifact_id": 7, "artifact_digest": "sha256:" + "d" * 64, "job_id": 9},
    {"step": "inputs", "artifact_id": 7},
    {"step": "Inputs"},
    {"step": ""},
    {"step": 7},
    {},
])
def test_gerador_recusa_referencia_de_payload_fora_da_forma_fechada_so_por_nome(
    payload: dict[str, Any],
) -> None:
    with pytest.raises(ValueError, match="payloads=reference"):
        _descriptor(_claim(), "plan-operational", {"inputs": payload})


def test_fragmento_de_payloads_e_o_texto_canonico_do_vetor_do_infra_ansible() -> None:
    vector = json.loads(IA_VECTOR.read_text(encoding="utf-8"))
    payloads = {"plan": {"step": "prepared"}, "audit": {"step": "audited"}}
    document = json.loads(vector["descriptor_canonical_utf8"])
    assert ia.canonical(payloads) == ia.canonical(document["payloads"])


def test_vetor_do_descritor_completo_tem_o_sha256_fixado_e_o_consumidor_o_admite() -> None:
    vector = json.loads(FULL_VECTOR.read_text(encoding="utf-8"))
    claim = _claim()
    raw = _descriptor(claim, "plan-operational", {"inputs": REFERENCE})
    assert raw.decode() == vector["descriptor_canonical_utf8"]
    assert descriptor_sha256(raw) == vector["descriptor_sha256"]
    assert _admit(raw, claim, "plan-operational")["payloads"] == {"inputs": REFERENCE}


def test_gerador_recusa_step_fora_do_formato_do_consumidor() -> None:
    with pytest.raises(ValueError, match="step="):
        request_descriptor(_claim(), "Plan_Operational", {}, frozenset())


def test_sha_do_descriptor_e_o_que_o_consumidor_confere() -> None:
    raw = _descriptor(_claim(), "application-inventory", {})
    assert descriptor_sha256(raw) == ia.sha(raw)


@pytest.mark.parametrize("payload", [
    {"artifact_id": 7, "artifact_digest": "sha256:" + "d" * 64, "job_id": 9},
    {"step": "outro"},
    {"step": "inputs", "job_id": 9},
])
def test_consumidor_recusa_payload_por_id_ou_de_outro_produtor(payload: dict[str, Any]) -> None:
    claim = _claim()
    raw = _rewrite(_descriptor(claim, "plan-operational", {"inputs": REFERENCE}),
                   payloads={"inputs": payload})
    with pytest.raises(ia.Refused):
        _admit(raw, claim, "plan-operational")
