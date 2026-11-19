"""Registro, controle, ticket e locators do canary de modo: campos fechados e uniões."""
from __future__ import annotations

import json
from copy import deepcopy
from typing import Any

import pytest

from cnes_domain.orchestration.mode_canary import (
    AuditObserveLocator,
    ModeCanaryBinding,
    ModeCanaryControl,
    ModeCanaryRecord,
    SyntheticEffectTicket,
)

H = "a" * 64
G = "b" * 64
CANARY_ID = "0123456789abcdef0123456789abcdef"
T0 = "2026-11-12T12:00:00Z"
T1 = "2026-11-12T12:30:00Z"
T2 = "2026-11-12T13:00:00Z"
RECORD_FIELDS = (
    "schema",
    "activation_sha256",
    "candidate_sha256",
    "source_sha",
    "release_id",
    "canary_id",
    "owner_sha256",
    "promotion_id",
    "promotion_generation",
    "phase_receipt_sha256",
    "machine_arn",
    "revisions",
    "run_ids",
    "fixture_manifest_sha256",
    "seed_manifest_sha256",
    "fixed_deadline",
    "stage_deadline",
    "canary_deadline",
    "phase",
    "freeze_policy_sha256",
    "expires_at",
    "context_kind",
    "context_manifest_sha256",
    "scenario",
    "fault",
    "mode",
)


def _promotion() -> dict[str, Any]:
    return {
        "schema": "cnesdata.mode-canary/v2", "activation_sha256": H, "candidate_sha256": G,
        "source_sha": "d" * 40, "release_id": "r-499", "canary_id": CANARY_ID,
        "owner_sha256": H, "promotion_id": "promo-1", "promotion_generation": 3,
        "phase_receipt_sha256": G, "machine_arn": "arn:aws:states:us-east-2:0:stateMachine:m",
        "revisions": {"unit": "u:1", "recovery": "r:1", "audit": "a:1"},
        "run_ids": ["unit-aaaaaaaaaaaaaaaa"], "fixture_manifest_sha256": H,
        "seed_manifest_sha256": G, "fixed_deadline": T2, "stage_deadline": T1,
        "canary_deadline": T1, "phase": "unit", "freeze_policy_sha256": H, "expires_at": T2,
        "context_kind": "promotion", "context_manifest_sha256": G, "scenario": "unit_wave",
        "fault": "none", "mode": "unit",
    }


def _readiness() -> dict[str, Any]:
    record = _promotion()
    record.update({
        "context_kind": "readiness", "phase": "readiness", "fixed_deadline": None,
        "stage_deadline": T2, "canary_deadline": T2, "scenario": "readiness_pipeline",
        "promotion_id": None, "promotion_generation": None,
    })
    return record


def _control() -> dict[str, Any]:
    return {
        "schema": "cnesdata.mode-canary-control/v1", "owner_sha256": H, "generation": 3,
        "context_kind": "promotion", "context_manifest_sha256": G, "admission_state": "ACTIVE",
        "admitted_at": T0, "deadline": T2, "sealed_at": None, "ledger_sha256": H,
        "admitted_counts": {"unit_tasks": 0, "recovery_resumes": 0, "recovery_hangs": 0},
    }


def _ticket() -> dict[str, Any]:
    return {
        "schema": "cnesdata.mode-canary-effect/v1", "effect_id": H, "owner_sha256": G,
        "generation": 3, "context_manifest_sha256": H, "phase_record_sha256": G,
        "effect_kind": "start-execution", "request_sha256": H, "target_sha256": G,
        "admitted_at": T0, "deadline": T2, "state": "PENDING", "receipt_sha256": None,
    }


def _with(base: dict[str, Any], **changes: Any) -> dict[str, Any]:
    mapping = deepcopy(base)
    mapping.update(changes)
    return mapping


def test_registro_de_promocao_valido_preserva_campos() -> None:
    record = ModeCanaryRecord.from_mapping(_promotion())

    assert record.run_ids == ("unit-aaaaaaaaaaaaaaaa",)
    assert record.canary_deadline.isoformat() == "2026-11-12T12:30:00+00:00"
    assert record.revisions == {"unit": "u:1", "recovery": "r:1", "audit": "a:1"}


def test_registro_de_readiness_valido_exige_prazo_unico() -> None:
    record = ModeCanaryRecord.from_mapping(_readiness())

    assert record.fixed_deadline is None
    assert record.stage_deadline == record.canary_deadline


def test_registro_tem_exatamente_os_vinte_e_seis_campos() -> None:
    assert sorted(_promotion()) == sorted(RECORD_FIELDS)
    assert len(RECORD_FIELDS) == 26


@pytest.mark.parametrize("field", RECORD_FIELDS)
def test_registro_rejeita_campo_ausente(field: str) -> None:
    mapping = _promotion()
    del mapping[field]
    with pytest.raises(ValueError, match="reason=fields_mismatch kind=record"):
        ModeCanaryRecord.from_mapping(mapping)


def test_record_fault_extra_ou_sem_binding_rejeitado() -> None:
    with pytest.raises(ValueError, match="reason=fields_mismatch kind=record"):
        ModeCanaryRecord.from_mapping(_with(_promotion(), extra="x"))
    with pytest.raises(ValueError, match="field=fault"):
        ModeCanaryRecord.from_mapping(_with(_promotion(), fault="hang_no_dispatch"))
    with pytest.raises(ValueError, match="field=fault"):
        ModeCanaryRecord.from_mapping(_with(_promotion(), fault="shell"))


@pytest.mark.parametrize(
    ("changes", "field"),
    [
        ({"schema": "outro"}, "schema"),
        ({"source_sha": "d" * 39}, "source_sha"),
        ({"canary_id": "A" * 32}, "canary_id"),
        ({"activation_sha256": "A" * 64}, "activation_sha256"),
        ({"run_ids": []}, "run_ids"),
        ({"run_ids": ["a", "b", "c"]}, "run_ids"),
        ({"revisions": {"unit": "u:1"}}, "revisions"),
        ({"revisions": {"unit": "", "recovery": "r", "audit": "a"}}, "revisions"),
        ({"fixed_deadline": "2026-11-12T13:00:00"}, "fixed_deadline"),
        ({"fixed_deadline": "2026-11-12T13:00:00+01:00"}, "fixed_deadline"),
        ({"fixed_deadline": 5}, "fixed_deadline"),
        ({"fixed_deadline": None}, "fixed_deadline"),
        ({"phase": "readiness"}, "phase"),
        ({"mode": "recovery"}, "mode"),
        ({"mode": "readiness"}, "mode"),
        ({"context_kind": "outro"}, "context_kind"),
        ({"canary_deadline": T2}, "canary_deadline"),
        ({"stage_deadline": T0}, "canary_deadline"),
        ({"scenario": "readiness_pipeline"}, "scenario"),
        ({"scenario": "recovery_failure"}, "fault"),
        ({"promotion_id": None}, "promotion_id"),
        ({"promotion_generation": 0}, "promotion_generation"),
        ({"promotion_generation": True}, "promotion_generation"),
    ],
)
def test_registro_de_promocao_rejeita_valor_invalido(changes: dict[str, Any], field: str) -> None:
    with pytest.raises(ValueError, match=f"field={field}"):
        ModeCanaryRecord.from_mapping(_with(_promotion(), **changes))


@pytest.mark.parametrize(
    ("changes", "field"),
    [
        ({"fixed_deadline": T2}, "fixed_deadline"),
        ({"phase": "unit"}, "phase"),
        ({"canary_deadline": T1}, "canary_deadline"),
        ({"scenario": "unit_wave"}, "scenario"),
        ({"promotion_id": "promo-1"}, "promotion_id"),
        ({"promotion_generation": 3}, "promotion_generation"),
    ],
)
def test_registro_de_readiness_rejeita_valor_invalido(changes: dict[str, Any], field: str) -> None:
    with pytest.raises(ValueError, match=f"field={field}"):
        ModeCanaryRecord.from_mapping(_with(_readiness(), **changes))


def test_registro_aceita_pares_cenario_falha_do_contrato() -> None:
    cases = (
        ("recovery_failure", "synthetic_read_failure", "recovery"),
        ("recovery_deadline", "hang_no_dispatch", "recovery"),
        ("audit_delivery", "none", "audit"),
    )
    for scenario, fault, phase in cases:
        mapping = _with(_promotion(), scenario=scenario, fault=fault, phase=phase, mode=phase)
        assert ModeCanaryRecord.from_mapping(mapping).fault == fault
    hang = _with(_readiness(), scenario="readiness_hang_first", fault="hang_no_dispatch")
    assert ModeCanaryRecord.from_mapping(hang).scenario == "readiness_hang_first"


def test_controle_ativo_e_selado_validos() -> None:
    active = ModeCanaryControl.from_mapping(_control())
    sealed = ModeCanaryControl.from_mapping(
        _with(_control(), admission_state="SEALED", sealed_at=T1)
    )

    assert active.sealed_at is None
    assert sealed.sealed_at is not None
    assert active.admitted_counts == {"unit_tasks": 0, "recovery_resumes": 0, "recovery_hangs": 0}


@pytest.mark.parametrize(
    ("changes", "field"),
    [
        ({"admission_state": "OPEN"}, "admission_state"),
        ({"sealed_at": T1}, "sealed_at"),
        ({"admission_state": "SEALED"}, "sealed_at"),
        ({"generation": 0}, "generation"),
        ({"deadline": T0}, "deadline"),
        ({"admitted_counts": {"unit_tasks": 0}}, "admitted_counts"),
        ({"admitted_counts": {"unit_tasks": -1, "recovery_resumes": 0, "recovery_hangs": 0}},
         "admitted_counts"),
        ({"schema": "x"}, "schema"),
    ],
)
def test_controle_rejeita_valor_invalido(changes: dict[str, Any], field: str) -> None:
    with pytest.raises(ValueError, match=f"field={field}"):
        ModeCanaryControl.from_mapping(_with(_control(), **changes))


def test_controle_rejeita_campos_extras() -> None:
    with pytest.raises(ValueError, match="reason=fields_mismatch kind=control"):
        ModeCanaryControl.from_mapping(_with(_control(), extra=1))


def test_ticket_pendente_e_liquidado_validos() -> None:
    pending = SyntheticEffectTicket.from_mapping(_ticket())
    settled = SyntheticEffectTicket.from_mapping(
        _with(_ticket(), state="SETTLED", receipt_sha256=H)
    )

    assert pending.receipt_sha256 is None
    assert settled.state == "SETTLED"


@pytest.mark.parametrize(
    ("changes", "field"),
    [
        ({"state": "SETTLED"}, "receipt_sha256"),
        ({"state": "DONE"}, "state"),
        ({"effect_kind": "shell"}, "effect_kind"),
        ({"deadline": T0}, "deadline"),
        ({"effect_id": "x"}, "effect_id"),
        ({"receipt_sha256": "zz"}, "receipt_sha256"),
    ],
)
def test_ticket_rejeita_valor_invalido(changes: dict[str, Any], field: str) -> None:
    with pytest.raises(ValueError, match=f"field={field}"):
        SyntheticEffectTicket.from_mapping(_with(_ticket(), **changes))


def test_ticket_rejeita_campos_extras() -> None:
    with pytest.raises(ValueError, match="reason=fields_mismatch kind=ticket"):
        SyntheticEffectTicket.from_mapping(_with(_ticket(), extra=1))


def _locator() -> dict[str, Any]:
    return {
        "tenant_id": f"prod-canary-{CANARY_ID}", "run_ids": ["unit-aaaaaaaaaaaaaaaa"],
        "owner_sha256": H, "record_sha256": G, "phase": "unit", "context_kind": "promotion",
    }


def test_locator_de_binding_valido_vira_binding_imutavel() -> None:
    binding = ModeCanaryBinding.from_locator(json.dumps(_locator()))

    assert binding.permits(f"prod-canary-{CANARY_ID}", "unit-aaaaaaaaaaaaaaaa")


@pytest.mark.parametrize(
    "text",
    [
        "", "[]", "null", "{", json.dumps(_with(_locator(), extra=1)),
        json.dumps({k: v for k, v in _locator().items() if k != "phase"}),
        '{"tenant_id":"a","tenant_id":"b"}', json.dumps(_with(_locator(), phase="x")),
    ],
)
def test_locator_de_binding_rejeita_texto_invalido(text: str) -> None:
    with pytest.raises(ValueError, match="reason="):
        ModeCanaryBinding.from_locator(text)


def test_locator_de_observacao_de_audit_e_fechado() -> None:
    mapping = {"event_ids": ["e1", "e2"], "owner_sha256": H, "record_sha256": G}
    locator = AuditObserveLocator.from_json(json.dumps(mapping))

    assert locator.event_ids == ("e1", "e2")
    for text in (
        json.dumps(_with(mapping, extra=1)),
        json.dumps(_with(mapping, event_ids=[])),
        json.dumps(_with(mapping, event_ids=["a", "a"])),
        json.dumps(_with(mapping, owner_sha256="x")),
        "[]",
    ):
        with pytest.raises(ValueError, match="reason="):
            AuditObserveLocator.from_json(text)
