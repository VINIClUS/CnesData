"""Formatos fechados do ledger de modo: dispatch-unit, ledger-admit e transições do ticket."""
from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta, timezone
from hashlib import sha256
from typing import Any

import pytest

from cnes_domain.control_plane.environment_gate import PermitHolder
from cnes_domain.orchestration.mode_canary import (
    ModeCanaryBinding,
    ModeCanaryControl,
    ModeCanaryRecord,
    SyntheticContextRequired,
)
from cnes_infra.aws.mode_canary_frames import (
    CEILINGS,
    COUNTERS,
    EFFECT_KINDS,
    GENESIS_LEDGER_SHA256,
    Effect,
    LedgerState,
    admission,
    canonical_text,
    dispatch_effect,
    effect_id,
    initial_control,
    ledger_effect,
    record_item,
    require_control,
    start_request,
    transition,
)
from cnes_infra.control_plane.dynamodb_keys import run_partition, tenant_partition
from packages.cnes_domain.tests.orchestration.test_mode_canary_records import (
    CANARY_ID,
    _control,
    _promotion,
    _ticket,
)

_TABLE = "cnesdata-prod-control-plane"
_TENANT = f"prod-canary-{CANARY_ID}"
_OWNER = "a" * 64
_RUN = "unit-aaaaaaaaaaaaaaaa"
_HOLDER = PermitHolder(_TENANT, _RUN, "0123456789abcdef")
_UNITS = ("u1", "u2")
_NOW = datetime(2026, 11, 12, 12, 10, tzinfo=UTC)


def _record(**changes: Any) -> tuple[ModeCanaryRecord, str]:
    text = canonical_text(_promotion() | {"owner_sha256": _OWNER} | changes)
    return ModeCanaryRecord.from_mapping(json.loads(text)), text


def _binding(record_text: str, phase: str = "unit") -> ModeCanaryBinding:
    record = json.loads(record_text)
    return ModeCanaryBinding(_TENANT, tuple(record["run_ids"]), _OWNER,
                             sha256(record_text.encode()).hexdigest(), phase, "promotion")


def _state(control: dict[str, Any] | None = None, ticket: str | None = None,
           **record: Any) -> LedgerState:
    parsed, text = _record(**record)
    control_text = canonical_text((_control() | {"owner_sha256": _OWNER}) | (control or {}))
    return LedgerState(_binding(text), parsed, control_text, ticket)


def _dispatch(state: LedgerState) -> Effect:
    return dispatch_effect(state.binding, state.record, _HOLDER, _UNITS)


def test_tetos_e_contadores_do_contexto_promotion_sao_os_do_ledger() -> None:
    assert CEILINGS["promotion"] == {"unit_tasks": 8, "recovery_resumes": 8, "recovery_hangs": 1}
    assert CEILINGS["readiness"] == {"unit_tasks": 8, "recovery_resumes": 8, "recovery_hangs": 2}
    assert COUNTERS == {"start-execution": "unit_tasks", "run-recovery": "recovery_resumes",
                        "run-hang": "recovery_hangs"}
    assert {*COUNTERS, "native-mutation", "settle-permit"} == EFFECT_KINDS
    assert sha256(b"[]\n").hexdigest() == GENESIS_LEDGER_SHA256


def test_dispatch_unit_e_uma_transacao_de_ticket_control_e_binding() -> None:
    state = _state()
    plan = admission(_TABLE, state, _dispatch(state), _NOW)
    request = start_request(_HOLDER, _UNITS)
    assert plan.effect_id == effect_id(state.binding.record_sha256, "start-execution", request)
    assert plan.items is not None
    ticket, control, binding = (item["Put"] for item in plan.items)
    pk = tenant_partition(_TENANT)
    assert (ticket["TableName"], ticket["Item"]["pk"], ticket["Item"]["sk"]) == (
        _TABLE, {"S": pk}, {"S": f"PROD_CANARY_EFFECT#{_OWNER}#{plan.effect_id}"})
    assert ticket["ConditionExpression"] == "attribute_not_exists(pk)"
    assert ticket["Item"]["entity"] == {"S": "MODECANARYEFFECT"}
    assert control["Item"]["sk"] == {"S": f"PROD_CANARY_CONTROL#{_OWNER}"}
    assert control["ConditionExpression"] == "payload = :old"
    assert control["ExpressionAttributeValues"] == {":old": {"S": state.control_text}}
    counts = json.loads(control["Item"]["payload"]["S"])["admitted_counts"]
    assert counts == {"unit_tasks": 2, "recovery_resumes": 0, "recovery_hangs": 0}
    assert binding["Item"]["pk"] == {"S": run_partition(_TENANT, _RUN)}
    assert binding["Item"]["sk"] == {"S": "PROD_CANARY#0123456789abcdef"}
    assert binding["ConditionExpression"] == "attribute_not_exists(pk)"
    assert json.loads(binding["Item"]["payload"]["S"]) == {
        "schema": "cnesdata.mode-canary-binding/v1", "tenant_id": _TENANT, "run_id": _RUN,
        "dispatch_id": "0123456789abcdef", "owner_sha256": _OWNER,
        "record_sha256": state.binding.record_sha256, "phase": "unit",
        "context_kind": "promotion"}


def test_ticket_novo_encadeia_o_ledger_do_control() -> None:
    state = _state()
    plan = admission(_TABLE, state, _dispatch(state), _NOW)
    assert plan.items is not None
    ticket = plan.items[0]["Put"]["Item"]["payload"]["S"]
    control = json.loads(plan.items[1]["Put"]["Item"]["payload"]["S"])
    assert control["ledger_sha256"] == sha256(canonical_text({
        "previous_ledger_sha256": _control()["ledger_sha256"],
        "ticket_sha256": sha256(ticket.encode()).hexdigest()}).encode()).hexdigest()
    assert json.loads(ticket) | {"admitted_at": None, "deadline": None} == {
        "schema": "cnesdata.mode-canary-effect/v1", "effect_id": plan.effect_id,
        "owner_sha256": _OWNER, "generation": 3,
        "context_manifest_sha256": _control()["context_manifest_sha256"],
        "phase_record_sha256": state.binding.record_sha256, "effect_kind": "start-execution",
        "request_sha256": start_request(_HOLDER, _UNITS),
        "target_sha256": sha256(state.record.machine_arn.encode()).hexdigest(),
        "admitted_at": None, "deadline": None, "state": "PENDING", "receipt_sha256": None}


def test_reentrada_com_o_mesmo_pedido_nao_escreve_nada() -> None:
    first = _state()
    plan = admission(_TABLE, first, _dispatch(first), _NOW)
    assert plan.items is not None
    ticket = plan.items[0]["Put"]["Item"]["payload"]["S"]
    again = _state(ticket=ticket)
    replay = admission(_TABLE, again, _dispatch(again), _NOW + timedelta(seconds=5))
    assert (replay.effect_id, replay.items) == (plan.effect_id, None)


def test_ticket_existente_de_outro_pedido_recusa_ticket_conflict() -> None:
    other = canonical_text(_ticket() | {"effect_kind": "run-recovery"})
    state = _state(ticket=other)
    with pytest.raises(SyntheticContextRequired, match="reason=ticket_conflict"):
        admission(_TABLE, state, _dispatch(state), _NOW)


@pytest.mark.parametrize(("kind", "counter", "amount"), [
    ("start-execution", "unit_tasks", 7), ("run-recovery", "recovery_resumes", 8),
    ("run-hang", "recovery_hangs", 1)])
def test_teto_do_contador_recusa_budget_exhausted(kind: str, counter: str, amount: int) -> None:
    counts = {"unit_tasks": 0, "recovery_resumes": 0, "recovery_hangs": 0} | {counter: amount}
    state = _state({"admitted_counts": counts})
    effect = _dispatch(state) if kind == "start-execution" else ledger_effect(kind, "1" * 64,
                                                                             "2" * 64)
    with pytest.raises(SyntheticContextRequired, match="reason=budget_exhausted"):
        admission(_TABLE, state, effect, _NOW)


def test_ledger_admit_sem_contador_so_encadeia_o_ledger() -> None:
    state = _state()
    plan = admission(_TABLE, state, ledger_effect("native-mutation", "1" * 64, "2" * 64), _NOW)
    assert plan.items is not None
    assert len(plan.items) == 2
    control = json.loads(plan.items[1]["Put"]["Item"]["payload"]["S"])
    assert control["admitted_counts"] == _control()["admitted_counts"]


def test_efeito_fora_do_enum_recusa_effect_kind_invalid() -> None:
    with pytest.raises(SyntheticContextRequired, match="reason=effect_kind_invalid"):
        ledger_effect("delete-everything", "1" * 64, "2" * 64)


def test_contexto_vencido_recusa_antes_de_montar_o_ticket() -> None:
    state = _state()
    with pytest.raises(SyntheticContextRequired, match="reason=context_expired"):
        admission(_TABLE, state, _dispatch(state), datetime(2026, 11, 12, 12, 30, tzinfo=UTC))


@pytest.mark.parametrize(("control", "reason"), [
    (None, "control_missing"),
    ({"context_manifest_sha256": "9" * 64}, "control_mismatch"),
    ({"admission_state": "SEALED", "sealed_at": "2026-11-12T12:05:00Z"}, "control_sealed"),
])
def test_control_ausente_divergente_ou_selado_recusa(control: dict[str, Any] | None,
                                                     reason: str) -> None:
    record, _ = _record()
    text = None if control is None else canonical_text(
        _control() | {"owner_sha256": _OWNER} | control)
    with pytest.raises(SyntheticContextRequired, match=f"reason={reason}"):
        require_control(text, record)


def test_control_valido_volta_parseado() -> None:
    record, _ = _record()
    text = canonical_text(_control() | {"owner_sha256": _OWNER})
    assert require_control(text, record) == ModeCanaryControl.from_mapping(json.loads(text))


@pytest.mark.parametrize("holder", [
    PermitHolder("prod-canary-" + "f" * 32, _RUN, "0123456789abcdef"),
    PermitHolder(_TENANT, "unit-bbbbbbbbbbbbbbbb", "0123456789abcdef"),
])
def test_dispatch_de_holder_fora_do_binding_recusa_run_not_bound(holder: PermitHolder) -> None:
    state = _state()
    with pytest.raises(SyntheticContextRequired, match="reason=run_not_bound"):
        dispatch_effect(state.binding, state.record, holder, _UNITS)


def test_dispatch_de_run_do_binding_fora_do_record_recusa_run_not_bound() -> None:
    state = _state()
    binding = ModeCanaryBinding(_TENANT, (_RUN, "unit-bbbbbbbbbbbbbbbb"), _OWNER,
                                state.binding.record_sha256, "unit", "promotion")
    holder = PermitHolder(_TENANT, "unit-bbbbbbbbbbbbbbbb", "0123456789abcdef")
    with pytest.raises(SyntheticContextRequired, match="reason=run_not_bound"):
        dispatch_effect(binding, state.record, holder, _UNITS)


@pytest.mark.parametrize(("state", "receipt"), [("UNKNOWN", None), ("SETTLED", "3" * 64)])
def test_transicao_do_ticket_e_um_cas_sobre_o_payload_antigo(state: str,
                                                            receipt: str | None) -> None:
    ledger = _state()
    ticket = canonical_text(_ticket() | {"owner_sha256": _OWNER})
    request = transition(_TABLE, ledger.binding, ticket, (state, receipt))
    assert request is not None
    assert request["ConditionExpression"] == "payload = :old"
    assert request["ExpressionAttributeValues"] == {":old": {"S": ticket}}
    assert request["Item"]["sk"] == {"S": f"PROD_CANARY_EFFECT#{_OWNER}#{_ticket()['effect_id']}"}
    changed = json.loads(request["Item"]["payload"]["S"])
    assert (changed["state"], changed["receipt_sha256"]) == (state, receipt)


def test_transicao_ja_aplicada_nao_escreve() -> None:
    ledger = _state()
    ticket = canonical_text(_ticket() | {"state": "UNKNOWN"})
    assert transition(_TABLE, ledger.binding, ticket, ("UNKNOWN", None)) is None


@pytest.mark.parametrize(("ticket", "reason"), [
    (None, "ticket_missing"),
    (canonical_text(_ticket() | {"state": "SETTLED", "receipt_sha256": "4" * 64}),
     "ticket_settled"),
])
def test_transicao_sem_ticket_ou_ja_liquidado_recusa(ticket: str | None, reason: str) -> None:
    ledger = _state()
    with pytest.raises(SyntheticContextRequired, match=f"reason={reason}"):
        transition(_TABLE, ledger.binding, ticket, ("SETTLED", "3" * 64))


def test_ticket_nao_canonico_recusa() -> None:
    ledger = _state()
    with pytest.raises(SyntheticContextRequired, match="reason=ticket_not_canonical"):
        transition(_TABLE, ledger.binding, json.dumps(_ticket()), ("UNKNOWN", None))


def test_ticket_invalido_recusa() -> None:
    ledger = _state()
    with pytest.raises(SyntheticContextRequired, match="reason=ticket_invalid"):
        transition(_TABLE, ledger.binding, canonical_text(_ticket() | {"state": "X"}),
                   ("UNKNOWN", None))


def test_item_do_record_e_o_payload_exato_na_chave_do_owner_e_fase() -> None:
    record, text = _record()
    assert record_item(record, text) == {
        "pk": {"S": tenant_partition(_TENANT)}, "sk": {"S": f"PROD_CANARY#{_OWNER}#unit"},
        "entity": {"S": "MODECANARYRECORD"}, "payload": {"S": text}}


def test_item_do_record_recusa_texto_de_outro_record() -> None:
    record, _ = _record()
    _, other = _record(phase_receipt_sha256="0" * 64)
    with pytest.raises(SyntheticContextRequired, match="reason=record_mismatch"):
        record_item(record, other)


def test_item_do_record_recusa_texto_que_nao_e_record() -> None:
    record, _ = _record()
    with pytest.raises(SyntheticContextRequired, match="reason=record_invalid"):
        record_item(record, canonical_text({"schema": "x"}))


def test_control_inicial_sai_do_record_e_do_inicio_do_unit() -> None:
    record, _ = _record()
    admitted = datetime(2026, 11, 12, 12, 1, tzinfo=UTC)
    control = ModeCanaryControl.from_mapping(initial_control(record, admitted))
    assert (control.owner_sha256, control.generation, control.context_kind) == (
        _OWNER, 3, "promotion")
    assert control.context_manifest_sha256 == record.context_manifest_sha256
    assert (control.admitted_at, control.deadline) == (admitted, record.fixed_deadline)
    assert (control.admission_state, control.sealed_at, control.ledger_sha256) == (
        "ACTIVE", None, GENESIS_LEDGER_SHA256)
    assert set(control.admitted_counts.values()) == {0}


def test_control_inicial_recusa_record_sem_fixed_deadline() -> None:
    record = ModeCanaryRecord.from_mapping(json.loads(canonical_text(_promotion())) | {
        "context_kind": "readiness", "phase": "readiness", "mode": "unit",
        "scenario": "readiness_pipeline", "fixed_deadline": None, "promotion_id": None,
        "promotion_generation": None, "canary_deadline": "2026-11-12T12:30:00Z"})
    with pytest.raises(SyntheticContextRequired, match="reason=control_invalid"):
        initial_control(record, _NOW)


def test_relogio_fora_de_utc_recusa_clock_not_utc() -> None:
    state = _state()
    local = _NOW.astimezone(timezone(timedelta(hours=-3)))
    with pytest.raises(SyntheticContextRequired, match="reason=clock_not_utc"):
        admission(_TABLE, state, _dispatch(state), local)
