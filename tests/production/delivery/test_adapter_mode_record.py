"""Operações puras -record de modo: record do estágio, prazos, seed do audit e ids (#465)."""

from __future__ import annotations

import json
from datetime import timedelta
from typing import Any

import pytest

from cnes_domain.orchestration.mode_canary import ModeCanaryRecord
from scripts.promotion import _adapter_abi as abi
from scripts.promotion import promote_api
from scripts.promotion.mode_canary_contract import SESSION_OPERATIONS
from scripts.promotion.promote_ready import machine_arn
from tests.production.delivery import _mode_record_fixtures as fx
from tests.production.delivery.test_promote_close import ACCOUNT, OWNER, canonical, sha

MODES = ("unit", "recovery", "audit")
STAGES = {"unit": "unit", "recovery": "phase-c-recovery", "audit": "phase-c-audit"}
BASE_INPUTS = frozenset({"activation", "close-drain", "fixture-manifest", "mode-input"})
OTHER_OWNER = OWNER | {"run_id": 456, "generation": 8, "owner_sha256": "8" * 64}
FOREIGN_OWNER_FIELDS = {"run_id": 456, "run_attempt": 2, "generation": 8}


def _record(mp: pytest.MonkeyPatch, mode: str, **changes: bytes) -> tuple[dict[str, Any], Any]:
    run = fx.execute(mp, f"{mode}-record", fx.record_inputs(mode, **changes))
    observation = json.loads(run["raw"])
    assert run["kind"] == "mode-record"
    assert run["raw"] == canonical(observation)
    return observation, json.loads(observation["record"])


def test_tabelas_abi_tem_as_tres_operacoes_record_puras_e_onze_operacoes() -> None:
    assert len(abi.MODE_OPERATIONS) == 11
    for mode in MODES:
        name = f"{mode}-record"
        assert abi.MODE_OPERATIONS[name] == (mode, STAGES[mode])
        assert abi.MODE_STAGES[name] == frozenset({STAGES[mode]})
        assert abi.MODE_OBSERVATIONS[name] == "mode-record"
        expected = BASE_INPUTS | ({"mode-seed"} if mode == "audit" else set())
        assert abi.MODE_INPUTS[name] == expected
        assert name not in abi.MODE_EFFECT_CALLS
        assert name not in abi.MODE_EFFECT_METHODS
        assert name not in abi.MODE_EFFECT_STEPS
        assert name not in SESSION_OPERATIONS


@pytest.mark.parametrize("mode", MODES)
def test_record_do_modo_sai_das_fontes_autenticadas_do_estagio(
        monkeypatch: pytest.MonkeyPatch, mode: str) -> None:
    observation, record = _record(monkeypatch, mode)
    assert set(observation) == {"version", "kind", "record", "audit_event_ids"}
    assert (observation["version"], observation["kind"]) == (1, "mode-record")
    parsed = ModeCanaryRecord.from_mapping(record)
    assert observation["record"].encode() == canonical(record)
    activation = json.loads(fx.ACTIVATION)
    assert (parsed.mode, parsed.phase, parsed.context_kind) == (mode, mode, "promotion")
    assert parsed.scenario == {"unit": "unit_wave", "recovery": "recovery_resume",
                               "audit": "audit_delivery"}[mode]
    assert parsed.fault == "none"
    assert parsed.canary_id == activation["canary_id"] == fx.CANARY
    assert parsed.owner_sha256 == OWNER["owner_sha256"]
    assert list(parsed.run_ids) == fx.runs(mode)
    assert parsed.revisions == activation["revisions"]
    assert parsed.machine_arn == machine_arn(ACCOUNT)
    assert (parsed.promotion_id, parsed.promotion_generation) == (
        OWNER["owner_sha256"], OWNER["generation"])
    assert record["activation_sha256"] == sha(fx.ACTIVATION)
    assert record["fixture_manifest_sha256"] == sha(fx.fixture_manifest())
    assert record["phase_receipt_sha256"] == sha(fx.close_drain())
    assert record["fixed_deadline"] == fx.FIXED
    assert record["stage_deadline"] == "2026-11-13T12:07:00Z"
    assert record["canary_deadline"] == "2026-11-13T12:07:00Z"
    assert record["expires_at"] == fx.CONTEXT_DEADLINE
    wrapper = json.loads(fx.mode_input())["context_manifest"]
    assert record["context_manifest_sha256"] == wrapper["sha256"]


def test_tres_records_compartilham_canary_fixed_deadline_e_tenant(
        monkeypatch: pytest.MonkeyPatch) -> None:
    records = {mode: _record(monkeypatch, mode)[1] for mode in MODES}
    assert len({record["fixed_deadline"] for record in records.values()}) == 1
    assert len({record["canary_id"] for record in records.values()}) == 1
    assert len({record["freeze_policy_sha256"] for record in records.values()}) == 1
    assert records["recovery"]["run_ids"] == records["audit"]["run_ids"]


def test_canary_deadline_e_o_menor_entre_fixed_e_stage(monkeypatch: pytest.MonkeyPatch) -> None:
    inputs = fx.record_inputs("unit")
    late = fx.NOW + timedelta(hours=2)
    run = fx.execute(monkeypatch, "unit-record", inputs, deadline=late)
    record = json.loads(json.loads(run["raw"])["record"])
    assert record["stage_deadline"] == "2026-11-13T14:05:00Z"
    assert record["canary_deadline"] == fx.FIXED


def test_audit_amarra_o_seed_do_recovery_e_emite_os_ids_publicados(
        monkeypatch: pytest.MonkeyPatch) -> None:
    observation, record = _record(monkeypatch, "audit")
    assert record["seed_manifest_sha256"] == sha(fx.seed_manifest())
    assert observation["audit_event_ids"] == [
        f"reconciliation.published:{fx.TENANT}:{run}" for run in fx.runs("recovery")]


@pytest.mark.parametrize("mode", ["unit", "recovery"])
def test_unit_e_recovery_nao_tem_ids_de_audit_e_ligam_o_seed_ao_fixture(
        monkeypatch: pytest.MonkeyPatch, mode: str) -> None:
    observation, record = _record(monkeypatch, mode)
    assert observation["audit_event_ids"] is None
    assert record["seed_manifest_sha256"] == sha(fx.fixture_manifest())


@pytest.mark.parametrize("mode", MODES)
def test_record_e_puro_sem_chamada_nem_observacao_intermediaria(
        monkeypatch: pytest.MonkeyPatch, mode: str) -> None:
    run = fx.execute(monkeypatch, f"{mode}-record", fx.record_inputs(mode))
    assert run["result"]["observations"] == [
        {"kind": "mode-record", "sha256": sha(run["raw"]), "size": len(run["raw"])}]


@pytest.mark.parametrize(("caso", "changes"), [
    ("tenant-fora-do-canary", {"mode-input": fx.mode_input(scope={"tenant_id": "prod-canary-"
                                                                  + "d" * 32})}),
    ("run-fora-do-escopo", {"mode-input": fx.mode_input(scope={"run_ids": fx.runs("recovery")})}),
    ("manifest-de-outra-activation", {"mode-input": fx.mode_input(activation_sha256="0" * 64)}),
    ("manifest-de-outro-fixture", {"mode-input": fx.mode_input(fixture_manifest_sha256="0" * 64)}),
    ("close-de-outro-canary", {"close-drain": fx.close_drain(canary_id="d" * 32)}),
    ("close-de-outra-activation", {"close-drain": fx.close_drain(activation_sha256="0" * 64)}),
])
def test_record_recusa_binding_divergente(
        monkeypatch: pytest.MonkeyPatch, caso: str, changes: dict[str, bytes]) -> None:
    with pytest.raises(ValueError, match=r"^mode=record_binding$"):
        fx.execute(monkeypatch, "unit-record", fx.record_inputs("unit", **changes))


def test_record_recusa_manifest_de_outro_owner(monkeypatch: pytest.MonkeyPatch) -> None:
    other = OWNER | {"owner_sha256": "8" * 64}
    inputs = fx.record_inputs("unit", **{"mode-input": fx.mode_input(owner=other)})
    with pytest.raises(ValueError, match=r"^mode=record_binding$"):
        fx.execute(monkeypatch, "unit-record", inputs)


@pytest.mark.parametrize("mode", MODES)
def test_record_recusa_close_drain_de_outro_owner_da_mesma_activation(
        monkeypatch: pytest.MonkeyPatch, mode: str) -> None:
    close = fx.close_drain(**fx.identity(OTHER_OWNER))
    inputs = fx.record_inputs(mode, **{"close-drain": close})
    with pytest.raises(ValueError, match=r"^mode=record_binding$"):
        fx.execute(monkeypatch, f"{mode}-record", inputs)


@pytest.mark.parametrize("campo", ["promotion_id", "promotion_generation", "caller"])
def test_record_recusa_close_drain_com_um_campo_da_identidade_de_outro_owner(
        monkeypatch: pytest.MonkeyPatch, campo: str) -> None:
    close = fx.close_drain(**{campo: fx.identity(OTHER_OWNER)[campo]})
    with pytest.raises(ValueError, match=r"^mode=record_binding$"):
        fx.execute(monkeypatch, "unit-record", fx.record_inputs("unit", **{"close-drain": close}))


@pytest.mark.parametrize("campo", sorted(FOREIGN_OWNER_FIELDS))
def test_record_recusa_mode_input_do_mesmo_owner_sha_com_outro_campo_do_owner(
        monkeypatch: pytest.MonkeyPatch, campo: str) -> None:
    owner = OWNER | {campo: FOREIGN_OWNER_FIELDS[campo]}
    inputs = fx.record_inputs("unit", **{"mode-input": fx.mode_input(owner=owner)})
    with pytest.raises(ValueError, match=r"^mode=record_binding$"):
        fx.execute(monkeypatch, "unit-record", inputs)


def test_record_recusa_context_manifest_vencido_antes_do_canary(
        monkeypatch: pytest.MonkeyPatch) -> None:
    manifest = fx.mode_input(deadline="2026-11-13T12:04:00Z")
    inputs = fx.record_inputs("unit", **{"mode-input": manifest})
    with pytest.raises(ValueError, match=r"^mode=record_deadline$"):
        fx.execute(monkeypatch, "unit-record", inputs)


def test_audit_recusa_seed_de_outro_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    seed = fx.seed_manifest(schema="cnesdata.mode-canary-seed/v0")
    with pytest.raises(ValueError, match=r"^mode=record_seed$"):
        fx.execute(monkeypatch, "audit-record", fx.record_inputs("audit", **{"mode-seed": seed}))


def test_record_recusa_prazo_do_estagio_vencido(monkeypatch: pytest.MonkeyPatch) -> None:
    inputs = fx.record_inputs("recovery")
    with pytest.raises(ValueError, match=r"^mode=record_deadline$"):
        fx.execute(monkeypatch, "recovery-record", inputs, now=fx.NOW + timedelta(seconds=120))


@pytest.mark.parametrize("changes", [
    {"owner": "8" * 64}, {"tenant": "prod-canary-" + "d" * 32},
    {"run_ids": fx.runs("unit")}, {"fixture_manifest_sha256": "0" * 64},
])
def test_audit_recusa_seed_de_outro_owner_tenant_run_ou_fixture(
        monkeypatch: pytest.MonkeyPatch, changes: dict[str, Any]) -> None:
    seed = fx.seed_manifest(**changes)
    with pytest.raises(ValueError, match=r"^mode=record_seed$"):
        fx.execute(monkeypatch, "audit-record", fx.record_inputs("audit", **{"mode-seed": seed}))


def test_record_e_operacao_do_handler_do_adapter() -> None:
    assert {"unit-record", "recovery-record", "audit-record"} <= set(promote_api.MODE_OPERATIONS)
