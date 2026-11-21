"""Seed compartilhado entre modos e incerteza do start dos efeitos de modo (#465)."""

from __future__ import annotations

import hashlib

import pytest

from scripts.promotion import _adapter_abi as abi
from tests.production.delivery.test_adapter_mode_effects import (
    Keeper,
    keeper,
    run,
    seed_inputs,
    start_doc,
    start_inputs,
)


def test_seed_de_unit_e_recovery_da_mesma_promocao_gravam_no_mesmo_armazem(
        monkeypatch: pytest.MonkeyPatch) -> None:
    first = Keeper(seed_inputs("unit"))
    run(monkeypatch, "unit-seed", first)
    later = seed_inputs("recovery") | {"mode-start": start_doc(started="2026-11-13T12:02:00Z")}
    second = Keeper(later, items=first.items, objects=first.objects)
    run(monkeypatch, "recovery-seed", second)
    raw = [key for key in second.items if key[1].startswith("RAW_MANIFEST")]
    assert len(raw) == 4
    assert len({key[1] for key in raw}) == 4
    replay = Keeper(later, items=second.items, objects=second.objects)
    assert run(monkeypatch, "recovery-seed", replay)


@pytest.mark.parametrize(("name", "launcher"), [("recovery-start", "mode_dispatch"),
                                                ("audit-start", "mode_audit_launch")])
def test_start_nunca_usa_o_alias_mode_observe(monkeypatch: pytest.MonkeyPatch, name: str,
                                              launcher: str) -> None:
    mode = name.split("-", 1)[0]
    context = keeper(mode, start_inputs(mode))
    run(monkeypatch, name, context)
    assert [call[:3] for call in context.calls] == [
        (launcher, "ecs", "run_task"), (launcher, "ecs", "describe_tasks")]
    assert all(alias != "mode_observe" for alias, _, _ in abi.MODE_EFFECT_CALLS[name])


@pytest.mark.parametrize("failure", ["describe", "observe"])
def test_recovery_start_marca_unknown_quando_a_observacao_falha_depois_do_run_task(
        monkeypatch: pytest.MonkeyPatch, failure: str) -> None:
    context = keeper("recovery", start_inputs("recovery"))
    if failure == "describe":
        context.responses["describe_tasks"] = RuntimeError("describe failed")
    else:
        context.observe = lambda kind, raw: "0" * 64
    with pytest.raises((RuntimeError, ValueError)):
        run(monkeypatch, "recovery-start", context)
    kinds = [event[0] for event in context.events if event[0] in {"admit", "unknown", "settle"}]
    assert kinds == ["admit", "unknown"]
    admitted = next(event[1:] for event in context.events if event[0] == "admit")
    assert next(event for event in context.events if event[0] == "unknown")[1] == (
        hashlib.sha256(f"{admitted[0]}{admitted[1]}".encode()).hexdigest())


def test_audit_start_propaga_a_falha_da_observacao_sem_ledger(
        monkeypatch: pytest.MonkeyPatch) -> None:
    context = keeper("audit", start_inputs("audit"))
    context.responses["describe_tasks"] = RuntimeError("describe failed")
    with pytest.raises(RuntimeError, match="describe failed"):
        run(monkeypatch, "audit-start", context)
    assert not any(event[0] in {"admit", "settle", "unknown"} for event in context.events)
