"""P17c: reopen observa o roteamento em recovery sem escrita e terminal-readback o consome."""

from __future__ import annotations

import json
from typing import Any

import pytest

from scripts.promotion import promote_api
from scripts.promotion.promote_ready import ReadOnlyShim
from scripts.promotion.promote_terminal import REOPEN_EXPRESSION
from tests.production.delivery.test_adapter_handlers import (
    API,
    FINAL,
    NULL,
    PREPARE,
    RECOVERY,
    TASKS,
    Context,
    _activation,
    _canaries,
    _canary,
    _clock,
    _closed,
    _membership,
    _observation,
    _relogio,
    _reopen_inputs,
    _run,
    _writes,
    canonical,
    sha,
)
from tests.production.delivery.test_api_static_rollback import decision
from tests.production.delivery.test_phase_b import Probe
from tests.production.delivery.test_promote_close import _environment

__all__ = ["_relogio"]
KEEPER_ROUTING_FIELDS = "version kind fence routes_match"
KEEPER_ROUTING_FACTS = {"fence": {"OPEN", "CLOSED"}, "routes_match": {True, False}}
MIXED = canonical(json.loads(_activation())["revisions"] | {"audit": TASKS + "audit:0"})


def test_reopen_em_recovery_devolve_o_roteamento_com_os_campos_fechados_do_keeper(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _closed("terminal", monkeypatch) as (raw, adapter, probe, close):
        _clock(monkeypatch, RECOVERY)
        context = Context(raw, probe, _reopen_inputs(close))
        _run(context, "reopen", RECOVERY)
        state = adapter.observe_environment_gate("prod")
    record = _observation(context, "routing")
    assert set(record) == set(KEEPER_ROUTING_FIELDS.split())
    assert record == {"version": 1, "kind": "routing", "fence": "CLOSED", "routes_match": True}
    assert state.fence.state.value == "CLOSED"


def test_reopen_em_recovery_nao_escreve_mesmo_com_canarios_aceitos_e_rotas_iguais(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _closed("terminal", monkeypatch) as (raw, adapter, probe, close):
        _clock(monkeypatch, RECOVERY)
        context = Context(raw, probe, _reopen_inputs(close))
        _run(context, "reopen", RECOVERY)
        assert adapter.observe_environment_gate("prod").fence.state.value == "CLOSED"
    assert _writes(context) == []
    assert {call[2] for call in context.calls} == {"get_item"}


def test_reopen_em_recovery_com_rotas_mistas_observa_routes_match_falso(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _closed("terminal", monkeypatch) as (raw, _, probe, close):
        _clock(monkeypatch, RECOVERY)
        context = Context(raw, probe, _reopen_inputs(close, routing=MIXED))
        _run(context, "reopen", RECOVERY)
    assert _observation(context, "routing")["routes_match"] is False
    assert _writes(context) == []


def test_reopen_em_recovery_nao_exige_canarios_aceitos(monkeypatch: pytest.MonkeyPatch) -> None:
    refused = _canaries(_canary("unit", False), _canary("recovery", False), _canary("audit", False))
    with _closed("terminal", monkeypatch) as (raw, _, probe, close):
        _clock(monkeypatch, RECOVERY)
        context = Context(raw, probe, _reopen_inputs(close, canaries=refused))
        _run(context, "reopen", RECOVERY)
    assert _observation(context, "routing")["fence"] == "CLOSED"
    assert _writes(context) == []


def test_reopen_em_recovery_recusa_fence_que_nao_esta_fechado_por_este_dono(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _environment("empty", monkeypatch) as (raw, _, probe):
        _clock(monkeypatch, RECOVERY)
        empty = canonical({"frozen_membership_sha256": sha(canonical([])), "members": []})
        context = Context(raw, probe, _reopen_inputs(empty))
        with pytest.raises(ValueError, match="reopen=fence_not_closed_by_owner"):
            _run(context, "reopen", RECOVERY)
    assert _writes(context) == []


def test_reopen_em_recovery_recusa_depois_do_prazo_do_estagio(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _closed("terminal", monkeypatch) as (raw, _, probe, close):
        _clock(monkeypatch, RECOVERY | {"admitted_at": RECOVERY["deadline"]})
        context = Context(raw, probe, _reopen_inputs(close))
        with pytest.raises(ValueError, match="reopen=stage_deadline"):
            _run(context, "reopen", RECOVERY)
    assert context.calls == []


@pytest.mark.parametrize("stage", [PREPARE, API])
def test_reopen_continua_recusando_os_outros_estagios(stage: dict[str, Any]) -> None:
    close = canonical({"frozen_membership_sha256": sha(canonical([])), "members": []})
    context = Context(None, Probe(), _reopen_inputs(close) | {"membership": close})
    with pytest.raises(ValueError, match=r"reopen=|stage"):
        _run(context, "reopen", stage)
    assert context.calls == []


def test_leitura_selada_do_recovery_recusa_cas_e_qualquer_escrita(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _closed("terminal", monkeypatch) as (raw, _, probe, _close):
        context = Context(raw, probe, {})
        shim = ReadOnlyShim(context, [])
        parameters = {"TableName": "t", "UpdateExpression": REOPEN_EXPRESSION}
        with pytest.raises(ValueError, match="shim=sealed"):
            shim.client("dynamodb").update_item(**parameters)
        with pytest.raises(ValueError, match="shim=sealed"):
            shim.client("dynamodb").transact_write_items(TransactItems=[])
    assert _writes(context) == []


def _routing(fence: str = "CLOSED", routes_match: bool = True) -> bytes:
    return canonical({"version": 1, "kind": "routing", "fence": fence,
                      "routes_match": routes_match})


def _recovery_readback(close: bytes, **changes: bytes) -> dict[str, bytes]:
    return {"routing": _routing(), "membership": _membership(close), "host-receipt": NULL,
            "static-receipt": NULL, "decision": canonical(decision())} | changes


def _readback(monkeypatch: pytest.MonkeyPatch, **changes: bytes) -> dict[str, Any]:
    close = canonical({"frozen_membership_sha256": sha(canonical([])), "members": []})
    with _closed("terminal", monkeypatch) as (raw, _, probe, _close):
        _clock(monkeypatch, RECOVERY)
        context = Context(raw, probe, _recovery_readback(close, **changes))
        _run(context, "terminal-readback", RECOVERY)
    assert _writes(context) == []
    return _observation(context, "terminal")


def test_terminal_readback_em_recovery_com_roteamento_limpo_tem_state_clean_verdadeiro(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    record = _readback(monkeypatch)
    assert record["state_clean"] is True


@pytest.mark.parametrize(
    "routing",
    [pytest.param(_routing("OPEN"), id="fence-aberto"),
     pytest.param(_routing(routes_match=False), id="rotas-mistas")],
)
def test_terminal_readback_em_recovery_so_e_limpo_com_fence_fechado_e_rotas_iguais(
    monkeypatch: pytest.MonkeyPatch, routing: bytes,
) -> None:
    assert _readback(monkeypatch, routing=routing)["state_clean"] is False


def test_terminal_readback_em_recovery_exige_o_roteamento(monkeypatch: pytest.MonkeyPatch) -> None:
    close = canonical({"frozen_membership_sha256": sha(canonical([])), "members": []})
    _clock(monkeypatch, RECOVERY)
    context = Context(None, Probe(), _recovery_readback(close, routing=NULL))
    with pytest.raises(ValueError, match="terminal_readback=stage_inputs"):
        _run(context, "terminal-readback", RECOVERY)
    assert context.calls == []


def test_terminal_readback_em_final_continua_recusando_decision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    close = canonical({"frozen_membership_sha256": sha(canonical([])), "members": []})
    context = Context(None, Probe(), _recovery_readback(close))
    with pytest.raises(ValueError, match="terminal_readback=stage_inputs"):
        _run(context, "terminal-readback", FINAL)


def test_paridade_do_roteamento_com_os_campos_fechados_do_keeper(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert frozenset(KEEPER_ROUTING_FIELDS.split()) == promote_api.ROUTING_FIELDS
    with _closed("terminal", monkeypatch) as (raw, _, probe, close):
        _clock(monkeypatch, RECOVERY)
        context = Context(raw, probe, _reopen_inputs(close))
        _run(context, "reopen", RECOVERY)
    [(_, observed)] = context.observed
    record = json.loads(observed)
    assert observed == canonical(record)
    assert list(record) == sorted(KEEPER_ROUTING_FIELDS.split())
    assert record["version"] == 1
    assert record["kind"] == "routing"
    assert record["fence"] in KEEPER_ROUTING_FACTS["fence"]
    assert type(record["routes_match"]) is bool
