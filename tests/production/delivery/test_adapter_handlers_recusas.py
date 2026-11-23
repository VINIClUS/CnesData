"""Recusas e pendências dos handlers ABI2 closed-ready, reopen e terminal-readback (#465)."""

from __future__ import annotations

import json
from datetime import timedelta
from typing import Any

import pytest

from scripts.promotion import promote_api
from tests.production.delivery.test_adapter_handlers import (
    API,
    FINAL,
    NULL,
    PREPARE,
    RECOVERY,
    _canaries,
    _canary,
    _clock,
    _closed,
    _commitments,
    _observation,
    _readback_inputs,
    _ready_inputs,
    _reopen_inputs,
    _run,
    _writes,
)
from tests.production.delivery.test_api_static_rollback import decision, handoff
from tests.production.delivery.test_phase_b import WINDOWS, Probe, _seed
from tests.production.delivery.test_promote_close import (
    NOW,
    Context,
    _activation,
    _environment,
    canonical,
    sha,
)

MEMBER = {"dispatch_id": "a" * 16, "request_sha256": "1" * 64, "generation": 1,
          "initial_state": "NOT_SENT", "execution_ref": None, "resolution": "CANCEL_BEFORE_SEND",
          "proof_sha256": "2" * 64, "observed_at": "2026-11-09T12:00:00Z",
          "send_disabled_at": "2026-11-09T12:00:00Z"}
ROLLBACK = canonical({"schema": "cnesdata.static-rollback/v1", "plan_sha256": "1" * 64,
                      "restored": [], "residual": [], "invalidation_id": None})


@pytest.fixture(autouse=True)
def _relogio(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(promote_api, "CLOCK", lambda: NOW)


def _frozen(*members: dict[str, Any]) -> bytes:
    return canonical({"frozen_membership_sha256": sha(canonical(list(members))),
                      "members": list(members)})


def _late(monkeypatch: pytest.MonkeyPatch, stage: dict[str, Any]) -> None:
    monkeypatch.setattr(promote_api, "CLOCK", lambda: NOW + timedelta(
        seconds=stage["deadline"] - int(NOW.timestamp())))


def _refused(inputs: dict[str, bytes], operation: str, stage: dict[str, Any], error: str) -> None:
    context = Context(None, Probe(), inputs)
    with pytest.raises(ValueError, match=error):
        _run(context, operation, stage)
    assert context.calls == []


@pytest.mark.parametrize(
    ("canaries", "error"),
    [pytest.param(canonical({"unit": 1}), "reopen=canaries", id="nao-lista"),
     pytest.param(_canaries(_canary("unit"), _canary("recovery")), "reopen=canaries",
                  id="dois-modos"),
     pytest.param(_canaries(_canary("recovery"), _canary("unit"), _canary("audit")),
                  "reopen=canary_kind", id="ordem-trocada"),
     pytest.param(_canaries(_canary("unit", version=2), _canary("recovery"), _canary("audit")),
                  "reopen=canary_kind", id="versao-2"),
     pytest.param(_canaries(_canary("unit") | {"accepted": "yes"}, _canary("recovery"),
                            _canary("audit")), "reopen=canary_kind", id="accepted-texto"),
     pytest.param(_canaries(_canary("unit", checks=["x"]), _canary("recovery"),
                            _canary("audit")), "reopen=canary_checks", id="aceito-com-falha"),
     pytest.param(_canaries(_canary("unit", commitments={}), _canary("recovery"),
                            _canary("audit")), "reopen=canary_commitments", id="sem-compromissos"),
     pytest.param(_canaries(_canary("unit", commitments=_commitments(**{"mode-result": "F" * 64})),
                            _canary("recovery"), _canary("audit")), "reopen=canary_commitments",
                  id="hash-invalido"),
     pytest.param(_canaries(_canary("unit", commitments=_commitments(owner="0" * 64)),
                            _canary("recovery"), _canary("audit")), "reopen=canary_binding",
                  id="outro-owner"),
     pytest.param(_canaries(_canary("unit", commitments=_commitments(activation="0" * 64)),
                            _canary("recovery"), _canary("audit")), "reopen=canary_binding",
                  id="outra-activation")],
)
def test_reopen_recusa_canary_fora_do_contrato(canaries: bytes, error: str) -> None:
    _refused(_reopen_inputs(_frozen()) | {"canaries": canaries}, "reopen", FINAL, error)


@pytest.mark.parametrize(
    ("changes", "error"),
    [pytest.param({"routing": canonical({"unit": 1, "recovery": None, "audit": None})},
                  "reopen=routing", id="rota-nao-texto"),
     pytest.param({"membership": _frozen(MEMBER | {"resolution": "PENDING"})},
                  "reopen=membership_unresolved", id="membro-nao-resolvido"),
     pytest.param({"membership": canonical({"frozen_membership_sha256": "0" * 64,
                                            "members": []})},
                  "reopen=membership_commitment", id="compromisso-divergente"),
     pytest.param({"canaries": b"[1.5]\n"}, "input=encoding", id="numero-nao-inteiro"),
     pytest.param({"canaries": b"\xff\n"}, "input=encoding", id="utf8-invalido")],
)
def test_reopen_recusa_insumo_invalido_antes_de_chamar(changes: dict[str, bytes],
                                                       error: str) -> None:
    _refused(_reopen_inputs(_frozen()) | changes, "reopen", FINAL, error)


def test_reopen_e_terminal_readback_recusam_depois_do_prazo(
    monkeypatch: pytest.MonkeyPatch
) -> None:
    _late(monkeypatch, FINAL)
    _refused(_reopen_inputs(_frozen()), "reopen", FINAL, "reopen=stage_deadline")
    _refused(_readback_inputs(_frozen()), "terminal-readback", FINAL,
             "terminal_readback=stage_deadline")


def test_reopen_recusa_fence_que_o_owner_nao_fechou(monkeypatch: pytest.MonkeyPatch) -> None:
    with _environment("empty", monkeypatch) as (raw, _, probe):
        _clock(monkeypatch, FINAL)
        context = Context(raw, probe, _reopen_inputs(_frozen()))
        with pytest.raises(ValueError, match="reopen=fence_not_closed_by_owner"):
            _run(context, "reopen", FINAL)
    assert _writes(context) == []


def test_closed_ready_recusa_phase_b_de_outro_close_e_prazo_vencido(
    monkeypatch: pytest.MonkeyPatch
) -> None:
    with _closed("terminal", monkeypatch) as (raw, _, probe, close):
        _clock(monkeypatch, API)
        inputs = _ready_inputs(close)
        phase = json.loads(inputs["phase-b"]) | {"close_drain_sha256": "0" * 64}
        context = Context(raw, probe, inputs | {"phase-b": canonical(phase)})
        with pytest.raises(ValueError, match="closed_ready=binding"):
            _run(context, "closed-ready", API)
        other = json.loads(close) | {"kind": "phase-b"}
        context = Context(raw, probe, inputs | {"close-drain": canonical(other)})
        with pytest.raises(ValueError, match="closed_ready=kind name=close-drain"):
            _run(context, "closed-ready", API)
        _late(monkeypatch, API)
        late = Context(raw, probe, inputs)
        with pytest.raises(ValueError, match="closed_ready=stage_deadline"):
            _run(late, "closed-ready", API)
    assert context.calls == late.calls == []


def test_terminal_readback_marca_membro_sem_execucao_terminal_como_pendente(
    monkeypatch: pytest.MonkeyPatch
) -> None:
    with _closed("terminal", monkeypatch) as (raw, _, probe, close):
        _clock(monkeypatch, FINAL)
        reopen = Context(raw, probe, _reopen_inputs(close))
        _run(reopen, "reopen", FINAL)
        probe.terminal = False
        context = Context(raw, probe, _readback_inputs(close, routing=reopen.observed[0][1]))
        _run(context, "terminal-readback", FINAL)
    record = _observation(context, "terminal")
    [member] = record["members"]
    assert member["resolution"] == "UNRESOLVED"
    assert record["pending"] == [{"kind": "member-unresolved",
                                  "reference_sha256": sha(canonical(member))}]
    assert (record["quiescent"], record["state_clean"]) == (False, True)


def test_terminal_readback_reporta_permit_em_voo(monkeypatch: pytest.MonkeyPatch) -> None:
    with _environment("empty", monkeypatch) as (raw, adapter, probe):
        _, _, permit = _seed(adapter)
        context = Context(raw, probe, _readback_inputs(None))
        _run(context, "terminal-readback", PREPARE)
    record = _observation(context, "terminal")
    held = {"dispatch_id": permit.holder.dispatch_id, "token": permit.token}
    assert record["pending"] == [{"kind": "permit-held", "reference_sha256": sha(canonical(held))}]
    assert (record["quiescent"], record["state_clean"]) == (False, True)


def test_terminal_readback_em_recovery_aceita_rollback_e_sem_routing_nao_e_limpo(
    monkeypatch: pytest.MonkeyPatch
) -> None:
    with _closed("terminal", monkeypatch) as (raw, _, probe, close):
        _clock(monkeypatch, RECOVERY)
        inputs = _readback_inputs(close, routing=NULL, **{"static-receipt": ROLLBACK},
                                  decision=canonical(decision()))
        context = Context(raw, probe, inputs)
        _run(context, "terminal-readback", RECOVERY)
    record = _observation(context, "terminal")
    assert (record["quiescent"], record["state_clean"], record["pending"]) == (True, False, [])


OTHER_REGION = "arn:aws:states:us-east-1:x:execution:cnesdata-prod:" + "a" * 16
TERMINAL = MEMBER | {"resolution": "TERMINAL", "initial_state": "ACKNOWLEDGED",
                     "execution_ref": OTHER_REGION}


@pytest.mark.parametrize(
    ("changes", "error"),
    [pytest.param({"routing": canonical({"version": 1, "kind": "routing", "fence": "HALF",
                                         "routes_match": True})}, "terminal_readback=routing",
                  id="fence-fora"),
     pytest.param({"routing": canonical({"version": 1, "kind": "terminal", "fence": "OPEN",
                                         "routes_match": True})},
                  "terminal_readback=kind name=routing", id="routing-de-outro-kind"),
     pytest.param({"host-receipt": handoff("accepted", kind="closed-ready")},
                  "terminal_readback=kind name=host-receipt", id="handoff-de-outro-kind"),
     pytest.param({"static-receipt": canonical({"schema": "outro"})},
                  "terminal_readback=fields name=static-receipt", id="schema-desconhecido"),
     pytest.param({"membership": _frozen(TERMINAL)}, "terminal_readback=execution_ref",
                  id="execucao-de-outra-regiao")],
)
def test_terminal_readback_recusa_fonte_invalida(changes: dict[str, bytes], error: str) -> None:
    _refused(_readback_inputs(_frozen()) | changes, "terminal-readback", FINAL, error)


def test_terminal_readback_recusa_decision_de_outra_versao() -> None:
    inputs = _readback_inputs(_frozen(), decision=canonical(decision() | {"version": 2}))
    _refused(inputs, "terminal-readback", RECOVERY, "terminal_readback=kind name=decision")


def test_activation_do_reopen_e_conferida_contra_o_request() -> None:
    other = _activation(canary_id="d" * 32)
    _refused(_reopen_inputs(_frozen()) | {"activation": other}, "reopen", FINAL,
             "activation=sha256")


def test_terminal_readback_marca_membro_ja_nao_resolvido_como_pendente(
    monkeypatch: pytest.MonkeyPatch
) -> None:
    member = MEMBER | {"resolution": "UNRESOLVED"}
    with _environment("empty", monkeypatch) as (raw, _, probe):
        _clock(monkeypatch, FINAL)
        context = Context(raw, probe, _readback_inputs(_frozen(member)))
        _run(context, "terminal-readback", FINAL)
    record = _observation(context, "terminal")
    identity = {key: member[key] for key in promote_api.MEMBER_IDENTITY}
    assert record["members"] == [identity]
    assert record["pending"] == [{"kind": "member-unresolved",
                                  "reference_sha256": sha(canonical(identity))}]
    assert record["quiescent"] is False


def test_closed_ready_recusa_close_drain_com_janelas_diferentes_das_do_request(
    monkeypatch: pytest.MonkeyPatch
) -> None:
    other = {"stages": WINDOWS["stages"] | {"api": 181}, "fixed_deadline_seconds": 1801}
    with _closed("terminal", monkeypatch) as (raw, _, probe, close):
        _clock(monkeypatch, API)
        context = Context(raw, probe, _ready_inputs(close))
        with pytest.raises(ValueError, match=r"^closed_ready=windows$"):
            _run(context, "closed-ready", API | {"windows": other})
    assert context.calls == []


def test_request_recusa_estagio_com_chave_a_mais() -> None:
    context = Context(None, Probe(), {"activation": b"x", "close-drain": b"x", "phase-b": b"x",
                                   "membership": b"x"})
    with pytest.raises(ValueError, match=r"^request=stage$"):
        _run(context, "closed-ready", API | {"extra": 1})
