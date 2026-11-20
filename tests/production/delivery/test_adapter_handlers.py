"""Handlers ABI2 closed-ready, reopen e terminal-readback sobre o context.call (#465)."""

from __future__ import annotations

import json
import re
from contextlib import contextmanager
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from scripts.promotion import promote_api
from scripts.promotion.phase_b import build_phase_b_evidence
from tests.production.delivery.test_adapter_abi import (
    INFRA_CLOSED_READY_FIELDS,
    INFRA_MEMBER_IDENTITY,
    INFRA_ROUTING_FIELDS,
    INFRA_TERMINAL_READBACK_FIELDS,
)
from tests.production.delivery.test_api_static_rollback import decision, handoff
from tests.production.delivery.test_phase_b import Probe
from tests.production.delivery.test_promote_close import (
    EXECUTION,
    NOW,
    OWNER,
    PREPARE,
    SHA,
    TASKS,
    Context,
    _activation,
    _environment,
    _epoch,
    _shape,
    canonical,
    sha,
)
from tests.production.delivery.test_promote_close import _inputs as close_inputs
from tests.production.delivery.test_promote_close import _request as close_request

if TYPE_CHECKING:
    from collections.abc import Iterator

READY_VECTOR = Path(__file__).with_name("closed_ready_call_vector.json")
REOPEN_VECTOR = Path(__file__).with_name("reopen_call_vector.json")
API = PREPARE | {"close": _epoch(0), "fixed_deadline": _epoch(1800), "stage": "api",
                 "admitted_at": _epoch(3), "deadline": _epoch(183), "sequence": 2}
FINAL = API | {"stage": "final", "admitted_at": _epoch(1200), "deadline": _epoch(1260),
               "sequence": 7}
RECOVERY = FINAL | {"stage": "recovery", "sequence": 8}
MACHINE_ARN = EXECUTION.replace(":execution:", ":stateMachine:")
ROUTES = {"mode": "initial", "state_machine_arn": MACHINE_ARN,
          "unit_revision": TASKS + "unit:2", "recovery_revision": TASKS + "recovery:1",
          "audit_revision": TASKS + "audit:1", "unit_environment_state_machine_arn": MACHINE_ARN,
          "recovery_environment_state_machine_arn": MACHINE_ARN}
NULL = canonical(None)
PUBLICATION = canonical({"schema": "cnesdata.static-publication/v1", "plan_sha256": "1" * 64,
                         "closed_ready_sha256": "2" * 64, "uploaded": [["index.html", "v2"]],
                         "already_published": [], "invalidation_id": "I1"})


def _request(operation: str, inputs: dict[str, bytes], stage: dict[str, Any]) -> dict[str, Any]:
    return {"abi_version": 2, "operation": operation, "repository": OWNER["repository"],
            "source_sha": SHA, "owner": OWNER, "checkpoint_sha256": stage["checkpoint_sha256"],
            "activation_sha256": sha(_activation()), "stage": stage,
            "inputs": {name: {"sha256": sha(raw), "size": len(raw)}
                       for name, raw in sorted(inputs.items())}}


def _run(context: Context, operation: str, stage: dict[str, Any]) -> dict[str, Any]:
    request = _request(operation, context.inputs, stage)
    return promote_api.execute_shared_operation(operation, request, context)


@pytest.fixture(autouse=True)
def _relogio(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(promote_api, "CLOCK", lambda: NOW)


def _clock(monkeypatch: pytest.MonkeyPatch, stage: dict[str, Any]) -> None:
    monkeypatch.setattr(promote_api, "CLOCK", lambda: NOW + timedelta(
        seconds=stage["admitted_at"] - _epoch(0)))


@contextmanager
def _closed(name: str, monkeypatch: pytest.MonkeyPatch) -> Iterator[tuple[Any, Any, Any, bytes]]:
    with _environment(name, monkeypatch) as (raw, adapter, probe):
        context = Context(raw, probe, close_inputs())
        promote_api.execute_shared_operation("close-drain", close_request(context.inputs),
                                             context)
        [(_, close)] = context.observed
        yield raw, adapter, probe, close


def _phase_b(close: bytes) -> bytes:
    before = {"lineage": "synthetic-phase-b-state", "serial": 1, "state_sha256": "1" * 64,
              "object_version": "v1"}
    after = before | {"serial": 2, "state_sha256": "2" * 64, "object_version": "v2"}
    return canonical(build_phase_b_evidence(json.loads(close), before, after, b"plan", b"audit",
                                            b"apply", ROUTES, NOW + timedelta(seconds=2)))


def _membership(close: bytes) -> bytes:
    record = json.loads(close)
    return canonical({key: record[key] for key in ("frozen_membership_sha256", "members")})


def _ready_inputs(close: bytes, **changes: bytes) -> dict[str, bytes]:
    return {"activation": _activation(), "close-drain": close, "phase-b": _phase_b(close),
            "membership": _membership(close)} | changes


def _canary(kind: str, accepted: bool = True, **changes: Any) -> dict[str, Any]:
    checks = [] if accepted else [f"{kind}_task_failed"]
    return {"version": 1, "kind": kind, "accepted": accepted, "checks": checks,
            "inputs": {"mode-result": "1" * 64}, "owner_sha256": OWNER["owner_sha256"],
            "activation_sha256": sha(_activation())} | changes


def _canaries(*items: dict[str, Any]) -> bytes:
    return canonical(list(items) or [_canary("unit"), _canary("recovery"), _canary("audit")])


def _reopen_inputs(close: bytes, **changes: bytes) -> dict[str, bytes]:
    routing = canonical(json.loads(_activation())["revisions"])
    return {"activation": _activation(), "routing": routing, "canaries": _canaries(),
            "membership": _membership(close)} | changes


def _readback_inputs(close: bytes | None, **changes: bytes) -> dict[str, bytes]:
    if close is None:
        empty = {"frozen_membership_sha256": sha(canonical([])), "members": []}
        values = {"routing": NULL, "membership": canonical(empty), "host-receipt": NULL,
                  "static-receipt": NULL, "decision": NULL}
    else:
        routing = canonical({"version": 1, "kind": "routing", "fence": "OPEN",
                             "routes_match": True})
        values = {"routing": routing, "membership": _membership(close),
                  "host-receipt": handoff("accepted"), "static-receipt": PUBLICATION,
                  "decision": NULL}
    return values | changes


def _observation(context: Context, kind: str) -> dict[str, Any]:
    [(observed, raw)] = context.observed
    record = json.loads(raw)
    assert observed == kind
    assert raw == canonical(record)
    return record


def _writes(context: Context) -> list[str]:
    return [call[2] for call in context.calls if call[2] not in {"get_item", "describe_execution"}]


@pytest.mark.parametrize("name", ["empty", "terminal"])
def test_closed_ready_observa_documento_com_os_campos_do_keeper(
    name: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _closed(name, monkeypatch) as (raw, _, probe, close):
        _clock(monkeypatch, API)
        context = Context(raw, probe, _ready_inputs(close))
        result = _run(context, "closed-ready", API)
    record = _observation(context, "closed-ready")
    frozen = json.loads(close)
    assert set(record) == set(INFRA_CLOSED_READY_FIELDS.split())
    assert (record["version"], record["kind"], record["permit"]) == (1, "closed-ready", None)
    assert record["close_drain_sha256"] == sha(close)
    assert record["phase_b_sha256"] == sha(context.inputs["phase-b"])
    assert record["common"] == frozen["common"]
    assert record["fence"] == frozen["fence"]
    identity = INFRA_MEMBER_IDENTITY.split()
    assert [[m[k] for k in identity] for m in record["members"]] == [
        [m[k] for k in identity] for m in frozen["members"]]
    assert record["stage"]["name"] == "api"
    assert _writes(context) == []
    assert result["observations"] == [{"kind": "closed-ready", "sha256": sha(
        context.observed[0][1]), "size": len(context.observed[0][1])}]


def test_closed_ready_so_le_o_vetor_versionado(monkeypatch: pytest.MonkeyPatch) -> None:
    vector = json.loads(READY_VECTOR.read_bytes())
    assert READY_VECTOR.read_bytes() == canonical(vector)
    seen: set[bytes] = set()
    for name in ("empty", "terminal"):
        with _closed(name, monkeypatch) as (raw, _, probe, close):
            _clock(monkeypatch, API)
            context = Context(raw, probe, _ready_inputs(close))
            _run(context, "closed-ready", API)
        seen |= {canonical(_shape(call)) for call in context.calls}
    assert (vector["version"], vector["role"]) == (1, "fence_operator")
    assert sorted(seen) == sorted(map(canonical, vector["calls"]))


def _mutated(raw: bytes, change: str) -> bytes:
    value = json.loads(raw)
    target = value[0] if isinstance(value, list) else value
    first = sorted(target)[0]
    if change == "extra":
        target["extra"] = 1
    elif change == "missing":
        del target[first]
    elif change == "renamed":
        target[first + "_x"] = target.pop(first)
    else:
        return json.dumps(value, indent=1).encode()
    return canonical(value)


CHANGES = ("extra", "missing", "renamed", "noncanonical")


@pytest.mark.parametrize("change", CHANGES)
@pytest.mark.parametrize("name", ["close-drain", "phase-b", "membership"])
def test_closed_ready_recusa_insumo_fora_dos_campos_fechados(
    name: str, change: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _closed("terminal", monkeypatch) as (raw, _, probe, close):
        _clock(monkeypatch, API)
        inputs = _ready_inputs(close)
        inputs[name] = _mutated(inputs[name], change)
        context = Context(raw, probe, inputs)
        with pytest.raises(ValueError, match=r"closed_ready=|input="):
            _run(context, "closed-ready", API)
    assert (context.calls, context.observed) == ([], [])


def test_closed_ready_recusa_membership_diferente_do_close_drain(
    monkeypatch: pytest.MonkeyPatch
) -> None:
    with _closed("terminal", monkeypatch) as (raw, _, probe, close):
        _clock(monkeypatch, API)
        empty = canonical({"frozen_membership_sha256": sha(canonical([])), "members": []})
        context = Context(raw, probe, _ready_inputs(close, membership=empty))
        with pytest.raises(ValueError, match="closed_ready=membership"):
            _run(context, "closed-ready", API)
    assert context.calls == []


@pytest.mark.parametrize("stage", [PREPARE, FINAL])
def test_closed_ready_fora_do_estagio_api_e_recusado(stage: dict[str, Any]) -> None:
    context = Context(None, Probe(), {"activation": b"x", "close-drain": b"x", "phase-b": b"x",
                                   "membership": b"x"})
    with pytest.raises(ValueError, match="request=stage"):
        _run(context, "closed-ready", stage)


def test_reopen_reabre_o_fence_com_canaries_aceitos_e_rotas_iguais(
    monkeypatch: pytest.MonkeyPatch
) -> None:
    with _closed("terminal", monkeypatch) as (raw, adapter, probe, close):
        _clock(monkeypatch, FINAL)
        context = Context(raw, probe, _reopen_inputs(close))
        _run(context, "reopen", FINAL)
        state = adapter.observe_environment_gate("prod")
    record = _observation(context, "routing")
    assert set(record) == set(INFRA_ROUTING_FIELDS.split())
    assert record == {"version": 1, "kind": "routing", "fence": "OPEN", "routes_match": True}
    assert (state.fence.state.value, state.permit) == ("OPEN", None)
    assert _writes(context) == ["update_item"]
    vector = json.loads(REOPEN_VECTOR.read_bytes())
    assert REOPEN_VECTOR.read_bytes() == canonical(vector)
    assert sorted({canonical(_shape(call)) for call in context.calls}) == sorted(
        map(canonical, vector["calls"]))


@pytest.mark.parametrize(
    ("changes", "routes_match"),
    [pytest.param({"canaries": _canaries(_canary("unit"), _canary("recovery", False),
                                         _canary("audit"))}, True, id="recovery-nao-aceito"),
     pytest.param({"routing": canonical(json.loads(_activation())["revisions"] | {
         "audit": TASKS + "audit:0"})}, False, id="rotas-mistas")],
)
def test_reopen_sem_aceite_ou_com_rotas_mistas_nao_escreve(
    changes: dict[str, bytes], routes_match: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _closed("terminal", monkeypatch) as (raw, adapter, probe, close):
        _clock(monkeypatch, FINAL)
        context = Context(raw, probe, _reopen_inputs(close, **changes))
        _run(context, "reopen", FINAL)
        state = adapter.observe_environment_gate("prod")
    record = _observation(context, "routing")
    assert record == {"version": 1, "kind": "routing", "fence": "CLOSED",
                      "routes_match": routes_match}
    assert state.fence.state.value == "CLOSED"
    assert _writes(context) == []


@pytest.mark.parametrize("change", CHANGES)
@pytest.mark.parametrize("name", ["canaries", "membership"])
def test_reopen_recusa_insumo_fora_dos_campos_fechados(name: str, change: str) -> None:
    close = canonical({"frozen_membership_sha256": sha(canonical([])), "members": []})
    inputs = _reopen_inputs(close) | {"membership": close}
    inputs[name] = _mutated(inputs[name], change)
    context = Context(None, Probe(), inputs)
    with pytest.raises(ValueError, match=r"reopen=|input="):
        _run(context, "reopen", FINAL)
    assert context.calls == []


def test_reopen_em_recovery_e_recusado_sem_regra_do_produto() -> None:
    close = canonical({"frozen_membership_sha256": sha(canonical([])), "members": []})
    context = Context(None, Probe(), _reopen_inputs(close) | {"membership": close})
    with pytest.raises(ValueError, match="reopen=recovery_rule_absent"):
        _run(context, "reopen", RECOVERY)
    assert context.calls == []


def test_terminal_readback_em_application_prepare_observa_campos_do_keeper(
    monkeypatch: pytest.MonkeyPatch
) -> None:
    with _environment("empty", monkeypatch) as (raw, _, probe):
        context = Context(raw, probe, _readback_inputs(None))
        _run(context, "terminal-readback", PREPARE)
    record = _observation(context, "terminal")
    assert set(record) == set(INFRA_TERMINAL_READBACK_FIELDS.split())
    assert record == {"version": 1, "kind": "terminal", "members": [], "quiescent": True,
                      "state_clean": True, "pending": [], "unknown": [],
                      "observed_at": "2026-11-09T12:00:00Z"}
    assert _writes(context) == []


def test_terminal_readback_em_final_reprova_membros_congelados(
    monkeypatch: pytest.MonkeyPatch
) -> None:
    with _closed("terminal", monkeypatch) as (raw, _, probe, close):
        _clock(monkeypatch, FINAL)
        reopen = Context(raw, probe, _reopen_inputs(close))
        _run(reopen, "reopen", FINAL)
        context = Context(raw, probe, _readback_inputs(close, routing=reopen.observed[0][1]))
        _run(context, "terminal-readback", FINAL)
    record = _observation(context, "terminal")
    identity = INFRA_MEMBER_IDENTITY.split()
    frozen = json.loads(close)["members"]
    assert [list(member) for member in record["members"]] == [sorted(identity)] * len(frozen)
    assert record["members"] == [{key: member[key] for key in identity} for member in frozen]
    assert len(record["members"]) == 1
    assert (record["quiescent"], record["state_clean"], record["pending"]) == (True, True, [])
    assert re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ", record["observed_at"])
    assert _writes(context) == []
    vector = json.loads(READY_VECTOR.read_bytes())["calls"]
    assert all(_shape(call) in vector for call in context.calls)


@pytest.mark.parametrize("change", CHANGES)
@pytest.mark.parametrize("name", ["routing", "membership", "host-receipt", "static-receipt"])
def test_terminal_readback_recusa_insumo_fora_dos_campos_fechados(name: str, change: str) -> None:
    close = canonical({"version": 1, "kind": "close-drain", "frozen_membership_sha256": sha(
        canonical([])), "members": []})
    context = Context(None, Probe(), _readback_inputs(close))
    context.inputs[name] = _mutated(context.inputs[name], change)
    with pytest.raises(ValueError, match=r"terminal_readback=|input="):
        _run(context, "terminal-readback", FINAL)
    assert context.calls == []


@pytest.mark.parametrize(
    ("stage", "changes"),
    [pytest.param(PREPARE, {"routing": canonical({"version": 1, "kind": "routing",
                                                  "fence": "OPEN", "routes_match": True})},
                  id="routing-antes-do-close"),
     pytest.param(FINAL, {"decision": canonical(decision())}, id="decision-em-final"),
     pytest.param(FINAL, {"static-receipt": NULL}, id="publicacao-ausente-em-final"),
     pytest.param(RECOVERY, {"decision": NULL}, id="decision-ausente-em-recovery")],
)
def test_terminal_readback_recusa_insumo_impossivel_no_estagio(
    stage: dict[str, Any], changes: dict[str, bytes]
) -> None:
    close = None if stage is PREPARE else canonical(
        {"frozen_membership_sha256": sha(canonical([])), "members": []})
    context = Context(None, Probe(), _readback_inputs(close, **changes))
    with pytest.raises(ValueError, match="terminal_readback=stage_inputs"):
        _run(context, "terminal-readback", stage)
    assert context.calls == []
