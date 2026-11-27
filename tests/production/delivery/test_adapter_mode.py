"""Operações de modo do adapter ABI2: aceite dos -observe e handlers dos efeitos (#465)."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import pytest

from scripts.promotion import _phase_c_core, promote_api
from scripts.promotion import accept_routing as routing
from scripts.promotion import phase_b as phase_b_module
from scripts.promotion import phase_c_audit as audit_module
from scripts.promotion import phase_c_recovery as recovery_module
from tests.production.delivery import test_mode_canary as mc
from tests.production.delivery import test_mode_canary_unit as mu
from tests.production.delivery import test_phase_c as pc
from tests.production.delivery.test_adapter_handlers import API, OWNER, SHA
from tests.production.delivery.test_promote_close import Context, Probe, canonical, sha

if TYPE_CHECKING:
    from collections.abc import Callable

phase_c, operation, activation = pc.phase_c, pc.operation, pc.activation
case, ddb, github = pc.case, pc.ddb, pc.github
EFFECTS = ("unit-seed", "unit-start", "recovery-seed", "recovery-start", "audit-start")
OBSERVES = {"unit-observe": "unit", "recovery-observe": "phase-c-recovery",
            "audit-observe": "phase-c-audit"}
SCHEDULED = frozenset({"activation", "network", "close-drain", "phase-b", "phase-c",
                       "mode-result", "readbacks"})
FIELDS = {"version", "kind", "accepted", "checks", "commitments"}
PROMOTION_ID = OWNER["owner_sha256"]


class GateContext(Context):
    """Context do adapter sobre o gate do fixture de Phase C (tabela do adapter de teste)."""

    def __init__(self, op: Any, inputs: dict[str, bytes]) -> None:
        super().__init__(op.adapter._client, Probe(), inputs)
        self.table = op.adapter._table_name

    def call(self, role: str, service: str, operation: str, parameters: dict[str, Any]) -> Any:
        return super().call(role, service, operation, parameters | {"TableName": self.table})


def _stage(name: str) -> dict[str, Any]:
    now = int(datetime.now(UTC).timestamp())
    return API | {"stage": name, "close": now - 120, "fixed_deadline": now + 900,
                  "admitted_at": now - 5, "deadline": now + 600}


def _owner(close: dict[str, Any]) -> dict[str, Any]:
    common = close["common"]
    return OWNER | {"owner_sha256": common["promotion_id"],
                    "generation": common["promotion_generation"]}


def _request(op: Any, name: str, inputs: dict[str, bytes]) -> dict[str, Any]:
    return {"abi_version": 3, "operation": name, "repository": OWNER["repository"],
            "source_sha": SHA, "owner": _owner(op.close),
            "checkpoint_sha256": API["checkpoint_sha256"],
            "activation_sha256": sha(op.context.activation), "stage": _stage(OBSERVES[name]),
            "inputs": {key: {"sha256": sha(raw), "size": len(raw)}
                       for key, raw in sorted(inputs.items())}}


def _observe(op: Any, name: str, inputs: dict[str, bytes]) -> dict[str, Any]:
    op.mp.setattr(promote_api, "CLOCK", lambda: datetime.now(UTC))
    context = GateContext(op, inputs)
    promote_api.execute_shared_operation(name, _request(op, name, inputs), context)
    [(kind, raw)] = context.observed
    assert kind == "mode"
    assert raw == canonical(json.loads(raw))
    assert [call[2] for call in context.calls] == ["get_item", "get_item"]
    return json.loads(raw)


def _scheduled(op: Any, mode: str, raw: bytes, evidence: list[Any]) -> dict[str, bytes]:
    return {"activation": op.context.activation, "network": canonical(op.context.canary["network"]),
            "close-drain": canonical(op.close), "phase-b": canonical(op.phase_b),
            "phase-c": canonical(evidence), "mode-result": raw,
            "readbacks": canonical(pc._readbacks(op.ctx))}


def _recovery(op: Any, frozen: bool = False) -> tuple[dict[str, bytes], list[Any]]:
    evidence = [pc._apply(op, "recovery")]
    raw = pc._mode_run(op.ctx, "recovery", frozen)
    return _scheduled(op, "recovery", raw, evidence), evidence


def _audit(op: Any, frozen: bool = True) -> dict[str, bytes]:
    recovery, evidence = _recovery(op)
    pc._canary(op, "recovery")
    evidence.append(pc._apply(op, "audit"))
    inputs = _scheduled(op, "audit", pc._mode_run(op.ctx, "audit", frozen), evidence)
    return inputs | {"recovery-result": recovery["mode-result"],
                     "recovery-readbacks": recovery["readbacks"]}


def _unit_observed(op: Any, revision: str | None = None, run: str = mu._RUN) -> dict[str, Any]:
    return mu.observed_for({
        "machine": op.context.machine["arn"], "cluster": op.ctx.phase_b.canary["cluster"],
        "revision": revision or op.ctx.revisions["unit"], "run": run,
        "tenant": f"prod-canary-{op.context.canary['canary_id']}"})


def _unit(op: Any, revision: str | None = None,
          shape: Callable[[dict[str, Any]], dict[str, Any]] = dict) -> dict[str, bytes]:
    ctx, common = op.ctx, op.close["common"]
    unit = revision or ctx.revisions["unit"]
    fixed = pc._instant(op.close["fixed_deadline"]).strftime("%Y-%m-%dT%H:%M:%SZ")
    record = canonical(mc._record(
        "unit", activation_sha256=ctx.activation_sha256, machine_arn=op.context.machine["arn"],
        revisions=dict(ctx.revisions) | {"unit": unit},
        promotion_generation=common["promotion_generation"],
        owner_sha256=common["promotion_id"], promotion_id=common["promotion_id"],
        **dict.fromkeys(pc.DEADLINES, fixed), **{key: op.context.canary[key] for key in (
            "candidate_sha256", "source_sha", "release_id", "canary_id")}))
    run = _unit_observed(op, unit)
    calls = mc._Context({"unit": run})
    calls.now = lambda: datetime.now(UTC)
    readbacks = shape(pc._readbacks(ctx))
    machine = readbacks["state_machine"]
    machine["definition"] = machine["definition"].replace(ctx.revisions["unit"], unit)
    raw = mc.execute_mode_canary("unit", mc._originals(
        "unit", record=record, readbacks=readbacks, readbacks_after=readbacks), calls)
    return {"activation": op.context.activation, "close-drain": canonical(op.close),
            "record": record, "mode-result": raw, "readbacks": canonical(readbacks),
            "observed": canonical({key: run[key] for key in ("execution", "tasks", "outputs")})}


def _closed(record: dict[str, Any], inputs: dict[str, bytes], kind: str) -> None:
    assert set(record) == FIELDS
    assert (record["version"], record["kind"]) == (1, kind)
    owner = _owner(json.loads(inputs["close-drain"]))["owner_sha256"]
    expected = {"owner": owner} | {name: sha(raw) for name, raw in inputs.items()}
    assert record["commitments"] == expected


def test_tabela_de_modo_separa_observe_dos_efeitos() -> None:
    records = {"unit-record", "recovery-record", "audit-record"}
    assert set(promote_api.MODE_INPUTS) == set(OBSERVES) | set(EFFECTS) | records
    assert {promote_api.MODE_OBSERVATIONS[name] for name in OBSERVES} == {"mode"}
    assert {promote_api.MODE_OBSERVATIONS[name] for name in EFFECTS} == {"mode-seed",
                                                                         "mode-result"}
    assert {promote_api.MODE_OBSERVATIONS[name] for name in records} == {"mode-record"}
    assert {name: promote_api.MODE_OPERATIONS[name][1] for name in OBSERVES} == OBSERVES
    assert promote_api.MODE_INPUTS["recovery-observe"] == SCHEDULED
    assert promote_api.MODE_INPUTS["audit-observe"] == SCHEDULED | {"recovery-result",
                                                                      "recovery-readbacks"}
    assert promote_api.MODE_INPUTS["unit-observe"] == frozenset(
        {"activation", "close-drain", "record", "mode-result", "readbacks", "observed"})
    assert promote_api.MODE_OBSERVATION_FIELDS == FIELDS
    assert set(promote_api.INPUTS).isdisjoint(promote_api.MODE_OPERATIONS)


@pytest.mark.parametrize("name", EFFECTS)
def test_efeito_de_seed_e_start_tem_handler_e_valida_o_request(name: str) -> None:
    with pytest.raises(ValueError, match="request=fields"):
        promote_api.execute_shared_operation(name, {}, Context(None, Probe(), {}))


@pytest.mark.parametrize("name", sorted(OBSERVES))
def test_observe_recusa_request_fora_do_contrato(name: str) -> None:
    with pytest.raises(ValueError, match="request=fields"):
        promote_api.execute_shared_operation(name, {}, Context(None, Probe(), {}))


def test_nucleo_da_phase_c_e_reexportado_pelos_modulos_de_origem() -> None:
    assert routing.PhaseCWatchdog is _phase_c_core.PhaseCWatchdog
    assert routing.assert_fence is _phase_c_core.assert_fence
    assert routing.WindowCutoff is _phase_c_core.WindowCutoff
    assert recovery_module.mode_result is _phase_c_core.mode_result
    assert recovery_module.verify_mode_route is _phase_c_core.verify_mode_route
    assert recovery_module.accept_mode_canary is _phase_c_core.accept_mode_canary
    assert recovery_module.PhaseCContext is _phase_c_core.PhaseCContext
    assert audit_module.accept_audit_canary is _phase_c_core.accept_audit_canary
    assert phase_b_module.PhaseBContext is _phase_c_core.PhaseBContext
    assert phase_b_module.object_hash is _phase_c_core.object_hash


def test_recovery_observe_aceita_canary_do_contexto_original(phase_c: Any) -> None:
    inputs, _ = _recovery(phase_c)
    record = _observe(phase_c, "recovery-observe", inputs)
    _closed(record, inputs, "recovery")
    assert (record["accepted"], record["checks"]) == (True, [])


def test_recovery_observe_reprova_rota_fora_do_alvo_sem_recusar(phase_c: Any) -> None:
    inputs, _ = _recovery(phase_c)
    readbacks = json.loads(inputs["readbacks"])
    readbacks["alarms"]["recovery"]["ActionsEnabled"] = False
    inputs["readbacks"] = canonical(readbacks)
    record = _observe(phase_c, "recovery-observe", inputs)
    _closed(record, inputs, "recovery")
    assert (record["accepted"], record["checks"]) == (False, ["route=alarm"])


def test_audit_observe_aceita_com_prova_do_freeze(phase_c: Any) -> None:
    inputs = _audit(phase_c)
    record = _observe(phase_c, "audit-observe", inputs)
    _closed(record, inputs, "audit")
    assert (record["accepted"], record["checks"]) == (True, [])


def test_audit_observe_sem_prova_do_freeze_e_reprovado(phase_c: Any) -> None:
    inputs = _audit(phase_c, frozen=False)
    record = _observe(phase_c, "audit-observe", inputs)
    assert (record["accepted"], record["checks"]) == (False, ["canary=freeze_proof"])


def test_unit_observe_aceita_execucao_e_task_concluidas(phase_c: Any) -> None:
    inputs = _unit(phase_c)
    record = _observe(phase_c, "unit-observe", inputs)
    _closed(record, inputs, "unit")
    assert (record["accepted"], record["checks"]) == (True, [])


def test_unit_observe_aceita_sobre_o_readback_unit_real_do_keeper(phase_c: Any) -> None:
    inputs = _unit(phase_c, shape=mu.keeper_readbacks)
    assert set(json.loads(inputs["readbacks"])) == {"state_machine", "task_definitions"}
    record = _observe(phase_c, "unit-observe", inputs)
    assert (record["accepted"], record["checks"]) == (True, [])


def test_unit_observe_reprova_maquina_sem_cluster_unico(phase_c: Any) -> None:
    inputs = _unit(phase_c, shape=mu.keeper_readbacks)
    readbacks = json.loads(inputs["readbacks"])
    machine = readbacks["state_machine"]
    machine["definition"] = machine["definition"].replace('"Cluster"', '"ClusterName"')
    inputs["readbacks"] = canonical(readbacks)
    record = _observe(phase_c, "unit-observe", inputs)
    assert (record["accepted"], record["checks"]) == (
        False, ["canary=routing", "reason=invalid field=cluster.unit"])


def test_unit_observe_reprova_pelas_checagens_do_unit(phase_c: Any) -> None:
    inputs = _unit(phase_c)
    observed = json.loads(inputs["observed"])
    observed["tasks"][0]["containers"] = [{"exitCode": 1}]
    observed["outputs"] = []
    inputs["observed"] = canonical(observed)
    record = _observe(phase_c, "unit-observe", inputs)
    assert record["accepted"] is False
    assert record["checks"] == ["canary=receipts", "unit_output_missing", "unit_task_failed"]


@pytest.mark.parametrize(
    ("name", "error"),
    [pytest.param("activation", "activation=sha256", id="activation-de-outro-request"),
     pytest.param("network", "mode=fields name=network", id="network-com-campo-a-mais"),
     pytest.param("phase-c", "mode=phase_c", id="phase-c-nao-lista"),
     pytest.param("close-drain", "mode=close_binding", id="close-de-outra-phase-b")],
)
def test_recovery_observe_recusa_insumo_fora_do_contrato(phase_c: Any, name: str,
                                                         error: str) -> None:
    inputs, _ = _recovery(phase_c)
    bad = {"activation": b"{}\n", "network": canonical(
        json.loads(inputs["network"]) | {"extra": 1}), "phase-c": canonical({"a": 1}),
        "close-drain": canonical(json.loads(inputs["close-drain"]) | {"decision": "x"})}
    inputs[name] = bad[name]
    context = GateContext(phase_c, inputs)
    with pytest.raises(ValueError, match=error):
        promote_api.execute_shared_operation(
            "recovery-observe", _request(phase_c, "recovery-observe", inputs), context)
    assert context.calls == []


def test_observe_recusa_depois_do_prazo_do_estagio(phase_c: Any) -> None:
    inputs, _ = _recovery(phase_c)
    request = _request(phase_c, "recovery-observe", inputs)
    request["stage"]["deadline"] = request["stage"]["admitted_at"]
    context = GateContext(phase_c, inputs)
    with pytest.raises(ValueError, match="mode=stage_deadline"):
        promote_api.execute_shared_operation("recovery-observe", request, context)
    assert context.calls == []


def _refused(op: Any, name: str, inputs: dict[str, bytes], error: str) -> None:
    op.mp.setattr(promote_api, "CLOCK", lambda: datetime.now(UTC))
    context = GateContext(op, inputs)
    with pytest.raises(ValueError, match=error):
        promote_api.execute_shared_operation(name, _request(op, name, inputs), context)
    assert [call[2] for call in context.calls] in ([], ["get_item", "get_item"])


def _edited(raw: bytes, **changes: Any) -> bytes:
    return canonical(json.loads(raw) | changes)


def test_unit_observe_reprova_result_de_outra_janela_e_routing(phase_c: Any) -> None:
    inputs = _unit(phase_c)
    inputs["mode-result"] = _edited(
        inputs["mode-result"], stage_checkpoint_sha256="0" * 64, pending=["late"],
        fixed_deadline="2026-01-01T00:00:00Z", routing_after_sha256="0" * 64)
    record = _observe(phase_c, "unit-observe", inputs)
    assert record["checks"] == ["canary=common", "canary=pending", "canary=routing",
                                "canary=window"]


def test_unit_observe_reprova_result_de_outro_schema(phase_c: Any) -> None:
    inputs = _unit(phase_c)
    inputs["mode-result"] = _edited(inputs["mode-result"], mode="recovery")
    record = _observe(phase_c, "unit-observe", inputs)
    assert (record["accepted"], record["checks"]) == (False, ["canary=schema"])


@pytest.mark.parametrize(
    ("change", "error"),
    [pytest.param("record", "mode=record_binding", id="record-de-outra-activation"),
     pytest.param("observed", "mode=observed", id="observed-sem-forma"),
     pytest.param("readbacks", "mode=mapping name=readbacks", id="readbacks-nao-objeto")],
)
def test_unit_observe_recusa_insumo_fora_do_contrato(phase_c: Any, change: str,
                                                     error: str) -> None:
    inputs = _unit(phase_c)
    bad = {"record": _edited(inputs["record"], activation_sha256="0" * 64),
           "observed": _edited(inputs["observed"], tasks={}), "readbacks": canonical([])}
    _refused(phase_c, "unit-observe", inputs | {change: bad[change]}, error)


@pytest.mark.parametrize(
    ("change", "error"),
    [pytest.param("activation", "mode=activation_binding", id="phase-b-de-outra-activation"),
     pytest.param("unit", "mode=phase_b_unit", id="phase-b-de-outra-unit"),
     pytest.param("alarms", "mode=readbacks", id="readbacks-sem-alarme")],
)
def test_recovery_observe_recusa_binding_da_phase_b_e_readbacks(phase_c: Any, change: str,
                                                                error: str) -> None:
    inputs, _ = _recovery(phase_c)
    evidence = json.loads(inputs["phase-b"])
    if change == "activation":
        evidence["common"]["activation_sha256"] = "0" * 64
    if change == "unit":
        evidence["routes"]["unit_revision"] = "outra"
    readbacks = json.loads(inputs["readbacks"])
    if change == "alarms":
        del readbacks["alarms"]
    inputs |= {"phase-b": canonical(evidence), "readbacks": canonical(readbacks)}
    _refused(phase_c, "recovery-observe", inputs, error)


def test_audit_observe_reprova_quando_o_recovery_anterior_nao_reaceita(phase_c: Any) -> None:
    inputs = _audit(phase_c)
    readbacks = json.loads(inputs["recovery-readbacks"])
    readbacks["alarms"]["recovery"]["ActionsEnabled"] = False
    inputs["recovery-readbacks"] = canonical(readbacks)
    record = _observe(phase_c, "audit-observe", inputs)
    assert (record["accepted"], record["checks"]) == (False, ["recovery:route=alarm"])


def test_unit_observe_recusa_record_de_outra_revisao_unit(phase_c: Any) -> None:
    other = phase_c.ctx.revisions["unit"].rsplit(":", 1)[0] + ":99"
    _refused(phase_c, "unit-observe", _unit(phase_c, other), "mode=record_binding")


@pytest.mark.parametrize("campo", ["promotion_id", "promotion_generation", "caller"])
def test_unit_observe_recusa_close_drain_de_outro_owner_da_mesma_activation(
        phase_c: Any, campo: str) -> None:
    common = phase_c.close["common"]
    foreign = {"promotion_id": "8" * 64, "promotion_generation": common["promotion_generation"],
               "caller": common["caller"] | {"run_id": 456}}
    inputs = _unit(phase_c)
    if campo == "promotion_generation":
        inputs["record"] = canonical(json.loads(inputs["record"]) | {"promotion_generation": 8})
        foreign["promotion_generation"] = 8
    close = phase_c.close | {"common": common | {campo: foreign[campo]}}
    inputs["close-drain"] = canonical(close)
    _refused(phase_c, "unit-observe", inputs, "mode=record_binding")


@pytest.mark.parametrize(("campo", "valor"), [
    ("owner_sha256", "8" * 64), ("promotion_id", "8" * 64), ("promotion_generation", 8)],
    ids=["owner_sha256", "promotion_id", "promotion_generation"])
def test_unit_observe_recusa_record_de_outro_owner(phase_c: Any, campo: str, valor: Any) -> None:
    inputs = _unit(phase_c)
    inputs["record"] = canonical(json.loads(inputs["record"]) | {campo: valor})
    _refused(phase_c, "unit-observe", inputs, "^mode=record_binding$")


@pytest.mark.parametrize("name", ["unit-observe", "recovery-observe", "audit-observe"])
def test_observe_recusa_close_drain_com_janelas_diferentes_das_do_request(
    phase_c: Any, name: str
) -> None:
    inputs = {"unit-observe": lambda: _unit(phase_c),
              "recovery-observe": lambda: _recovery(phase_c)[0],
              "audit-observe": lambda: _audit(phase_c)}[name]()
    phase_c.mp.setattr(promote_api, "CLOCK", lambda: datetime.now(UTC))
    request = _request(phase_c, name, inputs)
    windows = request["stage"]["windows"]
    request["stage"]["windows"] = {
        "stages": windows["stages"] | {"api": windows["stages"]["api"] + 1},
        "fixed_deadline_seconds": windows["fixed_deadline_seconds"] + 1}
    context = GateContext(phase_c, inputs)
    with pytest.raises(ValueError, match=r"^mode=windows$"):
        promote_api.execute_shared_operation(name, request, context)
    assert context.calls == []
