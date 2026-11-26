"""Handlers ABI3 dos -observe de modo: aceite do canary no adapter, sem efeito nativo (#465)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Final, cast

from scripts.promotion._adapter_abi import (
    close_drain_document,
    closed,
    document,
    stage_deadline,
    versioned,
)
from scripts.promotion._api_static_records import canonical, sha
from scripts.promotion._phase_c_core import (
    _RESULT_COMMON,
    MODES,
    PhaseBContext,
    PhaseCContext,
    PhaseCWatchdog,
    _second,
    accept_audit_canary,
    accept_recovery_canary,
    assert_fence,
    object_hash,
)
from scripts.promotion.mode_canary import (
    RESULT_FIELDS,
    RESULT_SCHEMA,
    _common,
    _record,
    _text,
    observed_shape,
    route_from_readbacks,
    unit_binding,
)
from scripts.promotion.mode_canary_unit import unit_pending
from scripts.promotion.promote_close import (
    ENVIRONMENT,
    _activation,
    _Gate,
    bound_to_owner,
    owner_identity,
)
from scripts.promotion.promote_ready import PHASE_B_FIELDS, ReadOnlyShim, machine_arn

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from datetime import datetime

    from cnes_domain.control_plane.environment_gate import EnvironmentGateState
    from cnes_domain.orchestration.mode_canary import ModeCanaryRecord
    from scripts.promotion.promote_api import AdapterContext

Node = dict[str, Any]
NETWORK_FIELDS: Final = frozenset({"cluster", "subnets", "security_group", "public_ip",
                                   "platform"})
OBSERVED_FIELDS: Final = frozenset({"execution", "tasks", "outputs"})
_NO_FAILURES: Final = {"failures": []}
_RECEIPTS: Final = (("execution_receipts", "execution"), ("task_receipts", "tasks"),
                    ("output_receipts", "outputs"))


def unit_observe(request: Mapping[str, Any], inputs: Mapping[str, bytes],
                 context: AdapterContext) -> bytes:
    """Returns:
    Observação de modo do unit: aceite pelas checagens do unit sobre a execução observada.
    Raises:
    ValueError: insumo fora dos campos fechados, record de outra activation ou owner, prazo.
    """
    activation, _ = _activation(inputs["activation"], request)
    close = close_drain_document(inputs["close-drain"], "mode", request["stage"]["windows"])
    source = _record({"record": inputs["record"]}, "unit")
    record, identity = source[0], owner_identity(request)
    if (record.activation_sha256, record.owner_sha256, record.promotion_id,
            record.promotion_generation, dict(record.revisions)) != (
        request["activation_sha256"], request["owner"]["owner_sha256"], identity["promotion_id"],
        identity["promotion_generation"], activation["revisions"],
    ) or not bound_to_owner(close["common"], request):
        raise ValueError("mode=record_binding")
    readbacks = cast("Node", _mapping(inputs["readbacks"], "readbacks"))
    observed = _observed(inputs["observed"])
    result = document(inputs["mode-result"], "mode-result")
    state = _gate(request, context)
    checks = _unit_result(result, source, readbacks, close)
    if "canary=schema" not in checks:
        checks |= _unit_effects(cast("Node", result), observed, source, readbacks)
        checks |= set(_verdict(lambda: assert_fence(state, close))[1])
    return _observation("unit", sorted(checks), request, inputs)


def recovery_observe(request: Mapping[str, Any], inputs: Mapping[str, bytes],
                     context: AdapterContext) -> bytes:
    """Returns:
    Observação de modo do recovery: aceite do canary sobre a Phase C original.
    Raises:
    ValueError: insumo fora dos campos fechados, binding da Phase B/close ou prazo vencido.
    """
    return _scheduled("recovery", request, inputs, context)


def audit_observe(request: Mapping[str, Any], inputs: Mapping[str, bytes],
                  context: AdapterContext) -> bytes:
    """Returns:
    Observação de modo do audit: recovery reaceito, aceite do audit e prova do freeze.
    Raises:
    ValueError: insumo fora dos campos fechados, binding da Phase B/close ou prazo vencido.
    """
    return _scheduled("audit", request, inputs, context)


def _scheduled(mode: str, request: Mapping[str, Any], inputs: Mapping[str, bytes],
               context: AdapterContext) -> bytes:
    base = _phase_c_base(request, inputs)
    evidence = document(inputs["phase-c"], "phase-c")
    if not isinstance(evidence, list) or len(cast("list[Any]", evidence)) != (
        MODES.index(mode) + 1
    ):
        raise ValueError("mode=phase_c")
    runs = [(inputs["mode-result"], _mapping(inputs["readbacks"], "readbacks"))]
    if mode == "audit":
        prior = (inputs["recovery-result"],
                 _mapping(inputs["recovery-readbacks"], "recovery-readbacks"))
        runs.insert(0, prior)
    state = _gate(request, context)
    checks = _canaries(base, cast("list[Node]", evidence), runs, state)
    return _observation(mode, checks, request, inputs)


def _phase_c_base(request: Mapping[str, Any], inputs: Mapping[str, bytes]) -> PhaseCContext:
    activation, account = _activation(inputs["activation"], request)
    network = closed(document(inputs["network"], "network"), NETWORK_FIELDS, "mode", "network")
    close = close_drain_document(inputs["close-drain"], "mode", request["stage"]["windows"])
    evidence = versioned(inputs["phase-b"], "phase-b", PHASE_B_FIELDS, "mode")
    if evidence["common"].get("activation_sha256") != request["activation_sha256"]:
        raise ValueError("mode=activation_binding")
    if (object_hash(close), close["common"]) != (evidence["close_drain_sha256"],
                                                 evidence["common"]):
        raise ValueError("mode=close_binding")
    if evidence["routes"].get("unit_revision") != activation["revisions"]["unit"]:
        raise ValueError("mode=phase_b_unit")
    canary = {"account_id": account, "network": network, "cluster": network["cluster"],
              "revisions": dict(activation["revisions"]),
              **{key: activation[key] for key in _RESULT_COMMON}}
    phase_b = PhaseBContext(canary, {}, {"arn": machine_arn(account)}, inputs["activation"])
    PhaseCWatchdog.from_close(close)
    return PhaseCContext(phase_b, evidence, close)


def _canaries(context: PhaseCContext, evidence: list[Node], runs: list[tuple[bytes, Any]],
              state: EnvironmentGateState) -> list[str]:
    accepts = (accept_recovery_canary, accept_audit_canary)
    for index, (item, (raw, readbacks)) in enumerate(zip(evidence, runs, strict=True)):
        context = context.accept(item)
        step = accepts[index]
        accepted, checks = _verdict(lambda c=context, s=step, r=raw, b=readbacks: s(r, c, b, state))
        if checks:
            prefix = "" if index == len(runs) - 1 else f"{MODES[index]}:"
            return [prefix + check for check in checks]
        context = cast("PhaseCContext", accepted)
    return []


def _verdict(step: Callable[[], Any]) -> tuple[Any, list[str]]:
    try:
        return step(), []
    except (KeyError, TypeError) as error:
        raise ValueError("mode=readbacks") from error
    except ValueError as error:
        return None, [str(error)]


def _unit_result(result: Any, source: tuple[ModeCanaryRecord, str], readbacks: Node,
                 close: Node) -> set[str]:
    record, record_sha = source
    if (not isinstance(result, dict) or set(cast("Node", result)) != set(RESULT_FIELDS)
            or result["schema"] != RESULT_SCHEMA or result["mode"] != "unit"):
        return {"canary=schema"}
    checks: set[str] = set()
    if (result["stage_checkpoint_sha256"], result["common"]) != (record_sha, _common(record)):
        checks.add("canary=common")
    if _second(result["fixed_deadline"]) != _second(close["fixed_deadline"]):
        checks.add("canary=window")
    if result["pending"] or result["unknown"]:
        checks.add("canary=pending")
    routing = sha(_text(readbacks).encode())
    if not result["routing_before_sha256"] == routing == result["routing_after_sha256"]:
        checks.add("canary=routing")
    return checks


def _unit_effects(result: Node, observed: Node, source: tuple[ModeCanaryRecord, str],
                  readbacks: Node) -> set[str]:
    checks: set[str] = set()
    for receipts, name in _RECEIPTS:
        items = [observed[name]] if name == "execution" else observed[name]
        bound = [receipt.get("sha256") if isinstance(receipt, dict) else None
                 for receipt in result[receipts]]
        if bound != [sha(_text(item).encode()) for item in items]:
            checks.add("canary=receipts")
    route, failures = _verdict(lambda: route_from_readbacks(source[0], readbacks, "unit"))
    if failures:
        return checks | set(failures)
    return checks | unit_pending(observed | _NO_FAILURES, route, unit_binding(*source))


def _observed(raw: bytes) -> Node:
    value = closed(document(raw, "observed"), OBSERVED_FIELDS, "mode", "observed")
    if not observed_shape(value | _NO_FAILURES):
        raise ValueError("mode=observed")
    return value


def _mapping(raw: bytes, name: str) -> Node:
    value = document(raw, name)
    if not isinstance(value, dict):
        raise ValueError(f"mode=mapping name={name}")
    return cast("Node", value)


def _gate(request: Mapping[str, Any], context: AdapterContext) -> EnvironmentGateState:
    if _now().timestamp() >= stage_deadline(request["stage"]):
        raise ValueError("mode=stage_deadline")
    return _Gate(ReadOnlyShim(context, [])).observe_environment_gate(ENVIRONMENT)


def _observation(kind: str, checks: list[str], request: Mapping[str, Any],
                 inputs: Mapping[str, bytes]) -> bytes:
    commitments = {"owner": request["owner"]["owner_sha256"]}
    commitments |= {name: sha(raw) for name, raw in inputs.items()}
    return canonical({"version": 1, "kind": kind, "accepted": not checks,
                      "checks": sorted(checks), "commitments": commitments})


def _now() -> datetime:
    from scripts.promotion import promote_api

    return promote_api.CLOCK()


__all__ = ["audit_observe", "recovery_observe", "unit_observe"]
