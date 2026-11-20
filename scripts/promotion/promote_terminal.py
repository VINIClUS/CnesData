"""Handlers ABI2 reopen e terminal-readback: reabertura por CAS e leitura final do gate (#465)."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any, Final, cast

from cnes_domain.control_plane.environment_gate import ReopenPromotionFence
from cnes_infra.executor.step_functions_probe import StepFunctionsExecutionProbe
from scripts.promotion._adapter_abi import (
    MEMBER_IDENTITY,
    MEMBER_RESOLVED,
    MODE_OBSERVATION_FIELDS,
    ROUTING_FIELDS,
    closed,
    document,
    frozen_members,
    stage_deadline,
    versioned,
)
from scripts.promotion._api_static_records import (
    PUBLICATION_FIELDS,
    PUBLICATION_SCHEMA,
    ROLLBACK_SCHEMA,
    canonical,
    sha,
)
from scripts.promotion._closed_ready import utc_text
from scripts.promotion.environment_gate import DrainError, _probe_member
from scripts.promotion.promote_close import (
    _FENCE,
    _UPDATE_FIELDS,
    ENVIRONMENT,
    MACHINE,
    TABLE,
    FenceShim,
    _activation,
    _Gate,
)
from scripts.promotion.promote_ready import ReadOnlyShim
from scripts.promotion.rollback_api_static import DECISION_FIELDS, HANDOFF_FIELDS

if TYPE_CHECKING:
    from collections.abc import Mapping
    from datetime import datetime

    from cnes_domain.control_plane.environment_gate import EnvironmentGateState
    from scripts.promotion.promote_api import AdapterContext

REOPEN_EXPRESSION: Final = ("SET #entity = :entity, #state = :state, #promotion = :promotion, "
                            "#version = :version, #changed = :changed")
ROUTE_MODES: Final = ("unit", "recovery", "audit")
ROLLBACK_FIELDS: Final = frozenset(
    {"schema", "plan_sha256", "restored", "residual", "invalidation_id"}
)
OPTIONAL_INPUTS: Final = ("routing", "host-receipt", "static-receipt", "decision")
REQUIRED_INPUTS: Final[dict[str, frozenset[str]]] = {
    "application-prepare": frozenset(),
    "final": frozenset({"routing", "host-receipt", "static-receipt"}),
    "recovery": frozenset({"decision"}),
}
FORBIDDEN_INPUTS: Final[dict[str, frozenset[str]]] = {
    "application-prepare": frozenset(OPTIONAL_INPUTS), "final": frozenset({"decision"}),
    "recovery": frozenset(),
}
_DIGEST: Final = re.compile(r"[0-9a-f]{64}")
_EXECUTION: Final = re.compile(
    r"arn:aws:states:us-east-2:\d{12}:execution:" + MACHINE + r":[0-9a-f]{16}")


class ReopenShim(FenceShim):
    """Leituras fortes de FENCE/SEM e só a escrita CAS que reabre o fence desta promoção."""

    def __init__(self, context: AdapterContext, promotion: str) -> None:
        super().__init__(context, MACHINE)
        self._promotion = promotion

    def _allowed(self, operation: str, parameters: Mapping[str, Any]) -> bool:
        if operation != "update_item":
            return operation == "get_item" and super()._allowed(operation, parameters)
        values = parameters.get("ExpressionAttributeValues") or {}
        return (set(parameters) == _UPDATE_FIELDS
                and (parameters["TableName"], parameters["Key"]) == (TABLE, _FENCE)
                and parameters["UpdateExpression"] == REOPEN_EXPRESSION
                and parameters["ConditionExpression"] == "#version = :expected"
                and values.get(":state") == {"S": "OPEN"}
                and values.get(":promotion") == {"S": self._promotion})


def reopen(request: Mapping[str, Any], inputs: Mapping[str, bytes],
           context: AdapterContext) -> bytes:
    """Returns:
    Observação routing canônica; o fence só reabre com os três modos aceitos e rotas iguais.
    Raises:
    ValueError: estágio sem regra, insumo fora dos campos fechados, fence ou prazo divergente.
    """
    if request["stage"]["stage"] != "final":
        raise ValueError("reopen=recovery_rule_absent")
    activation, _ = _activation(inputs["activation"], request)
    routes = closed(document(inputs["routing"], "routing"), frozenset(ROUTE_MODES), "reopen",
                    "routing")
    if not all(value is None or isinstance(value, str) for value in routes.values()):
        raise ValueError("reopen=routing")
    accepted = _canaries(inputs["canaries"], request)
    if any(member["resolution"] not in MEMBER_RESOLVED
           for member in frozen_members(inputs["membership"], "reopen")):
        raise ValueError("reopen=membership_unresolved")
    now = _now()
    if now.timestamp() >= stage_deadline(request["stage"]):
        raise ValueError("reopen=stage_deadline")
    promotion = request["owner"]["owner_sha256"]
    gate = _Gate(ReopenShim(context, promotion))
    state = gate.observe_environment_gate(ENVIRONMENT)
    if (state.fence.state.value, state.fence.promotion_id, state.permit) != (
        "CLOSED", promotion, None
    ):
        raise ValueError("reopen=fence_not_closed_by_owner")
    routes_match = routes == activation["revisions"]
    if routes_match and accepted:
        command = ReopenPromotionFence(ENVIRONMENT, promotion, now.replace(microsecond=0))
        if not gate.reopen_promotion_fence(command):
            raise ValueError("reopen=fence_cas")
        state = gate.observe_environment_gate(ENVIRONMENT)
        if state.fence.state.value != "OPEN" or state.permit is not None:
            raise ValueError("reopen=readback")
    return canonical({"version": 1, "kind": "routing", "fence": state.fence.state.value,
                      "routes_match": routes_match})


def _canaries(raw: bytes, request: Mapping[str, Any]) -> bool:
    value = document(raw, "canaries")
    if not isinstance(value, list) or len(cast("list[Any]", value)) != len(ROUTE_MODES):
        raise ValueError("reopen=canaries")
    binding = {"owner": request["owner"]["owner_sha256"],
               "activation": request["activation_sha256"]}
    records = [closed(item, MODE_OBSERVATION_FIELDS, "reopen", "canary")
               for item in cast("list[Any]", value)]
    for kind, record in zip(ROUTE_MODES, records, strict=True):
        _canary(record, kind, binding)
    return all(record["accepted"] for record in records)


def _canary(record: dict[str, Any], kind: str, binding: dict[str, Any]) -> None:
    checks, references = record["checks"], record["commitments"]
    if (record["version"], record["kind"]) != (1, kind) or type(record["accepted"]) is not bool:
        raise ValueError(f"reopen=canary_kind kind={kind}")
    if not isinstance(checks, list) or record["accepted"] == bool(checks) or not all(
        isinstance(check, str) for check in cast("list[Any]", checks)
    ):
        raise ValueError(f"reopen=canary_checks kind={kind}")
    if not isinstance(references, dict) or not references or not all(
        isinstance(item, str) and _DIGEST.fullmatch(item)
        for item in cast("dict[str, Any]", references).values()
    ):
        raise ValueError(f"reopen=canary_commitments kind={kind}")
    if any(references.get(key) != expected for key, expected in binding.items()):
        raise ValueError(f"reopen=canary_binding kind={kind}")


def terminal_readback(request: Mapping[str, Any], inputs: Mapping[str, bytes],
                      context: AdapterContext) -> bytes:
    """Returns:
    Observação terminal canônica: membros reprovados, gate, pendências e instante da leitura.
    Raises:
    ValueError: insumo fora dos campos fechados, impossível no estágio ou prazo do estágio.
    """
    stage = request["stage"]
    sources = {name: _source(inputs[name], name) for name in OPTIONAL_INPUTS}
    members = frozen_members(inputs["membership"], "terminal_readback")
    present = {name for name, value in sources.items() if value is not None}
    if (not REQUIRED_INPUTS[stage["stage"]] <= present or present & FORBIDDEN_INPUTS[stage["stage"]]
            or (stage["stage"] == "application-prepare" and members)):
        raise ValueError("terminal_readback=stage_inputs")
    now = _now()
    if now.timestamp() >= stage_deadline(stage):
        raise ValueError("terminal_readback=stage_deadline")
    terminal = [member["execution_ref"] for member in members
                if member["resolution"] == "TERMINAL"]
    if not all(isinstance(ref, str) and _EXECUTION.fullmatch(ref) for ref in terminal):
        raise ValueError("terminal_readback=execution_ref")
    shim = ReadOnlyShim(context, terminal)
    state = _Gate(shim).observe_environment_gate(ENVIRONMENT)
    final, pending = _members(members, shim)
    if state.permit is not None:
        held = {"dispatch_id": state.permit.holder.dispatch_id, "token": state.permit.token}
        pending.append({"kind": "permit-held", "reference_sha256": sha(canonical(held))})
    return canonical({
        "version": 1, "kind": "terminal", "members": final, "quiescent": not pending,
        "state_clean": _clean(stage["stage"], state, sources["routing"],
                              request["owner"]["owner_sha256"]),
        "pending": pending, "unknown": [], "observed_at": utc_text(now.replace(microsecond=0)),
    })


def _source(raw: bytes, name: str) -> dict[str, Any] | None:
    value = document(raw, name)
    if value is None:
        return None
    if name == "routing":
        record = versioned(raw, name, ROUTING_FIELDS, "terminal_readback")
        if record["fence"] not in {"OPEN", "CLOSED"} or type(record["routes_match"]) is not bool:
            raise ValueError("terminal_readback=routing")
        return record
    if name == "host-receipt":
        record = closed(value, HANDOFF_FIELDS, "terminal_readback", name)
        if (record["version"], record["kind"]) != (1, "host-handoff"):
            raise ValueError("terminal_readback=kind name=host-receipt")
        return record
    if name == "decision":
        record = closed(value, DECISION_FIELDS, "terminal_readback", name)
        if record["version"] != 1:
            raise ValueError("terminal_readback=kind name=decision")
        return record
    return _static_receipt(value)


def _static_receipt(value: Any) -> dict[str, Any]:
    schema = value.get("schema") if isinstance(value, dict) else None
    fields = {PUBLICATION_SCHEMA: PUBLICATION_FIELDS, ROLLBACK_SCHEMA: ROLLBACK_FIELDS}.get(
        cast("Any", schema))
    if fields is None:
        raise ValueError("terminal_readback=fields name=static-receipt")
    return closed(value, fields, "terminal_readback", "static-receipt")


def _members(members: list[dict[str, Any]],
             shim: ReadOnlyShim) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    final: list[dict[str, Any]] = []
    pending: list[dict[str, Any]] = []
    for member in members:
        identity = {key: member[key] for key in MEMBER_IDENTITY}
        if member["resolution"] == "TERMINAL" and not _terminal(member, shim):
            identity["resolution"] = "UNRESOLVED"
            pending.append({"kind": "member-unresolved",
                            "reference_sha256": sha(canonical(identity))})
        final.append(identity)
    return final, pending


def _terminal(member: dict[str, Any], shim: ReadOnlyShim) -> bool:
    execution = cast("str", member["execution_ref"])
    machine = execution.rsplit(":", 1)[0].replace(":execution:", ":stateMachine:")
    probe = StepFunctionsExecutionProbe(cast("Any", shim.client("stepfunctions")), machine)
    try:
        _probe_member(member, probe)
    except DrainError:
        return False
    return True


def _clean(stage: str, state: EnvironmentGateState, routing: dict[str, Any] | None,
           promotion: str) -> bool:
    fence = state.fence
    if stage == "application-prepare":
        return not (fence.state.value == "CLOSED" and fence.promotion_id == promotion)
    return routing is not None and (fence.state.value, fence.promotion_id) == (
        routing["fence"], promotion)


def _now() -> datetime:
    from scripts.promotion import promote_api

    return promote_api.CLOCK()
