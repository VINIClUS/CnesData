"""Handler ABI2 closed-ready: reobserva FENCE/SEM e as execuções congeladas só por leitura (#465)."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Final, cast

from cnes_infra.executor.step_functions_probe import StepFunctionsExecutionProbe
from scripts.promotion._adapter_abi import frozen_members, versioned
from scripts.promotion._api_static_records import canonical, sha
from scripts.promotion._closed_ready import build_closed_ready
from scripts.promotion.promote_close import (
    MACHINE,
    FenceShim,
    _activation,
    _Gate,
    owner_identity,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    from scripts.promotion.promote_api import AdapterContext

CLOSE_DRAIN_FIELDS: Final = frozenset({
    "version", "kind", "common", "fence", "close_confirmed_at", "drain_observed_at",
    "frozen_membership_sha256", "members", "proofs", "permit", "input_expirations",
    "fixed_deadline", "decision",
})
PHASE_B_FIELDS: Final = frozenset({
    "version", "kind", "common", "close_drain_sha256", "before", "after", "plan_sha256",
    "audit_sha256", "apply_sha256", "routes", "observed_at",
})


class ReadOnlyShim(FenceShim):
    """Selado desde a criação: leituras fortes de FENCE/SEM e DescribeExecution das execuções."""

    def __init__(self, context: AdapterContext, executions: Iterable[str]) -> None:
        super().__init__(context, MACHINE)
        self.seal()
        self._executions = frozenset(executions)

    def _allowed(self, operation: str, parameters: Mapping[str, Any]) -> bool:
        if operation == "describe_execution":
            arn = parameters.get("executionArn")
            return parameters == {"executionArn": arn, "includedData": "METADATA_ONLY"} and (
                arn in self._executions)
        return operation == "get_item" and super()._allowed(operation, parameters)


def machine_arn(account: str) -> str:
    """Returns: ARN da state machine prod da conta da activation."""
    return f"arn:aws:states:us-east-2:{account}:stateMachine:{MACHINE}"


def closed_ready(request: Mapping[str, Any], inputs: Mapping[str, bytes],
                 context: AdapterContext) -> bytes:
    """Returns:
    Documento closed-ready canônico, com os campos fechados do host gate e do keeper.
    Raises:
    ValueError: insumo fora dos campos fechados, binding, membership ou prazo do estágio.
    DrainError: fence, permit, membership ou prova divergente na reobservação.
    """
    _, account = _activation(inputs["activation"], request)
    close = versioned(inputs["close-drain"], "close-drain", CLOSE_DRAIN_FIELDS, "closed_ready")
    phase = versioned(inputs["phase-b"], "phase-b", PHASE_B_FIELDS, "closed_ready")
    members = frozen_members(inputs["membership"], "closed_ready")
    if (members, sha(canonical(members))) != (close["members"], close["frozen_membership_sha256"]):
        raise ValueError("closed_ready=membership")
    common = close["common"]
    if phase["close_drain_sha256"] != sha(inputs["close-drain"]) or not isinstance(
        common, dict
    ) or cast("dict[str, Any]", common).get("activation_sha256") != request["activation_sha256"]:
        raise ValueError("closed_ready=binding")
    stage = request["stage"]
    admitted = datetime.fromtimestamp(stage["admitted_at"], UTC)
    deadline = datetime.fromtimestamp(stage["deadline"], UTC)
    if _now() >= deadline:
        raise ValueError("closed_ready=stage_deadline")
    machine = machine_arn(account)
    prefix = machine.replace(":stateMachine:", ":execution:") + ":"
    shim = ReadOnlyShim(context, (prefix + member["dispatch_id"] for member in members))
    probe = StepFunctionsExecutionProbe(cast("Any", shim.client("stepfunctions")), machine)
    identity = owner_identity(request)
    return canonical(build_closed_ready(close, phase, _Gate(shim), probe, admitted,
                                        identity["caller"], identity["promotion_generation"],
                                        admitted, deadline))


def _now() -> datetime:
    from scripts.promotion import promote_api

    return promote_api.CLOCK()
