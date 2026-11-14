"""Entrypoint fixo do adapter ABI2: inventário, publicação e rollback estáticos (#473)."""

from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Protocol

from scripts.promotion._api_static_handoff import HOST_GATE_SHA
from scripts.promotion._api_static_records import (
    PLAN_DRIFT_FIELDS,
    ROLLBACK_SCHEMA,
    STATIC_BUCKET,
    canonical,
    inventory_record,
    publication_record,
    read_object,
    read_plan,
    read_publication,
    read_record,
    rollback_targets,
    sha,
    static_plan,
    static_target,
)
from scripts.promotion.build_host_archive import (
    CALLER_REF,
    CALLER_WORKFLOW,
    REPOSITORY,
    archive_manifest,
)
from scripts.promotion.publish_dashboard import (
    PublicationError,
    StaticChannel,
    StaticTarget,
    UploadWindow,
    binding_release_id,
    publish_release,
    read_candidate,
    take_inventory,
)
from scripts.promotion.rollback_api_static import (
    HostPlan,
    HostStep,
    after_host,
    host_outcome,
    recovery_window,
    restore_entrypoints,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

__all__ = ["CLOCK", "canonical", "execute_shared_operation", "static_plan"]

ABI_VERSION = 2
REQUEST_FIELDS = frozenset({
    "abi_version", "operation", "repository", "source_sha", "owner", "checkpoint_sha256",
    "activation_sha256", "stage", "inputs",
})
INPUTS = {
    "inventory": frozenset({"candidate", "dashboard-artifact", "archive", "static-baseline"}),
    "publish-static": frozenset(
        {"candidate", "dashboard-artifact", "static-plan", "inventory", "host-receipt"}
    ),
    "rollback-api-static": frozenset(
        {"static-plan", "inventory", "publication", "host-recovery-receipt", "recovery-decision"}
    ),
}
OBSERVATIONS = {"inventory": "inventory", "publish-static": "publication",
                "rollback-api-static": "rollback"}
_DIGEST = re.compile(r"[0-9a-f]{64}")


class AdapterContext(Protocol):
    def read(self, name: str) -> bytes: ...

    def guard(self) -> None: ...

    def observe(self, kind: str, raw: bytes) -> str: ...

    def call(self, role: str, service: str, operation: str, parameters: dict[str, Any]) -> Any: ...

    def evidence_snapshot(self) -> dict[str, Any]: ...


def _utc_now() -> datetime:
    return datetime.now(UTC)


CLOCK: Callable[[], datetime] = _utc_now


def execute_shared_operation(
    operation: str, request: Mapping[str, Any], context: AdapterContext
) -> dict[str, Any]:
    """Returns:
    Resultado ABI2 com a observação registrada e a incerteza do journal compartilhado.
    Raises:
    ValueError: operação fora desta tarefa ou request/input fora do contrato ABI2.
    PublicationError: binding de candidato, archive, plano ou host divergente.
    """
    handler = _HANDLERS.get(operation)
    if handler is None:
        raise ValueError(f"operation=unsupported name={operation}")
    _require_request(operation, request)
    inputs = {name: _read(context, name, ref) for name, ref in request["inputs"].items()}
    observation = handler(request, inputs, context)
    kind = OBSERVATIONS[operation]
    digest = context.observe(kind, observation)
    snapshot = context.evidence_snapshot()
    return {
        "abi_version": ABI_VERSION,
        "operation": operation,
        "owner_sha256": request["owner"]["owner_sha256"],
        "generation": request["owner"]["generation"],
        "checkpoint_sha256": request["checkpoint_sha256"],
        "observations": [{"kind": kind, "sha256": digest, "size": len(observation)}],
        "call_receipts_sha256": snapshot["call_receipts_sha256"],
        "pending": snapshot["pending"],
        "unknown": snapshot["unknown"],
    }


def _inventory(request: Mapping[str, Any], inputs: Mapping[str, bytes],
               context: AdapterContext) -> bytes:
    candidate = _candidate(request, inputs)
    manifest = archive_manifest(inputs["archive"])
    if (manifest["source_sha"], manifest.get("static_release")) != (
        request["source_sha"], candidate["release_id"]
    ):
        raise PublicationError("inventory=archive_binding")
    channel = StaticChannel(context, StaticTarget(STATIC_BUCKET, ""))
    window = UploadWindow(_clock, _deadline(request["stage"]))
    inventory = take_inventory(channel, inputs["static-baseline"],
                               read_candidate(inputs["dashboard-artifact"]), window)
    return inventory_record(inventory, inputs["candidate"], inputs["dashboard-artifact"],
                            inputs["static-baseline"])


def _publish(request: Mapping[str, Any], inputs: Mapping[str, bytes],
             context: AdapterContext) -> bytes:
    _candidate(request, inputs)
    plan = read_plan(inputs["static-plan"], inputs["inventory"])
    host, closed_ready = _host_receipt(inputs["host-receipt"], request, "host_receipt")
    if host.step is not HostStep.PUBLISH_STATIC:
        raise PublicationError(f"host_receipt=not_accepted next={host.step.value}")
    planned = read_record(inputs["inventory"])
    baseline = canonical(planned["baseline"])
    channel = StaticChannel(context, static_target(plan))
    candidate = read_candidate(inputs["dashboard-artifact"])
    window = UploadWindow(_clock, _deadline(request["stage"]))
    inventory = take_inventory(channel, baseline, candidate, window)
    fresh = read_record(inventory_record(inventory, inputs["candidate"],
                                         inputs["dashboard-artifact"], baseline))
    if any(fresh[name] != planned[name] for name in PLAN_DRIFT_FIELDS):
        raise PublicationError("publish=inventory_drift")
    receipt = publish_release(channel, inventory, candidate, window)
    return publication_record(inputs["static-plan"], receipt, closed_ready)


def _rollback(request: Mapping[str, Any], inputs: Mapping[str, bytes],
              context: AdapterContext) -> bytes:
    plan = read_plan(inputs["static-plan"], inputs["inventory"])
    host, _ = _host_receipt(inputs["host-recovery-receipt"], request, "host_recovery_receipt")
    decision = read_object(inputs["recovery-decision"], "recovery_decision")
    if host.step is not HostStep.RECOVERY or host.decision != decision:
        raise PublicationError(f"host_recovery_receipt=decision next={host.step.value}")
    deadline = min(recovery_window(decision, _clock).deadline, _deadline(request["stage"]))
    publication = read_publication(inputs["publication"], inputs["static-plan"])
    receipt = restore_entrypoints(StaticChannel(context, static_target(plan)),
                                  rollback_targets(read_record(inputs["inventory"])),
                                  publication, UploadWindow(_clock, deadline))
    return canonical({
        "schema": ROLLBACK_SCHEMA,
        "plan_sha256": sha(inputs["static-plan"]),
        "restored": [list(item) for item in receipt.restored],
        "residual": receipt.residual,
        "invalidation_id": receipt.invalidation_id,
    })


_HANDLERS: dict[str, Callable[[Mapping[str, Any], Mapping[str, bytes], AdapterContext], bytes]] = {
    "inventory": _inventory, "publish-static": _publish, "rollback-api-static": _rollback,
}


def _require_request(operation: str, request: Mapping[str, Any]) -> None:
    if set(request) != REQUEST_FIELDS or request["abi_version"] != ABI_VERSION:
        raise ValueError("request=fields")
    if request["operation"] != operation or set(request["inputs"]) != INPUTS[operation]:
        raise ValueError("request=operation")
    owner = request["owner"]
    if (request["repository"], owner["repository"], owner["source_sha"]) != (
        REPOSITORY, REPOSITORY, request["source_sha"]
    ):
        raise ValueError("request=source")


def _read(context: AdapterContext, name: str, reference: Any) -> bytes:
    raw = context.read(name)
    if (not isinstance(reference, dict) or set(reference) != {"sha256", "size"}
            or reference["size"] != len(raw) or reference["sha256"] != sha(raw)):
        raise ValueError(f"input=mismatch name={name}")
    return raw


def _candidate(request: Mapping[str, Any], inputs: Mapping[str, bytes]) -> dict[str, Any]:
    candidate = read_object(inputs["candidate"], "candidate", strict=False)
    if (candidate.get("repository"), candidate.get("source_sha")) != (
        request["repository"], request["source_sha"]
    ):
        raise PublicationError("candidate=source")
    release = candidate.get("release_id")
    binding_release_id(release if isinstance(release, str) else "")
    if candidate.get("dashboard_sha256") != sha(inputs["dashboard-artifact"]):
        raise PublicationError("candidate=dashboard")
    return candidate


def _host_receipt(raw: bytes, request: Mapping[str, Any], name: str) -> tuple[HostPlan, str]:
    value = read_object(raw, name)
    closed_ready = value.get("closed_ready_sha256")
    if (value.get("producer_sha") != HOST_GATE_SHA or not isinstance(closed_ready, str)
            or _DIGEST.fullmatch(closed_ready) is None):
        raise PublicationError(f"{name}=binding")
    # The adapter receives no closed-ready input; the Keeper (#55 slice 2) binds this digest
    # to the closed-ready observation, so here it is only checked and carried in the record.
    outcome = host_outcome(raw, caller=_caller(request), closed_ready_sha256=closed_ready)
    return after_host(outcome), closed_ready


def _caller(request: Mapping[str, Any]) -> dict[str, Any]:
    owner = request["owner"]
    # workflow_sha is source_sha: the preamble job admits only github.sha == inputs.source_sha.
    return {"repository": REPOSITORY, "source_sha": request["source_sha"], "ref": CALLER_REF,
            "workflow_path": CALLER_WORKFLOW, "workflow_sha": request["source_sha"],
            "run_id": owner["run_id"], "run_attempt": owner["run_attempt"]}


def _clock() -> datetime:
    return CLOCK()


def _deadline(stage: Mapping[str, Any]) -> datetime:
    limit = stage["preparation_deadline"] if stage["close"] is None else stage["deadline"]
    return datetime.fromtimestamp(limit, UTC)
