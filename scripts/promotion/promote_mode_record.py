"""Operações puras -record de modo: record do estágio a partir das fontes autenticadas (#465)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Final, cast

from scripts.promotion._adapter_abi import close_drain_document, document, stage_deadline
from scripts.promotion._api_static_records import canonical, sha
from scripts.promotion.mode_canary_contract import _OWNER_FIELDS, record_bytes
from scripts.promotion.mode_canary_seed import SEED_SCHEMA
from scripts.promotion.prepare_mode_canary import check_mode_canary_input
from scripts.promotion.promote_close import _COMMON, _activation, bound_to_owner
from scripts.promotion.promote_ready import machine_arn

if TYPE_CHECKING:
    from collections.abc import Mapping

    from scripts.promotion.mode_canary_contract import ContextManifest
    from scripts.promotion.promote_api import AdapterContext

Node = dict[str, Any]
RECORD_SCHEMA: Final = "cnesdata.mode-canary/v2"
SCENARIOS: Final = {"unit": "unit_wave", "recovery": "recovery_resume", "audit": "audit_delivery"}
FREEZE_POLICY: Final = canonical({"schema": "cnesdata.mode-canary-freeze-policy/v1",
                                  "denied_aliases": ["mode_dispatch", "mode_seed"]})
PUBLISHED_EVENT_TYPE: Final = "reconciliation.published"
_STAMP: Final = "%Y-%m-%dT%H:%M:%SZ"
_SEED_FIELDS: Final = ("schema", "owner_sha256", "tenant_id", "run_ids", "fixture_manifest_sha256")


@dataclass(frozen=True, slots=True)
class _Sources:
    owner_sha256: str
    activation: Node
    account: str
    close: Node
    manifest: ContextManifest
    resources: Node


def unit_record(request: Mapping[str, Any], inputs: Mapping[str, bytes],
                context: AdapterContext) -> bytes:
    """Returns: Observação `mode-record` do unit. Raises: ValueError: binding ou prazo."""
    return _mode_record("unit", request, inputs)


def recovery_record(request: Mapping[str, Any], inputs: Mapping[str, bytes],
                    context: AdapterContext) -> bytes:
    """Returns: Observação `mode-record` do recovery. Raises: ValueError: binding ou prazo."""
    return _mode_record("recovery", request, inputs)


def audit_record(request: Mapping[str, Any], inputs: Mapping[str, bytes],
                 context: AdapterContext) -> bytes:
    """Returns: `mode-record` do audit com os ids publicados. Raises: ValueError: seed/prazo."""
    return _mode_record("audit", request, inputs)


def _mode_record(mode: str, request: Mapping[str, Any], inputs: Mapping[str, bytes]) -> bytes:
    sources = _sources(request, inputs)
    runs = _bind(mode, request, inputs, sources)
    seed_sha256, event_ids = _seed(mode, inputs, sources, runs)
    activation = sources.activation
    common = sources.close["common"]
    record = {
        "schema": RECORD_SCHEMA, "activation_sha256": request["activation_sha256"],
        **{key: activation[key] for key in ("candidate_sha256", "source_sha", "release_id",
                                            "canary_id")},
        "owner_sha256": sources.owner_sha256, "promotion_id": common["promotion_id"],
        "promotion_generation": common["promotion_generation"],
        "phase_receipt_sha256": sha(inputs["close-drain"]),
        "machine_arn": machine_arn(sources.account), "revisions": activation["revisions"],
        "run_ids": runs, "fixture_manifest_sha256": sha(inputs["fixture-manifest"]),
        "seed_manifest_sha256": seed_sha256, "phase": mode,
        "freeze_policy_sha256": sha(FREEZE_POLICY), "context_kind": "promotion",
        "context_manifest_sha256": sources.manifest.sha256, "scenario": SCENARIOS[mode],
        "fault": "none", "mode": mode, **_deadlines(request, sources),
    }
    text = record_bytes(record).decode()
    return canonical({"version": 1, "kind": "mode-record", "record": text,
                      "audit_event_ids": event_ids})


def _sources(request: Mapping[str, Any], inputs: Mapping[str, bytes]) -> _Sources:
    activation, account = _activation(inputs["activation"], request)
    close = close_drain_document(inputs["close-drain"], "mode", request["stage"]["windows"])
    value = cast("Node", document(inputs["mode-input"], "mode-input"))
    manifest = check_mode_canary_input(value)
    return _Sources(request["owner"]["owner_sha256"], activation, account, close, manifest,
                    value["resources"])


def _runs(mode: str, owner_sha256: str) -> list[str]:
    prefixes = ("unit",) if mode == "unit" else ("recovery-a", "recovery-b")
    return [f"{prefix}-{owner_sha256[:16]}" for prefix in prefixes]


def _bind(mode: str, request: Mapping[str, Any], inputs: Mapping[str, bytes],
          sources: _Sources) -> list[str]:
    activation, common = sources.activation, sources.close["common"]
    fields, resources = sources.manifest.fields, sources.resources
    scope, owner = resources["scope"], fields["owner"]
    actual = (
        tuple(common.get(key) for key in _COMMON), common.get("activation_sha256"),
        fields["context_kind"], {key: owner.get(key) for key in _OWNER_FIELDS},
        fields["activation_sha256"], fields["release_id"], fields["fixture_manifest_sha256"],
        resources["revisions"], resources["machine_arn"], scope["tenant_id"],
    )
    expected = (
        tuple(activation[key] for key in _COMMON), request["activation_sha256"], "promotion",
        {key: request["owner"][key] for key in _OWNER_FIELDS}, request["activation_sha256"],
        activation["release_id"], sha(inputs["fixture-manifest"]), activation["revisions"],
        machine_arn(sources.account), f"prod-canary-{activation['canary_id']}",
    )
    runs = _runs(mode, sources.owner_sha256)
    if (actual != expected or not bound_to_owner(common, request)
            or not set(runs) <= set(scope["run_ids"])):
        raise ValueError("mode=record_binding")
    return runs


def _seed(mode: str, inputs: Mapping[str, bytes], sources: _Sources,
          runs: list[str]) -> tuple[str, list[str] | None]:
    fixture = sha(inputs["fixture-manifest"])
    if mode != "audit":
        return fixture, None
    value = document(inputs["mode-seed"], "mode-seed")
    tenant = sources.resources["scope"]["tenant_id"]
    expected = (SEED_SCHEMA, sources.owner_sha256, tenant, runs, fixture)
    if not isinstance(value, dict) or tuple(
            cast("Node", value).get(field) for field in _SEED_FIELDS) != expected:
        raise ValueError("mode=record_seed")
    return sha(inputs["mode-seed"]), [f"{PUBLISHED_EVENT_TYPE}:{tenant}:{run}" for run in runs]


def _moment(text: Any) -> datetime:
    try:
        return datetime.strptime(cast("str", text), _STAMP).replace(tzinfo=UTC)
    except (TypeError, ValueError) as error:
        raise ValueError("mode=record_deadline") from error


def _deadlines(request: Mapping[str, Any], sources: _Sources) -> dict[str, str]:
    from scripts.promotion import promote_api

    fixed = _moment(sources.close["fixed_deadline"])
    stage = datetime.fromtimestamp(stage_deadline(request["stage"]), UTC)
    canary = min(fixed, stage)
    if promote_api.CLOCK() >= canary:
        raise ValueError("mode=record_deadline")
    return {"fixed_deadline": fixed.strftime(_STAMP), "stage_deadline": stage.strftime(_STAMP),
            "canary_deadline": canary.strftime(_STAMP), "expires_at": canary.strftime(_STAMP)}


__all__ = [
    "FREEZE_POLICY",
    "PUBLISHED_EVENT_TYPE",
    "RECORD_SCHEMA",
    "SCENARIOS",
    "audit_record",
    "recovery_record",
    "unit_record",
]
