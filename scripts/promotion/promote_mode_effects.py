"""Handlers ABI3 dos efeitos de modo: seed e start pelos aliases e métodos do keeper (#465)."""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from io import BytesIO
from typing import TYPE_CHECKING, Any, Final, cast

from cnes_infra.aws.mode_canary import initialize_control, require_seeded_control
from cnes_infra.aws.mode_canary_frames import initial_control, record_item
from scripts.promotion._adapter_abi import (
    MODE_EFFECT_CALLS,
    MODE_EFFECT_METHODS,
    MODE_EFFECT_STEPS,
    MODE_OPERATIONS,
    document,
    stage_deadline,
    versioned,
)
from scripts.promotion._api_static_records import sha
from scripts.promotion._canonical import decode_json
from scripts.promotion.mode_canary import (
    FreezeGuard,
    _record,
    _stamp,
    execute_mode_canary,
)
from scripts.promotion.mode_canary_contract import ContractError
from scripts.promotion.mode_canary_seed import (
    FIXTURE_MANIFEST,
    SEED_SCHEMA,
    SeedBinding,
    SeedClients,
    _put_item,
    build_mode_seed,
    execute_mode_seed,
)
from scripts.promotion.promote_close import TABLE
from scripts.promotion.promote_mode import _mapping

if TYPE_CHECKING:
    from collections.abc import Mapping

    from cnes_domain.orchestration.mode_canary import ModeCanaryBinding, ModeCanaryRecord
    from scripts.promotion.mode_canary import EffectLedger
    from scripts.promotion.promote_api import AdapterContext, ModeEffectContext

Node = dict[str, Any]
BUCKET: Final = "cnesdata-prod-data"
SDK_OPERATIONS: Final = {
    "dynamodb:PutItem": ("dynamodb", "put_item"), "dynamodb:GetItem": ("dynamodb", "get_item"),
    "s3:PutObject": ("s3", "put_object"), "s3:GetObject": ("s3", "get_object"),
    "ecs:RunTask": ("ecs", "run_task"), "ecs:DescribeTasks": ("ecs", "describe_tasks"),
}
_START_FIELDS: Final = frozenset({"version", "kind", "frozen", "started_at"})
_STAMP: Final = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z")
_FIXTURE_INPUTS: Final = {"CNES_LOCAL": "fixture-cnes-local",
                          "CNES_NACIONAL": "fixture-cnes-nacional"}


class _Ledger:
    def __init__(self, context: ModeEffectContext) -> None:
        self._context = context

    def admit_effect(self, kind: str, request: str, target: str, now: datetime) -> str:
        return self._context.ledger_admit_effect(kind, request, target, str(_stamp(now)))

    def mark_unknown(self, effect_id: str) -> None:
        self._context.ledger_mark_unknown(effect_id)

    def settle(self, effect_id: str, receipt: str) -> None:
        self._context.ledger_settle(effect_id, receipt)


class ModeCalls:
    """`ModeCanaryCalls` sobre o context ABI3: tabela fechada da operação antes de qualquer IO."""

    def __init__(self, context: AdapterContext, operation: str) -> None:
        self._context = cast("ModeEffectContext", context)
        self._operation = operation

    def now(self) -> datetime:
        from scripts.promotion import promote_api

        return promote_api.CLOCK()

    def call(self, alias: str, operation: str, request: Node) -> Node:
        """Raises: ValueError: Alias ou operação fora da tabela da operação ABI3."""
        service, name = SDK_OPERATIONS.get(operation, ("", ""))
        if (alias, service, name) not in MODE_EFFECT_CALLS[self._operation]:
            raise ValueError(f"mode=call alias={alias} operation={operation}")
        return self._context.call(alias, service, name, request)

    def observe(self, name: str, body: bytes) -> Node:
        """Raises: ValueError: Observação intermediária fora da operação ou digest divergente."""
        if name not in MODE_EFFECT_STEPS[self._operation]:
            raise ValueError(f"mode=observation name={name}")
        digest = self._context.observe(name, body)
        if digest != sha(body):
            raise ValueError(f"mode=observation_digest name={name}")
        return {"sha256": digest, "size": len(body)}

    def ledger(self) -> EffectLedger:
        self._method("ledger_admit_effect")
        return _Ledger(self._context)

    def dispatch_unit(self, binding: ModeCanaryBinding) -> Node:
        self._method("dispatch_unit")
        return self._context.dispatch_unit(_locator(binding))

    def readbacks(self) -> Node:
        self._method("mode_readbacks")
        return self._context.mode_readbacks()

    def _method(self, name: str) -> None:
        if name not in MODE_EFFECT_METHODS[self._operation]:
            raise ValueError(f"mode=method name={name}")


class _SeedChannel:
    def __init__(self, calls: ModeCalls) -> None:
        self._calls = calls

    def put_item(self, **request: Any) -> Node:
        return self._calls.call("mode_seed", "dynamodb:PutItem", request)

    def get_item(self, **request: Any) -> Node:
        return self._calls.call("mode_seed", "dynamodb:GetItem", request)

    def put_object(self, **request: Any) -> Node:
        return self._calls.call("mode_seed", "s3:PutObject", request)

    def get_object(self, **request: Any) -> Node:
        body = self._calls.call("mode_seed", "s3:GetObject", request).get("Body")
        if type(body) is not bytes:
            raise ValueError("mode=object_body")
        return {"Body": BytesIO(body)}


def unit_seed(request: Mapping[str, Any], inputs: Mapping[str, bytes],
              context: AdapterContext) -> bytes:
    """Returns: Artifact `mode-seed` do unit depois do Put insert-only e da releitura.

    Raises: ValueError: Binding, prazo ou freeze antes de qualquer chamada; `seed_conflict`.
    """
    return _seed("unit-seed", request, inputs, context)


def recovery_seed(request: Mapping[str, Any], inputs: Mapping[str, bytes],
                  context: AdapterContext) -> bytes:
    """Returns: Artifact `mode-seed` dos runs recovery-a/b depois do Put e da releitura.

    Raises: ValueError: Binding, prazo ou freeze antes de qualquer chamada; `seed_conflict`.
    """
    return _seed("recovery-seed", request, inputs, context)


def unit_start(request: Mapping[str, Any], inputs: Mapping[str, bytes],
               context: AdapterContext) -> bytes:
    """Returns: `cnesdata.mode-result/v1` do unit despachado pelo keeper.

    Raises: ValueError: Binding, prazo, rota ou freeze antes de qualquer efeito.
    """
    return _start("unit-start", request, inputs, context)


def recovery_start(request: Mapping[str, Any], inputs: Mapping[str, bytes],
                   context: AdapterContext) -> bytes:
    """Returns: `cnesdata.mode-result/v1` do RunTask de recovery admitido no ledger.

    Raises: ValueError: Binding, prazo, rota ou freeze antes de qualquer efeito.
    """
    return _start("recovery-start", request, inputs, context)


def audit_start(request: Mapping[str, Any], inputs: Mapping[str, bytes],
                context: AdapterContext) -> bytes:
    """Returns: `cnesdata.mode-result/v1` do RunTask de audit com a prova do freeze.

    Raises: ValueError: Binding, prazo, rota ou eventos fora do contrato antes do efeito.
    """
    return _start("audit-start", request, inputs, context)


def _seed(name: str, request: Mapping[str, Any], inputs: Mapping[str, bytes],
          context: AdapterContext) -> bytes:
    record, _, start = _bound(name, request, inputs)
    if sha(inputs["fixture-manifest"]) != record.fixture_manifest_sha256:
        raise ValueError("mode=fixture_binding")
    calls = ModeCalls(context, name)
    _admit_stage(request, calls)
    FreezeGuard(start["frozen"]).admit(name)
    if not calls.now() < min(record.canary_deadline, record.expires_at):
        raise ContractError("reason=invalid field=canary_deadline")
    binding = SeedBinding(f"prod-canary-{record.canary_id}", record.run_ids, record.owner_sha256,
                          _instant(start["started_at"]), BUCKET)
    seed = build_mode_seed(binding, _fixtures(inputs))
    channel = _SeedChannel(calls)
    if record.mode == "recovery":
        require_seeded_control(channel, TABLE, record)
    execute_mode_seed(seed, SeedClients(channel, channel), TABLE)
    _put_item(channel, TABLE, record_item(record, inputs["record"].decode()))
    if record.mode == "unit":
        control = initial_control(record, _instant(start["started_at"]))
        initialize_control(channel, TABLE, binding.tenant_id, control)
    return seed.manifest


def _start(name: str, request: Mapping[str, Any], inputs: Mapping[str, bytes],
           context: AdapterContext) -> bytes:
    record, _, start = _bound(name, request, inputs)
    seed_sha256 = _seed_binding(inputs["mode-seed"], record)
    calls = ModeCalls(context, name)
    _admit_stage(request, calls)
    previous = document(inputs["previous"], "previous")
    originals: Node = {
        "record": inputs["record"], "readbacks": _mapping(inputs["readbacks"], "readbacks"),
        "readbacks_after": calls.readbacks,
        "previous": None if previous is None else inputs["previous"],
        "frozen": start["frozen"], "seed_sha256": seed_sha256, "started_at": start["started_at"],
    }
    if record.mode == "audit":
        originals["audit_event_ids"] = document(inputs["audit-event-ids"], "audit-event-ids")
    return execute_mode_canary(record.mode, originals, calls)


def _bound(name: str, request: Mapping[str, Any],
           inputs: Mapping[str, bytes]) -> tuple[ModeCanaryRecord, str, Node]:
    record, record_sha = _record({"record": inputs["record"]}, MODE_OPERATIONS[name][0])
    if (record.activation_sha256, record.owner_sha256) != (
        request["activation_sha256"], request["owner"]["owner_sha256"]
    ):
        raise ValueError("mode=record_binding")
    start = versioned(inputs["mode-start"], "mode-start", _START_FIELDS, "mode")
    if type(start["frozen"]) is not bool or not (
        isinstance(start["started_at"], str) and _STAMP.fullmatch(start["started_at"])
    ):
        raise ValueError("mode=mode_start")
    _instant(start["started_at"])
    return record, record_sha, start


def _seed_binding(raw: bytes, record: ModeCanaryRecord) -> str:
    value = document(raw, "mode-seed")
    fields = ("schema", "owner_sha256", "tenant_id", "run_ids", "fixture_manifest_sha256")
    expected = (SEED_SCHEMA, record.owner_sha256, f"prod-canary-{record.canary_id}",
                list(record.run_ids), record.fixture_manifest_sha256)
    if not isinstance(value, dict) or tuple(cast("Node", value).get(f) for f in fields) != expected:
        raise ValueError("mode=seed_binding")
    return sha(raw)


def _admit_stage(request: Mapping[str, Any], calls: ModeCalls) -> None:
    if calls.now().timestamp() >= stage_deadline(request["stage"]):
        raise ValueError("mode=stage_deadline")


def _fixtures(inputs: Mapping[str, bytes]) -> dict[str, bytes]:
    manifest = inputs["fixture-manifest"]
    try:
        files = cast("Node", decode_json(manifest))["files"]
        names = {_FIXTURE_INPUTS[entry["source_type"]]: name for name, entry in files.items()}
    except (AttributeError, KeyError, TypeError, ValueError) as error:
        raise ContractError("reason=invalid field=fixture_manifest") from error
    return {FIXTURE_MANIFEST: manifest, **{file: inputs[key] for key, file in names.items()}}


def _instant(stamp: str) -> datetime:
    try:
        return datetime.strptime(stamp, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
    except ValueError as error:
        raise ValueError("mode=mode_start") from error


def _locator(binding: ModeCanaryBinding) -> str:
    return json.dumps({"context_kind": binding.context_kind, "owner_sha256": binding.owner_sha256,
                       "phase": binding.phase, "record_sha256": binding.record_sha256,
                       "run_ids": list(binding.run_ids), "tenant_id": binding.tenant_id},
                      sort_keys=True, separators=(",", ":"))


__all__ = [
    "BUCKET",
    "SDK_OPERATIONS",
    "ModeCalls",
    "audit_start",
    "recovery_seed",
    "recovery_start",
    "unit_seed",
    "unit_start",
]
