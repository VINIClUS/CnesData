"""Runtime do canary de modo no processor: facade, executor, locator e falhas."""

from __future__ import annotations

import json
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Final, cast

from cnes_domain.orchestration.mode_canary import (
    ModeCanaryBinding,
    SyntheticContextRequired,
    is_mode_canary_namespace,
)
from cnes_infra.aws.mode_canary import canonical_text, start_effect_id
from cnes_infra.control_plane.dynamodb_keys import run_partition
from data_processor.deadline import shorten_active_deadline

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import datetime

    from cnes_domain.control_plane.environment_gate import PermitHolder
    from cnes_domain.ports.object_store import ObjectStorePort
    from cnes_domain.ports.processing import StartRunExecution
    from cnes_infra.aws.mode_canary import ModeCanaryContext

_BINDING_FIELDS: Final = frozenset({
    "schema", "tenant_id", "run_id", "dispatch_id", "owner_sha256", "record_sha256", "phase",
    "context_kind",
})
_RAW_PREFIX: Final = "raw/"
_RECOVER_ONCE: Final = "recover-once"


class ModeCanaryFault(StrEnum):
    NONE = "none"
    SYNTHETIC_READ_FAILURE = "synthetic_read_failure"
    HANG_NO_DISPATCH = "hang_no_dispatch"


class SyntheticFixtureReadFailure(RuntimeError):
    """Falha controlada da leitura da fixture raw sintética, só sob record autenticado."""


def _refuse(reason: str) -> SyntheticContextRequired:
    return SyntheticContextRequired(f"reason={reason} field=tenant_id")


_RUN_SCOPED: Final = (
    "get_run", "list_run_units", "get_run_dispatch", "get_active_run_dispatch",
    "get_run_billing_state", "transition_run", "put_run_units", "claim_run_unit",
    "commit_run_unit", "fail_run_unit", "finalize_run_cancellation", "reserve_run_dispatch",
    "claim_run_dispatch_start", "cancel_reserved_run_dispatch", "bind_run_dispatch",
    "finish_run_dispatch", "bind_run_execution",
)
_TENANT_SCOPED: Final = (
    "get_dataset_pointer", "get_dataset_version", "publish_dataset", "list_raw_manifest_chain",
    "query_raw_manifest_by_id", "query_raw_manifest_chain", "list_waiting_runs_for_dependency",
    "query_waiting_runs_for_dependency",
)


def _scope(args: tuple[Any, ...], kwargs: dict[str, Any]) -> tuple[Any, Any]:
    if args and isinstance(args[0], str):
        return args[0], args[1] if len(args) > 1 else kwargs.get("run_id")
    if "tenant_id" in kwargs:
        return kwargs["tenant_id"], kwargs.get("run_id")
    first = args[0] if args else next(iter(kwargs.values()), None)
    return getattr(first, "tenant_id", None), getattr(first, "run_id", None)


class CanaryControlPlane:
    """Facade fail-closed do control plane: só o tenant/runs do binding e métodos nominais."""

    def __init__(self, inner: Any, binding: ModeCanaryBinding) -> None:
        self._inner = inner
        self._binding = binding

    def list_recoverable_runs(self, now: datetime, limit: int = 100) -> tuple[Any, ...]:
        """Returns: Runs do binding lidos fortemente por get_run, sem GSI4 global."""
        del now
        runs = (self._inner.get_run(self._binding.tenant_id, run_id)
                for run_id in self._binding.run_ids)
        return tuple(run for run in runs if run is not None)[:limit]

    def invoke(self, name: str, run_scoped: bool, *args: Any, **kwargs: Any) -> Any:
        """Returns: Resultado do método nominal do control plane dentro do binding.

        Raises: SyntheticContextRequired: Tenant ou run fora do binding.
        """
        tenant_id, run_id = _scope(args, kwargs)
        inside = tenant_id == self._binding.tenant_id and (
            not run_scoped or run_id in self._binding.run_ids
        )
        if not inside:
            raise _refuse("outside_binding")
        return getattr(self._inner, name)(*args, **kwargs)


def _delegate(name: str, run_scoped: bool) -> Callable[..., Any]:
    def method(self: CanaryControlPlane, *args: Any, **kwargs: Any) -> Any:
        return self.invoke(name, run_scoped, *args, **kwargs)

    method.__name__ = name
    return method


for _name in _RUN_SCOPED:
    setattr(CanaryControlPlane, _name, _delegate(_name, run_scoped=True))
for _name in _TENANT_SCOPED:
    setattr(CanaryControlPlane, _name, _delegate(_name, run_scoped=False))


class ModeCanaryExecutor:
    """Executor que confere o ticket vivo do dispatch sintético imediatamente antes do start."""

    def __init__(
        self, inner: Any, context: ModeCanaryContext, clock: Callable[[], datetime],
    ) -> None:
        self._inner = inner
        self._context = context
        self._clock = clock

    def start(self, request: StartRunExecution) -> str:
        """Raises: SyntheticContextRequired: Start sintético sem claim nativo."""
        if is_mode_canary_namespace(request.tenant_id):
            raise _refuse("claim_required")
        return cast("str", self._inner.start(request))

    def start_with_claim(self, request: StartRunExecution, claim: Any) -> str:
        """Raises: SyntheticContextRequired: Ticket do dispatch ausente, não pendente ou selado."""
        if is_mode_canary_namespace(request.tenant_id):
            effect_id = start_effect_id(
                self._context.binding.record_sha256, request.tenant_id, request.run_id,
                request.dispatch_id, request.unit_ids,
            )
            self._context.require_live(effect_id, self._clock())
        return cast("str", self._inner.start_with_claim(request, claim))

    def status(self, execution_ref: str) -> Any:
        """Returns: Status nativo da execução."""
        return self._inner.status(execution_ref)

    def cancel(self, request: Any) -> None:
        """Delegação nativa do cancelamento."""
        self._inner.cancel(request)


def _binding_mapping(text: str) -> dict[str, Any]:
    try:
        mapping = json.loads(text)
    except json.JSONDecodeError as error:
        raise _refuse("binding_invalid") from error
    if type(mapping) is not dict or canonical_text(cast("dict[str, Any]", mapping)) != text:
        raise _refuse("binding_invalid")
    return cast("dict[str, Any]", mapping)


def load_unit_binding(client: Any, table_name: str, holder: PermitHolder) -> ModeCanaryBinding:
    """Returns: Binding do contexto a partir do item insert-only do dispatch da unit.

    Raises: SyntheticContextRequired: Item ausente, não canônico ou de outro dispatch.
    """
    key = {"pk": {"S": run_partition(holder.tenant_id, holder.run_id)},
           "sk": {"S": f"PROD_CANARY#{holder.dispatch_id}"}}
    item = client.get_item(TableName=table_name, Key=key, ConsistentRead=True).get("Item")
    if item is None:
        raise _refuse("binding_missing")
    fields = _binding_mapping(cast("str", item["payload"]["S"]))
    expected = {"schema": "cnesdata.mode-canary-binding/v1", "tenant_id": holder.tenant_id,
                "run_id": holder.run_id, "dispatch_id": holder.dispatch_id}
    if frozenset(fields) != _BINDING_FIELDS or any(fields[k] != v for k, v in expected.items()):
        raise _refuse("binding_invalid")
    try:
        return ModeCanaryBinding(
            holder.tenant_id, (holder.run_id,), fields["owner_sha256"], fields["record_sha256"],
            fields["phase"], fields["context_kind"],
        )
    except (TypeError, ValueError) as error:
        raise _refuse("binding_invalid") from error


def enforce_mode_canary(
    context: ModeCanaryContext | None, mode: str, clock: Callable[[], datetime],
    wait: Callable[[], object],
) -> None:
    """Encurta o watchdog ao teto do contexto e aplica hang_no_dispatch antes do trabalho.

    Raises: SyntheticContextRequired: Contexto selado, divergente ou vencido.
    """
    if context is None:
        return
    bounds = context.runtime_bounds()
    remaining = (bounds.deadline - clock()).total_seconds()
    if remaining <= 0:
        raise _refuse("context_expired")
    shorten_active_deadline(remaining)
    if mode == _RECOVER_ONCE and bounds.fault == ModeCanaryFault.HANG_NO_DISPATCH:
        wait()


class _FailingRawStore:
    def __init__(self, inner: ObjectStorePort) -> None:
        self._inner = inner

    def put(self, key: str, body: Any, expected_sha256: str) -> Any:
        return self._inner.put(key, body, expected_sha256)

    def open(self, key: str) -> Any:
        if key.startswith(_RAW_PREFIX):
            raise SyntheticFixtureReadFailure("reason=synthetic_read_failure")
        return self._inner.open(key)


def fault_object_store(context: ModeCanaryContext | None, store: ObjectStorePort) -> Any:
    """Returns: Store que falha só na leitura raw quando o record pede synthetic_read_failure."""
    if context is None:
        return store
    if context.runtime_bounds().fault != ModeCanaryFault.SYNTHETIC_READ_FAILURE:
        return store
    return _FailingRawStore(store)


__all__ = [
    "CanaryControlPlane",
    "ModeCanaryExecutor",
    "ModeCanaryFault",
    "SyntheticFixtureReadFailure",
    "enforce_mode_canary",
    "fault_object_store",
    "load_unit_binding",
]
