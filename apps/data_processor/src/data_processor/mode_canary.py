"""Runtime do canary de modo no processor: facade, executor, locator e falhas."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Final, cast

from cnes_domain.orchestration.mode_canary import (
    SyntheticContextRequired,
    is_mode_canary_namespace,
)
from cnes_infra.aws.mode_canary import start_effect_id

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import datetime

    from cnes_domain.orchestration.mode_canary import ModeCanaryBinding
    from cnes_domain.ports.processing import StartRunExecution
    from cnes_infra.aws.mode_canary import ModeCanaryContext


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


__all__ = ["CanaryControlPlane", "ModeCanaryExecutor"]
