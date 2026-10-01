"""Autoriza a criação de Run pelo gate de entitlement e dispara o launch."""
from __future__ import annotations

from typing import TYPE_CHECKING

from cnes_domain.billing.commands import AuthorizedRunCommand
from cnes_domain.control_plane.enums import RunState

if TYPE_CHECKING:
    from central_api.services.run_planning import RunPlanningService
    from cnes_domain.billing.commands import CreateRunRequest
    from cnes_domain.billing.gate import EntitlementGate
    from cnes_domain.control_plane.entities import Run
    from cnes_domain.ports.control_plane import ControlPlanePort

_LAUNCHABLE_STATES = frozenset({RunState.PLANNED, RunState.WAITING_INPUTS})


class RunAuthorizationService:
    def __init__(
        self, entitlement_gate: EntitlementGate, control_plane: ControlPlanePort,
        run_planning: RunPlanningService,
    ) -> None:
        self._gate = entitlement_gate
        self._control_plane = control_plane
        self._run_planning = run_planning

    def authorize_and_create(self, command: CreateRunRequest) -> Run:
        """Args: command: Pedido de criação do Run.
        Returns: Run após o launch, ou o Run existente em replay.
        Raises: EntitlementDenied: gate negou; RuntimeError: reserva sem Run.
        """
        authorization = self._gate.authorize_create_run(command)
        if authorization.budget_reservation_id is None:
            run = self._control_plane.create_unmetered_run(
                AuthorizedRunCommand(request=command, authorization=authorization)
            )
        else:
            run = self._existing_run(command)
        if run.state in _LAUNCHABLE_STATES:
            return self._run_planning.launch(run.tenant_id, run.run_id).run
        return run

    def _existing_run(self, command: CreateRunRequest) -> Run:
        run = self._control_plane.get_run(command.tenant_id, command.run_id)
        if run is None:
            raise RuntimeError(f"reason=run_missing_after_reservation run_id={command.run_id}")
        return run


__all__ = ["RunAuthorizationService"]
