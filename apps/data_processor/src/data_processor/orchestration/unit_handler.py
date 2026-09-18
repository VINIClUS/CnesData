"""Maps a queue message into an exact claim command for the unit worker."""
from __future__ import annotations

from typing import TYPE_CHECKING

from cnes_domain.control_plane.commands import ClaimRunUnit

if TYPE_CHECKING:
    from cnes_domain.control_plane.entities import RunUnit
    from cnes_domain.ports.processing import RunUnitMessage
    from data_processor.orchestration.unit_worker import UnitWorker


class RunUnitCommandHandler:
    def __init__(self, worker: UnitWorker) -> None:
        self._worker = worker

    def handle(self, message: RunUnitMessage) -> RunUnit:
        command = ClaimRunUnit(
            tenant_id=message.tenant_id, run_id=message.run_id, unit_id=message.unit_id,
            dispatch_id=message.dispatch_id, owner=message.owner, now=message.now,
            lease_seconds=message.lease_seconds,
        )
        return self._worker.execute(command)
