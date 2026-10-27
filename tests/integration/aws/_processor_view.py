"""View somente de leitura dos grafos reais dos dois modos do processor."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from cnes_domain.ports.control_plane import ControlPlanePort
    from cnes_domain.ports.processing import ProcessorExecutorPort
    from data_processor.aws_composition import AwsRecoveryRuntime, AwsUnitRuntime
    from data_processor.orchestration.coordinator import PipelineCoordinator
    from data_processor.orchestration.publisher import DatasetPublisher
    from data_processor.orchestration.unit_handler import RunUnitCommandHandler
    from data_processor.recovery import ProcessorRecovery


@dataclass(frozen=True, slots=True)
class ProcessorView:
    control_plane: ControlPlanePort
    coordinator: PipelineCoordinator
    publisher: DatasetPublisher
    executor: ProcessorExecutorPort
    unit_handler: RunUnitCommandHandler
    recovery: ProcessorRecovery


def processor_view(unit: AwsUnitRuntime, recovery: AwsRecoveryRuntime) -> ProcessorView:
    coordinator = recovery.recovery._coordinator
    dependencies = coordinator._dependencies
    return ProcessorView(
        control_plane=recovery.recovery._control_plane, coordinator=coordinator,
        publisher=dependencies.publisher, executor=dependencies.executor,
        unit_handler=unit.unit_handler, recovery=recovery.recovery,
    )
