"""Adapters do `ProcessorExecutorPort`."""

from cnes_control_plane.execution.local_pool import LocalWorkerPool
from cnes_control_plane.execution.step_functions import StepFunctionsExecutor

__all__ = ("LocalWorkerPool", "StepFunctionsExecutor")
