"""Adapters do `ProcessorExecutorPort`."""

from cnes_infra.executor.local_pool import LocalWorkerPool
from cnes_infra.executor.step_functions import StepFunctionsExecutor

__all__ = ("LocalWorkerPool", "StepFunctionsExecutor")
