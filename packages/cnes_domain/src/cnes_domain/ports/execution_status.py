"""Status normalizado da execução do processador, sem dependência de pydantic."""

from enum import StrEnum


class ExecutionStatus(StrEnum):
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELED = "CANCELED"
