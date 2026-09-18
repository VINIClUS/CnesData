"""Canonical control-plane identifiers."""

from dataclasses import dataclass
from hashlib import sha256

from cnes_domain.control_plane.entities import (
    _require_competencia,
    _require_key_component,
    _require_non_blank,
)
from cnes_domain.control_plane.enums import RunStage

_SEPARATOR = "\x1f"


def _identity_component(value: str) -> str:
    _require_non_blank(value)
    if _SEPARATOR in value:
        raise ValueError("invalid_identity_component")
    return value


@dataclass(frozen=True, slots=True)
class JobIdentity:
    tenant_id: str
    agent_id: str
    source_type: str
    file_subtype: str
    competencia: str
    idempotency_key: str

    def __post_init__(self) -> None:
        for value in (
            self.tenant_id,
            self.agent_id,
            self.source_type,
            self.file_subtype,
        ):
            _require_key_component(value)
            _identity_component(value)
        _identity_component(self.idempotency_key)
        _require_competencia(self.competencia)


@dataclass(frozen=True, slots=True)
class RunUnitIdentity:
    run_id: str
    stage: RunStage
    source_type: str = ""
    file_subtype: str = ""
    partition: str = "all"

    def __post_init__(self) -> None:
        _identity_component(self.run_id)
        _identity_component(self.partition)
        if not isinstance(self.stage, RunStage):
            raise TypeError("invalid_run_stage")
        if self.stage is RunStage.NORMALIZE:
            if not self.source_type or not self.file_subtype:
                raise ValueError("normalize_source_required")
            _require_key_component(self.source_type)
            _require_key_component(self.file_subtype)
            _identity_component(self.source_type)
            _identity_component(self.file_subtype)
        elif self.source_type or self.file_subtype:
            raise ValueError("downstream_source_forbidden")


def job_id(identity: JobIdentity) -> str:
    values = (
        identity.tenant_id,
        identity.agent_id,
        identity.source_type,
        identity.file_subtype,
        identity.competencia,
        identity.idempotency_key,
    )
    return sha256(_SEPARATOR.join(values).encode()).hexdigest()[:32]


def unit_id(identity: RunUnitIdentity) -> str:
    values = (
        identity.run_id,
        identity.stage.value,
        identity.source_type,
        identity.file_subtype,
        identity.partition,
    )
    return sha256(_SEPARATOR.join(values).encode()).hexdigest()[:32]


def run_dependency_key(
    tenant_id: str,
    source_type: str,
    file_subtype: str,
    competencia: str,
) -> str:
    components = (tenant_id, source_type, file_subtype)
    for component in components:
        _require_key_component(component)
    _require_competencia(competencia)
    return "RUN_DEP#" + "#".join((*components, competencia))
