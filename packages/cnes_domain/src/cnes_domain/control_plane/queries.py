"""Valores imutáveis para consultas do plano de controle."""

from dataclasses import dataclass

from cnes_domain.control_plane.entities import _require_competencia, _require_key_component


@dataclass(frozen=True, slots=True)
class RawIdentity:
    tenant_id: str
    source_type: str
    file_subtype: str
    competencia: str

    def __post_init__(self) -> None:
        _require_key_component(self.tenant_id)
        _require_key_component(self.source_type)
        _require_key_component(self.file_subtype)
        _require_competencia(self.competencia)


@dataclass(frozen=True, slots=True)
class LatestSucceededJobQuery:
    identity: RawIdentity
    agent_id: str

    def __post_init__(self) -> None:
        _require_key_component(self.agent_id)


@dataclass(frozen=True, slots=True)
class RawManifestChainQuery:
    identity: RawIdentity
    limit: int = 31


@dataclass(frozen=True, slots=True)
class WaitingRunsForDependencyQuery:
    identity: RawIdentity
    limit: int = 100
