"""Valores imutáveis para consultas do plano de controle."""

from dataclasses import dataclass

from cnes_domain.control_plane.entities import require_competencia, require_key_component


@dataclass(frozen=True, slots=True)
class RawIdentity:
    tenant_id: str
    source_type: str
    file_subtype: str
    competencia: str

    def __post_init__(self) -> None:
        require_key_component(self.tenant_id)
        require_key_component(self.source_type)
        require_key_component(self.file_subtype)
        require_competencia(self.competencia)


@dataclass(frozen=True, slots=True)
class LatestSucceededJobQuery:
    identity: RawIdentity
    agent_id: str

    def __post_init__(self) -> None:
        require_key_component(self.agent_id)


@dataclass(frozen=True, slots=True)
class RawManifestChainQuery:
    identity: RawIdentity
    limit: int = 31


@dataclass(frozen=True, slots=True)
class RawManifestByIdQuery:
    tenant_id: str
    manifest_id: str

    def __post_init__(self) -> None:
        require_key_component(self.tenant_id)
        require_key_component(self.manifest_id)


@dataclass(frozen=True, slots=True)
class AgentRawManifestChainQuery:
    identity: RawIdentity
    agent_id: str
    limit: int = 31

    def __post_init__(self) -> None:
        require_key_component(self.agent_id)


@dataclass(frozen=True, slots=True)
class RawResyncStateQuery:
    identity: RawIdentity
    agent_id: str

    def __post_init__(self) -> None:
        require_key_component(self.agent_id)


@dataclass(frozen=True, slots=True)
class WaitingRunsForDependencyQuery:
    identity: RawIdentity
    limit: int = 100
