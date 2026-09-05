from dataclasses import FrozenInstanceError
from inspect import signature

import pytest

import cnes_domain
from cnes_domain import control_plane, ports
from cnes_domain.control_plane.entities import Job, ManifestRef, Run
from cnes_domain.control_plane.queries import (
    LatestSucceededJobQuery,
    RawIdentity,
    RawManifestChainQuery,
    WaitingRunsForDependencyQuery,
)
from cnes_domain.ports.control_plane import ControlPlanePort, TypedRawQueryPort


class _TypedQueries:
    def query_latest_succeeded_job(self, query: LatestSucceededJobQuery) -> Job | None:
        return None

    def query_raw_manifest_chain(
        self, query: RawManifestChainQuery
    ) -> tuple[ManifestRef, ...]:
        return ()

    def query_waiting_runs_for_dependency(
        self, query: WaitingRunsForDependencyQuery
    ) -> tuple[Run, ...]:
        return ()


class _IncompleteQueries:
    def query_latest_succeeded_job(self, query: LatestSucceededJobQuery) -> Job | None:
        return None


def test_identidade_raw_aceita_componentes_canonicos() -> None:
    identity = RawIdentity("354130", "CNES_LOCAL", "CNES_VINCULO", "2026-07")

    assert identity.tenant_id == "354130"
    assert identity.source_type == "CNES_LOCAL"
    assert identity.file_subtype == "CNES_VINCULO"
    assert identity.competencia == "2026-07"


@pytest.mark.parametrize(
    "values",
    [
        ("", "CNES_LOCAL", "CNES_VINCULO", "2026-07"),
        ("354130", "CNES#LOCAL", "CNES_VINCULO", "2026-07"),
        ("354130", "CNES_LOCAL", "..", "2026-07"),
        ("354130", "CNES_LOCAL", "CNES_VINCULO", "2026-7"),
    ],
)
def test_identidade_raw_rejeita_componentes_invalidos(values: tuple[str, ...]) -> None:
    with pytest.raises(ValueError):
        RawIdentity(*values)


def test_identidade_raw_e_imutavel_e_sem_dicionario() -> None:
    identity = RawIdentity("354130", "CNES_LOCAL", "CNES_VINCULO", "2026-07")

    with pytest.raises(FrozenInstanceError):
        identity.tenant_id = "outro"
    with pytest.raises(TypeError):
        vars(identity)


def test_consultas_raw_preservam_identidade_e_limites_padrao() -> None:
    identity = RawIdentity("354130", "CNES_LOCAL", "CNES_VINCULO", "2026-07")

    latest = LatestSucceededJobQuery(identity, "agent-01")
    manifests = RawManifestChainQuery(identity)
    waiting = WaitingRunsForDependencyQuery(identity)

    assert latest.identity is identity
    assert latest.agent_id == "agent-01"
    assert manifests.limit == 31
    assert waiting.limit == 100


def test_consulta_de_job_rejeita_agente_invalido() -> None:
    identity = RawIdentity("354130", "CNES_LOCAL", "CNES_VINCULO", "2026-07")

    with pytest.raises(ValueError, match="invalid_key_component"):
        LatestSucceededJobQuery(identity, "agent#01")


def test_consultas_de_lista_aceitam_limite_nao_positivo() -> None:
    identity = RawIdentity("354130", "CNES_LOCAL", "CNES_VINCULO", "2026-07")

    assert RawManifestChainQuery(identity, limit=0).limit == 0
    assert WaitingRunsForDependencyQuery(identity, limit=-1).limit == -1


@pytest.mark.parametrize(
    "query",
    [
        LatestSucceededJobQuery(
            RawIdentity("354130", "CNES_LOCAL", "CNES_VINCULO", "2026-07"),
            "agent-01",
        ),
        RawManifestChainQuery(
            RawIdentity("354130", "CNES_LOCAL", "CNES_VINCULO", "2026-07")
        ),
        WaitingRunsForDependencyQuery(
            RawIdentity("354130", "CNES_LOCAL", "CNES_VINCULO", "2026-07")
        ),
    ],
)
def test_consultas_raw_sao_imutaveis_e_sem_dicionario(query: object) -> None:
    with pytest.raises(FrozenInstanceError):
        query.identity = RawIdentity("outro", "CNES_LOCAL", "CNES_VINCULO", "2026-07")
    with pytest.raises(TypeError):
        vars(query)


def test_porta_tipadas_reconhece_implementacao_em_runtime() -> None:
    assert isinstance(_TypedQueries(), TypedRawQueryPort)
    assert not isinstance(_IncompleteQueries(), TypedRawQueryPort)


def test_porta_tipadas_expoe_assinaturas_de_uma_consulta() -> None:
    assert str(signature(TypedRawQueryPort.query_latest_succeeded_job)) == (
        "(self, query: 'LatestSucceededJobQuery') -> 'Job | None'"
    )
    assert str(signature(TypedRawQueryPort.query_raw_manifest_chain)) == (
        "(self, query: 'RawManifestChainQuery') -> 'tuple[ManifestRef, ...]'"
    )
    assert str(signature(TypedRawQueryPort.query_waiting_runs_for_dependency)) == (
        "(self, query: 'WaitingRunsForDependencyQuery') -> 'tuple[Run, ...]'"
    )


def test_porta_da_fase_um_preserva_assinaturas_raw() -> None:
    assert str(signature(ControlPlanePort.latest_succeeded_job)) == (
        "(self, tenant_id: 'str', agent_id: 'str', source_type: 'str', "
        "file_subtype: 'str', competencia: 'str') -> 'Job | None'"
    )
    assert str(signature(ControlPlanePort.list_raw_manifest_chain)) == (
        "(self, tenant_id: 'str', source_type: 'str', file_subtype: 'str', "
        "competencia: 'str', limit: 'int' = 31) -> 'tuple[ManifestRef, ...]'"
    )
    assert str(signature(ControlPlanePort.list_waiting_runs_for_dependency)) == (
        "(self, tenant_id: 'str', source_type: 'str', file_subtype: 'str', "
        "competencia: 'str', limit: 'int' = 100) -> 'tuple[Run, ...]'"
    )


@pytest.mark.parametrize(
    "name",
    [
        "LatestSucceededJobQuery",
        "RawIdentity",
        "RawManifestChainQuery",
        "WaitingRunsForDependencyQuery",
    ],
)
def test_consultas_raw_sao_exportadas_pelo_dominio(name: str) -> None:
    expected = getattr(control_plane, name)

    assert getattr(cnes_domain, name) is expected


@pytest.mark.parametrize(
    "name",
    [
        "LatestSucceededJobQuery",
        "RawIdentity",
        "RawManifestChainQuery",
        "WaitingRunsForDependencyQuery",
    ],
)
def test_consultas_raw_sao_exportadas_pelas_portas(name: str) -> None:
    assert getattr(ports, name) is getattr(control_plane, name)


def test_porta_tipadas_e_exportada_pelo_dominio() -> None:
    assert cnes_domain.TypedRawQueryPort is TypedRawQueryPort
    assert ports.TypedRawQueryPort is TypedRawQueryPort
