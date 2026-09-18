from datetime import UTC, datetime, timedelta
from hashlib import sha256

import pytest

from central_api.services.national_ingestion import (
    NationalIngestionService,
    NationalRefreshRequest,
)
from central_api.services.raw_ingestion import RawAcceptance
from cnes_contracts import RawManifest, SnapshotMode, SourceType, manifest_sha256
from cnes_domain.control_plane.entities import Agent, Job, RawManifestRecord
from cnes_domain.control_plane.enums import AgentState, JobState
from cnes_domain.control_plane.errors import Conflict
from cnes_domain.control_plane.errors import ControlPlaneErrorCode as ErrorCode
from cnes_infra.ingestion import DatasusCnesRequest

NOW = datetime(2026, 1, 20, 9, tzinfo=UTC)
TENANT = "354130"
COMPETENCIA = "2026-01"
DATA = b"parquet"


def manifest(snapshot_id: str = "snap-1") -> RawManifest:
    return RawManifest(
        manifest_version=1,
        manifest_id=snapshot_id,
        tenant_id=TENANT,
        source_type=SourceType.CNES_NACIONAL,
        file_subtype="CNES_VINCULO",
        competencia=COMPETENCIA,
        agent_id="system-datasus",
        agent_version="1.0.0",
        schema_version="cnes-profissional-v1",
        snapshot_mode=SnapshotMode.FULL,
        snapshot_id=snapshot_id,
        base_snapshot_id=None,
        sequence=1,
        previous_manifest_sha256=None,
        object_sha256=sha256(DATA).hexdigest(),
        row_count=1,
        size_bytes=len(DATA),
        object_key=f"raw/{TENANT}/CNES_NACIONAL/{COMPETENCIA}/{snapshot_id}/data.parquet",
        created_at=NOW,
    )


def request(**updates: str) -> NationalRefreshRequest:
    values = {
        "tenant_id": TENANT,
        "competencia": COMPETENCIA,
        "snapshot_id": "snap-1",
        "idempotency_key": "key-1",
    }
    return NationalRefreshRequest(**(values | updates))


class ControlPlane:
    def __init__(self, agent: Agent | None = None, job: Job | None = None) -> None:
        self.agents = {} if agent is None else {(agent.tenant_id, agent.agent_id): agent}
        self.jobs = {} if job is None else {(job.tenant_id, job.job_id): job}
        self.mutations: list[str] = []
        self.events = []
        self.records: dict[str, RawManifestRecord] = {}

    def get_agent(self, tenant_id: str, agent_id: str) -> Agent | None:
        return self.agents.get((tenant_id, agent_id))

    def put_agent(self, agent: Agent) -> None:
        self.mutations.append("put_agent")
        self.agents[(agent.tenant_id, agent.agent_id)] = agent

    def get_job(self, tenant_id: str, job_id: str) -> Job | None:
        return self.jobs.get((tenant_id, job_id))

    def query_raw_manifest_by_id(self, query) -> RawManifestRecord | None:
        return self.records.get(query.manifest_id)

    def create_job(self, job: Job, event) -> Job:
        self.mutations.append("create_job")
        self.events.append(event)
        self.jobs[(job.tenant_id, job.job_id)] = job
        return job

    def claim_job(self, command) -> Job | None:
        self.mutations.append("claim_job")
        current = self.jobs.get((command.tenant_id, command.job_id))
        if current is None or current.state is not JobState.PENDING:
            return None
        claimed = current.model_copy(update={
            "state": JobState.LEASED,
            "fencing_token": current.fencing_token + 1,
            "lease_owner": command.owner,
            "lease_until": command.now + timedelta(seconds=command.lease_seconds),
        })
        self.jobs[(command.tenant_id, command.job_id)] = claimed
        return claimed

    def complete_job(self, command, event) -> Job:  # pragma: no cover - guarda de contrato
        self.mutations.append("complete_job")
        raise AssertionError("national_service_completed_job")

    def fail_job(self, command, event) -> Job:  # pragma: no cover - guarda de contrato
        self.mutations.append("fail_job")
        raise AssertionError("national_service_failed_job")


class RawAdapter:
    def __init__(self, raw: RawManifest | None = None) -> None:
        self.raw = raw or manifest()
        self.requests: list[DatasusCnesRequest] = []

    def extract(self, datasus_request: DatasusCnesRequest) -> RawManifest:
        self.requests.append(datasus_request)
        return self.raw


class RawIngestion:
    def __init__(self) -> None:
        self.commands = []

    def register(self, command) -> RawAcceptance:
        self.commands.append(command)
        return RawAcceptance(
            accepted=True,
            manifest_id=command.manifest.manifest_id,
            manifest_sha256=manifest_sha256(command.manifest),
            full_resync_required=False,
            reason=None,
        )


def service(
    control: ControlPlane,
    adapter: RawAdapter | None = None,
    ingestion: RawIngestion | None = None,
) -> NationalIngestionService:
    return NationalIngestionService(
        control, adapter or RawAdapter(), ingestion or RawIngestion(), lambda: NOW
    )


def _succeed(control: ControlPlane) -> None:
    raw = manifest()
    key = next(iter(control.jobs))
    manifest_key = f"raw/{TENANT}/CNES_NACIONAL/{COMPETENCIA}/snap-1/manifest.json"
    control.jobs[key] = control.jobs[key].model_copy(update={
        "state": JobState.SUCCEEDED,
        "lease_owner": None,
        "lease_until": None,
        "result_manifest_id": raw.manifest_id,
        "result_manifest_key": manifest_key,
    })
    control.records[raw.manifest_id] = RawManifestRecord(
        tenant_id=raw.tenant_id,
        manifest_id=raw.manifest_id,
        manifest_key=manifest_key,
        agent_id=raw.agent_id,
        source_type=raw.source_type.value,
        file_subtype=raw.file_subtype,
        competencia=raw.competencia,
        snapshot_mode=raw.snapshot_mode.value,
        snapshot_id=raw.snapshot_id,
        base_snapshot_id=raw.base_snapshot_id,
        sequence=raw.sequence,
        previous_manifest_sha256=raw.previous_manifest_sha256,
        manifest_sha256=manifest_sha256(raw),
        created_at=raw.created_at,
    )


def test_cria_agente_system_datasus_ativo_quando_ausente() -> None:
    control = ControlPlane()

    service(control).refresh(request())

    agent = control.agents[(TENANT, "system-datasus")]
    assert agent.state is AgentState.ACTIVE
    assert agent.agent_id == "system-datasus"
    assert control.mutations.count("put_agent") == 1


def test_reaproveita_agente_existente_sem_reescrever() -> None:
    control = ControlPlane()
    national = service(control)

    national.refresh(request())
    national.refresh(request(idempotency_key="key-2"))

    assert control.mutations.count("put_agent") == 1


def test_rejeita_agente_revogado() -> None:
    revoked = Agent(
        tenant_id=TENANT,
        agent_id="system-datasus",
        state=AgentState.REVOKED,
        version="1.0.0",
        certificate_fingerprint="b" * 64,
        last_seen_at=None,
        created_at=NOW,
    )
    control = ControlPlane(agent=revoked)

    with pytest.raises(Conflict):
        service(control).refresh(request())


def test_mesma_idempotency_key_reaproveita_o_mesmo_job() -> None:
    control = ControlPlane()
    national = service(control)
    national.refresh(request())
    _succeed(control)

    national.refresh(request())

    assert control.mutations.count("create_job") == 1
    assert len(control.jobs) == 1


def test_idempotency_key_distinta_cria_outro_job() -> None:
    control = ControlPlane()
    national = service(control)

    national.refresh(request())
    national.refresh(request(idempotency_key="key-2"))

    assert control.mutations.count("create_job") == 2
    assert len(control.jobs) == 2


def test_registra_com_o_fence_do_claim() -> None:
    control = ControlPlane()
    ingestion = RawIngestion()

    service(control, ingestion=ingestion).refresh(request())

    claimed = next(iter(control.jobs.values()))
    command = ingestion.commands[-1]
    assert command.fencing_token == claimed.fencing_token
    assert command.owner == "system-datasus"
    assert command.agent_id == "system-datasus"
    assert command.job_id == claimed.job_id


def test_envia_bytes_canonicos_do_manifesto() -> None:
    control = ControlPlane()
    ingestion = RawIngestion()
    adapter = RawAdapter()

    service(control, adapter, ingestion).refresh(request())

    command = ingestion.commands[-1]
    assert command.manifest_bytes == adapter.raw.model_dump_json(
        exclude_none=False, by_alias=False
    ).encode()


def test_pede_ao_adapter_a_identidade_pf_deterministica() -> None:
    control = ControlPlane()
    adapter = RawAdapter()

    service(control, adapter).refresh(request())

    assert adapter.requests == [
        DatasusCnesRequest(
            tenant_id=TENANT,
            competencia=COMPETENCIA,
            file_subtype="CNES_VINCULO",
            snapshot_id="snap-1",
            agent_id="system-datasus",
            agent_version="1.0.0",
        )
    ]


def test_nao_completa_nem_falha_o_job_diretamente() -> None:
    control = ControlPlane()

    service(control).refresh(request())

    assert "complete_job" not in control.mutations
    assert "fail_job" not in control.mutations


def test_job_concluido_converge_por_replay_sem_novo_claim() -> None:
    control = ControlPlane()
    national = service(control)
    national.refresh(request())
    _succeed(control)
    control.mutations.clear()

    result = national.refresh(request())

    assert result.accepted
    assert result.manifest_id == "snap-1"
    assert "claim_job" not in control.mutations


def test_claim_indisponivel_gera_conflito() -> None:
    control = ControlPlane()
    national = service(control)
    national.refresh(request())
    key = next(iter(control.jobs))
    control.jobs[key] = control.jobs[key].model_copy(update={
        "state": JobState.LEASED,
        "lease_owner": "outro-agente",
        "lease_until": NOW + timedelta(minutes=5),
    })

    with pytest.raises(Conflict):
        national.refresh(request())


def test_rejeita_campos_em_branco() -> None:
    with pytest.raises(ValueError, match="blank_value"):
        request(snapshot_id="   ")


def test_criacao_concorrente_reaproveita_o_job_do_vencedor() -> None:
    control = ControlPlane()
    winner: list[Job] = []

    def create_job(job: Job, event) -> Job:
        control.mutations.append("create_job")
        control.jobs[(job.tenant_id, job.job_id)] = job
        winner.append(job)
        raise Conflict(ErrorCode.JOB_CREATION_CONFLICT)

    control.create_job = create_job

    service(control).refresh(request())

    assert control.mutations == ["put_agent", "create_job", "claim_job"]
    assert winner[0].job_id in {job_id for _, job_id in control.jobs}


def test_conflito_de_criacao_sem_job_persistido_propaga() -> None:
    control = ControlPlane()

    def create_job(job: Job, event) -> Job:
        raise Conflict(ErrorCode.JOB_CREATION_CONFLICT)

    control.create_job = create_job

    with pytest.raises(Conflict):
        service(control).refresh(request())


def test_job_terminal_sem_manifesto_conflita() -> None:
    control = ControlPlane()
    national = service(control)
    national.refresh(request())
    key = next(iter(control.jobs))
    control.jobs[key] = control.jobs[key].model_copy(update={
        "state": JobState.FAILED_FINAL,
        "lease_owner": None,
        "lease_until": None,
        "error_code": "SOURCE_UNAVAILABLE",
    })

    with pytest.raises(Conflict):
        national.refresh(request())


def test_job_concluido_sem_registro_conflita() -> None:
    control = ControlPlane()
    national = service(control)
    national.refresh(request())
    _succeed(control)
    control.records.clear()

    with pytest.raises(Conflict):
        national.refresh(request())
