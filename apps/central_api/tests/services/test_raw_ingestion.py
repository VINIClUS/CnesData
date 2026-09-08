from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from io import BytesIO

import pytest

from central_api.services.delta_policy import DeltaPolicy, ResyncReason
from central_api.services.raw_ingestion import RawIngestionService, RegisterRawManifest
from cnes_contracts import RawManifest, SnapshotMode, SourceType, manifest_sha256
from cnes_domain.control_plane.entities import Job, ManifestRef, RawManifestRecord, RawResyncState
from cnes_domain.control_plane.enums import JobState
from cnes_domain.control_plane.errors import Conflict
from cnes_domain.ports.object_store import ObjectStat

NOW = datetime(2026, 7, 15, 12, tzinfo=UTC)
DATA = b"parquet"
DATA_HASH = sha256(DATA).hexdigest()


def manifest(mode: SnapshotMode = SnapshotMode.FULL) -> RawManifest:
    delta = mode is SnapshotMode.DELTA
    snapshot_id = "delta-2" if delta else "base"
    return RawManifest(
        manifest_version=1,
        manifest_id=f"manifest-{snapshot_id}",
        tenant_id="354130",
        source_type=SourceType.CNES_LOCAL,
        file_subtype="CNES_VINCULO",
        competencia="2026-07",
        agent_id="agent-1",
        agent_version="1.0",
        schema_version="v1",
        snapshot_mode=mode,
        snapshot_id=snapshot_id,
        base_snapshot_id="base" if delta else None,
        sequence=2 if delta else 1,
        previous_manifest_sha256="a" * 64 if delta else None,
        object_sha256=DATA_HASH,
        row_count=1,
        size_bytes=len(DATA),
        object_key=f"raw/354130/CNES_LOCAL/2026-07/{snapshot_id}/data.parquet",
        created_at=NOW,
    )


def job(mode: SnapshotMode = SnapshotMode.FULL, state: JobState = JobState.LEASED) -> Job:
    return Job(
        tenant_id="354130",
        job_id="job-1",
        agent_id="agent-1",
        source_type="CNES_LOCAL",
        file_subtype="CNES_VINCULO",
        competencia="2026-07",
        requested_snapshot_mode=mode.value,
        state=state,
        attempt=1,
        fencing_token=7,
        lease_owner="worker" if state is JobState.LEASED else None,
        lease_until=NOW + timedelta(minutes=5) if state is JobState.LEASED else None,
        result_manifest_id=None,
        result_manifest_key=None,
        error_code=None,
        created_at=NOW,
    )


def record(raw: RawManifest) -> RawManifestRecord:
    return RawManifestRecord(
        tenant_id=raw.tenant_id,
        manifest_id=raw.manifest_id,
        manifest_key=raw.object_key.removesuffix("data.parquet") + "manifest.json",
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


class ObjectStore:
    def __init__(self, raw: RawManifest) -> None:
        self.objects = {raw.object_key: DATA}
        self.calls: list[str] = []

    def stat(self, key: str) -> ObjectStat | None:
        self.calls.append(f"stat:{key}")
        body = self.objects.get(key)
        return None if body is None else ObjectStat(key, len(body), sha256(body).hexdigest())

    def put(self, key: str, body, expected_sha256: str) -> ObjectStat:
        self.calls.append(f"put:{key}")
        value = body.read()
        current = self.objects.setdefault(key, value)
        if current != value:
            raise Conflict("object=immutable")
        return ObjectStat(key, len(value), expected_sha256)

    @contextmanager
    def open(self, key: str):
        self.calls.append(f"open:{key}")
        yield BytesIO(self.objects[key])


class ControlPlane:
    def __init__(self, current: Job) -> None:
        self.job = current
        self.records: dict[str, RawManifestRecord] = {}
        self.marker = None
        self.latest = None
        self.chain = ()
        self.mutations: list[str] = []
        self.query_calls: list[str] = []
        self.events = []
        self.completions = []

    def get_job(self, tenant_id: str, job_id: str) -> Job | None:
        return self.job if (tenant_id, job_id) == (self.job.tenant_id, self.job.job_id) else None

    def query_raw_manifest_by_id(self, query):
        self.query_calls.append("by-id")
        return self.records.get(query.manifest_id)

    def query_latest_succeeded_job(self, query):
        self.query_calls.append("latest")
        return self.latest

    def query_agent_raw_manifest_chain(self, query):
        self.query_calls.append("agent-chain")
        return self.chain

    def query_raw_resync_state(self, query):
        self.query_calls.append("resync")
        return self.marker

    def complete_job(self, command, event):
        self.mutations.append("complete")
        self.completions.append(command)
        self.events.append(event)
        self.records[command.manifest.manifest_id] = command.manifest
        self.job = self.job.model_copy(update={
            "state": JobState.SUCCEEDED,
            "lease_owner": None,
            "lease_until": None,
            "result_manifest_id": command.manifest.manifest_id,
            "result_manifest_key": command.manifest.manifest_key,
        })
        return self.job

    def fail_job(self, command, event):
        self.mutations.append("fail")
        self.events.append(event)
        self.job = self.job.model_copy(update={
            "state": JobState.FAILED_FINAL,
            "lease_owner": None,
            "lease_until": None,
            "error_code": command.error_code,
            "rejected_manifest_sha256": command.rejected_manifest_sha256,
        })
        return self.job


def command(raw: RawManifest, **updates: object) -> RegisterRawManifest:
    values = {
        "tenant_id": "354130",
        "agent_id": "agent-1",
        "job_id": "job-1",
        "owner": "worker",
        "fencing_token": 7,
        "manifest": raw,
        "manifest_bytes": raw.model_dump_json(exclude_none=False, by_alias=False).encode(),
        "now": NOW,
    }
    return RegisterRawManifest(**(values | updates))


def test_aceite_grava_sidecar_antes_do_commit_atomico_e_callback() -> None:
    raw = manifest()
    store = ObjectStore(raw)
    control = ControlPlane(job())
    order = []

    def accepted(item: RawManifestRecord) -> None:
        assert control.mutations == ["complete"]
        order.append(item.manifest_id)

    result = RawIngestionService(control, store, DeltaPolicy(), accepted).register(command(raw))

    assert result.accepted
    assert control.mutations == ["complete"]
    assert store.calls[-1].startswith("put:")
    assert order == [raw.manifest_id]


def test_identidade_divergente_falha_antes_de_acessar_objeto() -> None:
    raw = manifest()
    store = ObjectStore(raw)
    control = ControlPlane(job())

    with pytest.raises(Conflict, match="manifest_identity"):
        RawIngestionService(control, store, DeltaPolicy()).register(
            command(raw, agent_id="other")
        )

    assert store.calls == []
    assert control.mutations == []


@pytest.mark.parametrize(
    ("mode", "raw_updates", "updates", "error"),
    [
        (SnapshotMode.FULL, {}, {"fencing_token": 8}, "job_fence_rejected"),
        (SnapshotMode.FULL, {}, {"manifest_bytes": b"{}"}, "manifest=noncanonical"),
        (SnapshotMode.FULL,
         {"object_key": "raw/354130/CNES_LOCAL/2026-07/base/other.parquet"}, {},
         "object=divergent"),
        (SnapshotMode.FULL, {"manifest_id": "part/id"}, {}, "invalid_key_component"),
        (SnapshotMode.DELTA, {"manifest_id": "part/id"}, {}, "invalid_key_component"),
    ],
)
def test_conflito_vivo_nao_altera_control_plane(mode, raw_updates, updates, error) -> None:
    raw = manifest(mode).model_copy(update=raw_updates)
    store = ObjectStore(raw)
    control = ControlPlane(job(mode))

    with pytest.raises((Conflict, ValueError), match=error):
        RawIngestionService(control, store, DeltaPolicy()).register(command(raw, **updates))

    assert control.mutations == []
    assert all(not call.startswith("put:") for call in store.calls)


def test_objeto_divergente_vence_rejeicao_delta_potencial() -> None:
    raw = manifest(SnapshotMode.DELTA)
    store = ObjectStore(raw)
    store.objects[raw.object_key] = b"wrong"
    control = ControlPlane(job(SnapshotMode.DELTA))

    with pytest.raises(Conflict, match="object=divergent"):
        RawIngestionService(control, store, DeltaPolicy()).register(command(raw))

    assert control.query_calls == []
    assert control.mutations == []


def test_rejeicao_delta_finaliza_job_sem_sidecar() -> None:
    raw = manifest(SnapshotMode.DELTA)
    store = ObjectStore(raw)
    control = ControlPlane(job(SnapshotMode.DELTA))

    result = RawIngestionService(control, store, DeltaPolicy()).register(command(raw))

    assert result.reason is ResyncReason.BASE_UNKNOWN
    assert control.job.rejected_manifest_sha256 == manifest_sha256(raw)
    assert control.mutations == ["fail"]
    assert all("manifest.json" not in call for call in store.calls)
    event = control.events[0]
    digest = manifest_sha256(raw)
    identity = "\x1f".join((event.event_type, "354130", "job-1", digest))
    assert event.event_id == sha256(identity.encode()).hexdigest()
    assert event.aggregate_id == "job-1"
    assert event.created_at == NOW
    assert event.payload == {
        "job_id": "job-1", "agent_id": "agent-1", "manifest_id": raw.manifest_id,
        "manifest_sha256": digest, "source_type": "CNES_LOCAL",
        "file_subtype": "CNES_VINCULO", "competencia": "2026-07",
        "snapshot_mode": "DELTA", "reason": "BASE_UNKNOWN",
    }


def test_replay_terminal_rejeitado_sem_lease_nao_acessa_objeto() -> None:
    raw = manifest(SnapshotMode.DELTA)
    digest = manifest_sha256(raw)
    failed = job(SnapshotMode.DELTA, JobState.FAILED_FINAL).model_copy(update={
        "error_code": "RAW_RESYNC_BASE_UNKNOWN",
        "rejected_manifest_sha256": digest,
    })
    store = ObjectStore(raw)
    control = ControlPlane(failed)

    result = RawIngestionService(control, store, DeltaPolicy()).register(command(raw))

    assert result.reason is ResyncReason.BASE_UNKNOWN
    assert store.calls == []
    assert control.query_calls == []


def test_replay_terminal_aceito_carrega_por_id_sem_cadeia() -> None:
    raw = manifest()
    projection = record(raw)
    succeeded = job().model_copy(update={
        "state": JobState.SUCCEEDED,
        "lease_owner": None,
        "lease_until": None,
        "result_manifest_id": raw.manifest_id,
        "result_manifest_key": projection.manifest_key,
    })
    store = ObjectStore(raw)
    sidecar = raw.model_dump_json(exclude_none=False, by_alias=False).encode()
    store.objects[projection.manifest_key] = sidecar
    control = ControlPlane(succeeded)
    control.records[raw.manifest_id] = projection

    result = RawIngestionService(control, store, DeltaPolicy()).register(command(raw))

    assert result.accepted
    assert control.query_calls == ["by-id"]
    assert control.mutations == []


def test_replay_terminal_divergente_nao_muta_estado() -> None:
    raw = manifest(SnapshotMode.DELTA)
    failed = job(SnapshotMode.DELTA, JobState.FAILED_FINAL).model_copy(update={
        "error_code": "RAW_RESYNC_BASE_UNKNOWN",
        "rejected_manifest_sha256": "b" * 64,
    })
    control = ControlPlane(failed)

    with pytest.raises(Conflict, match="terminal_replay=conflict"):
        RawIngestionService(control, ObjectStore(raw), DeltaPolicy()).register(command(raw))

    assert control.mutations == []


def test_callback_falha_depois_do_aceite_duravel(caplog) -> None:
    raw = manifest()
    control = ControlPlane(job())

    def fail(_: RawManifestRecord) -> None:
        raise RuntimeError("cpf=12345678900")

    result = RawIngestionService(control, ObjectStore(raw), DeltaPolicy(), fail).register(
        command(raw)
    )

    assert result.accepted
    assert control.mutations == ["complete"]
    assert "12345678900" not in caplog.text


@pytest.mark.parametrize("latest_key_matches", [True, False])
def test_delta_valida_chave_do_ultimo_job(latest_key_matches: bool) -> None:
    base = manifest()
    base_record = record(base)
    current = manifest(SnapshotMode.DELTA).model_copy(
        update={"previous_manifest_sha256": manifest_sha256(base)}
    )
    store = ObjectStore(current)
    store.objects[base_record.manifest_key] = (
        base.model_dump_json(exclude_none=False, by_alias=False).encode()
    )
    control = ControlPlane(job(SnapshotMode.DELTA))
    control.records[base.manifest_id] = base_record
    control.chain = (ManifestRef(
        manifest_id=base.manifest_id,
        manifest_key=base_record.manifest_key,
    ),)
    control.latest = job(state=JobState.LEASED).model_copy(update={
        "state": JobState.SUCCEEDED,
        "lease_owner": None,
        "lease_until": None,
        "result_manifest_id": base.manifest_id,
        "result_manifest_key": (
            base_record.manifest_key if latest_key_matches else "raw/x/y/manifest.json"
        ),
    })

    if not latest_key_matches:
        with pytest.raises(Conflict, match="raw_history=divergent"):
            RawIngestionService(control, store, DeltaPolicy()).register(command(current))
        return
    result = RawIngestionService(control, store, DeltaPolicy()).register(command(current))

    assert result.accepted
    assert control.query_calls == ["resync", "latest", "agent-chain", "by-id"]
    assert control.completions[0].expected_head_manifest_id == base.manifest_id


def test_marcador_existente_tem_precedencia_sem_consultar_cadeia() -> None:
    raw = manifest(SnapshotMode.DELTA)
    control = ControlPlane(job(SnapshotMode.DELTA))
    control.marker = RawResyncState(
        tenant_id="354130",
        agent_id="agent-1",
        source_type="CNES_LOCAL",
        file_subtype="CNES_VINCULO",
        competencia="2026-07",
        required_since=NOW,
    )

    result = RawIngestionService(control, ObjectStore(raw), DeltaPolicy()).register(command(raw))

    assert result.reason is ResyncReason.AGENT_RESYNC_REQUIRED
    assert control.query_calls == ["resync"]


@pytest.mark.parametrize(
    ("current", "updates", "error"),
    [
        (None, {}, "job_missing"),
        (job(state=JobState.PENDING), {}, "job_not_leased"),
        (job(), {"owner": "other"}, "job_owner_lost"),
        (
            job().model_copy(update={"lease_until": NOW}),
            {},
            "job_lease_expired",
        ),
    ],
)
def test_job_invalido_nao_acessa_objeto(current, updates, error) -> None:
    raw = manifest()
    control = ControlPlane(current or job())
    if current is None:
        control.job = control.job.model_copy(update={"job_id": "other"})
    store = ObjectStore(raw)

    with pytest.raises((Conflict, LookupError), match=error):
        RawIngestionService(control, store, DeltaPolicy()).register(command(raw, **updates))

    assert store.calls == []


@pytest.mark.parametrize(
    ("field", "value", "error"),
    [
        ("tenant_id", "", "blank_value"),
        ("fencing_token", -1, "negative_counter"),
        ("now", NOW.replace(tzinfo=None), "datetime_not_utc"),
    ],
)
def test_comando_rejeita_envelope_invalido(field, value, error) -> None:
    with pytest.raises(ValueError, match=error):
        command(manifest(), **{field: value})


def test_objeto_ausente_nao_muta_estado() -> None:
    raw = manifest()
    store = ObjectStore(raw)
    store.objects.clear()
    control = ControlPlane(job())

    with pytest.raises(Conflict, match="object=missing"):
        RawIngestionService(control, store, DeltaPolicy()).register(command(raw))

    assert control.mutations == []


@pytest.mark.parametrize("corruption", ["record", "job-key", "sidecar-stat", "sidecar-bytes"])
def test_replay_aceito_corrompido_falha_fechado(corruption: str) -> None:
    raw = manifest()
    projection = record(raw)
    succeeded = job().model_copy(update={
        "state": JobState.SUCCEEDED,
        "lease_owner": None,
        "lease_until": None,
        "result_manifest_id": raw.manifest_id,
        "result_manifest_key": projection.manifest_key,
    })
    store = ObjectStore(raw)
    sidecar = raw.model_dump_json(exclude_none=False, by_alias=False).encode()
    store.objects[projection.manifest_key] = sidecar
    control = ControlPlane(succeeded)
    control.records[raw.manifest_id] = projection
    if corruption == "record":
        control.records.clear()
    elif corruption == "job-key":
        control.job = succeeded.model_copy(update={"result_manifest_key": "raw/x/y/manifest.json"})
    elif corruption == "sidecar-stat":
        store.objects.pop(projection.manifest_key)
    else:
        store.objects[projection.manifest_key] = b"divergent"

    with pytest.raises(Conflict, match="terminal_replay=conflict"):
        RawIngestionService(control, store, DeltaPolicy()).register(command(raw))

    assert control.mutations == []


def test_historico_corrompido_nao_finaliza_job() -> None:
    raw = manifest(SnapshotMode.DELTA)
    control = ControlPlane(job(SnapshotMode.DELTA))
    control.latest = job().model_copy(update={
        "state": JobState.SUCCEEDED,
        "lease_owner": None,
        "lease_until": None,
        "result_manifest_id": "missing",
        "result_manifest_key": "raw/354130/CNES_LOCAL/2026-07/base/manifest.json",
    })

    with pytest.raises(Conflict, match="raw_history=divergent"):
        RawIngestionService(control, ObjectStore(raw), DeltaPolicy()).register(command(raw))

    assert control.mutations == []
