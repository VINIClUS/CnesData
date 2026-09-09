from datetime import UTC, datetime, timedelta
from hashlib import sha256
from io import BytesIO
from threading import get_ident

import pytest

from central_api.services import raw_upload
from central_api.services.raw_upload import (
    RAW_UPLOAD_MAX_BYTES,
    RawUploadConflict,
    RawUploadEmpty,
    RawUploadFenceRejected,
    RawUploadIdentityRejected,
    RawUploadKeyRejected,
    RawUploadLeaseRejected,
    RawUploadNotFound,
    RawUploadRequest,
    RawUploadService,
    RawUploadTooLarge,
)
from cnes_domain.control_plane.entities import Job
from cnes_domain.control_plane.enums import JobState
from cnes_domain.control_plane.errors import Conflict
from cnes_domain.ports.object_store import ObjectStat

NOW = datetime(2026, 7, 15, 12, tzinfo=UTC)
KEY = "raw/354130/CNES_LOCAL/2026-07/snapshot-1/data.parquet"


def job(**updates: object) -> Job:
    values = {
        "tenant_id": "354130",
        "job_id": "job-1",
        "agent_id": "agent-1",
        "source_type": "CNES_LOCAL",
        "file_subtype": "CNES_VINCULO",
        "competencia": "2026-07",
        "requested_snapshot_mode": "FULL",
        "state": JobState.LEASED,
        "attempt": 1,
        "fencing_token": 7,
        "lease_owner": "agent-1",
        "lease_until": NOW + timedelta(minutes=5),
        "result_manifest_id": None,
        "result_manifest_key": None,
        "error_code": None,
        "created_at": NOW,
    }
    return Job(**(values | updates))


class ControlPlane:
    def __init__(self, jobs: list[Job | None]) -> None:
        self.jobs = jobs
        self.calls = 0

    def get_job(self, tenant_id: str, job_id: str) -> Job | None:
        self.calls += 1
        current = self.jobs[min(self.calls - 1, len(self.jobs) - 1)]
        if current is None:
            return None
        if (tenant_id, job_id) != (current.tenant_id, current.job_id):
            return None
        return current


class ObjectStore:
    def __init__(self, objects: dict[str, bytes] | None = None) -> None:
        self.objects = dict(objects or {})
        self.put_calls = 0

    def stat(self, key: str) -> ObjectStat | None:
        body = self.objects.get(key)
        if body is None:
            return None
        return ObjectStat(key, len(body), sha256(body).hexdigest())

    def put(self, key: str, body, expected_sha256: str) -> ObjectStat:
        self.put_calls += 1
        value = body.read()
        current = self.objects.setdefault(key, value)
        if current != value:
            raise RuntimeError("object=immutable")
        return ObjectStat(key, len(value), expected_sha256)


def request(**updates: object) -> RawUploadRequest:
    values = {
        "tenant_id": "354130",
        "agent_id": "agent-1",
        "job_id": "job-1",
        "fencing_token": 7,
        "object_key": KEY,
    }
    return RawUploadRequest(**(values | updates))


async def chunks(*values: bytes):
    for value in values:
        yield value


@pytest.mark.anyio
async def test_upload_faz_stream_sem_carregar_payload_inteiro(monkeypatch) -> None:
    writes: list[bytes] = []
    thresholds: list[int] = []

    class SpoolSpy(BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            self.close()

        def write(self, value: bytes) -> int:
            writes.append(value)
            return super().write(value)

    def spool(*, max_size: int, mode: str):
        thresholds.append(max_size)
        assert mode == "w+b"
        return SpoolSpy()

    monkeypatch.setattr(raw_upload, "SpooledTemporaryFile", spool)
    store = ObjectStore()
    service = RawUploadService(ControlPlane([job()]), store, lambda: NOW)

    result = await service.upload(request(), chunks(b"abc", b"def"))

    assert writes == [b"abc", b"def"]
    assert thresholds == [8 * 1024**2]
    assert result == ObjectStat(KEY, 6, sha256(b"abcdef").hexdigest())


@pytest.mark.anyio
async def test_upload_rejeita_mais_de_um_gibibyte(monkeypatch) -> None:
    assert RAW_UPLOAD_MAX_BYTES == 1024**3
    monkeypatch.setattr(raw_upload, "RAW_UPLOAD_MAX_BYTES", 5)
    store = ObjectStore()
    service = RawUploadService(ControlPlane([job()]), store, lambda: NOW)

    with pytest.raises(RawUploadTooLarge, match="payload_too_large"):
        await service.upload(request(), chunks(b"123", b"456"))

    assert store.put_calls == 0


@pytest.mark.anyio
async def test_upload_rejeita_corpo_vazio_sem_publicar() -> None:
    store = ObjectStore()
    service = RawUploadService(ControlPlane([job()]), store, lambda: NOW)

    with pytest.raises(RawUploadEmpty, match="payload_empty"):
        await service.upload(request(), chunks())

    assert store.put_calls == 0


@pytest.mark.anyio
async def test_upload_publica_objeto_fora_da_thread_do_event_loop() -> None:
    class ThreadRecordingStore(ObjectStore):
        thread_id: int | None = None

        def put(self, key: str, body, expected_sha256: str) -> ObjectStat:
            self.thread_id = get_ident()
            return super().put(key, body, expected_sha256)

    store = ThreadRecordingStore()
    service = RawUploadService(ControlPlane([job()]), store, lambda: NOW)

    await service.upload(request(), chunks(b"payload"))

    assert store.thread_id != get_ident()


@pytest.mark.anyio
async def test_upload_revalida_fence_antes_de_publicar() -> None:
    changed = job(fencing_token=8)
    store = ObjectStore()
    service = RawUploadService(ControlPlane([job(), changed]), store, lambda: NOW)

    with pytest.raises(RawUploadFenceRejected, match="job_fence_rejected"):
        await service.upload(request(), chunks(b"payload"))

    assert store.put_calls == 0


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("current", "error"),
    [
        (None, RawUploadNotFound),
        (job(agent_id="other"), RawUploadIdentityRejected),
        (job(state=JobState.PENDING, lease_owner=None, lease_until=None), RawUploadLeaseRejected),
        (job(lease_owner="other"), RawUploadLeaseRejected),
        (job(lease_until=NOW), RawUploadLeaseRejected),
        (job(fencing_token=8), RawUploadFenceRejected),
    ],
)
async def test_job_invalido_rejeita_antes_do_primeiro_byte(current, error) -> None:
    consumed = False

    async def body():
        nonlocal consumed
        consumed = True
        yield b"payload"

    with pytest.raises(error):
        await RawUploadService(ControlPlane([current]), ObjectStore(), lambda: NOW).upload(
            request(), body()
        )

    assert not consumed


@pytest.mark.anyio
@pytest.mark.parametrize(
    "key",
    [
        "raw/other/CNES_LOCAL/2026-07/snapshot-1/data.parquet",
        "raw/354130/SIHD/2026-07/snapshot-1/data.parquet",
        "raw/354130/CNES_LOCAL/2026-08/snapshot-1/data.parquet",
        "raw/354130/CNES_LOCAL/2026-07/../data.parquet",
        "/raw/354130/CNES_LOCAL/2026-07/snapshot-1/data.parquet",
        "raw/354130/CNES_LOCAL/2026-07/snapshot-1/other.parquet",
    ],
)
async def test_upload_rejeita_chave_incompativel_antes_do_corpo(key: str) -> None:
    with pytest.raises(RawUploadKeyRejected, match="object_key_invalid"):
        await RawUploadService(ControlPlane([job()]), ObjectStore(), lambda: NOW).upload(
            request(object_key=key), chunks(b"payload")
        )


@pytest.mark.anyio
async def test_replay_identico_retorna_stat_sem_nova_escrita() -> None:
    body = b"payload"
    store = ObjectStore({KEY: body})
    service = RawUploadService(ControlPlane([job()]), store, lambda: NOW)

    result = await service.upload(request(), chunks(body))

    assert result.sha256 == sha256(body).hexdigest()
    assert store.put_calls == 0


@pytest.mark.anyio
async def test_replay_terminal_identico_retorna_stat_sem_nova_escrita() -> None:
    body = b"payload"
    terminal = job(
        state=JobState.SUCCEEDED,
        lease_owner=None,
        lease_until=None,
        result_manifest_id="manifest-1",
        result_manifest_key="raw/354130/CNES_LOCAL/2026-07/snapshot-1/manifest.json",
    )
    store = ObjectStore({KEY: body})
    service = RawUploadService(ControlPlane([terminal]), store, lambda: NOW)

    result = await service.upload(request(), chunks(body))

    assert result.sha256 == sha256(body).hexdigest()
    assert store.put_calls == 0


@pytest.mark.anyio
async def test_replay_resync_terminal_identico_retorna_stat() -> None:
    body = b"payload"
    terminal = job(
        state=JobState.FAILED_FINAL,
        lease_owner=None,
        lease_until=None,
        error_code="RAW_RESYNC_BASELINE_MISSING",
        rejected_manifest_sha256="a" * 64,
    )
    store = ObjectStore({KEY: body})
    service = RawUploadService(ControlPlane([terminal]), store, lambda: NOW)

    result = await service.upload(request(), chunks(body))

    assert result.sha256 == sha256(body).hexdigest()
    assert store.put_calls == 0


@pytest.mark.anyio
async def test_job_terminal_sem_objeto_rejeita_antes_do_primeiro_byte() -> None:
    consumed = False
    terminal = job(
        state=JobState.SUCCEEDED,
        lease_owner=None,
        lease_until=None,
        result_manifest_id="manifest-1",
        result_manifest_key="raw/354130/CNES_LOCAL/2026-07/snapshot-1/manifest.json",
    )

    async def body():
        nonlocal consumed
        consumed = True
        yield b"payload"

    service = RawUploadService(ControlPlane([terminal]), ObjectStore(), lambda: NOW)
    with pytest.raises(RawUploadLeaseRejected, match="job_not_leased"):
        await service.upload(request(), body())

    assert not consumed


@pytest.mark.anyio
async def test_replay_divergente_preserva_objeto_anterior() -> None:
    store = ObjectStore({KEY: b"original"})
    service = RawUploadService(ControlPlane([job()]), store, lambda: NOW)

    with pytest.raises(RawUploadConflict, match="object_conflict"):
        await service.upload(request(), chunks(b"different"))

    assert store.objects[KEY] == b"original"
    assert store.put_calls == 0


@pytest.mark.anyio
async def test_corrida_de_publicacao_identica_retorna_objeto_vencedor() -> None:
    body = b"payload"

    class RacingStore(ObjectStore):
        def stat(self, key: str) -> ObjectStat | None:
            if self.put_calls == 0:
                return None
            return ObjectStat(key, len(body), sha256(body).hexdigest())

        def put(self, key: str, stream, expected_sha256: str) -> ObjectStat:
            self.put_calls += 1
            raise Conflict("object=immutable")

    store = RacingStore()
    service = RawUploadService(ControlPlane([job()]), store, lambda: NOW)

    result = await service.upload(request(), chunks(body))

    assert result == ObjectStat(KEY, len(body), sha256(body).hexdigest())
