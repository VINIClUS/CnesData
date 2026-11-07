"""Contrato do repositório SQL legado."""
from datetime import date
from typing import Any, cast
from uuid import uuid4

from cnes_data.storage.protocols import ExtractionRepoPort


class _FakeRepo:
    def enqueue(
        self, engine, *, tenant_id, source_type, competencia,
        files, depends_on=None,
    ):
        return uuid4()

    def claim_next(
        self, engine, *, lease_seconds=300,
    ):
        return None

    def register(
        self, engine, *, job_id, files,
        agent_version=None, machine_id=None,
    ):
        return job_id

    def mark_completed(self, engine, *, job_id):
        return None

    def mark_failed(self, engine, *, job_id, reason):
        return None

    def reap_expired(self, engine):
        return 0



def test_extraction_repo_protocol_satisfeito():
    repo: ExtractionRepoPort = _FakeRepo()
    engine = object()  # placeholder; Protocol is structural, runtime value irrelevant
    job_id = uuid4()
    assert repo.enqueue(
        engine, tenant_id="354130", source_type="BPA_MAG",
        competencia=date(2026, 1, 1),
        files=[{"minio_key": "x.parquet.gz"}],
    ) is not None
    assert repo.claim_next(engine) is None
    assert repo.register(
        engine, job_id=job_id, files=[],
    ) == job_id
    assert repo.mark_completed(engine, job_id=job_id) is None
    assert repo.mark_failed(engine, job_id=job_id, reason="x") is None
    assert repo.reap_expired(engine) == 0



def test_stubs_do_repositorio_legado_preservados():
    repo_port = cast("Any", ExtractionRepoPort)
    repo = _FakeRepo()
    engine = object()
    assert repo_port.enqueue(
        repo, engine, tenant_id="354130", source_type="BPA_MAG",
        competencia=date(2026, 1, 1),
        files=[{"minio_key": "x.parquet.gz"}],
    ) is None
    assert repo_port.claim_next(repo, engine) is None
    assert repo_port.register(
        repo, engine, job_id=uuid4(), files=[],
    ) is None
    assert repo_port.mark_completed(
        repo, engine, job_id=uuid4(),
    ) is None
    assert repo_port.mark_failed(
        repo, engine, job_id=uuid4(), reason="e",
    ) is None
    assert repo_port.reap_expired(repo, engine) is None
