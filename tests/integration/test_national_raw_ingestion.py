"""Vertical raw nacional Phase 3 com transporte falso e registro CND-030."""

from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from unittest.mock import patch

import polars as pl
import pytest
from fastapi.testclient import TestClient

from central_api.app import create_app
from central_api.services.national_ingestion import (
    NationalIngestionService,
    NationalRefreshRequest,
)
from cnes_contracts import SnapshotMode, SourceType
from cnes_domain.control_plane.enums import JobState
from cnes_domain.control_plane.queries import RawIdentity, RawManifestChainQuery
from cnes_infra.ingestion import DatasusCnesRawAdapter, DatasusCnesRequest

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

pytestmark = pytest.mark.local_profile

TENANT = "354130"
COMPETENCIA = "2026-01"
SNAPSHOT = "snapshot-1"
PF_COLUMNS = (
    "CPF",
    "CNS",
    "NOME_PROFISSIONAL",
    "NOME_SOCIAL",
    "SEXO",
    "CBO",
    "CNES",
    "TIPO_VINCULO",
    "SUS",
    "CH_TOTAL",
    "CH_AMBULATORIAL",
    "CH_OUTRAS",
    "CH_HOSPITALAR",
    "FONTE",
)


class FakeTransport:
    """Transporte determinístico — nenhum acesso real ao FTP do DATASUS."""

    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.rows = rows
        self.requests: list[DatasusCnesRequest] = []

    def fetch(self, request: DatasusCnesRequest) -> Iterator[dict[str, object]]:
        self.requests.append(request)
        yield from self.rows


class SpyControlPlane:
    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self.direct_calls: list[str] = []
        self.created_jobs: list[str] = []

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    def create_job(self, job: Any, event: Any) -> Any:
        self.created_jobs.append(job.job_id)
        return self._inner.create_job(job, event)

    def complete_job(self, command: Any, event: Any) -> Any:
        self.direct_calls.append("complete_job")
        return self._inner.complete_job(command, event)

    def fail_job(self, command: Any, event: Any) -> Any:
        self.direct_calls.append("fail_job")
        return self._inner.fail_job(command, event)

    def put_manifest_record(self, connection: Any, manifest: Any) -> Any:
        self.direct_calls.append("put_manifest_record")
        return self._inner.put_manifest_record(connection, manifest)


def row(**updates: object) -> dict[str, object]:
    values: dict[str, object] = {
        "CPF_PROF": "90000000001",
        "CNS_PROF": "700000000000001",
        "NOMEPROF": " Profissional Teste ",
        "CBO": "123",
        "CNES": "456",
        "VINCULAC": "12",
        "PROF_SUS": "1",
        "HORAOUTR": 8,
        "HORAHOSP": "20",
        "HORA_AMB": "12",
        "COMPETEN": "202601",
        "CODUFMUN": TENANT,
    }
    return values | updates


def now() -> datetime:
    return datetime.now(UTC)


@contextmanager
def local_app(data_dir: Path) -> Iterator[Any]:
    env = {"PROFILE": "local", "TENANT_ID": TENANT, "DATA_DIR": str(data_dir)}
    with patch.dict(os.environ, env):
        app = create_app()
        with TestClient(app):
            yield app


def national_service(app: Any, transport: FakeTransport) -> NationalIngestionService:
    control_plane = SpyControlPlane(app.state.control_plane)
    adapter = DatasusCnesRawAdapter(transport, app.state.object_store, now)
    return NationalIngestionService(
        control_plane, adapter, app.state.raw_ingestion, now
    )


def refresh_request(**updates: str) -> NationalRefreshRequest:
    values = {
        "tenant_id": TENANT,
        "competencia": COMPETENCIA,
        "snapshot_id": SNAPSHOT,
        "idempotency_key": "refresh-1",
    }
    return NationalRefreshRequest(**(values | updates))


def data_key(snapshot_id: str = SNAPSHOT) -> str:
    return f"raw/{TENANT}/CNES_NACIONAL/{COMPETENCIA}/{snapshot_id}/data.parquet"


def chain(app: Any) -> tuple[Any, ...]:
    identity = RawIdentity(TENANT, SourceType.CNES_NACIONAL.value, "CNES_VINCULO", COMPETENCIA)
    return app.state.control_plane.query_raw_manifest_chain(RawManifestChainQuery(identity))


def test_refresh_nacional_fake_gera_mesmo_contrato_pf(tmp_path: Path) -> None:
    transport = FakeTransport([row()])
    with local_app(tmp_path) as app:
        acceptance = national_service(app, transport).refresh(refresh_request())

        assert acceptance.accepted is True
        assert acceptance.full_resync_required is False
        with app.state.object_store.open(data_key()) as stored:
            frame = pl.read_parquet(stored)
        assert tuple(frame.columns) == PF_COLUMNS
        assert frame.height == 1
        assert frame.row(0, named=True)["FONTE"] == "NACIONAL"
        assert frame.row(0, named=True)["NOME_SOCIAL"] is None
        assert frame.row(0, named=True)["SEXO"] is None
        assert frame.row(0, named=True)["CH_TOTAL"] == 40
        assert transport.requests == [
            DatasusCnesRequest(
                tenant_id=TENANT,
                competencia=COMPETENCIA,
                file_subtype="CNES_VINCULO",
                snapshot_id=SNAPSHOT,
                agent_id="system-datasus",
                agent_version="1.0.0",
            )
        ]


def test_refresh_nacional_registra_apenas_via_raw_ingestion(tmp_path: Path) -> None:
    transport = FakeTransport([row()])
    with local_app(tmp_path) as app:
        control_plane = SpyControlPlane(app.state.control_plane)
        adapter = DatasusCnesRawAdapter(transport, app.state.object_store, now)
        service = NationalIngestionService(
            control_plane, adapter, app.state.raw_ingestion, now
        )

        service.refresh(refresh_request())

        assert control_plane.direct_calls == []
        assert [ref.manifest_id for ref in chain(app)] == [SNAPSHOT]
        job = app.state.control_plane.get_job(TENANT, control_plane.created_jobs[0])
        assert job.state is JobState.SUCCEEDED
        assert job.result_manifest_id == SNAPSHOT
        assert job.requested_snapshot_mode == SnapshotMode.FULL.value


def test_replay_apos_crash_converge_sem_duplicar_cadeia(tmp_path: Path) -> None:
    transport = FakeTransport([row()])
    with local_app(tmp_path) as app:
        first = national_service(app, transport).refresh(refresh_request())

    with local_app(tmp_path) as restarted:
        second = national_service(restarted, transport).refresh(refresh_request())

        assert first == second
        assert [ref.manifest_id for ref in chain(restarted)] == [SNAPSHOT]
        accepted = [
            event.event_type
            for event in restarted.state.control_plane.pending_outbox(100)
            if event.event_type == "raw.manifest.accepted"
        ]
        assert accepted == ["raw.manifest.accepted"]


def test_refresh_nacional_nao_toca_dataset_ativo(tmp_path: Path) -> None:
    transport = FakeTransport([row()])
    with local_app(tmp_path) as app:
        national_service(app, transport).refresh(refresh_request())

        assert app.state.control_plane.get_dataset_pointer(TENANT, "cnes") is None
