from datetime import UTC, datetime

import pytest

from central_api.services.raw_enqueue import RawEnqueueRequest, RawEnqueueService
from cnes_domain.control_plane.enums import JobState
from cnes_domain.control_plane.errors import Conflict
from cnes_infra.control_plane.sqlite_adapter import SQLiteControlPlane

NOW = datetime(2026, 9, 26, tzinfo=UTC)


def _service(tmp_path):
    control = SQLiteControlPlane(tmp_path / "raw.db", lambda: NOW)
    control.initialize()
    return control, RawEnqueueService(control, lambda: NOW)


def test_cria_dez_pares_raw_e_replay_idempotente(tmp_path) -> None:
    control, service = _service(tmp_path)
    request = RawEnqueueRequest(
        tenant_id="354130", agent_id="agent-1", competencia="2026-09",
    )

    first = service.enqueue(request, "same-key")
    second = service.enqueue(request, "same-key")

    assert second == first
    assert len(first) == 10
    assert {(
        control.get_job("354130", job_id).source_type,
        control.get_job("354130", job_id).file_subtype,
    ) for job_id in first} == {
        ("CNES_LOCAL", "CNES_VINCULO"),
        ("SIHD", "SIHD_INTERNACAO"), ("SIHD", "SIHD_PROC_AIH"),
        ("BPA_MAG", "BPA_C"), ("BPA_MAG", "BPA_I"),
        ("SIA_LOCAL", "SIA_APA"), ("SIA_LOCAL", "SIA_BPI"),
        ("SIA_LOCAL", "SIA_BPIHST"), ("SIA_LOCAL", "DIM_SIGTAP"),
        ("SIA_LOCAL", "DIM_MUNICIPIO"),
    }
    assert all(control.get_job("354130", job_id).state is JobState.PENDING for job_id in first)


def test_replay_completa_criacao_interrompida(tmp_path) -> None:
    control, service = _service(tmp_path)
    request = RawEnqueueRequest(tenant_id="354130", agent_id="agent-1", competencia="2026-09")
    original = control.create_job
    calls = 0

    def fail_after_first(job, event):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("interrupted")
        return original(job, event)

    control.create_job = fail_after_first
    with pytest.raises(RuntimeError, match="interrupted"):
        service.enqueue(request, "same-key")
    control.create_job = original

    assert len(service.enqueue(request, "same-key")) == 10


def test_mesma_chave_com_pedido_diferente_rejeitada(tmp_path) -> None:
    _, service = _service(tmp_path)
    request = RawEnqueueRequest(tenant_id="354130", agent_id="agent-1", competencia="2026-09")
    service.enqueue(request, "same-key")

    with pytest.raises(Conflict):
        service.enqueue(request.model_copy(update={"competencia": "2026-08"}), "same-key")
