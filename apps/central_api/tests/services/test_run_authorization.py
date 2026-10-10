"""TDD do RunAuthorizationService: gate, criacao sem medicao, replay e launch."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, cast
from unittest.mock import Mock

import pytest

from central_api.services.run_authorization import RunAuthorizationService
from cnes_domain.billing.commands import AuthorizedRunCommand, CreateRunRequest
from cnes_domain.billing.errors import EntitlementDenied
from cnes_domain.billing.models import RunAuthorization
from cnes_domain.control_plane.entities import Run, RunDependency
from cnes_domain.control_plane.enums import RunState

if TYPE_CHECKING:
    from cnes_domain.billing.gate import EntitlementGate

_NOW = datetime(2026, 1, 15, 12, tzinfo=UTC)
_ACCOUNT = "ba_01"
_TENANT = "354130"
_RUN_ID = "run-01"
_LAUNCHING = (RunState.PLANNED, RunState.WAITING_INPUTS)
_REPLAYED = (
    RunState.PROCESSING, RunState.PUBLISHING, RunState.PUBLISHED, RunState.FAILED,
    RunState.CANCELED,
)


@dataclass
class _FakeGate:
    authorization: RunAuthorization | None = None
    denial: Exception | None = None
    received: list[CreateRunRequest] = field(default_factory=list)

    def authorize_create_run(self, command: CreateRunRequest) -> RunAuthorization:
        self.received.append(command)
        if self.denial is not None:
            raise self.denial
        assert self.authorization is not None
        return self.authorization


def _request() -> CreateRunRequest:
    return CreateRunRequest(
        billing_account_id=_ACCOUNT, tenant_id=_TENANT, run_id=_RUN_ID, competencia="2026-01",
        dataset_name="cnes",
        dependencies=(RunDependency(source_type="CNES", file_subtype="LFCES", required=True),),
        idempotency_key="req-01", request_hash="a" * 64, requested_concurrency=2,
        estimated_scan_bytes=1_000,
    )


def _authorization(reservation_id: str | None) -> RunAuthorization:
    return RunAuthorization(
        billing_account_id=_ACCOUNT, plan_version_id="plan-1", entitlement_version=1,
        max_concurrency=2, budget_reservation_id=reservation_id, authorized_at=_NOW,
    )


def _run(state: RunState) -> Run:
    request = _request()
    return Run(
        tenant_id=request.tenant_id, run_id=request.run_id, competencia=request.competencia,
        dataset_name=request.dataset_name, state=state, dependencies=request.dependencies,
        missing_sources=(), created_at=_NOW,
    )


def _service(
    authorization: RunAuthorization | None = None, denial: Exception | None = None,
) -> tuple[RunAuthorizationService, _FakeGate, Mock, Mock]:
    gate = _FakeGate(authorization, denial)
    control_plane = Mock()
    run_planning = Mock()
    service = RunAuthorizationService(cast("EntitlementGate", gate), control_plane, run_planning)
    return service, gate, control_plane, run_planning


def test_caminho_sem_medicao_cria_run_com_request_e_autorizacao_originais():
    authorization = _authorization(None)
    service, gate, control_plane, _ = _service(authorization)
    control_plane.create_unmetered_run.return_value = _run(RunState.CANCELED)
    command = _request()

    service.authorize_and_create(command)

    assert gate.received == [command]
    control_plane.create_unmetered_run.assert_called_once_with(
        AuthorizedRunCommand(request=command, authorization=authorization)
    )
    control_plane.get_run.assert_not_called()


def test_caminho_reservado_le_run_existente_sem_criar():
    service, _, control_plane, _ = _service(_authorization("res-01"))
    control_plane.get_run.return_value = _run(RunState.PUBLISHED)

    service.authorize_and_create(_request())

    control_plane.get_run.assert_called_once_with(_TENANT, _RUN_ID)
    control_plane.create_unmetered_run.assert_not_called()


def test_run_ausente_apos_reserva_levanta_erro_com_motivo():
    service, _, control_plane, run_planning = _service(_authorization("res-01"))
    control_plane.get_run.return_value = None

    with pytest.raises(RuntimeError, match="reason=run_missing_after_reservation run_id=run-01"):
        service.authorize_and_create(_request())

    run_planning.launch.assert_not_called()


@pytest.mark.parametrize("state", _LAUNCHING)
@pytest.mark.parametrize("reservation_id", [None, "res-01"])
def test_estado_lancavel_dispara_launch_uma_vez_e_devolve_run_resultante(state, reservation_id):
    service, _, control_plane, run_planning = _service(_authorization(reservation_id))
    created = _run(state)
    control_plane.create_unmetered_run.return_value = created
    control_plane.get_run.return_value = created

    result = service.authorize_and_create(_request())

    run_planning.launch.assert_called_once_with(_TENANT, _RUN_ID)
    assert result is run_planning.launch.return_value.run


@pytest.mark.parametrize("state", _REPLAYED)
def test_replay_em_estado_avancado_devolve_run_sem_launch(state):
    service, _, control_plane, run_planning = _service(_authorization("res-01"))
    existing = _run(state)
    control_plane.get_run.return_value = existing

    result = service.authorize_and_create(_request())

    assert result is existing
    run_planning.launch.assert_not_called()


def test_negativa_do_gate_propaga_antes_de_qualquer_escrita():
    denial = EntitlementDenied("reason=quota_exhausted")
    service, _, control_plane, run_planning = _service(denial=denial)

    with pytest.raises(EntitlementDenied):
        service.authorize_and_create(_request())

    assert control_plane.method_calls == []
    assert run_planning.method_calls == []
