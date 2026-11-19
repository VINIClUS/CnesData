"""RunPlanningService recusa o namespace sintético antes de qualquer leitura ou escrita."""
from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from central_api.services.run_planning import RunPlanningDependencies, RunPlanningService
from cnes_domain.orchestration.mode_canary import SyntheticContextRequired
from cnes_domain.ports.processing import ExecutionCallbacks, ExecutionPolicyConfig

_CANARY = "prod-canary-0123456789abcdef0123456789abcdef"


def _service() -> tuple[RunPlanningService, MagicMock, MagicMock]:
    control_plane = MagicMock()
    executor = MagicMock()
    dependencies = RunPlanningDependencies(
        control_plane=control_plane, object_store=MagicMock(), executor=executor,
        source_catalog=MagicMock(),
    )
    execution = ExecutionPolicyConfig(2, 300, ExecutionCallbacks(MagicMock(), MagicMock()))
    service = RunPlanningService(
        dependencies, execution, lambda: datetime(2026, 1, 15, tzinfo=UTC), dispatch_enabled=True
    )
    return service, control_plane, executor


@pytest.mark.parametrize("tenant", [_CANARY, _CANARY.upper(), "Prod-Canary-sufixo-invalido"])
def test_api_launch_sintetico_rejeita_sem_get_ou_write(tenant: str) -> None:
    service, control_plane, executor = _service()

    with pytest.raises(SyntheticContextRequired, match="tenant_id"):
        service.launch(tenant, "run-a")

    assert control_plane.method_calls == []
    assert executor.method_calls == []


def test_api_launch_de_tenant_normal_continua_lendo_o_run() -> None:
    service, control_plane, _ = _service()
    control_plane.get_run.return_value = None

    with pytest.raises(ValueError, match="run_not_found"):
        service.launch("354130", "run-a")

    control_plane.get_run.assert_called_once_with("354130", "run-a")
