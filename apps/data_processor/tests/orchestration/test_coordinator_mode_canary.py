"""PipelineCoordinator.resume recusa o namespace sintético sem contexto instalado."""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from cnes_domain.orchestration.mode_canary import ModeCanaryBinding, SyntheticContextRequired
from cnes_domain.ports.processing import ExecutionCallbacks, ExecutionPolicyConfig
from data_processor.orchestration.coordinator import CoordinatorDependencies, PipelineCoordinator

_TENANT = "prod-canary-0123456789abcdef0123456789abcdef"
_RUN = "unit-aaaaaaaaaaaaaaaa"
_BINDING = ModeCanaryBinding(
    tenant_id=_TENANT, run_ids=(_RUN,), owner_sha256="a" * 64, record_sha256="b" * 64,
    phase="unit", context_kind="promotion",
)


def _coordinator(
    binding: ModeCanaryBinding | None = None,
) -> tuple[PipelineCoordinator, MagicMock, MagicMock]:
    control_plane = MagicMock()
    executor = MagicMock()
    dependencies = CoordinatorDependencies(
        control_plane=control_plane, executor=executor, publisher=MagicMock(), clock=MagicMock(),
    )
    execution = ExecutionPolicyConfig(2, 300, ExecutionCallbacks(MagicMock(), MagicMock()))
    return PipelineCoordinator(dependencies, execution, binding), control_plane, executor


def test_coordinator_resume_direto_sem_contexto_nao_muta() -> None:
    coordinator, control_plane, executor = _coordinator()

    with pytest.raises(SyntheticContextRequired, match="field=tenant_id"):
        coordinator.resume(_TENANT, _RUN)

    assert control_plane.method_calls == []
    assert executor.method_calls == []


@pytest.mark.parametrize(
    ("tenant", "run"),
    [(_TENANT, "unit-cccccccccccccccc"), ("prod-canary-" + "f" * 32, _RUN), (_TENANT.upper(), _RUN)],
)
def test_resume_fora_do_binding_instalado_nao_muta(tenant: str, run: str) -> None:
    coordinator, control_plane, executor = _coordinator(_BINDING)

    with pytest.raises(SyntheticContextRequired):
        coordinator.resume(tenant, run)

    assert control_plane.method_calls == []
    assert executor.method_calls == []


def test_resume_dentro_do_binding_instalado_segue_o_caminho_nativo() -> None:
    coordinator, control_plane, _ = _coordinator(_BINDING)
    control_plane.get_run.return_value = None

    with pytest.raises(ValueError, match="run_not_found"):
        coordinator.resume(_TENANT, _RUN)

    control_plane.get_run.assert_called_once_with(_TENANT, _RUN)


def test_resume_de_tenant_normal_ignora_o_binding() -> None:
    coordinator, control_plane, _ = _coordinator(_BINDING)
    control_plane.get_run.return_value = None

    with pytest.raises(ValueError, match="run_not_found"):
        coordinator.resume("354130", "run-a")

    control_plane.get_run.assert_called_once_with("354130", "run-a")
