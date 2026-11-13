"""Facade do control plane e executor do canary de modo no runtime do processor."""
from __future__ import annotations

import inspect
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import boto3
import pytest
from moto import mock_aws

from cnes_domain.orchestration.mode_canary import ModeCanaryBinding, SyntheticContextRequired
from cnes_domain.ports.processing import StartRunExecution
from data_processor.mode_canary import CanaryControlPlane, ModeCanaryExecutor
from packages.cnes_infra.tests.aws.test_mode_canary_ledger import (
    _DISPATCH,
    _HOLDER,
    _NOW,
    _OWNER,
    _RUN,
    _TENANT,
    _UNITS,
    _Ledger,
)
from packages.cnes_infra.tests.control_plane.test_dynamodb_adapter import _create_table

if TYPE_CHECKING:
    from collections.abc import Iterator
    from datetime import datetime


@pytest.fixture
def ledger() -> Iterator[_Ledger]:
    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        _create_table(client)
        yield _Ledger(client)


@dataclass
class _Plane:
    runs: dict[tuple[str, str], object]
    calls: list[str] = field(default_factory=list[str])

    def get_run(self, tenant_id: str, run_id: str) -> object | None:
        self.calls.append(f"get_run:{run_id}")
        return self.runs.get((tenant_id, run_id))

    def list_recoverable_runs(self, now: datetime, limit: int = 100) -> tuple[object, ...]:
        raise AssertionError("global_gsi4")

    def reserve_run_dispatch(self, command: Any) -> str:
        self.calls.append(f"reserve:{command.run_id}")
        return "reserved"


def _facade(plane: _Plane) -> CanaryControlPlane:
    binding = ModeCanaryBinding(_TENANT, (_RUN, "unit-bbbbbbbbbbbbbbbb"), _OWNER, "b" * 64,
                                "recovery", "promotion")
    return CanaryControlPlane(cast("Any", plane), binding)


def test_recovery_canary_nao_consulta_gsi4_global() -> None:
    run = SimpleNamespace(tenant_id=_TENANT, run_id=_RUN)
    plane = _Plane({(_TENANT, _RUN): run})

    runs = _facade(plane).list_recoverable_runs(_NOW, 100)

    assert runs == (run,)
    assert plane.calls == [f"get_run:{_RUN}", "get_run:unit-bbbbbbbbbbbbbbbb"]
    assert _facade(plane).list_recoverable_runs(_NOW, 1) == (run,)


def test_facade_recusa_tenant_ou_run_fora_do_binding_e_metodos_globais() -> None:
    plane = _Plane({})
    facade = cast("Any", _facade(plane))
    command = SimpleNamespace(tenant_id=_TENANT, run_id=_RUN)

    assert facade.reserve_run_dispatch(cast("Any", command)) == "reserved"
    for tenant, run in (("354130", _RUN), (_TENANT, "unit-x")):
        with pytest.raises(SyntheticContextRequired, match="reason=outside_binding"):
            facade.get_run(tenant, run)
        with pytest.raises(SyntheticContextRequired, match="reason=outside_binding"):
            facade.reserve_run_dispatch(cast("Any", SimpleNamespace(tenant_id=tenant, run_id=run)))
    with pytest.raises(SyntheticContextRequired, match="reason=outside_binding"):
        facade.get_dataset_pointer("354130", "cnes")
    for name in ("read_outbox_page", "put_tenant", "create_job", "list_revocable_runs"):
        with pytest.raises(AttributeError):
            getattr(facade, name)
    assert inspect.getattr_static(facade, "claim_run_unit") is not None


@dataclass
class _Executor:
    calls: list[str] = field(default_factory=list[str])

    def start(self, request: StartRunExecution) -> str:
        self.calls.append("start")
        return "arn:exec"

    def start_with_claim(self, request: StartRunExecution, claim: Any) -> str:
        self.calls.append("start_with_claim")
        return "arn:exec"

    def status(self, execution_ref: str) -> str:
        return "RUNNING"

    def cancel(self, request: Any) -> None:
        self.calls.append("cancel")


def _request(tenant: str = _TENANT) -> StartRunExecution:
    return StartRunExecution(tenant_id=tenant, run_id=_RUN, wave_id="a" * 16,
                             dispatch_id=_DISPATCH, unit_ids=_UNITS, max_concurrency=1)


def test_start_sintetico_checa_ticket_vivo_imediatamente_antes(ledger: _Ledger) -> None:
    ledger.seed()
    context = ledger.context()
    inner = _Executor()
    executor = ModeCanaryExecutor(cast("Any", inner), context, lambda: _NOW)
    with pytest.raises(SyntheticContextRequired, match="reason=ticket_missing"):
        executor.start_with_claim(_request(), object())
    with pytest.raises(SyntheticContextRequired, match="reason=claim_required"):
        executor.start(_request())
    assert inner.calls == []
    context.binding_for(_HOLDER, _NOW)

    assert executor.start_with_claim(_request(), object()) == "arn:exec"
    assert executor.start(_request("354130")) == "arn:exec"
    assert executor.status("arn:exec") == "RUNNING"
    executor.cancel(object())
    assert inner.calls == ["start_with_claim", "start", "cancel"]
    context.seal(_NOW)
    with pytest.raises(SyntheticContextRequired, match="reason=control_sealed"):
        executor.start_with_claim(_request(), object())


def test_facade_aceita_identidade_nomeada_e_executor_normal_dispensa_ticket() -> None:
    plane = _Plane({(_TENANT, _RUN): "run"})
    facade = cast("Any", _facade(plane))

    assert facade.get_run(tenant_id=_TENANT, run_id=_RUN) == "run"
    with pytest.raises(SyntheticContextRequired, match="reason=outside_binding"):
        facade.get_run(tenant_id="354130", run_id=_RUN)
    inner = _Executor()
    executor = ModeCanaryExecutor(cast("Any", inner), cast("Any", None), lambda: _NOW)
    assert executor.start_with_claim(_request("354130"), object()) == "arn:exec"
    assert inner.calls == ["start_with_claim"]
