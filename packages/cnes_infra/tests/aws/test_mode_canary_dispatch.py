"""Contrato do dispatch nativo do unit: dispatch_id, unit_ids, item RUNDISPATCH e StartExecution."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from hashlib import sha256
from typing import Any, cast

import boto3
import pytest
from moto import mock_aws

from cnes_domain.control_plane.dispatch_start import DispatchStartClaim, request_sha256
from cnes_domain.control_plane.entities import RunDispatch, RunUnit
from cnes_domain.control_plane.enums import DispatchState, RunUnitState
from cnes_domain.orchestration.mode_canary import ModeCanaryBinding, SyntheticContextRequired
from cnes_domain.orchestration.planner import RunPlan, logical_wave_id, ready_units
from cnes_domain.ports.processing import StartRunExecution
from cnes_infra.aws.mode_canary_dispatch import (
    DISPATCH_ENTITY,
    PRODUCTION_MAX_CONCURRENCY,
    DispatchStep,
    run_dispatch_item,
    start_claim,
    start_execution,
    start_request_sha256,
    unit_dispatch,
)
from cnes_infra.aws.processor_settings import PRODUCTION_PROCESSOR_MAX_CONCURRENCY
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.executor.step_functions import StepFunctionsExecutor
from packages.cnes_infra.tests.control_plane.test_dynamodb_adapter import (
    _TABLE_NAME,
    _create_table,
)

_TENANT = "prod-canary-" + "c" * 32
_RUN = "unit-aaaaaaaaaaaaaaaa"
_OWNER = "a" * 64
_LOCATOR = ModeCanaryBinding(_TENANT, (_RUN,), _OWNER, "1" * 64, "unit", "promotion")
_NOW = datetime(2026, 11, 13, 12, 5, tzinfo=UTC)
_MACHINE = "arn:aws:states:us-east-2:111122223333:stateMachine:cnesdata-prod"


def _unit(unit_id: str, state: str = "PENDING", depends: tuple[str, ...] = (),
          lease_until: str | None = None, run_id: str = _RUN) -> dict[str, Any]:
    return {"tenant_id": _TENANT, "run_id": run_id, "unit_id": unit_id, "state": state,
            "depends_on_unit_ids": list(depends), "lease_until": lease_until}


_UNITS = [_unit("n-b"), _unit("n-a"), _unit("rec", depends=("n-a", "n-b")),
          _unit("mat", depends=("rec",))]


def _model(unit: dict[str, Any]) -> RunUnit:
    lease = unit["lease_until"]
    return RunUnit.model_construct(
        unit_id=unit["unit_id"], state=RunUnitState(unit["state"]),
        depends_on_unit_ids=tuple(unit["depends_on_unit_ids"]),
        lease_until=None if lease is None else datetime.fromisoformat(lease))


def _product_ready(units: list[dict[str, Any]]) -> tuple[str, ...]:
    plan = RunPlan(cast("Any", None), tuple(_model(unit) for unit in units), (), (), 4)
    return tuple(sorted(unit.unit_id for unit in ready_units(plan, _NOW)))


def test_unit_ids_e_wave_sao_os_do_planner_sobre_as_unidades_semeadas() -> None:
    dispatch = unit_dispatch(_LOCATOR, _UNITS, _NOW)
    assert dispatch.unit_ids == _product_ready(_UNITS) == ("n-a", "n-b")
    plan = RunPlan(cast("Any", None), tuple(_model(u) for u in _UNITS), (), (), 4)
    assert dispatch.wave_id == logical_wave_id(ready_units(plan, _NOW))
    assert (dispatch.tenant_id, dispatch.run_id, dispatch.max_concurrency) == (_TENANT, _RUN, 1)


@pytest.mark.parametrize("units", [
    [_unit("a", "SUCCEEDED"), _unit("b", depends=("a",)), _unit("c", "FAILED_RETRYABLE")],
    [_unit("a", "SUCCEEDED_DEGRADED"), _unit("b", depends=("a",))],
    [_unit("a", "LEASED", lease_until="2026-11-13T12:04:00+00:00"), _unit("b", "FAILED_FINAL")],
])
def test_prontidao_segue_estados_candidatos_e_predecessores_satisfeitos(
        units: list[dict[str, Any]]) -> None:
    assert unit_dispatch(_LOCATOR, units, _NOW).unit_ids == _product_ready(units)


def test_lease_vivo_ou_nenhuma_unidade_pronta_recusa_dispatch_units() -> None:
    live = [_unit("a", "LEASED", lease_until="2026-11-13T12:06:00+00:00"), _unit("b")]
    assert _product_ready(live) == ()
    for units in (live, [_unit("a", "SUCCEEDED")]):
        with pytest.raises(SyntheticContextRequired, match=r"^reason=dispatch_units "):
            unit_dispatch(_LOCATOR, units, _NOW)


def test_unidade_de_outro_run_recusa_dispatch_units() -> None:
    with pytest.raises(SyntheticContextRequired, match=r"^reason=dispatch_units "):
        unit_dispatch(_LOCATOR, [*_UNITS, _unit("x", run_id="unit-bbbbbbbbbbbbbbbb")], _NOW)


@pytest.mark.parametrize("locator", [
    ModeCanaryBinding(_TENANT, ("recovery-a-aaaaaaaaaaaaaaaa",), _OWNER, "1" * 64, "recovery",
                      "promotion"),
    ModeCanaryBinding(_TENANT, (_RUN, "unit-bbbbbbbbbbbbbbbb"), _OWNER, "1" * 64, "unit",
                      "promotion"),
])
def test_locator_fora_do_unit_de_um_run_recusa_dispatch_locator(
        locator: ModeCanaryBinding) -> None:
    with pytest.raises(SyntheticContextRequired, match=r"^reason=dispatch_locator "):
        unit_dispatch(locator, _UNITS, _NOW)


def test_concorrencia_vendorizada_e_a_da_politica_de_producao_do_processor() -> None:
    assert PRODUCTION_MAX_CONCURRENCY == PRODUCTION_PROCESSOR_MAX_CONCURRENCY
    dispatch = unit_dispatch(_LOCATOR, _UNITS, _NOW)
    assert len(dispatch.unit_ids) == 2
    assert dispatch.max_concurrency == min(len(dispatch.unit_ids),
                                           PRODUCTION_PROCESSOR_MAX_CONCURRENCY)


def test_dependencia_desconhecida_de_unidade_candidata_recusa_como_o_planner() -> None:
    units = [_unit("a"), _unit("b", depends=("ghost",))]
    with pytest.raises(KeyError, match="ghost"):
        _product_ready(units)
    with pytest.raises(SyntheticContextRequired, match=r"^reason=dispatch_units "):
        unit_dispatch(_LOCATOR, units, _NOW)


@pytest.mark.parametrize("units", [
    [_unit("a"), _unit("b", "SUCCEEDED", depends=("ghost",))],
    [_unit("a"), _unit("b", depends=("a", "ghost"))],
])
def test_dependencia_desconhecida_que_o_planner_nao_le_nao_recusa(
        units: list[dict[str, Any]]) -> None:
    assert unit_dispatch(_LOCATOR, units, _NOW).unit_ids == _product_ready(units) == ("a",)


def test_relogio_nao_utc_recusa_clock_not_utc() -> None:
    other = _NOW.astimezone(timezone(timedelta(hours=-3)))
    with pytest.raises(SyntheticContextRequired, match=r"^reason=clock_not_utc "):
        unit_dispatch(_LOCATOR, _UNITS, other)


def test_dispatch_id_e_a_regra_da_reserva_na_primeira_geracao() -> None:
    dispatch = unit_dispatch(_LOCATOR, _UNITS, _NOW)
    raw = f"{_TENANT}\x1f{_RUN}\x1f{dispatch.wave_id}\x1f1"
    assert dispatch.dispatch_id == sha256(raw.encode()).hexdigest()[:16]


class _Sfn:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def start_execution(self, **request: Any) -> dict[str, Any]:
        self.calls.append(request)
        return {"executionArn": "arn"}


def _request(dispatch: Any) -> StartRunExecution:
    return StartRunExecution(tenant_id=dispatch.tenant_id, run_id=dispatch.run_id,
                             wave_id=dispatch.wave_id, dispatch_id=dispatch.dispatch_id,
                             unit_ids=dispatch.unit_ids, max_concurrency=dispatch.max_concurrency)


def test_start_execution_e_o_do_executor_do_produto() -> None:
    dispatch = unit_dispatch(_LOCATOR, _UNITS, _NOW)
    sfn = _Sfn()
    StepFunctionsExecutor(cast("Any", sfn), _MACHINE).start(_request(dispatch))
    assert sfn.calls == [{"stateMachineArn": _MACHINE} | start_execution(dispatch)]
    assert start_request_sha256(dispatch) == request_sha256(_request(dispatch))


def _steps(dispatch: Any) -> list[tuple[DispatchStep, RunDispatch]]:
    claim = start_claim(dispatch, 3, 5, "p" * 64)
    lease = _NOW + timedelta(seconds=90)
    later = _NOW + timedelta(seconds=150, microseconds=250)
    reserved = RunDispatch(
        tenant_id=dispatch.tenant_id, run_id=dispatch.run_id, wave_id=dispatch.wave_id,
        dispatch_id=dispatch.dispatch_id, generation=1, unit_ids=dispatch.unit_ids,
        state=DispatchState.RESERVED, lease_until=lease)
    starting = reserved.model_copy(update={
        "state": DispatchState.STARTING, "start_claim": DispatchStartClaim(**claim)})
    started = starting.model_copy(update={
        "state": DispatchState.STARTED, "execution_ref": "arn:exec", "lease_until": later})
    return [(DispatchStep("RESERVED", lease), reserved),
            (DispatchStep("STARTING", lease, claim), starting),
            (DispatchStep("STARTED", later, claim, "arn:exec"), started)]


def test_item_rundispatch_e_o_do_adapter_em_cada_estado() -> None:
    dispatch = unit_dispatch(_LOCATOR, _UNITS, _NOW)
    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        _create_table(client)
        adapter = DynamoDBControlPlane(client, _TABLE_NAME, lambda: _NOW)
        for step, model in _steps(dispatch):
            item = run_dispatch_item(dispatch, step)
            assert item == adapter._dispatch_item(model)
            assert item["entity"] == {"S": DISPATCH_ENTITY}


def test_claim_carrega_o_digest_do_request_e_a_primeira_geracao() -> None:
    dispatch = unit_dispatch(_LOCATOR, _UNITS, _NOW)
    claim = start_claim(dispatch, 3, 5, None)
    assert list(claim) == ["dispatch_id", "generation", "permit_token", "fence_version",
                           "request_sha256", "promotion_id"]
    assert (claim["generation"], claim["promotion_id"]) == (1, None)
    assert claim["request_sha256"] == request_sha256(_request(dispatch))


def test_estado_ou_lease_fora_do_contrato_recusa() -> None:
    dispatch = unit_dispatch(_LOCATOR, _UNITS, _NOW)
    for step in (DispatchStep("TERMINAL", _NOW), DispatchStep("STARTING", _NOW),
                 DispatchStep("STARTED", _NOW, start_claim(dispatch, 1, 0, None)),
                 DispatchStep("RESERVED", _NOW.astimezone(timezone(timedelta(hours=1))))):
        with pytest.raises(SyntheticContextRequired):
            run_dispatch_item(dispatch, step)
