"""Ida e volta das janelas governadas entre o adapter do produto e o consumidor espelhado (P36b)."""

from __future__ import annotations

import json
from datetime import timedelta
from typing import Any

import pytest

from scripts.promotion import promote_api
from tests.production.delivery import _ia_stage_windows_consumer as ia
from tests.production.delivery.test_adapter_handlers import API, _ready_inputs
from tests.production.delivery.test_adapter_handlers import _request as handler_request
from tests.production.delivery.test_promote_close import (
    NOW,
    PREPARE,
    Context,
    _environment,
    _epoch,
    _inputs,
    _request,
)
from tests.production.delivery.test_stage_windows import INITIAL, MINIMUM, windows

INFRA_BOUND = _epoch(3600)


@pytest.fixture(autouse=True)
def _relogio(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(promote_api, "CLOCK", lambda: NOW)


def _prepare(governed: dict[str, Any]) -> dict[str, Any]:
    return PREPARE | {"windows": governed}


def _close_in(raw: Any, probe: Any, governed: dict[str, Any]) -> bytes:
    context = Context(raw, probe, _inputs())
    promote_api.execute_shared_operation("close-drain",
                                         _request(context.inputs, _prepare(governed)), context)
    [(kind, observation)] = context.observed
    assert kind == "close-drain"
    return observation


def _close(governed: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> bytes:
    with _environment("empty", monkeypatch) as (raw, _, probe):
        return _close_in(raw, probe, governed)


@pytest.mark.parametrize("governed", [MINIMUM, INITIAL, windows(phase_b=720, api=270)],
                         ids=["minimas", "iniciais", "soma-no-teto"])
def test_close_drain_do_produto_e_aceito_pelo_keeper_com_as_janelas_governadas(
    governed: dict[str, Any], monkeypatch: pytest.MonkeyPatch,
) -> None:
    observation = _close(governed, monkeypatch)
    document = json.loads(observation)
    assert (document["version"], document["windows"]) == (2, governed)
    fixed = int(NOW.timestamp()) + governed["fixed_deadline_seconds"]
    phase_b = ia.close_window(_prepare(governed), observation, infra_bound=INFRA_BOUND,
                              now=_epoch(10))
    assert phase_b["fixed_deadline"] == fixed
    assert phase_b["deadline"] == min(fixed, phase_b["close"] + governed["stages"]["phase-b"])


def test_keeper_recusa_close_drain_com_janelas_diferentes_das_suas(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observation = _close(INITIAL, monkeypatch)
    with pytest.raises(ia.Refused, match="close-drain windows differ from keeper windows"):
        ia.close_window(_prepare(MINIMUM), observation, infra_bound=INFRA_BOUND, now=_epoch(10))


@pytest.mark.parametrize("governed", [MINIMUM, INITIAL, windows(api=360, final=180)],
                         ids=["minimas", "iniciais", "api-e-final-no-max"])
def test_closed_ready_do_produto_e_aceito_pelo_host_no_ultimo_segundo_do_orcamento(
    governed: dict[str, Any], monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _environment("empty", monkeypatch) as (raw, _, probe):
        close = _close_in(raw, probe, governed)
        phase_b = ia.close_window(_prepare(governed), close, infra_bound=INFRA_BOUND,
                                  now=_epoch(10))
        budget = governed["stages"]["api"] + ia.later_seconds(governed, "api") + ia.FINAL_RESERVE
        admitted = phase_b["fixed_deadline"] - budget
        api = ia.api_window(phase_b, now=admitted)
        stage = ia.adapter_stage({key: api[key] for key in ia.ADAPTER_STAGE_FIELDS.split()}, api)
        monkeypatch.setattr(promote_api, "CLOCK",
                            lambda: NOW + timedelta(seconds=admitted - _epoch(0)))
        context = Context(raw, probe, _ready_inputs(close))
        promote_api.execute_shared_operation(
            "closed-ready", handler_request("closed-ready", context.inputs, stage), context)
    [(_, ready_raw)] = context.observed
    ready = json.loads(ready_raw)
    accepted = ia.closed_ready_api_stage(json.loads(close), ready)
    assert accepted["later_seconds"] == ia.later_seconds(governed, "api")
    assert ia.instant(accepted["deadline"]) == admitted + governed["stages"]["api"]


def _request_with(governed: Any = INITIAL, **changes: Any) -> dict[str, Any]:
    stage = _prepare(governed)
    if governed is None:
        stage = {key: value for key, value in stage.items() if key != "windows"}
    return _request(_inputs(), stage) | changes


@pytest.mark.parametrize(("request_value", "reason"), [
    pytest.param(_request_with(abi_version=2), "^request=fields$", id="abi2"),
    pytest.param(_request_with(None), "^request=stage$", id="sem-janelas"),
    pytest.param(_request_with(windows(api=179)),
                 "^windows=stage stage=api value=179 min=180 max=360$", id="api-abaixo-do-min"),
    pytest.param(_request_with(windows(phase_b=720, api=271)),
                 "^windows=sum sum=1831 ceiling=1830$", id="soma-acima-do-teto"),
    pytest.param(_request_with(INITIAL | {"fixed_deadline_seconds": 2069}),
                 "^windows=fixed value=2069 expected=2070$", id="prazo-fixo-incoerente"),
])
def test_request_fora_do_abi3_ou_dos_limites_e_recusado_antes_de_qualquer_chamada(
    request_value: dict[str, Any], reason: str,
) -> None:
    context = Context(None, None, _inputs())  # type: ignore[arg-type]
    with pytest.raises(ValueError, match=reason):
        promote_api.execute_shared_operation("close-drain", request_value, context)
    assert context.calls == []
    assert context.observed == []


def test_resultado_do_adapter_declara_abi3(monkeypatch: pytest.MonkeyPatch) -> None:
    with _environment("empty", monkeypatch) as (raw, _, probe):
        context = Context(raw, probe, _inputs())
        result = promote_api.execute_shared_operation(
            "close-drain", _request(context.inputs, _prepare(INITIAL)), context)
    assert result["abi_version"] == ia.ADAPTER_ABI_VERSION


@pytest.mark.parametrize(("operation", "stage"), [
    ("inventory", PREPARE), ("closed-ready", API), ("unit-observe", API | {"stage": "unit"}),
])
def test_janelas_fora_dos_limites_sao_recusadas_no_request_de_qualquer_operacao(
    operation: str, stage: dict[str, Any],
) -> None:
    names = promote_api.INPUTS.get(operation) or promote_api.MODE_INPUTS[operation]
    inputs = dict.fromkeys(sorted(names), b"x")
    request = handler_request(operation, inputs, stage | {"windows": windows(api=179)})
    context = Context(None, None, inputs)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match=r"^windows=stage stage=api value=179 min=180 max=360$"):
        promote_api.execute_shared_operation(operation, request, context)
    assert (context.calls, context.observed) == ([], [])
