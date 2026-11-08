"""Orçamento compartilhado por request, incluindo probes e reserva de cleanup."""

import subprocess
from dataclasses import replace

import pytest

from tests.integration.aws_capability import lifecycle
from tests.integration.aws_capability._oracle import assert_fresh_inventory, assert_no_mutation
from tests.integration.aws_capability._testing import Lab
from tests.integration.aws_capability.cli import Cli, Request
from tests.integration.aws_capability.contracts import (
    BudgetExhausted,
    Limits,
    Ports,
    RequestBudget,
    RequestContext,
)
from tests.integration.aws_capability.test_cli import ProcessRecorder, environment


def configured(lab: Lab, limits: Limits) -> Ports:
    ports = lab.ports()
    ports.budget.limits = limits
    return replace(ports, policy=replace(ports.policy, limits=limits))


def test_reserva_inviavel_recusa_antes_de_criar(lab: Lab) -> None:
    ports = configured(lab, Limits(calls=20, cleanup_reserve=40))
    outcome = lifecycle.run(lab.spec, ports)
    assert outcome.status == "refused"
    assert outcome.reason == "cleanup_reserve_unavailable"
    assert_no_mutation(lab.events)


def test_trabalho_esgotado_apos_primeira_criacao_ainda_limpa_com_reserva(lab: Lab) -> None:
    ports = configured(lab, Limits(calls=38, cleanup_reserve=32))
    outcome = lifecycle.run(lab.spec, ports)
    assert outcome.status == "failed"
    assert outcome.reason == "request_budget_exhausted"
    assert len([event for event in lab.events if event[0] == "created"]) == 1
    assert lab.remote.resources == {}
    assert outcome.remaining == ()
    assert_fresh_inventory(lab.events)
    assert lab.remote.request_lanes.count("work") == 6
    assert 0 < lab.remote.request_lanes.count("cleanup") <= 32
    assert lab.remote.calls <= 38


def test_probe_opaco_compartilha_limite_por_request_e_nao_por_metodo(lab: Lab) -> None:
    ports = configured(lab, Limits(calls=54, cleanup_reserve=34))
    lab.remote.probe_requests = 100
    outcome = lifecycle.run(lab.spec, ports)
    assert outcome.status == "failed"
    assert outcome.reason == "request_budget_exhausted"
    assert len([event for event in lab.events if event[0] == "probe"]) == 1
    assert lab.remote.request_lanes.count("work") == 20
    assert lab.remote.calls <= 54
    assert lab.remote.resources == {}
    assert outcome.remaining == ()
    assert_fresh_inventory(lab.events)


def test_adapters_distintos_nao_reiniciam_orcamento_compartilhado(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorder = ProcessRecorder()
    monkeypatch.setattr(subprocess, "run", recorder.run)
    budget = RequestBudget(Limits(calls=3, cleanup_reserve=1), 1060, lambda: 1000)
    context = RequestContext(budget, "work")
    first = Cli("us-east-2", 7, environment(), context)
    second = Cli("us-east-2", 7, environment(), context)
    request = Request("sts", "get-caller-identity", {})
    first.invoke(request)
    second.invoke(request)
    with pytest.raises(BudgetExhausted):
        second.invoke(request)
    cleanup = Cli("us-east-2", 7, environment(), RequestContext(budget, "cleanup"))
    cleanup.invoke(request)
    with pytest.raises(BudgetExhausted):
        cleanup.invoke(request)
    assert len(recorder.calls) == 3


def test_deadline_compartilhado_bloqueia_proxima_request_sem_subprocesso(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorder = ProcessRecorder()
    monkeypatch.setattr(subprocess, "run", recorder.run)
    clock = [1000.0]
    budget = RequestBudget(Limits(), 1001, lambda: clock[0])
    adapter = Cli("us-east-2", 7, environment(), RequestContext(budget, "work"))
    request = Request("sts", "get-caller-identity", {})
    adapter.invoke(request)
    clock[0] = 1002
    with pytest.raises(BudgetExhausted):
        adapter.invoke(request)
    assert len(recorder.calls) == 1


def test_timeout_do_subprocesso_respeita_tempo_restante_do_orcamento(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorder = ProcessRecorder()
    monkeypatch.setattr(subprocess, "run", recorder.run)
    budget = RequestBudget(Limits(), 1000.5, lambda: 1000)
    adapter = Cli("us-east-2", 7, environment(), RequestContext(budget, "work"))
    adapter.invoke(Request("sts", "get-caller-identity", {}))
    assert len(recorder.calls) == 1
    assert 0 < recorder.calls[0][1]["timeout"] <= 0.5
