"""Travessia real conserva ordem de dependencias e limites sem executar delete."""

import json

import pytest

from tests.integration.aws_capability._cleanup import Cleanup
from tests.integration.aws_capability._inventory import dependents
from tests.integration.aws_capability._journal import Journal
from tests.integration.aws_capability._safety import SafetyFailure
from tests.integration.aws_capability._service_read_assertions import (
    FIXED_FLAGS,
    assert_environment,
    assert_process,
)
from tests.integration.aws_capability._service_read_testing import ReadLab
from tests.integration.aws_capability._testing import ProcessBoundary
from tests.integration.aws_capability.contracts import (
    BudgetExhausted,
    Limits,
    Policy,
    Ports,
    Resource,
    ServiceFailure,
)
from tests.integration.aws_capability.test_service_cognito_children_cursors import (
    enqueue_page,
    page_trace,
)
from tests.integration.aws_capability.test_service_cognito_children_lookup import (
    DATA,
    assert_failure,
    enqueue,
    read_lab,
    remote_error,
    resource,
    trace,
)

__all__ = ["read_lab"]


def cleanup(lab: ReadLab) -> Cleanup:
    ports = Ports(
        lab.services,
        ProcessBoundary([]),
        lambda: 1000,
        Policy("111111111111", frozenset({"us-east-2"})),
        lab.context.budget,
    )
    return Cleanup(Journal(lab.services.spec), ports)


def test_removal_order_real_coloca_branding_antes_de_client_e_pool_em_29_calls(
    read_lab: ReadLab,
) -> None:
    enqueue(read_lab, *DATA["expected"]["graph_replies"])
    janitor = cleanup(read_lab)
    order = janitor.removal_order(resource("pool"))
    assert set(order) == {resource(name) for name in ("pool", "client", "branding")}
    assert order.index(resource("branding")) < order.index(resource("client"))
    assert order.index(resource("client")) < order.index(resource("pool"))
    assert read_lab.context.budget.used == DATA["expected"]["graph_calls"] == 29
    assert read_lab.context.budget.work_used == DATA["expected"]["graph_work_calls"] == 0
    assert read_lab.context.budget.limits == Limits()
    assert janitor.journal.created == janitor.journal.baseline == []
    assert janitor.attempted == janitor.absent == set()
    assert list(read_lab.directory.iterdir()) == []
    assert_process(read_lab, trace(*DATA["expected"]["graph_trace"]))


def test_dependents_real_rejeita_duplicata_entre_paginas_sem_delete(read_lab: ReadLab) -> None:
    enqueue_page(read_lab, "root")
    enqueue_page(read_lab, "clients_first")
    enqueue(
        read_lab,
        "root",
        "tags",
        "clients",
        "root",
        "tags",
        "servers_empty",
        "root",
        "tags",
        "users_empty",
    )
    janitor = cleanup(read_lab)
    with pytest.raises(SafetyFailure) as error:
        dependents(resource("pool"), janitor.ports, read_lab.context)
    assert error.value.reason == "inventory_incomplete"
    assert_failure(error.value, read_lab)
    expected = page_trace("domains") + page_trace("clients") + page_trace("clients_next")
    expected += page_trace("servers") + page_trace("users")
    assert_process(read_lab, expected)


def test_erro_na_segunda_colecao_impede_travessia_como_se_fosse_fim(read_lab: ReadLab) -> None:
    enqueue(read_lab, "root", "tags", "root", "root", "tags")
    remote_error(read_lab, "NotAuthorizedException")
    janitor = cleanup(read_lab)
    with pytest.raises(SafetyFailure) as error:
        dependents(resource("pool"), janitor.ports, read_lab.context)
    assert error.value.reason == "inventory_incomplete"
    assert_failure(error.value, read_lab)
    assert_process(read_lab, page_trace("domains") + page_trace("clients"))


def test_branding_com_pai_aninhado_ciclico_recusa_antes_do_grafo(read_lab: ReadLab) -> None:
    valid = resource("branding")
    envelope = json.loads(valid.resource_id)
    envelope["parent_id"] = valid.resource_id
    cyclic = Resource("cognito", json.dumps(envelope), valid.name)
    janitor = cleanup(read_lab)
    with pytest.raises(ServiceFailure) as error:
        janitor.removal_order(cyclic)
    assert error.value.args == ("service=resource_invalid",)
    assert_failure(error.value, read_lab)
    assert_process(read_lab, [])


def test_tag_removida_antes_da_arvore_bloqueia_descoberta_e_delete(read_lab: ReadLab) -> None:
    enqueue(read_lab, "root")
    read_lab.process.outcomes.append((0, '{"Tags":{}}', ""))
    with pytest.raises(SafetyFailure) as error:
        cleanup(read_lab).removal_order(resource("pool"))
    assert error.value.reason == "ownership_mismatch"
    assert_failure(error.value, read_lab)
    assert_process(read_lab, trace("root", "tags"))


def test_grafo_nao_ignora_branding_incompleto_apos_client_owned(read_lab: ReadLab) -> None:
    enqueue(read_lab, *DATA["expected"]["graph_replies"][:20])
    read_lab.process.outcomes.append((0, '{"ManagedLoginBranding":{}}', ""))
    with pytest.raises(SafetyFailure) as error:
        cleanup(read_lab).removal_order(resource("pool"))
    assert error.value.reason == "inventory_incomplete"
    assert_failure(error.value, read_lab)
    assert_process(read_lab, trace(*DATA["expected"]["graph_trace"][:21]))


def test_grafo_esgota_budget_compartilhado_sem_pular_ultima_prova(read_lab: ReadLab) -> None:
    for _ in range(52):
        read_lab.context.budget.acquire("cleanup")
    enqueue(read_lab, *DATA["expected"]["graph_replies"][:28])
    with pytest.raises(BudgetExhausted) as error:
        cleanup(read_lab).removal_order(resource("pool"))
    assert error.value.args == ("budget=requests",)
    assert_failure(error.value, read_lab)
    assert read_lab.context.budget.used == 80
    assert read_lab.context.budget.work_used == 0
    assert read_lab.process.observed_debits == list(range(53, 81))
    assert len(read_lab.process.calls) == 28
    assert read_lab.process.outcomes == []
    for (argv, options), (_, operation, region, payload) in zip(
        read_lab.process.calls, trace(*DATA["expected"]["graph_trace"][:28]), strict=True
    ):
        assert argv == [
            "aws",
            "cognito-idp",
            operation,
            "--region",
            region,
            *FIXED_FLAGS,
            "7",
            "--cli-read-timeout",
            "7",
        ]
        assert json.loads(options["input"]) == payload
        assert_environment(options["env"], region)
        assert options["timeout"] == 7
        assert options["shell"] is False
        assert options["capture_output"] is True
        assert options["text"] is True
        assert options["check"] is False
    assert read_lab.context.budget.limits == Limits()
