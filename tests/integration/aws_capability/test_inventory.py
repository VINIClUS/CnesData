"""Inventário completo e autorização de remoção em fronteiras de serviço."""

from dataclasses import replace

import pytest

from tests.integration.aws_capability import lifecycle
from tests.integration.aws_capability._oracle import assert_complete, assert_no_mutation
from tests.integration.aws_capability._testing import (
    NAME,
    OTHER_ACCOUNT,
    SERVICES,
    Lab,
    owned_tags,
    seed_journal,
)
from tests.integration.aws_capability.contracts import Ownership, Page, Resource, ServiceFailure


@pytest.mark.parametrize("service", SERVICES)
def test_pagina_posterior_com_colisao_impede_toda_criacao(lab: Lab, service: str) -> None:
    unrelated = Resource(service, "preexisting-other", "cnesdata-cap-eeeeffff")
    collision = Resource(service, "preexisting-collision", NAME)
    lab.remote.add(unrelated, Ownership(
        "111111111111", {"Project": "other"}, "aws-global" if service == "iam" else "us-east-2"
    ))
    lab.remote.add(collision)
    outcome = lifecycle.run(lab.spec, lab.ports())
    assert outcome.reason == "run_collision"
    assert ("inventory", service, "1") in lab.events
    assert set(lab.remote.resources) == {"preexisting-other", "preexisting-collision"}
    assert_no_mutation(lab.events)


@pytest.mark.parametrize("service", SERVICES)
@pytest.mark.parametrize("failure", ["broken", "repeated", "unavailable"])
def test_paginacao_incompleta_nunca_e_inventario_vazio(
    lab: Lab, service: str, failure: str
) -> None:
    pages: list[Page | Exception] = [Page(cursor="next")]
    pages.append(
        {
            "broken": Page(cursor=""),
            "repeated": Page(cursor="next"),
            "unavailable": ServiceFailure("list=denied"),
        }[failure]
    )
    lab.remote.pages[service] = pages
    outcome = lifecycle.run(lab.spec, lab.ports())
    assert outcome.status == "refused"
    assert outcome.reason == "inventory_incomplete"
    assert outcome.remaining is None
    assert outcome.evidence.get("inventory_complete") is False
    assert_no_mutation(lab.events)


def test_limite_de_paginas_aborta_cursores_que_nunca_terminam(lab: Lab) -> None:
    lab.remote.pages["iam"] = [Page(cursor=f"page-{i}") for i in range(20)]
    ports = lab.ports()
    limits = replace(ports.policy.limits, pages=3)
    ports = replace(ports, policy=replace(ports.policy, limits=limits))
    outcome = lifecycle.run(lab.spec, ports)
    assert outcome.reason == "inventory_incomplete"
    assert len([event for event in lab.events if event[:2] == ("inventory", "iam")]) <= 3
    assert_no_mutation(lab.events)


def test_ownership_inacessivel_aborta_preflight(lab: Lab) -> None:
    item = Resource("s3", "unknown-owner", "cnesdata-cap-eeeeffff")
    lab.remote.add(item)
    lab.remote.inspect_errors.add(item.resource_id)
    outcome = lifecycle.run(lab.spec, lab.ports())
    assert outcome.reason == "inventory_incomplete"
    assert_no_mutation(lab.events)


@pytest.mark.parametrize(
    "mismatch", ["name", "project", "purpose", "run", "expiry", "account", "missing_tags"]
)
def test_janitor_preserva_recurso_com_qualquer_prova_de_ownership_divergente(
    lab: Lab, mismatch: str
) -> None:
    item = Resource("s3", "foreign", NAME)
    tags = owned_tags()
    account = "111111111111"
    if mismatch == "name":
        item = replace(item, name="production-bucket")
    elif mismatch == "account":
        account = OTHER_ACCOUNT
    elif mismatch == "missing_tags":
        tags = {}
    else:
        key = {"project": "Project", "purpose": "Purpose", "run": "Run", "expiry": "ExpiresAt"}
        tags[key[mismatch]] = "foreign"
    lab.remote.add(item, Ownership(account, tags, "us-east-2"))
    seed_journal(lab, [item])
    outcome = lifecycle.janitor(lab.spec, lab.ports())
    assert outcome.status == "incomplete"
    assert outcome.reason == "ownership_mismatch"
    assert "foreign" in lab.remote.resources
    assert not any(event[0] == "delete" for event in lab.events)


def test_recursos_preexistentes_de_outros_runs_sao_preservados(lab: Lab) -> None:
    item = Resource("s3", "other-run", "cnesdata-cap-eeeeffff")
    tags = {**owned_tags(), "Run": "eeeeffff"}
    lab.remote.add(item, Ownership("111111111111", tags, "us-east-2"))
    outcome = lifecycle.run(lab.spec, lab.ports())
    assert_complete(outcome, lab.events)
    assert set(lab.remote.resources) == {"other-run"}
    assert not any(event[:2] == ("delete", "other-run") for event in lab.events)


@pytest.mark.parametrize("service", SERVICES)
def test_falha_no_inventario_final_preserva_evidencia_incompleta(lab: Lab, service: str) -> None:
    lab.remote.pages[service] = [Page(), ServiceFailure("list=unavailable")]
    outcome = lifecycle.run(lab.spec, lab.ports())
    assert outcome.status == "incomplete"
    assert outcome.remaining is None
    assert outcome.evidence.get("inventory_complete") is False
    assert lab.journal().exists()


def test_janitor_apaga_todas_as_versoes_e_dependentes_antes_do_pai(lab: Lab) -> None:
    parent = Resource("s3", "bucket", NAME)
    children = [Resource("s3", value, NAME) for value in ["version-1", "version-2", "marker"]]
    lab.remote.add(parent)
    for child in children:
        lab.remote.add(child)
    lab.remote.children[parent.resource_id] = children
    seed_journal(lab, [parent])
    outcome = lifecycle.janitor(lab.spec, lab.ports())
    assert_complete(outcome, lab.events)
    deleted = [event[1] for event in lab.events if event[0] == "delete"]
    assert set(deleted) == {"version-1", "version-2", "marker", "bucket"}
    assert deleted[-1] == "bucket"
    assert ("dependents", "bucket", "2") in lab.events
    assert all(event[2] is True for event in lab.events if event[0] == "delete")


def test_dependentes_incompletos_impedem_remocao_do_pai(lab: Lab) -> None:
    parent = Resource("iam", "role", NAME)
    lab.remote.add(parent)
    lab.remote.child_pages["role"] = [Page(cursor="more"), ServiceFailure("list=denied")]
    seed_journal(lab, [parent])
    outcome = lifecycle.janitor(lab.spec, lab.ports())
    assert outcome.status == "incomplete"
    assert "role" in lab.remote.resources
    assert not any(event[:2] == ("delete", "role") for event in lab.events)


def test_dependente_estrangeiro_bloqueia_exclusao_do_pai(lab: Lab) -> None:
    parent = Resource("iam", "role", NAME)
    child = Resource("iam", "foreign-policy", NAME)
    lab.remote.add(parent)
    lab.remote.add(child, Ownership(OTHER_ACCOUNT, owned_tags(), "aws-global"))
    lab.remote.children["role"] = [child]
    seed_journal(lab, [parent])
    outcome = lifecycle.janitor(lab.spec, lab.ports())
    assert outcome.status == "incomplete"
    assert set(lab.remote.resources) == {"role", "foreign-policy"}
    assert not any(event[0] == "delete" for event in lab.events)


@pytest.mark.parametrize("service", SERVICES)
@pytest.mark.parametrize("failure", ["error", "repeated", "residual"])
def test_segunda_pagina_final_incompleta_ou_com_residuo_impede_sucesso(
    lab: Lab, service: str, failure: str
) -> None:
    initial = Resource(service, "initial", NAME)
    hidden = Resource(service, "hidden-final-page", NAME)
    lab.remote.add(initial)
    seed_journal(lab, [initial])

    def arm_final_pages() -> None:
        if failure == "residual":
            lab.remote.add(hidden)
            lab.remote.delete_noop.add(hidden.resource_id)
        tail = {
            "error": ServiceFailure("final_page=unavailable"),
            "repeated": Page(cursor="next"),
            "residual": Page((hidden,)),
        }[failure]
        lab.remote.pages[service] = [Page(cursor="next"), tail]
        lab.remote.at_delete = lambda: None

    lab.remote.at_delete = arm_final_pages
    outcome = lifecycle.janitor(lab.spec, lab.ports())
    assert outcome.status == "incomplete"
    assert ("inventory", service, "next") in lab.events
    assert lab.journal().exists()
    if failure == "residual":
        assert hidden.resource_id in lab.remote.resources
        assert outcome.remaining
        assert hidden in outcome.remaining
    else:
        assert outcome.remaining is None


def test_segundo_janitor_ve_novo_recurso_e_nao_reutiliza_inventario_anterior(lab: Lab) -> None:
    initial = Resource("cognito", "initial", NAME)
    lab.remote.add(initial)
    seed_journal(lab, [initial])
    first = lifecycle.janitor(lab.spec, lab.ports())
    assert_complete(first, lab.events)
    late = Resource("cognito", "late-after-first-janitor", NAME)
    lab.remote.add(late)
    second = lifecycle.janitor(lab.spec, lab.ports())
    assert_complete(second, lab.events)
    assert lab.remote.resources == {}
    assert ("delete", "late-after-first-janitor", True) in lab.events
