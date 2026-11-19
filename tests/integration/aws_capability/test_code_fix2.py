"""Referências duráveis usam serviço e ID mesmo com nome observado diferente."""

import json
from dataclasses import replace

import pytest

from tests.integration.aws_capability import lifecycle
from tests.integration.aws_capability._oracle import assert_complete, assert_no_mutation
from tests.integration.aws_capability._testing import NAME, SERVICES, Lab, seed_journal
from tests.integration.aws_capability.contracts import Resource


def confirmed_resource(lab: Lab, service: str) -> Resource:
    resource = Resource(service, "stable-id", NAME)
    lab.remote.add(resource)
    seed_journal(lab, [resource])
    return resource


@pytest.mark.parametrize("service", SERVICES)
@pytest.mark.parametrize("observed_name", [NAME, "changed-observed-name"])
def test_residuo_preserva_identidade_duravel_em_duas_recuperacoes(
    lab: Lab, service: str, observed_name: str
) -> None:
    resource = confirmed_resource(lab, service)
    observed = replace(resource, name=observed_name)
    lab.remote.delete_noop.add(resource.resource_id)
    lab.remote.at_delete = lambda: lab.remote.add(observed)
    first = lifecycle.janitor(lab.spec, lab.ports())
    first_journal = json.loads(lab.journal().read_text())
    assert ("delete", resource.resource_id, True) in lab.events
    assert lab.remote.calls <= 80
    second = lifecycle.janitor(lab.spec, lab.ports())
    second_journal = json.loads(lab.journal().read_text())
    assert first.status == "incomplete"
    assert second.status == "incomplete"
    assert first.remaining == second.remaining == (observed,)
    assert lab.remote.resources == {resource.resource_id: observed}
    for data in (first_journal, second_journal):
        references = {(row["service"], row["resource_id"]) for row in data["created"]}
        assert (service, resource.resource_id) in references
        assert data["phase"] != "complete"
    if observed_name != NAME:
        assert_no_mutation(lab.events)
    assert lab.remote.calls <= 80


@pytest.mark.parametrize("service", SERVICES)
def test_exclusao_real_aposenta_id_e_mantem_segundo_janitor_idempotente(
    lab: Lab, service: str
) -> None:
    resource = confirmed_resource(lab, service)
    first = lifecycle.janitor(lab.spec, lab.ports())
    assert_complete(first, lab.events)
    assert ("delete", resource.resource_id, True) in lab.events
    assert lab.remote.resources == {}
    assert json.loads(lab.journal().read_text())["created"] == []
    second = lifecycle.janitor(lab.spec, lab.ports())
    assert_complete(second, lab.events)
    assert_no_mutation(lab.events)
    assert lab.remote.resources == {}
    data = json.loads(lab.journal().read_text())
    assert data["created"] == []
    assert data["phase"] == "complete"
