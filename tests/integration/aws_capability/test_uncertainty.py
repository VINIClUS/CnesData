"""Intenção incerta nunca termina com sucesso por contagem de leituras vazias."""

import json

import pytest

from tests.integration.aws_capability import lifecycle
from tests.integration.aws_capability._oracle import assert_complete
from tests.integration.aws_capability._testing import NAME, Lab, seed_journal
from tests.integration.aws_capability.contracts import Page, Resource


@pytest.mark.parametrize("empty_scans", [3, 5])
def test_criacao_tardia_apos_varios_inventarios_vazios_ainda_e_removida(
    lab: Lab, empty_scans: int
) -> None:
    resource = Resource("cognito", "late", NAME)
    lab.remote.add(resource)
    pages: list[Page | Exception] = [Page()] * empty_scans
    pages.append(Page((resource,)))
    lab.remote.pages["cognito"] = pages
    seed_journal(lab)
    lab.time = 1061
    outcome = lifecycle.janitor(lab.spec, lab.ports())
    assert_complete(outcome, lab.events)
    assert lab.remote.resources == {}
    assert ("delete", "late", True) in lab.events


def test_incerteza_persistente_nao_e_encerrada_por_inventarios_vazios(lab: Lab) -> None:
    seed_journal(lab)
    lab.time = 1061
    outcome = lifecycle.janitor(lab.spec, lab.ports())
    assert outcome.status == "incomplete"
    assert outcome.reason == "creation_uncertain"
    assert outcome.remaining is None
    assert lab.journal().exists()
    data = json.loads(lab.journal().read_text())
    assert data["pending_service"] == "cognito"
    assert data["operation_state"] == "unknown"
    assert lab.remote.calls <= 80


def test_rejeicao_conclusiva_da_criacao_permite_encerrar_run_vazio(lab: Lab) -> None:
    seed_journal(lab)
    data = json.loads(lab.journal().read_text())
    data.update(operation_state="rejected", pending_service=None)
    lab.journal().write_text(json.dumps(data))
    outcome = lifecycle.janitor(lab.spec, lab.ports())
    assert_complete(outcome, lab.events)
