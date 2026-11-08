"""Create atômico retorna o ID remoto após intenção durável."""

from dataclasses import replace

import pytest

from tests.integration.aws_capability._journal import Journal
from tests.integration.aws_capability._service_cognito_root_testing import (
    ROOT,
    RootLab,
    assert_journal,
    assert_private_channels,
    assert_trace,
    install_fault,
    literal_reply,
    load_replies,
    observe_durability,
    queue_trace,
    root_lab,
)
from tests.integration.aws_capability._service_read_assertions import assert_rejected
from tests.integration.aws_capability._worker import Worker
from tests.integration.aws_capability.contracts import Creation, RequestContext, ServiceFailure

__all__ = ["root_lab"]


def test_cria_root_com_tags_atomicas(root_lab: RootLab) -> None:
    creation = Creation(root_lab.services.spec, ROOT.name, load_replies()["tags_owner"]["Tags"])
    queue_trace(root_lab, [["C", "pool_create"]])
    try:
        resource = root_lab.services.create("cognito", creation, root_lab.context)
    except ServiceFailure as error:
        assert_private_channels(root_lab, error)
        assert_rejected(error)
        raise
    assert resource == ROOT
    assert resource.resource_id != root_lab.services.spec.run_id
    assert_trace(root_lab, [["C", "pool_create"]], ["work"])
    assert list(root_lab.directory.iterdir()) == []


@pytest.mark.parametrize(
    "case", ["spec", "nome", "tags", "lane"], ids=["spec", "nome", "tags", "lane"]
)
def test_rejeita_creation_divergente(root_lab: RootLab, case: str) -> None:
    creation = Creation(root_lab.services.spec, ROOT.name, load_replies()["tags_owner"]["Tags"])
    context = root_lab.context
    if case == "spec":
        creation = replace(creation, spec=replace(creation.spec, account="222222222222"))
    elif case == "nome":
        creation = replace(creation, name="cnesdata-cap-a1b2c3d4-suffix")
    elif case == "tags":
        creation = replace(creation, tags=load_replies()["tags_project"]["Tags"])
    else:
        context = RequestContext(context.budget, "cleanup")
    with pytest.raises(ServiceFailure) as error:
        root_lab.services.create("cognito", creation, context)
    assert_trace(root_lab, [], [])
    assert_private_channels(root_lab, error.value)
    assert_rejected(error.value)
    assert error.value.args != ("probe=not_implemented",)


@pytest.mark.parametrize(
    "reply",
    ["pool_no_id", "pool_bad_name", "pool_bad_arn_account", "pool_bad_arn_region", "pool_bad_id"],
    ids=["id_ausente", "nome_errado", "arn_conta", "arn_regiao", "arn_id"],
)
def test_rejeita_identidade_de_create(root_lab: RootLab, reply: str) -> None:
    creation = Creation(root_lab.services.spec, ROOT.name, load_replies()["tags_owner"]["Tags"])
    steps = [["C", reply]]
    queue_trace(root_lab, steps)
    with pytest.raises(ServiceFailure) as error:
        root_lab.services.create("cognito", creation, root_lab.context)
    assert_trace(root_lab, steps, ["work"])
    assert_private_channels(root_lab, error.value)
    assert_rejected(error.value)
    assert error.value.args != ("probe=not_implemented",)
    assert list(root_lab.directory.iterdir()) == []


def test_intencao_duravel_precede_create(root_lab: RootLab) -> None:
    journal = Journal(root_lab.services.spec)
    journal.save()
    assert_journal(root_lab, "j_prepared")
    observed: list[bytes] = []

    def at_create(argv: object, options: object) -> None:
        observe_durability(root_lab, "j_unknown")
        observed.append(journal.path.read_bytes())

    root_lab.at_request = at_create
    queue_trace(root_lab, [["C", "pool_create"]])
    try:
        Worker(journal, root_lab.ports, 9876).create("cognito")
    except ServiceFailure as error:
        assert_private_channels(root_lab, error)
        assert_rejected(error)
        raise
    assert len(observed) == 1
    assert_journal(root_lab, "j_confirmed")
    assert_trace(root_lab, [["C", "pool_create"]], ["work"])
    observe_durability(root_lab, "j_confirmed")


@pytest.mark.parametrize(
    "fault",
    ["arquivo_fsync", "replace", "diretorio_fsync"],
    ids=["arquivo_fsync", "replace", "diretorio_fsync"],
)
def test_save_de_intencao_falhou(root_lab: RootLab, fault: str) -> None:
    journal = Journal(root_lab.services.spec)
    journal.save()
    before = assert_journal(root_lab, "j_prepared")
    install_fault(root_lab, fault)
    with pytest.raises(OSError, match="native=journal_fault"):
        Worker(journal, root_lab.ports, 9876).create("cognito")
    assert_trace(root_lab, [], [])
    if fault == "diretorio_fsync":
        assert_journal(root_lab, "j_unknown")
    else:
        assert_journal(root_lab, "j_prepared")
        assert journal.path.read_bytes() == before
    assert not list(root_lab.directory.glob(".journal-*"))


def test_create_perdido_nao_repete_request(root_lab: RootLab) -> None:
    journal = Journal(root_lab.services.spec)
    journal.save()
    queue_trace(root_lab, [["C", "error_timeout"], ["C", "pool_create"]])
    with pytest.raises(ServiceFailure) as error:
        Worker(journal, root_lab.ports, 9876).create("cognito")
    assert_journal(root_lab, "j_unknown")
    assert root_lab.process.outcomes == [literal_reply("pool_create")]
    root_lab.process.outcomes.clear()
    assert_trace(root_lab, [["C", "error_timeout"]], ["work"])
    assert_private_channels(root_lab, error.value)
    assert_rejected(error.value)
