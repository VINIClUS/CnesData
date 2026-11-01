"""DeleteTable aceito espera a ausência: a janela DELETING não gera inventário incompleto."""

from pathlib import Path

import pytest

from tests.integration.aws_capability import lifecycle
from tests.integration.aws_capability._aws_fake_ddb_testing import FakeTable
from tests.integration.aws_capability._b1_testing import NAME, cleanup_context
from tests.integration.aws_capability._journal import Journal, prepare_directory
from tests.integration.aws_capability._probe_testing import ProbeLab, janitor_ports, make_probe_lab
from tests.integration.aws_capability._safety import tags
from tests.integration.aws_capability.contracts import Resource, ServiceFailure

TABLE = Resource("dynamodb", NAME, NAME)


@pytest.fixture
def lab(tmp_path: Path) -> ProbeLab:
    lab = make_probe_lab(tmp_path / "private", "ddb-transactions", seeded=False)
    prepare_directory(lab.spec.journal_dir)
    lab.aws.tables[NAME] = FakeTable(tags(lab.spec))
    journal = Journal(lab.spec)
    journal.created = [TABLE]
    journal.phase = "cleanup_requested"
    journal.save()
    return lab


def test_janitor_conclui_quando_a_tabela_fica_deleting_antes_de_sumir(lab: ProbeLab) -> None:
    lab.aws.tables[NAME].deleting_describes = 3
    outcome = lifecycle.janitor(lab.spec, janitor_ports(lab))
    assert (outcome.status, outcome.reason) == ("succeeded", "complete")
    assert lab.aws.tables == {}
    assert set(lab.pauses) == {2.0}


def test_recibo_e_gravado_antes_da_espera(lab: ProbeLab) -> None:
    lab.aws.tables[NAME].deleting_describes = 3
    seen: list[bool] = []
    original = lab.aws._dynamodb_describe_table

    def describe(payload: dict, who: str):
        journal = Journal(lab.spec)
        journal.load()
        if lab.aws.tables.get(NAME) and lab.aws.tables[NAME].status == "DELETING":
            seen.append(TABLE in journal.dynamodb_delete_accepted)
        return original(payload, who)

    lab.aws._dynamodb_describe_table = describe
    lifecycle.janitor(lab.spec, janitor_ports(lab))
    assert seen
    assert all(seen)


def test_espera_limitada_nao_levanta_quando_a_tabela_segue_deleting(lab: ProbeLab) -> None:
    lab.aws.tables[NAME].status = "DELETING"
    lab.aws.tables[NAME].deleting_describes = 10**6
    lab.budget.limits = lab.budget.limits.__class__(192, 8, 2, 1000, 96)
    lab.services.settle_delete(TABLE, cleanup_context(lab))
    describes = lab.aws.operations().count("describe-table")
    assert describes == 30
    assert len(lab.pauses) == describes - 1


def test_espera_respeita_o_tempo_restante_da_cleanup(lab: ProbeLab) -> None:
    lab.aws.tables[NAME].status = "DELETING"
    lab.aws.tables[NAME].deleting_describes = 10**6
    lab.budget.limits = lab.budget.limits.__class__(192, 8, 2, 8, 96)
    lab.services.settle_delete(TABLE, cleanup_context(lab))
    assert lab.clock[0] - 1000 <= 8 + 2
    assert lab.aws.operations().count("describe-table") < 8


def test_status_inesperado_durante_a_espera_e_falha(lab: ProbeLab) -> None:
    lab.aws.tables[NAME].status = "UPDATING"
    with pytest.raises(ServiceFailure) as error:
        lab.services.settle_delete(TABLE, cleanup_context(lab))
    assert error.value.args == ("service=ddb_delete_state",)


def test_erro_diferente_de_ausencia_durante_a_espera_e_falha(lab: ProbeLab) -> None:
    lab.aws.tables[NAME].status = "DELETING"
    lab.aws._dynamodb_describe_table = lambda payload, who: __import__(
        "tests.integration.aws_capability._aws_fake_types_testing", fromlist=["denied"]
    ).denied("AccessDeniedException")
    with pytest.raises(ServiceFailure) as error:
        lab.services.settle_delete(TABLE, cleanup_context(lab))
    assert error.value.args == ("service=describe_failed",)


def test_servicos_sem_espera_nao_fazem_io(lab: ProbeLab) -> None:
    lab.services.settle_delete(Resource("s3", NAME, NAME), cleanup_context(lab))
    assert lab.aws.calls == []
