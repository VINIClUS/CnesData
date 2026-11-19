"""Observações do probe e contagens de inventário carregadas até o Outcome."""

import pytest

from tests.integration.aws_capability import lifecycle
from tests.integration.aws_capability._testing import NAME, Lab, seed_journal
from tests.integration.aws_capability.contracts import Resource

ZEROS = {"cognito": 0, "dynamodb": 0, "s3": 0, "iam": 0}


def test_run_mescla_observacoes_do_probe_no_evidence(lab: Lab) -> None:
    lab.remote.probe_result = {"tx100_aceita": True, "tx101_codigo": "ValidationException"}
    outcome = lifecycle.run(lab.spec, lab.ports())
    assert outcome.status == "succeeded"
    assert outcome.evidence["probe"] == {
        "tx100_aceita": True,
        "tx101_codigo": "ValidationException",
    }


def test_probe_com_falha_nao_carrega_observacoes_e_nao_e_sucesso(lab: Lab) -> None:
    lab.remote.probe_result = {"parcial": True}
    lab.remote.probe_error = RuntimeError("boundary=probe_down")
    outcome = lifecycle.run(lab.spec, lab.ports())
    assert outcome.status == "failed"
    assert outcome.reason == "operation_failed"
    assert outcome.evidence["probe"] == {}


def test_run_registra_contagens_iniciais_e_finais_apenas_do_prefixo_cap(lab: Lab) -> None:
    lab.remote.add(Resource("s3", "other-run-bucket", "cnesdata-cap-eeeeffff"))
    lab.remote.add(Resource("iam", "foreign-role", "outro-projeto"))
    outcome = lifecycle.run(lab.spec, lab.ports())
    assert outcome.status == "succeeded"
    assert outcome.evidence["initial_counts"] == {**ZEROS, "s3": 1}
    assert outcome.evidence["final_counts_all_cap"] == {**ZEROS, "s3": 1}
    assert outcome.evidence["counts"] == ZEROS
    assert "foreign-role" in lab.remote.resources


def test_contagem_final_global_e_bruta_sem_tratamento_especial_de_iam(lab: Lab) -> None:
    lab.remote.add(Resource("iam", "operator-role", "cnesdata-cap-operator"))
    outcome = lifecycle.run(lab.spec, lab.ports())
    assert outcome.evidence["initial_counts"] == {**ZEROS, "iam": 1}
    assert outcome.evidence["final_counts_all_cap"] == {**ZEROS, "iam": 1}


def test_janitor_registra_contagem_final_global(lab: Lab) -> None:
    resource = Resource("cognito", "pool", NAME)
    lab.remote.add(resource)
    lab.remote.add(Resource("s3", "other-run-bucket", "cnesdata-cap-eeeeffff"))
    seed_journal(lab, [resource])
    outcome = lifecycle.janitor(lab.spec, lab.ports())
    assert outcome.status == "succeeded"
    assert outcome.evidence["final_counts_all_cap"] == {**ZEROS, "s3": 1}


@pytest.mark.parametrize("probe_result", [{}, {"x": 1}])
def test_evidence_sempre_carrega_chave_probe_em_run_concluido(
    lab: Lab, probe_result: dict[str, object]
) -> None:
    lab.remote.probe_result = probe_result
    outcome = lifecycle.run(lab.spec, lab.ports())
    assert outcome.evidence["probe"] == probe_result
