"""Recuperação limitada após falha parcial, interrupção e resposta perdida."""

import json
from dataclasses import replace

import pytest

from tests.integration.aws_capability import lifecycle
from tests.integration.aws_capability._oracle import assert_complete, assert_no_mutation
from tests.integration.aws_capability._testing import (
    NAME,
    SECRETS,
    SERVICES,
    Lab,
    make_lab,
    ref_json,
    seed_journal,
)
from tests.integration.aws_capability.contracts import Page, Resource, ServiceFailure


@pytest.mark.parametrize("service", SERVICES)
def test_falha_em_cada_criacao_limpa_recursos_confirmados_e_nao_confirmados(
    lab: Lab, service: str
) -> None:
    lab.remote.fail_service = service
    lab.remote.create_error = ServiceFailure("create=lost_response " + " ".join(SECRETS))
    lab.remote.remote_created_on_error = True
    outcome = lifecycle.run(lab.spec, lab.ports())
    assert outcome.status == "failed"
    assert outcome.reason == "operation_failed"
    assert outcome.remaining == ()
    assert outcome.evidence.get("inventory_complete") is True
    assert lab.remote.resources == {}
    assert any(event[:2] == ("delete", f"synthetic-{service}") for event in lab.events)


@pytest.mark.parametrize("error", [TimeoutError("timeout=worker"), KeyboardInterrupt()])
def test_interrupcao_do_worker_ainda_limpa_e_retorna_falha(lab: Lab, error: BaseException) -> None:
    lab.remote.probe_error = error
    outcome = lifecycle.run(lab.spec, lab.ports())
    assert outcome.status == "failed"
    assert outcome.reason in {"timeout", "interrupted"}
    assert outcome.remaining == ()
    assert outcome.evidence.get("inventory_complete") is True
    assert lab.remote.resources == {}


def test_deadline_interrompe_novas_criacoes_mas_reserva_cleanup(lab: Lab) -> None:
    lab.remote.after_create = lambda: setattr(lab, "time", 1061)
    outcome = lifecycle.run(lab.spec, lab.ports())
    assert outcome.status == "failed"
    assert outcome.reason == "timeout"
    assert len([event for event in lab.events if event[0] == "create"]) == 1
    assert lab.remote.resources == {}
    assert outcome.remaining == ()


def test_morte_do_supervisor_durante_run_interrompe_novas_criacoes(lab: Lab) -> None:
    lab.remote.after_create = lambda: setattr(lab.processes, "available", False)
    outcome = lifecycle.run(lab.spec, lab.ports())
    assert outcome.status == "failed"
    assert outcome.reason == "supervisor_unavailable"
    assert len([event for event in lab.events if event[0] == "create"]) == 1
    assert lab.remote.resources == {}


def test_janitor_novo_reconcilia_criacao_remota_sem_id_no_journal(lab: Lab) -> None:
    orphan = Resource("cognito", "remote-response-lost", NAME)
    lab.remote.add(orphan)
    seed_journal(lab)
    recovered = make_lab(lab.spec.journal_dir)
    recovered.remote = lab.remote
    recovered.events = lab.events
    recovered.time = 1061
    outcome = lifecycle.janitor(recovered.spec, recovered.ports())
    assert_complete(outcome, recovered.events)
    assert lab.remote.resources == {}
    assert any(event[:2] == ("delete", "remote-response-lost") for event in lab.events)
    assert not any(event[0] in {"create", "probe"} for event in lab.events)


def test_intencao_pendente_exige_reconciliar_resposta_tardia(lab: Lab) -> None:
    orphan = Resource("cognito", "late-visible", NAME)
    lab.remote.add(orphan)
    lab.remote.pages["cognito"] = [Page(), Page((orphan,))]
    seed_journal(lab)
    lab.time = 1061
    outcome = lifecycle.janitor(lab.spec, lab.ports())
    assert_complete(outcome, lab.events)
    assert lab.remote.resources == {}
    assert any(event[:2] == ("delete", "late-visible") for event in lab.events)
    scans = [event for event in lab.events if event[:2] == ("inventory", "cognito")]
    assert len(scans) >= 3


def test_janitor_repetido_e_idempotente_com_journal_preservado(lab: Lab) -> None:
    resource = Resource("dynamodb", "table", NAME)
    lab.remote.add(resource)
    seed_journal(lab, [resource])
    first = lifecycle.janitor(lab.spec, lab.ports())
    assert_complete(first, lab.events)
    second = lifecycle.janitor(lab.spec, lab.ports())
    assert_complete(second, lab.events)
    assert lab.journal().exists()
    assert len([event for event in lab.events if event[:2] == ("delete", "table")]) == 0


def test_falha_transitoria_de_cleanup_admite_retry_limitado(lab: Lab) -> None:
    resource = Resource("s3", "bucket", NAME)
    lab.remote.add(resource)
    lab.remote.delete_errors["bucket"] = 1
    seed_journal(lab, [resource])
    outcome = lifecycle.janitor(lab.spec, lab.ports())
    assert_complete(outcome, lab.events)
    assert len([event for event in lab.events if event[:2] == ("delete", "bucket")]) == 2


def test_cleanup_negado_preserva_journal_e_residuo_sem_inventar_vazio(lab: Lab) -> None:
    resource = Resource("s3", "bucket", NAME)
    lab.remote.add(resource)
    lab.remote.delete_errors["bucket"] = 1000
    seed_journal(lab, [resource])
    outcome = lifecycle.janitor(lab.spec, lab.ports())
    assert outcome.status == "incomplete"
    assert outcome.reason == "cleanup_failed"
    assert outcome.remaining == (resource,)
    assert outcome.evidence.get("inventory_complete") is True
    assert lab.journal().exists()
    assert len([event for event in lab.events if event[:2] == ("delete", "bucket")]) <= 2
    assert lab.remote.calls <= 80


def test_nova_execucao_do_janitor_retoma_cleanup_anterior_falho(lab: Lab) -> None:
    resource = Resource("iam", "role", NAME)
    lab.remote.add(resource)
    lab.remote.delete_errors["role"] = 1000
    seed_journal(lab, [resource])
    failed = lifecycle.janitor(lab.spec, lab.ports())
    assert failed.status == "incomplete"
    lab.remote.delete_errors.clear()
    resumed = lifecycle.janitor(lab.spec, lab.ports())
    assert_complete(resumed, lab.events)
    assert lab.remote.resources == {}


def test_delete_bem_sucedido_nao_substitui_inventario_vazio(lab: Lab) -> None:
    resource = Resource("s3", "still-present", NAME)
    lab.remote.add(resource)
    lab.remote.delete_noop.add("still-present")
    seed_journal(lab, [resource])
    outcome = lifecycle.janitor(lab.spec, lab.ports())
    assert outcome.status == "incomplete"
    assert outcome.remaining == (resource,)
    assert outcome.evidence.get("inventory_complete") is True
    assert lab.journal().exists()


def test_tempo_de_cleanup_esgotado_preserva_evidencia_e_limita_retry(lab: Lab) -> None:
    resource = Resource("iam", "role", NAME)
    lab.remote.add(resource)
    lab.remote.delete_errors["role"] = 1000
    lab.remote.at_delete = lambda: setattr(lab, "time", 1031)
    seed_journal(lab, [resource])
    outcome = lifecycle.janitor(lab.spec, lab.ports())
    assert outcome.status == "incomplete"
    assert outcome.reason == "cleanup_timeout"
    assert len([event for event in lab.events if event[:2] == ("delete", "role")]) == 1
    assert lab.journal().exists()


@pytest.mark.parametrize("field", ["account", "region", "run_id", "schema_version"])
def test_janitor_rejeita_journal_que_nao_corresponde_ao_run(lab: Lab, field: str) -> None:
    resource = Resource("iam", "role", NAME)
    lab.remote.add(resource)
    seed_journal(lab, [resource])
    data = json.loads(lab.journal().read_text())
    data[field] = "foreign"
    lab.journal().write_text(json.dumps(data))
    outcome = lifecycle.janitor(lab.spec, lab.ports())
    assert outcome.status == "refused"
    assert outcome.reason == "journal_mismatch"
    assert_no_mutation(lab.events)


def test_janitor_sem_journal_nao_adota_recursos_por_prefixo(lab: Lab) -> None:
    lab.remote.add(Resource("s3", "unproven", NAME))
    outcome = lifecycle.janitor(lab.spec, lab.ports())
    assert outcome.reason == "journal_missing"
    assert_no_mutation(lab.events)


def test_janitor_confirma_identidade_novamente_antes_de_apagar(lab: Lab) -> None:
    resource = Resource("iam", "role", NAME)
    lab.remote.add(resource)
    seed_journal(lab, [resource])
    lab.remote.identity_account = "222222222222"
    outcome = lifecycle.janitor(lab.spec, lab.ports())
    assert outcome.reason == "identity_mismatch"
    assert ("identity",) in lab.events
    assert_no_mutation(lab.events)


def test_janitor_sem_bypass_explicito_nao_apaga_governance(lab: Lab) -> None:
    resource = Resource("s3", "bucket", NAME)
    lab.remote.add(resource)
    seed_journal(lab, [resource])
    outcome = lifecycle.janitor(replace(lab.spec, allow_governance_bypass=False), lab.ports())
    assert outcome.reason == "cleanup_bypass_required"
    assert_no_mutation(lab.events)


def test_baseline_no_journal_nao_pode_ser_apagada(lab: Lab) -> None:
    resource = Resource("iam", "preexisting", NAME)
    lab.remote.add(resource)
    seed_journal(lab)
    data = json.loads(lab.journal().read_text())
    data["baseline"] = [ref_json(resource)]
    lab.journal().write_text(json.dumps(data))
    outcome = lifecycle.janitor(lab.spec, lab.ports())
    assert outcome.status == "incomplete"
    assert "preexisting" in lab.remote.resources
    assert_no_mutation(lab.events)


def test_journal_resultado_e_logs_nao_vazam_segredos(
    lab: Lab, caplog: pytest.LogCaptureFixture, capsys: pytest.CaptureFixture[str]
) -> None:
    lab.remote.probe_error = ServiceFailure(" ".join(SECRETS))
    outcome = lifecycle.run(lab.spec, lab.ports())
    assert outcome.status == "failed"
    assert lab.journal().exists()
    captured = capsys.readouterr()
    artifact = lab.journal().read_text() + repr(outcome) + caplog.text + captured.out + captured.err
    for secret in SECRETS:
        assert secret not in artifact
    assert "111111111111" not in repr(outcome.evidence)
    assert "synthetic-cognito" not in repr(outcome.evidence)
    data = json.loads(lab.journal().read_text())
    assert set(data) <= {
        "schema_version",
        "account",
        "region",
        "run_id",
        "deadline",
        "phase",
        "baseline",
        "pending_service",
        "operation_state",
        "created",
        "remaining",
        "reason",
    }


def test_cli_janitor_recupera_journal_sem_recriar_recursos(lab: Lab) -> None:
    resource = Resource("cognito", "pool", NAME)
    lab.remote.add(resource)
    seed_journal(lab, [resource])
    args = [
        "janitor",
        "--account",
        "111111111111",
        "--region",
        "us-east-2",
        "--run-id",
        "a1b2c3d4",
        "--deadline",
        "1060",
        "--journal-dir",
        str(lab.spec.journal_dir),
        "--retention",
        "GOVERNANCE",
        "--allow-governance-bypass",
    ]
    outcome = lifecycle.main(args, lab.ports())
    assert_complete(outcome, lab.events)
    assert not any(event[0] in {"create", "probe"} for event in lab.events)
