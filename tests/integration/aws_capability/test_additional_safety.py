"""Regressões adicionais da revisão de segurança da implementação."""

import json
import os
import subprocess
from dataclasses import replace
from pathlib import Path

import pytest

from tests.integration.aws_capability import lifecycle
from tests.integration.aws_capability._testing import NAME, Lab, ref_json, seed_journal
from tests.integration.aws_capability.contracts import Resource
from tests.integration.aws_capability.test_cli import ProcessRecorder, transport


def test_delete_de_dependente_sem_efeito_preserva_pai_e_residuo(lab: Lab) -> None:
    parent = Resource("s3", "bucket", NAME)
    child = Resource("s3", "version", NAME)
    lab.remote.add(parent)
    lab.remote.add(child)
    lab.remote.children[parent.resource_id] = [child]
    lab.remote.delete_noop.add(child.resource_id)
    seed_journal(lab, [parent])
    outcome = lifecycle.janitor(lab.spec, lab.ports())
    assert outcome.status == "incomplete"
    assert set(lab.remote.resources) == {"bucket", "version"}
    assert not any(event[:2] == ("delete", "bucket") for event in lab.events)


def test_falha_ao_persistir_resposta_criada_retorna_incompleto(
    lab: Lab, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = os.replace

    def fail_confirmation(source: str | Path, target: str | Path) -> None:
        data = json.loads(Path(source).read_text())
        if data["created"]:
            raise OSError("journal=confirmation_failed")
        original(source, target)

    monkeypatch.setattr(os, "replace", fail_confirmation)
    outcome = lifecycle.run(lab.spec, lab.ports())
    assert outcome.status == "incomplete"
    assert outcome.reason == "journal_unavailable"
    assert outcome.remaining is None
    data = json.loads(lab.journal().read_text())
    assert data["operation_state"] == "unknown"
    assert data["pending_service"] == "cognito"
    assert len(lab.remote.resources) == 1


def test_baseline_e_identificada_por_servico_e_id_mesmo_com_nome_alterado(lab: Lab) -> None:
    resource = Resource("iam", "preexisting", NAME)
    lab.remote.add(resource)
    seed_journal(lab, [resource])
    data = json.loads(lab.journal().read_text())
    data["baseline"] = [ref_json(replace(resource, name="old-name"))]
    lab.journal().write_text(json.dumps(data))
    outcome = lifecycle.janitor(lab.spec, lab.ports())
    assert outcome.status == "incomplete"
    assert "preexisting" in lab.remote.resources
    assert not any(event[0] == "delete" for event in lab.events)


def test_monitor_nao_aceita_conclusao_com_criacao_pendente(lab: Lab) -> None:
    seed_journal(lab)
    data = json.loads(lab.journal().read_text())
    data["phase"] = "complete"
    lab.journal().write_text(json.dumps(data))
    outcome = lifecycle.monitor(lab.spec, lab.ports(), os.getpid())
    assert outcome.status == "refused"
    assert outcome.reason == "journal_corrupt"


def test_erro_inesperado_de_probe_ainda_dispara_cleanup(lab: Lab) -> None:
    lab.remote.probe_error = RuntimeError("adapter=unexpected")
    outcome = lifecycle.run(lab.spec, lab.ports())
    assert outcome.status == "failed"
    assert outcome.reason == "operation_failed"
    assert outcome.remaining == ()
    assert lab.remote.resources == {}


def test_cli_isola_configuracao_que_poderia_persistir_historico_sensivel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tests.integration.aws_capability.cli import Request

    recorder = ProcessRecorder()
    monkeypatch.setattr(subprocess, "run", recorder.run)
    cli = transport()
    inherited = dict(cli.environment)
    inherited.update(AWS_CONFIG_FILE="unsafe-config", AWS_PROFILE="unsafe-profile")
    replace(cli, environment=inherited).invoke(Request("sts", "get-caller-identity", {}))
    environment = recorder.calls[0][1]["env"]
    assert environment["AWS_CONFIG_FILE"] == os.devnull
    assert environment["AWS_SHARED_CREDENTIALS_FILE"] == os.devnull
    assert environment["AWS_EC2_METADATA_DISABLED"] == "true"
    assert "AWS_PROFILE" not in environment
