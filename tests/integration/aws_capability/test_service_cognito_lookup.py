"""Lookup Cognito distingue ausência remota de leitura inconclusiva."""

import subprocess
from pathlib import Path

import pytest

from tests.integration.aws_capability._service_cognito_testing import (
    ROOT,
    ReadLab,
    fixture_data,
    make_read_lab,
    respond,
    respond_error,
)
from tests.integration.aws_capability._service_contracts import ApiError, Reply
from tests.integration.aws_capability._service_read_assertions import (
    assert_private,
    assert_process,
)
from tests.integration.aws_capability.cli import Request
from tests.integration.aws_capability.contracts import Resource, ServiceFailure
from tests.integration.aws_capability.test_service_cognito_ownership import DESCRIBE, TAGS


@pytest.fixture
def read_lab(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> ReadLab:
    return make_read_lab(monkeypatch, tmp_path)


def test_lookup_ausente_exige_not_found_remoto_no_describe_exato(read_lab: ReadLab) -> None:
    respond_error(read_lab, "remote_not_found")
    assert read_lab.services.lookup(ROOT, read_lab.context) is None
    assert_process(read_lab, [DESCRIBE])


@pytest.mark.parametrize(
    "failure",
    [
        "local_not_found",
        "remote_denied",
        "remote_403",
        "remote_404",
        "bad_json",
        "empty_stdout",
        "empty_object",
        "bad_stderr",
    ],
)
def test_erro_ambiguo_no_describe_nao_e_ausencia(
    read_lab: ReadLab,
    failure: str,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    respond_error(read_lab, failure)
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(ROOT, read_lab.context)
    expected = (
        "service=describe_invalid" if failure == "empty_object" else "service=describe_failed"
    )
    assert error.value.args == (expected,)
    captured = capsys.readouterr()
    assert_private(error.value, read_lab, caplog.text + captured.out + captured.err)
    assert_process(read_lab, [DESCRIBE])


@pytest.mark.parametrize("failure", ["remote_not_found", "tags_denied", "bad_json"])
def test_erro_de_tags_depois_do_describe_nao_e_ausencia(
    read_lab: ReadLab,
    failure: str,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    respond(read_lab, fixture_data("cognito")["describe"])
    respond_error(read_lab, failure)
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(ROOT, read_lab.context)
    assert error.value.args == ("service=request_failed",)
    captured = capsys.readouterr()
    assert_private(error.value, read_lab, caplog.text + captured.out + captured.err)
    assert_process(read_lab, [DESCRIBE, TAGS])


def test_timeout_de_transporte_nao_e_ausencia(read_lab: ReadLab) -> None:
    read_lab.process.outcomes.append(subprocess.TimeoutExpired("aws", 7))
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(ROOT, read_lab.context)
    assert error.value.args == ("service=describe_failed",)
    assert_process(read_lab, [DESCRIBE])


def test_timeout_e_json_invalido_produzem_mesma_reply_local(read_lab: ReadLab) -> None:
    request = Request("cognito-idp", "describe-user-pool", {"UserPoolId": ROOT.resource_id})
    read_lab.process.outcomes.append(subprocess.TimeoutExpired("aws", 7))
    timeout_reply = read_lab.services.ports.commands.call(request, read_lab.context)
    respond_error(read_lab, "bad_json")
    invalid_json_reply = read_lab.services.ports.commands.call(request, read_lab.context)
    expected = Reply(False, {}, ApiError("operation_failed", {}, "local"))
    assert timeout_reply == expected
    assert invalid_json_reply == expected
    assert_process(read_lab, [DESCRIBE, DESCRIBE])


def test_inspect_exige_presenca_mesmo_com_not_found_conclusivo(
    read_lab: ReadLab,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    respond_error(read_lab, "remote_not_found")
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.inspect(ROOT, read_lab.context)
    assert error.value.args == ("service=not_found",)
    captured = capsys.readouterr()
    assert_private(error.value, read_lab, caplog.text + captured.out + captured.err)
    assert_process(read_lab, [DESCRIBE])


def test_lista_vazia_nao_prova_ausencia_do_pool(read_lab: ReadLab) -> None:
    respond(read_lab, fixture_data("cognito")["empty_inventory"])
    page = read_lab.services.inventory("cognito", None, read_lab.context)
    assert page.resources == ()
    assert page.cursor is None
    respond(read_lab, fixture_data("cognito")["describe"])
    respond(read_lab, fixture_data("cognito")["tags"])
    owner = read_lab.services.lookup(ROOT, read_lab.context)
    assert owner is not None
    assert owner.account == "111111111111"
    assert_process(
        read_lab,
        [
            ("cognito-idp", "list-user-pools", "us-east-2", {"MaxResults": 60}),
            DESCRIBE,
            TAGS,
        ],
    )


def test_lookup_rele_presenca_depois_de_ausencia_sem_cache(read_lab: ReadLab) -> None:
    respond_error(read_lab, "remote_not_found")
    assert read_lab.services.lookup(ROOT, read_lab.context) is None
    respond(read_lab, fixture_data("cognito")["describe"])
    respond(read_lab, fixture_data("cognito")["tags"])
    owner = read_lab.services.lookup(ROOT, read_lab.context)
    assert owner is not None
    assert_process(read_lab, [DESCRIBE, DESCRIBE, TAGS])


def test_lookup_rele_ausencia_depois_de_presenca_sem_cache(read_lab: ReadLab) -> None:
    respond(read_lab, fixture_data("cognito")["describe"])
    respond(read_lab, fixture_data("cognito")["tags"])
    assert read_lab.services.lookup(ROOT, read_lab.context) is not None
    respond_error(read_lab, "remote_not_found")
    assert read_lab.services.lookup(ROOT, read_lab.context) is None
    assert_process(read_lab, [DESCRIBE, TAGS, DESCRIBE])


def test_lookup_falha_de_privacidade_nao_publica_arn_ou_canary(
    read_lab: ReadLab,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    respond(read_lab, fixture_data("cognito")["private_arn"])
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(ROOT, read_lab.context)
    assert error.value.args == ("service=describe_invalid",)
    captured = capsys.readouterr()
    assert_private(error.value, read_lab, caplog.text + captured.out + captured.err)
    assert_process(read_lab, [DESCRIBE])


@pytest.mark.parametrize(
    "resource",
    [
        Resource("dynamodb", "us-east-2_a1b2c3d4", "cnesdata-cap-a1b2c3d4"),
        Resource("cognito", "foreign", "cnesdata-cap-a1b2c3d4"),
    ],
    ids=["servico_errado", "id_errado"],
)
def test_lookup_recurso_invalido_nao_inicia_processo(
    read_lab: ReadLab, resource: Resource
) -> None:
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(resource, read_lab.context)
    assert error.value.args == ("service=resource_invalid",)
    assert_process(read_lab, [])
