"""Contratos B1 de resultado, privacidade e streaming do CLI real sem rede."""

import importlib.util
import json
import subprocess
import traceback
from collections.abc import Mapping
from pathlib import Path

import pytest

from tests.integration.aws_capability._cli_testing import PRIVATE_INPUT
from tests.integration.aws_capability._service_testing import (
    BODY,
    CANARIES,
    REMOTE_TRANSACTION,
    CliLab,
    ProcessScript,
    make_cli_lab,
    private_auth_request,
    stdin_payload,
)
from tests.integration.aws_capability.cli import CliFailure, Request


@pytest.fixture
def cli_lab(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> CliLab:
    lab = make_cli_lab(tmp_path)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(subprocess, "run", lab.process.run)
    return lab


@pytest.mark.parametrize("stdout", ["{}", "", "\n"])
def test_invoke_delete_sem_documento_retorna_mapping(cli_lab: CliLab, stdout: str) -> None:
    cli_lab.respond((0, stdout, ""))
    try:
        result = cli_lab.cli.invoke(Request("iam", "delete-role", {"RoleName": "synthetic"}))
    except CliFailure as error:
        result = error
    assert isinstance(result, Mapping)
    assert result == {}
    assert len(cli_lab.process.calls) == cli_lab.cli.context.budget.used == 1
    assert cli_lab.process.observed_debits == [1]


@pytest.mark.parametrize("stdout", ["{}", "", "\n"])
def test_call_delete_sem_documento_e_sucesso(cli_lab: CliLab, stdout: str) -> None:
    cli_lab.respond((0, stdout, ""))
    reply = cli_lab.call(Request("iam", "delete-role", {"RoleName": "synthetic"}))
    assert reply.ok is True
    assert reply.data == {}
    assert reply.error is None
    assert cli_lab.process.observed_debits == [1]


def test_call_preserva_code_e_reasons_modelados_em_memoria(cli_lab: CliLab) -> None:
    cli_lab.respond((254, "", json.dumps(REMOTE_TRANSACTION)))
    reply = cli_lab.call(Request("dynamodb", "transact-write-items", {"TransactItems": []}))
    assert reply.ok is False
    assert reply.data == {}
    actual = (reply.error.origin, reply.error.code, reply.error.details.get("CancellationReasons"))
    assert actual == (
        "service",
        "TransactionCanceledException",
        [
            {"Code": "None"},
            {
                "Code": "ConditionalCheckFailed",
                "Message": "B1_PASSWORD_CANARY",
                "Item": {"pk": {"S": "B1_TOKEN_CANARY"}},
            },
            {"Code": "None"},
        ],
    )
    assert cli_lab.process.observed_debits == [1]


@pytest.mark.parametrize(
    "code", ["AccessDeniedException", "ResourceNotFoundException", "ValidationException"]
)
def test_call_preserva_codigo_remoto_sem_confundir_validacao_local(
    cli_lab: CliLab, code: str
) -> None:
    cli_lab.respond((254, "{}", json.dumps({"Code": code, "Message": CANARIES[2]})))
    reply = cli_lab.call(Request("dynamodb", "describe-table", {"TableName": "synthetic"}))
    assert reply.ok is False
    assert (reply.error.origin, reply.error.code) == ("service", code)
    assert reply.data == {}


@pytest.mark.parametrize(
    ("returncode", "code"),
    [(252, "ParamValidation"), (253, "NoRegion"), (253, "NoCredentials")],
)
def test_call_classifica_json_local_sem_provar_negacao_aws(
    cli_lab: CliLab, returncode: int, code: str
) -> None:
    cli_lab.respond((returncode, "", json.dumps({"Code": code, "Message": CANARIES[2]})))
    reply = cli_lab.call(Request("dynamodb", "transact-write-items", {"TransactItems": []}))
    assert reply.ok is False
    assert reply.data == {}
    assert reply.error.origin == "local"
    assert reply.error.code == code


@pytest.mark.parametrize("returncode", [252, 255])
def test_call_erro_textual_local_nao_vira_access_denied(cli_lab: CliLab, returncode: int) -> None:
    cli_lab.respond((returncode, "{}", "aws: [ERROR]: " + CANARIES[2]))
    reply = cli_lab.call(Request("sts", "get-caller-identity", {}))
    assert reply.ok is False
    assert reply.error.origin == "local"
    assert reply.error.code
    assert "AccessDenied" not in reply.error.code
    assert cli_lab.process.observed_debits == [1]


@pytest.mark.parametrize(
    "outcome",
    [
        OSError(CANARIES[2]),
        subprocess.TimeoutExpired("aws", 1, CANARIES[0], CANARIES[2]),
        (0, "not-json-" + CANARIES[0], ""),
        (0, "[]", ""),
        (0, "null", ""),
        (254, "{}", "not-json-" + CANARIES[2]),
        (254, "{}", '{"Message":"B1_STDERR_CANARY"}'),
    ],
    ids=["os", "timeout", "invalid_stdout", "array", "null", "invalid_stderr", "no_code"],
)
def test_call_falha_de_transporte_ou_json_nao_prova_negacao(
    cli_lab: CliLab, outcome: tuple[int, str, str] | BaseException
) -> None:
    cli_lab.respond(outcome)
    reply = cli_lab.call(Request("sts", "get-caller-identity", {}))
    assert reply.ok is False
    assert reply.data == {}
    assert reply.error.origin == "local"
    assert reply.error.code
    assert "AccessDenied" not in reply.error.code
    assert cli_lab.process.observed_debits == [1]
    assert len(cli_lab.process.calls) == 1


def test_flags_deterministas_e_json_privado_so_em_stdin(cli_lab: CliLab) -> None:
    cli_lab.respond((0, "{}", ""))
    payload = {
        "UserPoolId": "us-east-2_synthetic",
        "Username": "synthetic",
        "TemporaryPassword": CANARIES[0],
        "MessageAction": "SUPPRESS",
    }
    cli_lab.cli.invoke(Request("cognito-idp", "admin-create-user", payload))
    argv, options = cli_lab.process.calls[0]
    assert argv[:3] == ["aws", "cognito-idp", "admin-create-user"]
    for flag, value in [
        ("--cli-error-format", "json"),
        ("--color", "off"),
        ("--output", "json"),
        ("--region", "us-east-2"),
        ("--cli-input-json", PRIVATE_INPUT),
    ]:
        assert argv.count(flag) == 1
        assert argv[argv.index(flag) + 1] == value
    for flag in ["--no-paginate", "--no-cli-pager", "--no-cli-auto-prompt"]:
        assert flag in argv
    for flag in ["--debug", "--endpoint-url", "--profile", "--no-verify-ssl", "--no-sign-request"]:
        assert flag not in argv
    assert options["shell"] is False
    assert options["check"] is False
    assert options["capture_output"] is True
    assert options["text"] is True
    assert options["timeout"] == 7
    assert stdin_payload(cli_lab.process) == {
        "UserPoolId": "us-east-2_synthetic",
        "Username": "synthetic",
        "TemporaryPassword": "B1_PASSWORD_CANARY",
        "MessageAction": "SUPPRESS",
    }
    assert CANARIES[0] not in repr(argv)
    environment = options["env"]
    assert environment["AWS_MAX_ATTEMPTS"] == "1"
    assert environment["AWS_CONFIG_FILE"] == "/dev/null"
    assert environment["AWS_SHARED_CREDENTIALS_FILE"] == "/dev/null"
    assert environment["AWS_EC2_METADATA_DISABLED"] == "true"
    assert not any(key.startswith("AWS_ENDPOINT_URL") for key in environment)
    assert "AWS_PROFILE" not in environment
    assert "AWS_DEFAULT_PROFILE" not in environment


@pytest.mark.parametrize("method", ["invoke", "call"])
def test_uma_request_uma_aquisicao_sem_retry_oculto(cli_lab: CliLab, method: str) -> None:
    cli_lab.respond((254, "", '{"Code":"ThrottlingException","Message":"retry later"}'))
    request = Request("dynamodb", "describe-table", {"TableName": "synthetic"})
    if method == "invoke":
        with pytest.raises(CliFailure):
            cli_lab.cli.invoke(request)
    else:
        assert cli_lab.call(request).ok is False
    assert len(cli_lab.process.calls) == 1
    assert cli_lab.cli.context.budget.used == cli_lab.cli.context.budget.work_used == 1
    assert cli_lab.process.observed_debits == [1]
    assert cli_lab.process.outcomes == []


@pytest.mark.parametrize("method", ["invoke", "call"])
def test_retry_explicito_debita_cada_request(cli_lab: CliLab, method: str) -> None:
    request = Request("sts", "get-caller-identity", {})
    for _ in range(2):
        cli_lab.respond((0, "{}", ""))
        if method == "invoke":
            assert cli_lab.cli.invoke(request) == {}
        else:
            assert cli_lab.call(request).ok is True
    assert cli_lab.process.observed_debits == [1, 2]
    assert cli_lab.cli.context.budget.used == 2


@pytest.mark.parametrize("method", ["invoke", "call"])
def test_resposta_sensivel_so_permanece_em_memoria(
    cli_lab: CliLab,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
    method: str,
) -> None:
    response = {"AuthenticationResult": {"AccessToken": CANARIES[1]}}
    cli_lab.respond((0, json.dumps(response), CANARIES[2]))
    request = private_auth_request()
    if method == "invoke":
        data = cli_lab.cli.invoke(request)
    else:
        data = cli_lab.call(request).data
    assert data == {"AuthenticationResult": {"AccessToken": "B1_TOKEN_CANARY"}}
    captured = capsys.readouterr()
    evidence = caplog.text + captured.out + captured.err + repr(cli_lab.process.calls[0][0])
    assert all(secret not in evidence for secret in CANARIES)
    assert list(cli_lab.directory.iterdir()) == []


@pytest.mark.parametrize("failure", ["service", "local", "timeout", "os", "invalid_json"])
def test_exception_invoke_nao_publica_message_details_causa_ou_corpos(
    cli_lab: CliLab,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
    failure: str,
) -> None:
    outcomes = {
        "service": (254, CANARIES[1], json.dumps(REMOTE_TRANSACTION)),
        "local": (
            252,
            CANARIES[1],
            json.dumps({"Code": "ParamValidation", "Message": CANARIES[2]}),
        ),
        "timeout": subprocess.TimeoutExpired("aws", 1, CANARIES[1], CANARIES[2]),
        "os": OSError(CANARIES[2]),
        "invalid_json": (0, CANARIES[1], CANARIES[2]),
    }
    cli_lab.respond(outcomes[failure])
    with pytest.raises(CliFailure) as caught:
        cli_lab.cli.invoke(private_auth_request())
    captured = capsys.readouterr()
    evidence = (
        str(caught.value)
        + repr(caught.value)
        + "".join(traceback.format_exception(caught.value))
        + caplog.text
        + captured.out
        + captured.err
        + repr(cli_lab.process.calls[0][0])
    )
    assert all(secret not in evidence for secret in CANARIES)
    assert list(cli_lab.directory.iterdir()) == []


def test_body_publico_tem_dois_bytes_e_md5_independente() -> None:
    import base64
    import hashlib

    assert BODY.read_bytes() == b"{}"
    assert BODY.stat().st_size == 2
    digest = hashlib.md5(b"{}", usedforsecurity=False).digest()
    assert base64.b64encode(digest).decode() == "mZFLkyvTelC5g8XnyQrpOw=="


def test_body_streaming_allowlisted_vai_em_flag_sem_file_prefix(cli_lab: CliLab) -> None:
    cli_lab.respond((0, '{"VersionId":"synthetic-version"}', ""))
    payload = {
        "Bucket": "synthetic-bucket",
        "Key": "audit/synthetic",
        "Body": str(BODY),
        "ContentMD5": "mZFLkyvTelC5g8XnyQrpOw==",
    }
    result = cli_lab.cli.invoke(Request("s3api", "put-object", payload))
    assert result == {"VersionId": "synthetic-version"}
    argv, _ = cli_lab.process.calls[0]
    assert "--body" in argv
    assert argv[argv.index("--body") + 1] == str(BODY)
    assert stdin_payload(cli_lab.process) == {
        "Bucket": "synthetic-bucket",
        "Key": "audit/synthetic",
        "ContentMD5": "mZFLkyvTelC5g8XnyQrpOw==",
    }
    assert payload["Body"] == str(BODY)
    assert BODY.read_bytes() == b"{}"
    assert list(cli_lab.directory.iterdir()) == []


@pytest.mark.parametrize("kind", ["arbitrary", "journal", "symlink", "file_uri", "fileb_uri"])
def test_body_nao_allowlisted_rejeita_antes_do_processo(cli_lab: CliLab, kind: str) -> None:
    private = cli_lab.directory / ("journal.json" if kind == "journal" else "arbitrary.json")
    private.write_text(CANARIES[1])
    body = str(private)
    if kind == "symlink":
        link = cli_lab.directory / "body.json"
        link.symlink_to(BODY)
        body = str(link)
    elif kind == "file_uri":
        body = "file://" + str(BODY)
    elif kind == "fileb_uri":
        body = "fileb://" + str(BODY)
    cli_lab.respond((0, "{}", ""))
    with pytest.raises(CliFailure):
        cli_lab.cli.invoke(Request("s3api", "put-object", {"Body": body}))
    assert cli_lab.process.calls == []
    assert cli_lab.cli.context.budget.used == 0
    assert private.read_text() == CANARIES[1]


def test_body_nao_e_aceito_em_outra_operacao(cli_lab: CliLab) -> None:
    cli_lab.respond((0, "{}", ""))
    with pytest.raises(CliFailure):
        cli_lab.cli.invoke(Request("sts", "get-caller-identity", {"Body": str(BODY)}))
    assert cli_lab.process.calls == []


def test_contratos_reply_apierror_sao_interface_explicita() -> None:
    name = "tests.integration.aws_capability._service_contracts"
    assert importlib.util.find_spec(name) is not None, "interface=_service_contracts_absent"


def test_controle_gravador_preserva_returncode_stdout_stderr_sem_check() -> None:
    script = ProcessScript(outcomes=[(254, "stdout-control", "stderr-control")])
    result = script.run(["aws", "dynamodb", "describe-table"], check=False)
    assert (result.returncode, result.stdout, result.stderr) == (
        254,
        "stdout-control",
        "stderr-control",
    )
    assert script.outcomes == []


def test_controle_gravador_propaga_timeout_com_corpos_privados() -> None:
    timeout = subprocess.TimeoutExpired("aws", 1, CANARIES[0], CANARIES[2])
    script = ProcessScript(outcomes=[timeout])
    with pytest.raises(subprocess.TimeoutExpired) as caught:
        script.run(["aws"], check=False)
    assert caught.value is timeout
    assert caught.value.stdout == CANARIES[0]
    assert caught.value.stderr == CANARIES[2]


def test_controle_gravador_rejeita_retry_nao_previsto() -> None:
    script = ProcessScript(outcomes=[(0, "{}", "")])
    script.run(["aws"], check=False)
    with pytest.raises(AssertionError, match="unexpected_request"):
        script.run(["aws"], check=False)
    assert len(script.calls) == 2


@pytest.mark.parametrize("method", ["invoke", "call"])
def test_returncode_remoto_com_stdout_json_nao_vira_sucesso(cli_lab: CliLab, method: str) -> None:
    cli_lab.respond((254, '{"Token":"B1_TOKEN_CANARY"}', json.dumps(REMOTE_TRANSACTION)))
    request = Request("dynamodb", "transact-write-items", {"TransactItems": []})
    if method == "invoke":
        with pytest.raises(CliFailure):
            cli_lab.cli.invoke(request)
    else:
        reply = cli_lab.call(request)
        assert reply.ok is False
        assert reply.data == {}
    assert cli_lab.process.observed_debits == [1]
