"""Regressões B1-U1 e B1-U2 sem alterar os oráculos B1 aprovados."""

import subprocess
from pathlib import Path

import pytest

from tests.integration.aws_capability._service_safety_testing import (
    BodyScenario,
    call_failure_outcome,
    isolated_body_scenario,
    private_failure_request,
    substituted_body,
)
from tests.integration.aws_capability._service_testing import (
    CANARIES,
    CliLab,
    make_cli_lab,
)


@pytest.fixture
def isolated_body(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> BodyScenario:
    scenario = isolated_body_scenario(tmp_path)
    monkeypatch.chdir(scenario.lab.directory)
    monkeypatch.setattr(subprocess, "run", scenario.lab.process.run)
    return scenario


@pytest.fixture
def private_cli(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> CliLab:
    lab = make_cli_lab(tmp_path)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(subprocess, "run", lab.process.run)
    return lab


@pytest.mark.parametrize(
    "kind", ["altered", "canary", "newline", "private_symlink", "journal_symlink", "directory"]
)
def test_body_canonico_adulterado_recusa_antes_de_io(
    isolated_body: BodyScenario, kind: str
) -> None:
    scenario = isolated_body
    filename = "journal.json" if kind == "journal_symlink" else "private.json"
    private = scenario.lab.directory / filename
    private.write_text(CANARIES[1])
    assert scenario.body.read_bytes() == b"{}"
    scenario.lab.respond((0, "{}", ""))
    rejected = False
    with substituted_body(scenario.body, kind, private):
        request = scenario.request_type(
            "s3api",
            "put-object",
            {
                "Bucket": "synthetic-bucket",
                "Key": "audit/synthetic",
                "Body": str(scenario.body),
                "ContentMD5": "mZFLkyvTelC5g8XnyQrpOw==",
            },
        )
        try:
            scenario.lab.cli.invoke(request)
        except scenario.failure:
            rejected = True
        assert private.read_text() == CANARIES[1]
    assert scenario.body.is_file()
    assert not scenario.body.is_symlink()
    assert scenario.body.read_bytes() == b"{}"
    assert private.read_text() == CANARIES[1]
    assert (rejected, len(scenario.lab.process.calls), scenario.lab.cli.context.budget.used) == (
        True,
        0,
        0,
    )


def test_body_canonico_regular_restaurado_e_admitido(isolated_body: BodyScenario) -> None:
    scenario = isolated_body
    private = scenario.lab.directory / "journal.json"
    private.write_text(CANARIES[1])
    with substituted_body(scenario.body, "journal_symlink", private):
        assert scenario.body.is_symlink()
    assert scenario.body.is_file()
    assert not scenario.body.is_symlink()
    assert scenario.body.read_bytes() == b"{}"
    scenario.lab.respond((0, '{"VersionId":"synthetic-version"}', ""))
    result = scenario.lab.cli.invoke(
        scenario.request_type(
            "s3api",
            "put-object",
            {
                "Bucket": "synthetic-bucket",
                "Key": "audit/synthetic",
                "Body": str(scenario.body),
                "ContentMD5": "mZFLkyvTelC5g8XnyQrpOw==",
            },
        )
    )
    assert result == {"VersionId": "synthetic-version"}
    assert len(scenario.lab.process.calls) == scenario.lab.cli.context.budget.used == 1
    assert scenario.body.read_bytes() == b"{}"
    assert private.read_text() == CANARIES[1]


@pytest.mark.parametrize("failure", ["service", "local", "timeout", "os", "invalid_json"])
def test_falha_call_preserva_memoria_sem_publicar_canaries(
    private_cli: CliLab,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
    failure: str,
) -> None:
    private_cli.respond(call_failure_outcome(failure))
    reply = private_cli.call(private_failure_request(failure))
    captured = capsys.readouterr()
    evidence = (
        caplog.text
        + captured.out
        + captured.err
        + repr(private_cli.process.calls[0][0])
        + repr(reply)
        + repr(reply.error)
    )
    assert all(secret not in evidence for secret in CANARIES), "privacy=call_failure_leak"
    assert list(private_cli.directory.rglob("*")) == [], "privacy=call_failure_file"
    assert reply.ok is False
    assert reply.data == {}
    assert reply.error is not None
    assert private_cli.process.observed_debits == [1]
    if failure == "service":
        actual = (
            reply.error.origin,
            reply.error.code,
            reply.error.details.get("CancellationReasons"),
        )
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
    elif failure == "local":
        assert (reply.error.origin, reply.error.code) == ("local", "ParamValidation")
    else:
        assert reply.error.origin == "local"
        assert reply.error.code
        assert "AccessDenied" not in reply.error.code
