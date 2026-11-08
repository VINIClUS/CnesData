"""Contrato do transporte CLI com subprocessos gravados e credenciais fictícias."""

import json
import os
import stat
import subprocess
import tempfile
import traceback
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from tests.integration.aws_capability import cli as cli_module
from tests.integration.aws_capability._testing import SECRETS
from tests.integration.aws_capability.cli import Cli, CliFailure, Request
from tests.integration.aws_capability.contracts import Limits, RequestBudget, RequestContext


@dataclass
class ProcessRecorder:
    calls: list[tuple[Sequence[str], dict]] = field(default_factory=list)
    response: str = '{"ok": true}'
    timeout: bool = False
    returncode: int = 0
    stderr: str = ""
    inputs: list["InputFile | None"] = field(default_factory=list)
    error: BaseException | None = None

    def run(self, argv: Sequence[str], **options: object) -> subprocess.CompletedProcess:
        self.calls.append((argv, options))
        self.inputs.append(_observe_input(argv))
        if self.error is not None:
            raise self.error
        if self.timeout:
            raise subprocess.TimeoutExpired(argv, 1, self.response, self.stderr)
        if self.returncode and options.get("check"):
            raise subprocess.CalledProcessError(self.returncode, argv, self.response, self.stderr)
        return subprocess.CompletedProcess(argv, self.returncode, self.response, self.stderr)


@dataclass(frozen=True)
class InputFile:
    path: Path
    content: str
    file_mode: int
    directory_mode: int


def _observe_input(argv: Sequence[str]) -> InputFile | None:
    if "--cli-input-json" not in argv:
        return None
    location = argv[list(argv).index("--cli-input-json") + 1]
    assert location.startswith("file:///"), "transport=file_uri_required"
    path = Path(location.removeprefix("file://"))
    return InputFile(
        path,
        path.read_text(encoding="utf-8"),
        stat.S_IMODE(path.stat().st_mode),
        stat.S_IMODE(path.parent.stat().st_mode),
    )


def transport(timeout: float = 7) -> Cli:
    budget = RequestBudget(Limits(), 1060, lambda: 1000)
    return Cli("us-east-2", timeout, environment(), RequestContext(budget, "work"))


def environment() -> dict[str, str]:
    return {
        "AWS_ACCESS_KEY_ID": "dummy",
        "AWS_SECRET_ACCESS_KEY": "dummy",
        "AWS_SESSION_TOKEN": "dummy",
        "AWS_ENDPOINT_URL": "http://127.0.0.1:4577",
        "AWS_ENDPOINT_URL_S3": "http://example.invalid",
        "AWS_ENDPOINT_URL_COGNITO_IDP": "http://example.invalid",
        "AWS_REGION": "eu-west-1",
        "AWS_DEFAULT_REGION": "eu-west-1",
        "AWS_PAGER": "unsafe-pager",
        "AWS_CLI_AUTO_PROMPT": "on",
        "AWS_MAX_ATTEMPTS": "9999",
        "AWS_IGNORE_CONFIGURED_ENDPOINT_URLS": "false",
    }


def test_transporte_json_sensivel_usa_arquivo_privado_sem_stdin_nem_shell(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    recorder = ProcessRecorder()
    monkeypatch.setattr(subprocess, "run", recorder.run)
    monkeypatch.chdir(tmp_path)
    opened: list[tuple[int, int]] = []
    real_open = os.open

    def recording_open(path: str | Path, flags: int, mode: int = 0o777, **kwargs: int) -> int:
        opened.append((flags, mode))
        return real_open(path, flags, mode, **kwargs)

    monkeypatch.setattr(os, "open", recording_open)
    previous = os.umask(0)
    payload: Mapping[str, object] = {"synthetic_secret": SECRETS[0]}
    try:
        result = transport().invoke(Request("sts", "get-caller-identity", payload))
    finally:
        os.umask(previous)
    assert result == {"ok": True}
    assert len(recorder.calls) == 1
    creations = [item for item in opened if item[0] & os.O_CREAT]
    assert len(creations) == 1
    flags, creation_mode = creations[0]
    assert flags & (os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW) == (
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW
    )
    assert creation_mode == 0o600
    argv, options = recorder.calls[0]
    assert not isinstance(argv, str)
    assert tuple(argv[:3]) == ("aws", "sts", "get-caller-identity")
    observed = recorder.inputs[0]
    assert observed is not None
    assert argv[argv.index("--cli-input-json") + 1] == f"file://{observed.path}"
    assert json.loads(observed.content) == payload
    assert observed.file_mode == 0o600
    assert observed.directory_mode == 0o700
    assert "input" not in options
    assert options["stdin"] == subprocess.DEVNULL
    assert SECRETS[0] not in repr(argv)
    assert options.get("shell", False) is False
    assert options["timeout"] == 7
    assert options.get("capture_output") is True
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize(
    "failure", ["sucesso", "returncode", "timeout", "oserror", "json_invalido", "erro_inesperado"]
)
def test_arquivo_privado_e_removido_em_qualquer_desfecho(
    monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    recorder = ProcessRecorder()
    if failure == "returncode":
        recorder.returncode, recorder.stderr = 254, '{"Code": "Denied"}'
    elif failure == "timeout":
        recorder.timeout = True
    elif failure == "oserror":
        recorder.error = OSError("boundary=down")
    elif failure == "json_invalido":
        recorder.response = "not-json"
    elif failure == "erro_inesperado":
        recorder.error = KeyboardInterrupt()
    monkeypatch.setattr(subprocess, "run", recorder.run)
    try:
        transport().call(Request("sts", "get-caller-identity", {"k": SECRETS[0]}))
    except KeyboardInterrupt:
        assert failure == "erro_inesperado"
    except Exception:
        assert failure == "json_invalido"
    observed = recorder.inputs[0]
    assert observed is not None
    assert not observed.path.exists()
    assert not observed.path.parent.exists()


def test_arquivo_privado_prefere_memoria_compartilhada_quando_existe(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    shared = tmp_path / "shm"
    shared.mkdir()
    monkeypatch.setattr(cli_module, "SHARED_MEMORY", shared)
    recorder = ProcessRecorder()
    monkeypatch.setattr(subprocess, "run", recorder.run)
    transport().call(Request("sts", "get-caller-identity", {}))
    assert recorder.inputs[0] is not None
    assert recorder.inputs[0].path.parent.parent == shared
    assert list(shared.iterdir()) == []


def test_arquivo_privado_usa_temporario_padrao_sem_memoria_compartilhada(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    default = tmp_path / "tmp"
    default.mkdir()
    monkeypatch.setattr(cli_module, "SHARED_MEMORY", tmp_path / "ausente")
    monkeypatch.setattr(tempfile, "tempdir", str(default))
    recorder = ProcessRecorder()
    monkeypatch.setattr(subprocess, "run", recorder.run)
    transport().call(Request("sts", "get-caller-identity", {}))
    assert recorder.inputs[0] is not None
    assert recorder.inputs[0].path.parent.parent == default
    assert recorder.inputs[0].directory_mode == 0o700
    assert list(default.iterdir()) == []


def test_falha_ao_criar_arquivo_privado_nao_inicia_subprocesso(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(cli_module, "SHARED_MEMORY", tmp_path / "ausente")
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path / "inexistente"))
    recorder = ProcessRecorder()
    monkeypatch.setattr(subprocess, "run", recorder.run)
    with pytest.raises(CliFailure, match="operation_failed"):
        transport().invoke(Request("sts", "get-caller-identity", {}))
    assert recorder.calls == []


def test_cli_fixa_regiao_e_desabilita_endpoints_pager_debug_e_retries_ilimitados(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorder = ProcessRecorder()
    monkeypatch.setattr(subprocess, "run", recorder.run)
    transport().invoke(Request("sts", "get-caller-identity", {}))
    assert len(recorder.calls) == 1
    argv, options = recorder.calls[0]
    assert argv[argv.index("--region") + 1] == "us-east-2"
    assert "--debug" not in argv
    assert "--endpoint-url" not in argv
    assert "--no-verify-ssl" not in argv
    assert "--no-sign-request" not in argv
    assert "--no-cli-pager" in argv
    assert "--no-cli-auto-prompt" in argv
    assert argv[argv.index("--output") + 1] == "json"
    child_env = options["env"]
    assert not any(key.startswith("AWS_ENDPOINT_URL") for key in child_env)
    assert child_env["AWS_IGNORE_CONFIGURED_ENDPOINT_URLS"] == "true"
    assert child_env["AWS_PAGER"] == ""
    assert child_env["AWS_CLI_AUTO_PROMPT"] == "off"
    assert child_env["AWS_MAX_ATTEMPTS"] == "1"
    for flag in ["--cli-connect-timeout", "--cli-read-timeout"]:
        assert 0 < int(argv[argv.index(flag) + 1]) <= 7


@pytest.mark.parametrize("timeout", [0, -1, float("inf"), float("nan")])
def test_timeout_invalido_nao_inicia_subprocesso(timeout: float) -> None:
    with pytest.raises(CliFailure, match="invalid_timeout"):
        transport(timeout).invoke(Request("sts", "get-caller-identity", {}))


@pytest.mark.parametrize("failure", ["nonzero", "timeout", "invalid_json"])
def test_erros_cli_sao_sanitizados_sem_stdout_stderr_ou_request(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    failure: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    secret_text = " ".join(SECRETS)
    recorder = ProcessRecorder(response=secret_text)
    if failure == "nonzero":
        recorder.returncode = 2
        recorder.response = json.dumps({"secret": secret_text})
        recorder.stderr = secret_text
    elif failure == "timeout":
        recorder.timeout = True
        recorder.stderr = secret_text
    monkeypatch.setattr(subprocess, "run", recorder.run)
    with pytest.raises(CliFailure) as caught:
        transport().invoke(Request("sts", "get-caller-identity", {}))
    assert len(recorder.calls) == 1
    captured = capsys.readouterr()
    rendered = "".join(traceback.format_exception(caught.value))
    evidence = str(caught.value) + repr(caught.value) + caplog.text + captured.out + captured.err
    for secret in SECRETS:
        assert secret not in evidence + rendered


def test_resposta_sensivel_permanece_apenas_na_memoria(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    recorder = ProcessRecorder(response=json.dumps({"synthetic_secret": SECRETS[1]}))
    monkeypatch.setattr(subprocess, "run", recorder.run)
    monkeypatch.chdir(tmp_path)
    result = transport().invoke(Request("sts", "get-caller-identity", {}))
    assert result.get("synthetic_secret") == SECRETS[1]
    assert SECRETS[1] not in caplog.text
    assert list(tmp_path.iterdir()) == []


def test_controle_subprocesso_retorna_codigo_nao_zero_quando_check_false() -> None:
    recorder = ProcessRecorder(returncode=7, response='{"valid": true}', stderr="denied")
    result = recorder.run(["aws"], check=False)
    assert result.returncode == 7
    assert result.stdout == '{"valid": true}'
    assert result.stderr == "denied"


def test_controle_subprocesso_so_levanta_por_returncode_quando_check_true() -> None:
    recorder = ProcessRecorder(returncode=7)
    with pytest.raises(subprocess.CalledProcessError) as caught:
        recorder.run(["aws"], check=True)
    assert caught.value.returncode == 7


def test_returncode_nao_zero_com_json_valido_nunca_vira_sucesso(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorder = ProcessRecorder(returncode=7, response='{"ok": true}')
    monkeypatch.setattr(subprocess, "run", recorder.run)
    with pytest.raises(CliFailure):
        transport().invoke(Request("sts", "get-caller-identity", {}))
    assert len(recorder.calls) == 1


def test_controle_oraculo_detecta_segredo_em_causa_explicita() -> None:
    error = CliFailure("operation=failed")
    error.__cause__ = ValueError(SECRETS[0])
    error.__suppress_context__ = True
    with pytest.raises(AssertionError):
        assert SECRETS[0] not in "".join(traceback.format_exception(error))
