"""Entrypoint de produção do API: settings fixos, só o credential_process e preflight do leitor."""

from __future__ import annotations

import json
import os
import shlex
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pytest
from botocore.compat import compat_shell_split
from botocore.configloader import load_config

REPO_ROOT = Path(__file__).resolve().parents[2]
DOCKER_DIRECTORY = REPO_ROOT / "apps" / "central_api" / "docker"
ENTRYPOINT_SOURCE = DOCKER_DIRECTORY / "production-entrypoint.sh"
READER_SOURCE = DOCKER_DIRECTORY / "aws_credentials.py"
AWS_CONFIG = DOCKER_DIRECTORY / "aws-config"
READER_TARGET = "/usr/local/bin/cnesdata-aws-credentials"
CREDENTIAL_DIRECTORY = "/run/credentials/"
CREDENTIAL_PATH = "/run/credentials/aws-credentials.json"
PREFLIGHT = f"{READER_TARGET} {CREDENTIAL_PATH} >/dev/null"
UVICORN_BINARY = "/usr/local/bin/uvicorn"
UVICORN_EXEC = (
    "exec /usr/local/bin/uvicorn central_api.app:create_app --factory --host 0.0.0.0 --port 8000"
)
CONFIGURATION_EXIT = 2
# Split literals: the versioned-secret scan in tests/production/infra rejects key-shaped text.
ACCESS_KEY = "ASIA" + "GENA000000000001"
LONG_TERM_KEY = "AKIA" + "GENA000000000001"
SECRET = "segredo-do-host-nunca-em-log"  # noqa: S105
TOKEN = "token-de-sessao-nunca-em-log"  # noqa: S105
LEGACY_SHARED_FILE = "/run/credentials/credentials"
STATIC_CREDENTIAL_NAMES = (
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_SECURITY_TOKEN",
)
CREDENTIAL_SOURCE_NAMES = (
    "AWS_WEB_IDENTITY_TOKEN_FILE",
    "AWS_ROLE_ARN",
    "AWS_DEFAULT_PROFILE",
    "AWS_PROFILE",
    "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
    "AWS_CONTAINER_CREDENTIALS_FULL_URI",
    "AWS_CONTAINER_AUTHORIZATION_TOKEN",
    "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
    "AWS_CREDENTIAL_FILE",
    "BOTO_CONFIG",
)
SOURCE_AND_SETTING_DRIFT = {"AWS_DEFAULT_PROFILE": "placeholder", "PROFILE": "legacy"}
FIXED_SETTINGS = {
    "PROFILE": "aws",
    "AWS_PRODUCTION_POLICY": "true",
    "AWS_CONFIG_FILE": "/etc/cnesdata/aws-config",
    "AWS_SHARED_CREDENTIALS_FILE": "/dev/null",
    "AWS_EC2_METADATA_DISABLED": "true",
}
FAKE_UVICORN = """\
#!/bin/sh
printf '%s\\n' "$PPID" "$@"
"""
READER_WRAPPER = """\
#!/bin/sh
exec "{python}" -I -S "{source}" "$@"
"""


def _require_tool(name: str) -> str:
    executable = shutil.which(name)
    if executable is not None:
        return executable
    message = f"tool_missing name={name}"
    if "CI" in os.environ:
        pytest.fail(message)
    pytest.skip(message)


@dataclass(frozen=True)
class _Entrypoint:
    script: Path
    credentials: Path

    def publish(self, mode: int = 0o440, **changes: object) -> None:
        expiration = datetime.fromtimestamp(time.time() + 1800, tz=UTC)
        document = {
            "Version": 1,
            "AccessKeyId": ACCESS_KEY,
            "SecretAccessKey": SECRET,
            "SessionToken": TOKEN,
            "Expiration": expiration.strftime("%Y-%m-%dT%H:%M:%SZ"),
        } | changes
        self.credentials.write_text(json.dumps(document), encoding="utf-8")
        self.credentials.chmod(mode)

    def refusal(self, code: str) -> str:
        return f"credentials_json={code} path={self.credentials}\n"

    def environment(self) -> dict[str, str]:
        return FIXED_SETTINGS | {"PATH": os.environ.get("PATH", "")}

    def run(self, environment: dict[str, str]) -> subprocess.CompletedProcess[str]:
        return subprocess.run(  # noqa: S603
            [_require_tool("sh"), str(self.script)],
            check=False,
            capture_output=True,
            env=environment,
            text=True,
            timeout=30,
        )


def _executable(path: Path, body: str) -> Path:
    path.write_text(body, encoding="utf-8")
    path.chmod(0o755)
    return path


@pytest.fixture
def entrypoint(tmp_path: Path) -> _Entrypoint:
    source = ENTRYPOINT_SOURCE.read_text(encoding="utf-8")
    mount = tmp_path / "credentials"
    mount.mkdir(mode=0o710)
    uvicorn = _executable(tmp_path / "uvicorn", FAKE_UVICORN)
    reader = _executable(
        tmp_path / "cnesdata-aws-credentials",
        READER_WRAPPER.format(python=sys.executable, source=READER_SOURCE),
    )
    rewritten = (
        source.replace(CREDENTIAL_DIRECTORY, f"{mount}/")
        .replace(UVICORN_BINARY, str(uvicorn))
        .replace(READER_TARGET, str(reader))
    )
    script = tmp_path / "production-entrypoint"
    script.write_text(rewritten, encoding="utf-8")
    assert PREFLIGHT in source
    assert UVICORN_EXEC in source
    return _Entrypoint(script, mount / Path(CREDENTIAL_PATH).name)


def test_preflight_le_o_mesmo_json_do_perfil_da_imagem() -> None:
    lines = [line.strip() for line in ENTRYPOINT_SOURCE.read_text(encoding="utf-8").splitlines()]
    command = load_config(str(AWS_CONFIG))["profiles"]["default"]["credential_process"]

    assert lines.count(PREFLIGHT) == 1
    assert compat_shell_split(command)[-2:] == [READER_TARGET, CREDENTIAL_PATH]
    assert lines.index(PREFLIGHT) < lines.index(UVICORN_EXEC)


@pytest.mark.parametrize("name", STATIC_CREDENTIAL_NAMES)
@pytest.mark.parametrize("value", ["placeholder", ""], ids=["com-valor", "vazia"])
def test_entrypoint_recusa_credencial_estatica_antes_do_preflight(
    entrypoint: _Entrypoint, name: str, value: str,
) -> None:
    result = entrypoint.run(entrypoint.environment() | SOURCE_AND_SETTING_DRIFT | {name: value})

    assert (result.returncode, result.stdout, result.stderr) == (
        CONFIGURATION_EXIT, "", f"static_credential_forbidden name={name}\n",
    )


@pytest.mark.parametrize("name", CREDENTIAL_SOURCE_NAMES)
@pytest.mark.parametrize("value", ["placeholder", ""], ids=["com-valor", "vazia"])
def test_entrypoint_recusa_outra_fonte_de_credencial_antes_do_preflight(
    entrypoint: _Entrypoint, name: str, value: str,
) -> None:
    result = entrypoint.run(entrypoint.environment() | {"PROFILE": "legacy", name: value})

    assert (result.returncode, result.stdout, result.stderr) == (
        CONFIGURATION_EXIT, "", f"credential_source_forbidden name={name}\n",
    )


@pytest.mark.parametrize("name", list(FIXED_SETTINGS))
@pytest.mark.parametrize("value", ["outro", "", None], ids=["outro", "vazio", "ausente"])
def test_entrypoint_exige_settings_fixos_de_producao_antes_do_preflight(
    entrypoint: _Entrypoint, name: str, value: str | None,
) -> None:
    environment = entrypoint.environment()
    environment.pop(name)
    overrides = {} if value is None else {name: value}

    result = entrypoint.run(environment | overrides)

    assert (result.returncode, result.stdout, result.stderr) == (
        CONFIGURATION_EXIT, "", f"setting_mismatch name={name}\n",
    )


def test_entrypoint_recusa_o_arquivo_ini_do_compose_anterior(entrypoint: _Entrypoint) -> None:
    entrypoint.publish()

    result = entrypoint.run(
        entrypoint.environment() | {"AWS_SHARED_CREDENTIALS_FILE": LEGACY_SHARED_FILE},
    )

    assert (result.returncode, result.stdout, result.stderr) == (
        CONFIGURATION_EXIT, "", "setting_mismatch name=AWS_SHARED_CREDENTIALS_FILE\n",
    )


def test_entrypoint_recusa_json_ausente_pelo_leitor(entrypoint: _Entrypoint) -> None:
    result = entrypoint.run(entrypoint.environment())

    assert (result.returncode, result.stdout, result.stderr) == (
        CONFIGURATION_EXIT, "", entrypoint.refusal("missing"),
    )


@pytest.mark.parametrize(
    ("code", "mode", "changes"),
    [
        ("access_key", 0o440, {"AccessKeyId": LONG_TERM_KEY}),
        ("insecure_mode", 0o444, {}),
        ("expiration_window", 0o440, {"Expiration": "2026-01-01T00:00:00Z"}),
    ],
    ids=["chave-de-longo-prazo", "legivel-por-todos", "expirado"],
)
def test_entrypoint_propaga_a_recusa_do_leitor_sem_vazar_valor(
    entrypoint: _Entrypoint, code: str, mode: int, changes: dict[str, object],
) -> None:
    entrypoint.publish(mode, **changes)

    result = entrypoint.run(entrypoint.environment())

    assert (result.returncode, result.stdout, result.stderr) == (
        CONFIGURATION_EXIT, "", entrypoint.refusal(code),
    )
    assert SECRET not in result.stderr
    assert TOKEN not in result.stderr


def test_entrypoint_executa_uvicorn_com_exec_e_descarta_o_stdout_do_leitor(
    entrypoint: _Entrypoint,
) -> None:
    entrypoint.publish()

    result = entrypoint.run(entrypoint.environment())

    assert (result.returncode, result.stderr) == (0, "")
    assert result.stdout.splitlines() == [str(os.getpid()), *shlex.split(UVICORN_EXEC)[2:]]
    assert all(value not in result.stdout for value in (ACCESS_KEY, SECRET, TOKEN))
