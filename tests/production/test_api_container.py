"""Contrato estático da imagem de produção do API: estágios, helper pinado e entrypoint."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shlex
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import urlsplit

import pytest
import yaml
from botocore.compat import compat_shell_split
from botocore.configloader import load_config

REPO_ROOT = Path(__file__).resolve().parents[2]
DOCKERFILE = REPO_ROOT / "apps" / "central_api" / "Dockerfile"
ENTRYPOINT_PATH = "apps/central_api/docker/production-entrypoint.sh"
ENTRYPOINT_SOURCE = REPO_ROOT / ENTRYPOINT_PATH
COMPOSE_PATH = REPO_ROOT / "deploy" / "compose.production.yaml"
AWS_CONFIG_SOURCE = REPO_ROOT / "deploy" / "aws" / "config"
GITATTRIBUTES = REPO_ROOT / ".gitattributes"
ENTRYPOINT_ATTRIBUTES = "apps/central_api/docker/*.sh text eol=lf"
DEVELOP_DOCKERFILE_SHA256 = "2d537a3fd87370d320f2db14d23cb41afdc3358223d8a6098ddde6406491d08a"
FINAL_STAGE_SHA256 = "115fb7d91c00c857f278d2f53fa443788cedfd811a1b94d004000bcfcb750142"
BASE_IMAGE = "python:3.13-slim"
RUNTIME_ID = 10001
MAX_COLUMNS = 100
HELPER_VERSION = "1.8.5"
HELPER_SHA256 = "beec9ed1c492d93db809890f16713e3556353294b823c2184ad4e891f1b2b54d"
HELPER_PATH = "/opt/aws/aws_signing_helper"
HELPER_INSTALL = [
    "mkdir -p /opt/aws",
    'release="https://rolesanywhere.amazonaws.com/releases/${AWS_SIGNING_HELPER_VERSION}"',
    (
        'python -c "import sys, urllib.request;'
        ' urllib.request.urlretrieve(sys.argv[1], sys.argv[2])"'
        ' "${release}/X86_64/Linux/Amzn2023/aws_signing_helper" /opt/aws/aws_signing_helper'
    ),
    (
        'echo "${AWS_SIGNING_HELPER_SHA256}  /opt/aws/aws_signing_helper"'
        " | sha256sum --check --strict -"
    ),
    "chmod 0555 /opt/aws/aws_signing_helper",
]
HELPER_TIMEOUT = ["/usr/bin/timeout", "60"]
ENTRYPOINT_TARGET = "/usr/local/bin/production-entrypoint"
CONFIG_PATH = "/etc/cnesdata/aws/config"
CREDENTIAL_DIRECTORY = "/etc/cnesdata/aws/"
LEAF_CERTIFICATE = "/etc/cnesdata/aws/leaf-certificate.pem"
LEAF_PRIVATE_KEY = "/etc/cnesdata/aws/leaf-private-key.pem"
UVICORN_BINARY = "/usr/local/bin/uvicorn"
UVICORN_EXEC = (
    "exec /usr/local/bin/uvicorn central_api.app:create_app --factory --host 0.0.0.0 --port 8000"
)
CONFIGURATION_EXIT = 2
STATIC_CREDENTIAL_NAMES = (
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_SECURITY_TOKEN",
)
CREDENTIAL_SOURCE_NAMES = ("AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_ROLE_ARN", "AWS_DEFAULT_PROFILE")
SOURCE_AND_SETTING_DRIFT = {"AWS_DEFAULT_PROFILE": "placeholder", "PROFILE": "legacy"}
FIXED_SETTINGS = {
    "PROFILE": "aws",
    "AWS_PRODUCTION_POLICY": "true",
    "AWS_PROFILE": "cnesdata-production",
    "AWS_CONFIG_FILE": CONFIG_PATH,
    "AWS_SHARED_CREDENTIALS_FILE": "/dev/null",
}
HEALTHCHECK = (
    "--interval=15s --timeout=15s --retries=3 --start-period=60s CMD python -c "
    '"import sys, urllib.request; urllib.request.urlopen(sys.argv[1])" '
    "http://localhost:8000/api/v1/system/health"
)
COMPOSE_HEALTH_URL = re.compile(r"http://[^']+")
CREDENTIAL_FILES = {
    "aws_config": "config",
    "leaf_certificate": "leaf-certificate.pem",
    "leaf_private_key": "leaf-private-key.pem",
}
PROVISIONED_MODES = {"aws_config": 0o644, "leaf_certificate": 0o644, "leaf_private_key": 0o640}
FAKE_UVICORN = """\
#!/bin/sh
printf '%s\\n' "$PPID" "$@"
"""
FROM_LINE = re.compile(r"^FROM\s", re.IGNORECASE)
STAGE_ALIAS = re.compile(r"^(?P<base>\S+)(?:\s+AS\s+(?P<name>\S+))?$", re.IGNORECASE)
LEAF_ASSIGNMENT = re.compile(r"^(LEAF_[A-Z_]+)=(\S+)$", re.MULTILINE)
BASHISMS = re.compile(r"\[\[|\bfunction\b|\bsource\b|\$'|<<<|\beval\b|`")


@dataclass(frozen=True)
class Instruction:
    keyword: str
    arguments: str


@dataclass(frozen=True)
class Stage:
    name: str | None
    base: str
    text: str
    instructions: tuple[Instruction, ...]

    def arguments(self, keyword: str) -> list[str]:
        return [item.arguments for item in self.instructions if item.keyword == keyword]


def _blocks(text: str) -> list[str]:
    blocks: list[list[str]] = []
    for line in text.splitlines(keepends=True):
        if FROM_LINE.match(line) or not blocks:
            blocks.append([])
        blocks[-1].append(line)
    return ["".join(block) for block in blocks]


def _logical_lines(block: str) -> list[str]:
    logical: list[str] = []
    pending = ""
    for raw in block.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.endswith("\\"):
            pending += line[:-1].strip() + " "
            continue
        logical.append(pending + line)
        pending = ""
    return logical


def _stage(block: str) -> Stage:
    instructions = tuple(
        Instruction(keyword.upper(), arguments)
        for keyword, _, arguments in (line.partition(" ") for line in _logical_lines(block))
    )
    alias = STAGE_ALIAS.fullmatch(instructions[0].arguments)
    assert alias is not None
    return Stage(alias["name"], alias["base"], block, instructions)


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


@pytest.fixture
def stages() -> list[Stage]:
    return [_stage(block) for block in _blocks(DOCKERFILE.read_text(encoding="utf-8"))]


@pytest.fixture
def production(stages: list[Stage]) -> Stage:
    matches = [stage for stage in stages if stage.name == "production"]
    assert len(matches) == 1
    return matches[0]


def _compose_api() -> dict[str, Any]:
    document = yaml.safe_load(COMPOSE_PATH.read_text(encoding="utf-8"))
    return document["services"]["central-api"]


def _key_values(arguments: list[str]) -> dict[str, str]:
    pairs = (token.partition("=") for argument in arguments for token in shlex.split(argument))
    return {key: value for key, _, value in pairs}


def _commands(production: Stage, marker: str) -> list[str]:
    runs = [run for run in production.arguments("RUN") if marker in run]
    assert len(runs) == 1
    return [command.strip() for command in runs[0].split("&&")]


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

    def path(self, name: str) -> Path:
        return self.credentials / CREDENTIAL_FILES[name]

    def refusal(self, code: str, name: str) -> str:
        return f"{code} name={name} path={self.path(name)}\n"

    def provision(self, *names: str) -> None:
        for name in names:
            self.path(name).write_text("placeholder\n", encoding="utf-8")
            self.path(name).chmod(PROVISIONED_MODES[name])

    def environment(self) -> dict[str, str]:
        return FIXED_SETTINGS | {
            "PATH": os.environ.get("PATH", ""),
            "AWS_CONFIG_FILE": str(self.path("aws_config")),
        }

    def run(self, environment: dict[str, str]) -> subprocess.CompletedProcess[str]:
        return subprocess.run(  # noqa: S603
            [_require_tool("sh"), str(self.script)],
            check=False,
            capture_output=True,
            env=environment,
            text=True,
            timeout=30,
        )


@pytest.fixture
def entrypoint(tmp_path: Path) -> _Entrypoint:
    source = ENTRYPOINT_SOURCE.read_text(encoding="utf-8")
    credentials = tmp_path / "credentials"
    credentials.mkdir()
    uvicorn = tmp_path / "uvicorn"
    uvicorn.write_text(FAKE_UVICORN, encoding="utf-8")
    uvicorn.chmod(0o755)
    script = tmp_path / "production-entrypoint"
    rewritten = source.replace(CREDENTIAL_DIRECTORY, f"{credentials}/")
    script.write_text(rewritten.replace(UVICORN_BINARY, str(uvicorn)), encoding="utf-8")
    assert CREDENTIAL_DIRECTORY in source
    assert UVICORN_BINARY in source
    return _Entrypoint(script, credentials)


def test_dockerfile_insere_production_antes_do_estagio_final(stages: list[Stage]) -> None:
    final = stages[-1]

    assert [stage.name for stage in stages] == ["builder", "production", None]
    assert stages[-2].base == final.base == BASE_IMAGE
    assert all("--from=production" not in argument for argument in final.arguments("COPY"))


def test_estagio_de_producao_roda_sem_root_e_read_only(production: Stage) -> None:
    runs = " ".join(production.arguments("RUN"))
    compose_api = _compose_api()

    assert production.arguments("USER") == [f"{RUNTIME_ID}:{RUNTIME_ID}"]
    assert f"--gid {RUNTIME_ID}" in runs
    assert f"--uid {RUNTIME_ID}" in runs
    assert "chown" not in runs
    assert not production.arguments("VOLUME")
    assert compose_api["user"] == production.arguments("USER")[0]
    assert compose_api["read_only"] is True


def test_estagio_de_producao_usa_perfil_roles_anywhere(production: Stage) -> None:
    entrypoints = [json.loads(argument) for argument in production.arguments("ENTRYPOINT")]

    assert _key_values(production.arguments("ENV")) == FIXED_SETTINGS
    assert entrypoints == [[ENTRYPOINT_TARGET]]
    assert (
        f"--chmod=0555 apps/central_api/docker/production-entrypoint.sh {ENTRYPOINT_TARGET}"
        in production.arguments("COPY")
    )


def test_helper_baixado_por_https_com_sha256_pinado_e_verificado(production: Stage) -> None:
    build_args = _key_values(production.arguments("ARG"))

    assert build_args["AWS_SIGNING_HELPER_VERSION"] == HELPER_VERSION
    assert build_args["AWS_SIGNING_HELPER_SHA256"] == HELPER_SHA256
    assert _commands(production, HELPER_PATH) == HELPER_INSTALL


def test_dockerfile_nao_usa_add(stages: list[Stage]) -> None:
    assert all(not stage.arguments("ADD") for stage in stages)


def test_imagem_de_producao_nao_carrega_chave_estatica() -> None:
    dockerfile = DOCKERFILE.read_text(encoding="utf-8")
    mentions = [
        line
        for line in ENTRYPOINT_SOURCE.read_text(encoding="utf-8").splitlines()
        if any(name in line for name in STATIC_CREDENTIAL_NAMES)
    ]

    assert not any(name in dockerfile for name in STATIC_CREDENTIAL_NAMES)
    assert mentions == [
        f'forbid_static_credential {name} "${{{name}+set}}"' for name in STATIC_CREDENTIAL_NAMES
    ]


def test_target_default_inalterado(stages: list[Stage]) -> None:
    default_target = "".join(stage.text for stage in stages if stage.name != "production")

    assert stages[-1].name is None
    assert _sha256(stages[-1].text) == FINAL_STAGE_SHA256
    assert _sha256(default_target) == DEVELOP_DOCKERFILE_SHA256


def test_arquivos_de_producao_cabem_em_100_colunas(production: Stage) -> None:
    texts = (
        production.text,
        ENTRYPOINT_SOURCE.read_text(encoding="utf-8"),
        COMPOSE_PATH.read_text(encoding="utf-8"),
    )

    assert [line for text in texts for line in text.splitlines() if len(line) > MAX_COLUMNS] == []


def test_healthcheck_de_producao_usa_porta_rota_e_tempos_do_compose(production: Stage) -> None:
    compose = _compose_api()["healthcheck"]
    [healthcheck] = production.arguments("HEALTHCHECK")
    [compose_target] = COMPOSE_HEALTH_URL.findall(compose["test"][-1])
    image_url = urlsplit(shlex.split(healthcheck)[-1])
    compose_url = urlsplit(compose_target)
    timings = {f"--{key.replace('_', '-')}={value}" for key, value in compose.items()}

    assert healthcheck == HEALTHCHECK
    assert (image_url.port, image_url.path) == (compose_url.port, compose_url.path)
    assert UVICORN_EXEC.endswith(f"--port {image_url.port}")
    assert timings - {f"--test={compose['test']}"} <= set(healthcheck.split())


def test_config_mounts_entrypoint_e_env_apontam_os_mesmos_paths(production: Stage) -> None:
    profile = load_config(str(AWS_CONFIG_SOURCE))["profiles"]["cnesdata-production"]
    argv = compat_shell_split(profile["credential_process"])
    leaf = {flag: argv[argv.index(flag) + 1] for flag in ("--certificate", "--private-key")}
    installed = _commands(production, HELPER_PATH)[-1].removeprefix("chmod 0555 ")
    mounts = {volume["target"] for volume in _compose_api()["volumes"]}
    assigned = dict(LEAF_ASSIGNMENT.findall(ENTRYPOINT_SOURCE.read_text(encoding="utf-8")))
    config_file = _key_values(production.arguments("ENV"))["AWS_CONFIG_FILE"]

    assert argv[: len(HELPER_TIMEOUT) + 1] == [*HELPER_TIMEOUT, installed]
    assert mounts == {config_file, *leaf.values()}
    assert assigned == {
        "LEAF_CERTIFICATE": leaf["--certificate"],
        "LEAF_PRIVATE_KEY": leaf["--private-key"],
    }


def test_entrypoint_de_producao_e_posix_e_termina_com_exec_do_uvicorn(
    stages: list[Stage],
) -> None:
    source = ENTRYPOINT_SOURCE.read_text(encoding="utf-8")
    lines = [line.strip() for line in source.splitlines() if line.strip()]
    default_entrypoint = json.loads(stages[-1].arguments("ENTRYPOINT")[0])
    exec_argv = shlex.split(UVICORN_EXEC)[1:]

    assert lines[0] == "#!/bin/sh"
    assert "set -eu" in lines
    assert lines[-1] == UVICORN_EXEC
    assert [Path(exec_argv[0]).name, *exec_argv[1:]] == default_entrypoint
    assert LEAF_CERTIFICATE in source
    assert LEAF_PRIVATE_KEY in source
    assert '"${AWS_CONFIG_FILE:-}"' in source
    assert not BASHISMS.search(source)


def test_entrypoint_versionado_com_lf_mesmo_em_checkout_windows() -> None:
    attributes = GITATTRIBUTES.read_text(encoding="utf-8").splitlines()
    pattern, *_ = ENTRYPOINT_ATTRIBUTES.split()

    effective = subprocess.run(  # noqa: S603
        [_require_tool("git"), "-c", "safe.directory=*", "check-attr", "eol", "text", "--",
         ENTRYPOINT_PATH],
        check=True,
        capture_output=True,
        cwd=REPO_ROOT,
        text=True,
        timeout=30,
    )

    assert b"\r" not in ENTRYPOINT_SOURCE.read_bytes()
    assert ENTRYPOINT_ATTRIBUTES in attributes
    assert PurePosixPath(ENTRYPOINT_PATH).full_match(pattern)
    assert effective.stdout.splitlines() == [
        f"{ENTRYPOINT_PATH}: eol: lf",
        f"{ENTRYPOINT_PATH}: text: set",
    ]


@pytest.mark.parametrize("name", STATIC_CREDENTIAL_NAMES)
@pytest.mark.parametrize("value", ["placeholder", ""], ids=["com-valor", "vazia"])
def test_entrypoint_recusa_credencial_estatica_antes_de_checar_arquivos(
    entrypoint: _Entrypoint, name: str, value: str,
) -> None:
    result = entrypoint.run(entrypoint.environment() | SOURCE_AND_SETTING_DRIFT | {name: value})

    assert (result.returncode, result.stdout, result.stderr) == (
        CONFIGURATION_EXIT, "", f"static_credential_forbidden name={name}\n",
    )


@pytest.mark.parametrize("name", CREDENTIAL_SOURCE_NAMES)
@pytest.mark.parametrize("value", ["placeholder", ""], ids=["com-valor", "vazia"])
def test_entrypoint_recusa_outra_fonte_de_credencial_antes_de_checar_arquivos(
    entrypoint: _Entrypoint, name: str, value: str,
) -> None:
    result = entrypoint.run(entrypoint.environment() | {"PROFILE": "legacy", name: value})

    assert (result.returncode, result.stdout, result.stderr) == (
        CONFIGURATION_EXIT, "", f"credential_source_forbidden name={name}\n",
    )


@pytest.mark.parametrize("name", list(FIXED_SETTINGS))
@pytest.mark.parametrize("value", ["outro", "", None], ids=["outro", "vazio", "ausente"])
def test_entrypoint_exige_settings_fixos_de_producao_antes_de_checar_arquivos(
    entrypoint: _Entrypoint, name: str, value: str | None,
) -> None:
    environment = entrypoint.environment()
    environment.pop(name)
    overrides = {} if value is None else {name: value}

    result = entrypoint.run(environment | overrides)

    assert (result.returncode, result.stdout, result.stderr) == (
        CONFIGURATION_EXIT, "", f"setting_mismatch name={name}\n",
    )


@pytest.mark.parametrize("missing", list(CREDENTIAL_FILES))
def test_entrypoint_recusa_cada_arquivo_ausente(entrypoint: _Entrypoint, missing: str) -> None:
    entrypoint.provision(*(name for name in CREDENTIAL_FILES if name != missing))

    result = entrypoint.run(entrypoint.environment())

    assert (result.returncode, result.stdout, result.stderr) == (
        CONFIGURATION_EXIT, "", entrypoint.refusal("credential_file_missing", missing),
    )


def test_entrypoint_recusa_aws_config_file_que_e_diretorio(entrypoint: _Entrypoint) -> None:
    entrypoint.provision("leaf_certificate", "leaf_private_key")
    entrypoint.path("aws_config").mkdir()

    result = entrypoint.run(entrypoint.environment())

    assert (result.returncode, result.stdout, result.stderr) == (
        CONFIGURATION_EXIT, "", entrypoint.refusal("credential_file_missing", "aws_config"),
    )


@pytest.mark.skipif(
    not hasattr(os, "geteuid") or os.geteuid() == 0,
    reason="unreadable_requires_non_root covered_by=prod_012_docker_smoke",
)
@pytest.mark.parametrize("name", list(CREDENTIAL_FILES))
def test_entrypoint_recusa_arquivo_ilegivel(entrypoint: _Entrypoint, name: str) -> None:
    entrypoint.provision(*CREDENTIAL_FILES)
    entrypoint.path(name).chmod(0)

    result = entrypoint.run(entrypoint.environment())

    assert (result.returncode, result.stdout, result.stderr) == (
        CONFIGURATION_EXIT, "", entrypoint.refusal("credential_file_unreadable", name),
    )


@pytest.mark.parametrize("mode", [0o644, 0o604], ids=["0644", "0604"])
def test_entrypoint_recusa_chave_privada_legivel_por_todos(
    entrypoint: _Entrypoint, mode: int,
) -> None:
    entrypoint.provision(*CREDENTIAL_FILES)
    entrypoint.path("leaf_private_key").chmod(mode)

    result = entrypoint.run(entrypoint.environment())

    assert (result.returncode, result.stdout, result.stderr) == (
        CONFIGURATION_EXIT, "", entrypoint.refusal("credential_file_insecure", "leaf_private_key"),
    )


def test_entrypoint_executa_uvicorn_com_exec_e_argv_do_target_default(
    entrypoint: _Entrypoint, stages: list[Stage],
) -> None:
    default_entrypoint = json.loads(stages[-1].arguments("ENTRYPOINT")[0])
    entrypoint.provision(*CREDENTIAL_FILES)

    result = entrypoint.run(entrypoint.environment())

    assert (result.returncode, result.stderr) == (0, "")
    assert result.stdout.splitlines() == [str(os.getpid()), *default_entrypoint[1:]]
