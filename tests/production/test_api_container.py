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
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
DOCKERFILE = REPO_ROOT / "apps" / "central_api" / "Dockerfile"
ENTRYPOINT_SOURCE = REPO_ROOT / "apps" / "central_api" / "docker" / "production-entrypoint.sh"
COMPOSE_PATH = REPO_ROOT / "deploy" / "compose.production.yaml"
DEVELOP_DOCKERFILE_SHA256 = "2d537a3fd87370d320f2db14d23cb41afdc3358223d8a6098ddde6406491d08a"
FINAL_STAGE_SHA256 = "115fb7d91c00c857f278d2f53fa443788cedfd811a1b94d004000bcfcb750142"
BASE_IMAGE = "python:3.13-slim"
RUNTIME_ID = 10001
HELPER_VERSION = "1.8.5"
HELPER_SHA256 = "beec9ed1c492d93db809890f16713e3556353294b823c2184ad4e891f1b2b54d"
HELPER_PATH = "/opt/aws/aws_signing_helper"
HELPER_URL = (
    "https://rolesanywhere.amazonaws.com/releases/${AWS_SIGNING_HELPER_VERSION}"
    "/X86_64/Linux/Amzn2023/aws_signing_helper"
)
ENTRYPOINT_TARGET = "/usr/local/bin/production-entrypoint"
CONFIG_PATH = "/etc/cnesdata/aws/config"
LEAF_CERTIFICATE = "/etc/cnesdata/aws/leaf-certificate.pem"
LEAF_PRIVATE_KEY = "/etc/cnesdata/aws/leaf-private-key.pem"
UVICORN_EXEC = (
    "exec /usr/local/bin/uvicorn central_api.app:create_app --factory --host 0.0.0.0 --port 8000"
)
CONFIGURATION_EXIT = 2
STATIC_KEY_NAMES = ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN")
FROM_LINE = re.compile(r"^FROM\s", re.IGNORECASE)
STAGE_ALIAS = re.compile(r"^(?P<base>\S+)(?:\s+AS\s+(?P<name>\S+))?$", re.IGNORECASE)
SHA256_HEX = re.compile(r"[0-9a-f]{64}")
INSECURE_DOWNLOAD = re.compile(
    r"--no-check-certificate|--insecure|\s-k\s|CERT_NONE|_create_unverified_context|http://",
)
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


def _compose_api() -> dict[str, object]:
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


def _run_entrypoint(config_file: str | None) -> subprocess.CompletedProcess[str]:
    environment = {"PATH": os.environ.get("PATH", "")}
    if config_file is not None:
        environment["AWS_CONFIG_FILE"] = config_file
    return subprocess.run(  # noqa: S603
        [_require_tool("sh"), str(ENTRYPOINT_SOURCE)],
        check=False,
        capture_output=True,
        env=environment,
        text=True,
        timeout=30,
    )


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

    assert _key_values(production.arguments("ENV")) == {
        "AWS_PROFILE": "cnesdata-production",
        "AWS_CONFIG_FILE": CONFIG_PATH,
    }
    assert entrypoints == [[ENTRYPOINT_TARGET]]
    assert (
        f"--chmod=0555 apps/central_api/docker/production-entrypoint.sh {ENTRYPOINT_TARGET}"
        in production.arguments("COPY")
    )


def test_helper_baixado_por_https_com_sha256_pinado_e_verificado(production: Stage) -> None:
    build_args = _key_values(production.arguments("ARG"))
    commands = _commands(production, HELPER_PATH)
    download = next(index for index, command in enumerate(commands) if HELPER_URL in command)
    verify = next(index for index, command in enumerate(commands) if "sha256sum" in command)
    install = next(index for index, command in enumerate(commands) if "chmod" in command)

    assert build_args["AWS_SIGNING_HELPER_VERSION"] == HELPER_VERSION
    assert build_args["AWS_SIGNING_HELPER_SHA256"] == HELPER_SHA256
    assert SHA256_HEX.fullmatch(build_args["AWS_SIGNING_HELPER_SHA256"])
    assert download < verify < install
    assert "${AWS_SIGNING_HELPER_SHA256}" in commands[verify]
    assert HELPER_PATH in commands[verify]
    assert re.search(r"sha256sum (--check|-c)\b", commands[verify])
    assert commands[install] == f"chmod 0555 {HELPER_PATH}"
    assert not INSECURE_DOWNLOAD.search(" ".join(commands))


def test_dockerfile_nao_usa_add(stages: list[Stage]) -> None:
    assert all(not stage.arguments("ADD") for stage in stages)


def test_imagem_de_producao_nao_carrega_chave_estatica() -> None:
    sources = DOCKERFILE.read_text(encoding="utf-8") + ENTRYPOINT_SOURCE.read_text(
        encoding="utf-8",
    )

    assert not any(name in sources for name in STATIC_KEY_NAMES)


def test_target_default_inalterado(stages: list[Stage]) -> None:
    default_target = "".join(stage.text for stage in stages if stage.name != "production")

    assert stages[-1].name is None
    assert _sha256(stages[-1].text) == FINAL_STAGE_SHA256
    assert _sha256(default_target) == DEVELOP_DOCKERFILE_SHA256


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


@pytest.mark.parametrize(
    ("config_kind", "missing_name", "missing_path"),
    [
        pytest.param(None, "aws_config", "", id="sem-aws-config-file"),
        pytest.param("absent", "aws_config", None, id="config-inexistente"),
        pytest.param("directory", "aws_config", None, id="config-diretorio"),
        pytest.param("file", "leaf_certificate", LEAF_CERTIFICATE, id="sem-certificado"),
    ],
)
def test_entrypoint_falha_fechado_sem_config_ou_material_da_folha(
    tmp_path: Path, config_kind: str | None, missing_name: str, missing_path: str | None,
) -> None:
    config_file = None if config_kind is None else tmp_path / config_kind
    if config_kind == "directory":
        tmp_path.joinpath(config_kind).mkdir()
    if config_kind == "file":
        tmp_path.joinpath(config_kind).write_text("[profile cnesdata-production]\n")
    expected_path = str(config_file) if missing_path is None else missing_path

    result = _run_entrypoint(None if config_file is None else str(config_file))

    assert not Path(LEAF_CERTIFICATE).exists()
    assert result.returncode == CONFIGURATION_EXIT
    assert result.stdout == ""
    assert result.stderr == f"credential_file_missing name={missing_name} path={expected_path}\n"
