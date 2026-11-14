"""Contrato estático da imagem de produção do API: estágios, perfil AWS e compose do host."""

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

from scripts.promotion.build_host_archive import CREDENTIALS, render_compose

REPO_ROOT = Path(__file__).resolve().parents[2]
DOCKERFILE = REPO_ROOT / "apps" / "central_api" / "Dockerfile"
ENTRYPOINT_PATH = "apps/central_api/docker/production-entrypoint.sh"
ENTRYPOINT_SOURCE = REPO_ROOT / ENTRYPOINT_PATH
READER_PATH = "apps/central_api/docker/aws_credentials.py"
AWS_CONFIG_PATH = "apps/central_api/docker/aws-config"
COMPOSE_PATH = REPO_ROOT / "deploy" / "compose.production.yaml"
GITATTRIBUTES = REPO_ROOT / ".gitattributes"
LF_ATTRIBUTES = {
    ENTRYPOINT_PATH: "apps/central_api/docker/*.sh text eol=lf",
    READER_PATH: "apps/central_api/docker/*.py text eol=lf",
    AWS_CONFIG_PATH: "apps/central_api/docker/aws-config text eol=lf",
}
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
ENTRYPOINT_TARGET = "/usr/local/bin/production-entrypoint"
READER_TARGET = "/usr/local/bin/cnesdata-aws-credentials"
AWS_CONFIG_TARGET = "/etc/cnesdata/aws-config"
PRODUCTION_COPIES = (
    f"--chmod=0555 {ENTRYPOINT_PATH} {ENTRYPOINT_TARGET}",
    f"--chmod=0555 {READER_PATH} {READER_TARGET}",
    f"--chmod=0444 {AWS_CONFIG_PATH} {AWS_CONFIG_TARGET}",
)
AWS_CONFIG_DIRECTORY = "install -d -m 0755 /etc/cnesdata"
ROLES_ANYWHERE_DIRECTORY = "/etc/cnesdata/aws/"
HOST_IMAGE = "ghcr.io/viniclus/cnesdata/central_api@sha256:" + "0" * 64
UVICORN_EXEC = (
    "exec /usr/local/bin/uvicorn central_api.app:create_app --factory --host 0.0.0.0 --port 8000"
)
STATIC_CREDENTIAL_NAMES = (
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_SECURITY_TOKEN",
)
FIXED_SETTINGS = {
    "PROFILE": "aws",
    "AWS_PRODUCTION_POLICY": "true",
    "AWS_CONFIG_FILE": AWS_CONFIG_TARGET,
    "AWS_SHARED_CREDENTIALS_FILE": "/dev/null",
    "AWS_EC2_METADATA_DISABLED": "true",
}
HEALTHCHECK = (
    "--interval=15s --timeout=15s --retries=3 --start-period=60s CMD python -c "
    '"import sys, urllib.request; urllib.request.urlopen(sys.argv[1])" '
    "http://localhost:8000/api/v1/system/health"
)
COMPOSE_HEALTH_URL = re.compile(r"http://[^']+")
FROM_LINE = re.compile(r"^FROM\s", re.IGNORECASE)
STAGE_ALIAS = re.compile(r"^(?P<base>\S+)(?:\s+AS\s+(?P<name>\S+))?$", re.IGNORECASE)
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


def test_estagio_de_producao_fixa_o_perfil_aws_da_imagem(production: Stage) -> None:
    entrypoints = [json.loads(argument) for argument in production.arguments("ENTRYPOINT")]
    copies = production.arguments("COPY")

    assert _key_values(production.arguments("ENV")) == FIXED_SETTINGS
    assert entrypoints == [[ENTRYPOINT_TARGET]]
    assert [copy for copy in PRODUCTION_COPIES if copy in copies] == list(PRODUCTION_COPIES)


def test_diretorio_do_perfil_aws_e_criado_atravessavel_antes_do_copy(production: Stage) -> None:
    instructions = list(production.instructions)
    created = [
        index for index, item in enumerate(instructions)
        if item.keyword == "RUN" and item.arguments == AWS_CONFIG_DIRECTORY
    ]
    copied = [
        index for index, item in enumerate(instructions)
        if item.keyword == "COPY" and item.arguments.endswith(f" {AWS_CONFIG_TARGET}")
    ]

    assert len(created) == len(copied) == 1
    assert created[0] < copied[0]


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
        (REPO_ROOT / READER_PATH).read_text(encoding="utf-8"),
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


def test_compose_do_host_monta_o_diretorio_do_perfil_sem_environment(production: Stage) -> None:
    host = json.loads(render_compose(HOST_IMAGE, 18080))["services"]["api"]
    mounts = {volume["target"]: volume for volume in host["volumes"]}
    command = load_config(str(REPO_ROOT / AWS_CONFIG_PATH))["profiles"]["default"]
    credential = PurePosixPath(compat_shell_split(command["credential_process"])[-1])

    assert "environment" not in host
    assert CREDENTIALS[1] == str(credential.parent)
    assert mounts[CREDENTIALS[1]] == {
        "type": "bind", "source": CREDENTIALS[0], "target": CREDENTIALS[1], "read_only": True,
    }
    assert host["user"] == production.arguments("USER")[0]


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
    assert ROLES_ANYWHERE_DIRECTORY not in source
    assert READER_TARGET in source
    assert not BASHISMS.search(source)


@pytest.mark.parametrize(("path", "attribute"), list(LF_ATTRIBUTES.items()))
def test_arquivos_da_imagem_versionados_com_lf_mesmo_em_checkout_windows(
    path: str, attribute: str,
) -> None:
    attributes = GITATTRIBUTES.read_text(encoding="utf-8").splitlines()
    pattern, *_ = attribute.split()

    effective = subprocess.run(  # noqa: S603
        [_require_tool("git"), "-c", "safe.directory=*", "check-attr", "eol", "text", "--", path],
        check=True,
        capture_output=True,
        cwd=REPO_ROOT,
        text=True,
        timeout=30,
    )

    assert b"\r" not in (REPO_ROOT / path).read_bytes()
    assert attribute in attributes
    assert PurePosixPath(path).full_match(pattern)
    assert effective.stdout.splitlines() == [f"{path}: eol: lf", f"{path}: text: set"]
