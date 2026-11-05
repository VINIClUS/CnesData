"""CAP_FARGATE_IMAGE: formato fechado, leitura só no run e recusa antes de qualquer mutação."""

import json
from dataclasses import replace
from pathlib import Path

import pytest

from tests.integration.aws_capability import _production as production
from tests.integration.aws_capability._b1_testing import ACCOUNT, operator_environment
from tests.integration.aws_capability._operator_inputs import (
    FARGATE_IMAGE_KEY,
    read_fargate_image,
)
from tests.integration.aws_capability._production_testing import environment
from tests.integration.aws_capability._safety import SafetyFailure
from tests.integration.aws_capability._testing import Lab
from tests.integration.aws_capability.contracts import RunSpec
from tests.integration.aws_capability.test_production_main import Console, arguments, runtime

DIGEST = "sha256:" + "a" * 64
VALID = [
    "cnesdata-cap-processor:current-develop",
    "cnesdata-cap-processor:volume",
    "processor",
    "registry.local:5000/team/processor:1.2.3",
    f"cnesdata-cap-processor@{DIGEST}",
    f"{ACCOUNT}.dkr.ecr.us-east-2.amazonaws.com/cnesdata-prod-processor@{DIGEST}",
]
INVALID = [
    None,
    "",
    " ",
    "   cnesdata-cap-processor:x",
    "cnesdata-cap-processor:x\n",
    "cnesdata-cap-processor:x ",
    "-v /:/host",
    "--privileged",
    "-cnesdata:x",
    "img;rm -rf /",
    "img && id",
    "img|cat",
    "$(id)",
    "`id`",
    "img:tag extra",
    "Img:Tag",
    "img:",
    ":tag",
    "img@sha256:short",
    "img@md5:" + "a" * 32,
    "user:secret@registry.local/img:1",
    "http://registry.local/img:1",
    "../img:1",
    "img:" + "a" * 129,
    "a" * 300,
]
SPEC_ARGS = ("--probe", "fargate-write")


def _read(value: str | None) -> str:
    env = {} if value is None else {FARGATE_IMAGE_KEY: value}
    spec = RunSpec(ACCOUNT, "us-east-2", "a1b2c3d4", 1060, Path("/x"), probe="fargate-write")
    return read_fargate_image(spec, env)


@pytest.mark.parametrize("image", VALID)
def test_aceita_referencia_de_imagem_no_formato_fechado(image: str) -> None:
    assert _read(image) == image


@pytest.mark.parametrize("image", INVALID, ids=repr)
def test_recusa_imagem_ausente_ou_fora_do_formato_com_motivo_fechado(image: str | None) -> None:
    with pytest.raises(SafetyFailure) as error:
        _read(image)
    assert error.value.reason == "fargate_image_invalid"


def test_recusa_nao_ecoa_o_valor_recebido() -> None:
    with pytest.raises(SafetyFailure) as error:
        _read("img;SEGREDO_NO_VALOR")
    assert "SEGREDO_NO_VALOR" not in str(error.value)
    assert error.value.reason == "fargate_image_invalid"


def _env(**extra: str) -> dict[str, str]:
    return {**environment(), **operator_environment(), **extra}


def test_run_sem_imagem_recusa_antes_de_montar_ports_e_de_qualquer_chamada(lab: Lab) -> None:
    console, built = Console(), []
    code = production.main(
        arguments(lab, "run", *SPEC_ARGS), _env(), runtime(lab, console, built)
    )
    document = json.loads(console.lines[0])
    assert code != 0
    assert (document["status"], document["reason"]) == ("refused", "fargate_image_invalid")
    assert built == []
    assert lab.events == []


@pytest.mark.parametrize("image", ["-v /:/host", "img;rm", "Img:Tag", ""])
def test_run_com_imagem_invalida_recusa_antes_de_montar_ports(lab: Lab, image: str) -> None:
    console, built = Console(), []
    values = _env(**{FARGATE_IMAGE_KEY: image})
    production.main(arguments(lab, "run", *SPEC_ARGS), values, runtime(lab, console, built))
    document = json.loads(console.lines[0])
    assert document["reason"] == "fargate_image_invalid"
    assert built == []


def test_run_com_imagem_valida_chega_a_montar_ports(lab: Lab) -> None:
    console, built = Console(), []
    values = _env(**{FARGATE_IMAGE_KEY: VALID[0]})
    production.main(arguments(lab, "run", *SPEC_ARGS), values, runtime(lab, console, built))
    assert [spec.probe for spec in built] == ["fargate-write"]
    assert json.loads(console.lines[0])["reason"] != "fargate_image_invalid"


@pytest.mark.parametrize("command", ["janitor", "monitor"])
def test_janitor_e_monitor_nao_dependem_da_imagem(lab: Lab, command: str) -> None:
    console, built = Console(), []
    extra = ("--worker-pid", "4321") if command == "monitor" else ()
    argv = arguments(lab, command, *SPEC_ARGS, *extra)
    production.main(argv, _env(), runtime(lab, console, built))
    assert [spec.probe for spec in built] == ["fargate-write"]
    assert json.loads(console.lines[0])["reason"] != "fargate_image_invalid"


@pytest.mark.parametrize("probe", ["s3-hold", "iam-trust", "all"])
def test_outros_perfis_ignoram_a_imagem_mesmo_invalida(lab: Lab, probe: str) -> None:
    console, built = Console(), []
    values = _env(**{FARGATE_IMAGE_KEY: "img;rm"})
    production.main(
        arguments(lab, "run", "--probe", probe), values, runtime(lab, console, built)
    )
    assert [spec.probe for spec in built] == [probe]


def test_build_ports_do_fargate_guarda_a_imagem_e_monta_a_porta_de_imagem(lab: Lab) -> None:
    spec = replace(lab.spec, probe="fargate-write")
    ports = production.build_ports(spec, _env(**{FARGATE_IMAGE_KEY: VALID[0]}),
                                   lab.processes, lambda: lab.time)
    service_ports = ports.services.ports  # type: ignore[attr-defined]
    assert service_ports.image == VALID[0]
    assert service_ports.images is not None


def test_build_ports_do_fargate_sem_imagem_nao_recusa_para_servir_o_janitor(lab: Lab) -> None:
    spec = replace(lab.spec, probe="fargate-write")
    ports = production.build_ports(spec, _env(), lab.processes, lambda: lab.time)
    assert ports.services.ports.image is None  # type: ignore[attr-defined]


@pytest.mark.parametrize(
    ("changes", "reason"),
    [
        ({"AWS_PROFILE": "root"}, "profile_credentials_forbidden"),
        ({"AWS_DEFAULT_PROFILE": "root"}, "profile_credentials_forbidden"),
        ({"AWS_SESSION_TOKEN": ""}, "session_credentials_required"),
    ],
)
def test_run_do_fargate_continua_recusando_perfil_e_credencial_sem_sessao(
    lab: Lab, changes: dict[str, str], reason: str
) -> None:
    console, built = Console(), []
    values = _env(**{FARGATE_IMAGE_KEY: VALID[0]}) | changes
    production.main(arguments(lab, "run", *SPEC_ARGS), values, runtime(lab, console, built))
    assert json.loads(console.lines[0])["reason"] == reason
    assert built == []
