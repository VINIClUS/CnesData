"""Porta de imagem real: docker com DOCKER_CONFIG efêmero, token só por stdin e sem vazamento."""

import subprocess
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

from tests.integration.aws_capability._fargate_images import DockerImages
from tests.integration.aws_capability._service_contracts import RegistryLogin
from tests.integration.aws_capability.contracts import (
    BudgetExhausted,
    Limits,
    RequestBudget,
    RequestContext,
    ServiceFailure,
)

TOKEN = "ECR_TOKEN_CANARY_do_not_log"  # noqa: S105
REGISTRY = "111111111111.dkr.ecr.us-east-2.amazonaws.com"
TARGET = f"{REGISTRY}/cnesdata-cap-a1b2c3d4:cap"
IMAGE = "cnesdata-cap-processor:current-develop"
DIGEST = "sha256:" + "ab" * 32
PUSH_OUTPUT = f"The push refers to repository\ncap: digest: {DIGEST} size: 1572\n"


@dataclass
class Call:
    argv: list[str]
    stdin: str | None
    env: dict[str, str]
    timeout: float | None
    config_existed: bool


@dataclass
class Runner:
    outputs: dict[str, tuple[int, str, str]] = field(default_factory=dict)
    calls: list[Call] = field(default_factory=list)

    def __call__(self, argv: list[str], **options: Any) -> subprocess.CompletedProcess[str]:
        env = dict(options.get("env") or {})
        stdin = options.get("input")
        if isinstance(stdin, bytes):
            stdin = stdin.decode()
        config = env.get("DOCKER_CONFIG")
        existed = config is not None and Path(config).is_dir()
        self.calls.append(Call(list(argv), stdin, env, options.get("timeout"), existed))
        default = (0, PUSH_OUTPUT if argv[1] == "push" else "", "")
        code, out, err = self.outputs.get(argv[1], default)
        return subprocess.CompletedProcess(argv, code, out, err)

    def verbs(self) -> list[str]:
        return [call.argv[1] for call in self.calls]


def make_context(remaining: float = 400.0) -> RequestContext:
    budget = RequestBudget(Limits(), 1000.0 + remaining, lambda: 1000.0)
    return RequestContext(budget, "work")


def login() -> RegistryLogin:
    return RegistryLogin(REGISTRY, "AWS", TOKEN)


def control(tmp_path: Path) -> None:
    port = DockerImages(run=Runner(), root=tmp_path / "control")
    (tmp_path / "control").mkdir()
    assert port.push(IMAGE, TARGET, login(), make_context()) == DIGEST


@pytest.fixture
def runner() -> Runner:
    return Runner()


@pytest.fixture
def port(runner: Runner, tmp_path: Path) -> DockerImages:
    return DockerImages(run=runner, root=tmp_path)


def test_publica_e_devolve_o_digest_impresso_pelo_push(port: DockerImages, runner: Runner) -> None:
    assert port.push(IMAGE, TARGET, login(), make_context()) == DIGEST
    assert runner.verbs()[:3] == ["login", "tag", "push"]
    assert set(runner.verbs()) <= {"login", "tag", "push", "rmi", "logout"}
    assert all(call.argv[0] == "docker" for call in runner.calls)


def test_tag_e_push_usam_a_imagem_local_e_o_alvo_exatos(
    port: DockerImages, runner: Runner
) -> None:
    port.push(IMAGE, TARGET, login(), make_context())
    by_verb = {call.argv[1]: call.argv for call in runner.calls}
    assert by_verb["tag"][2:4] == [IMAGE, TARGET]
    assert by_verb["push"][-1] == TARGET


def test_token_so_trafega_por_stdin_do_login(port: DockerImages, runner: Runner) -> None:
    port.push(IMAGE, TARGET, login(), make_context())
    logins = [call for call in runner.calls if call.argv[1] == "login"]
    assert len(logins) == 1
    assert (logins[0].stdin or "").strip() == TOKEN
    assert "--password-stdin" in logins[0].argv
    assert REGISTRY in logins[0].argv
    assert logins[0].argv[logins[0].argv.index("--username") + 1] == "AWS"
    for call in runner.calls:
        assert TOKEN not in " ".join(call.argv)
        assert all(TOKEN not in value for value in call.env.values())
        if call.argv[1] != "login":
            assert call.stdin in (None, "")


def test_nenhum_comando_recebe_credencial_aws_no_ambiente(
    port: DockerImages, runner: Runner, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("AWS_SESSION_TOKEN", "AWS_SESSION_CANARY")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "AWS_SECRET_CANARY")
    port.push(IMAGE, TARGET, login(), make_context())
    for call in runner.calls:
        assert not any(key.startswith("AWS_") for key in call.env)
        assert all("CANARY" not in value for value in call.env.values())


def test_docker_config_fica_sob_a_raiz_privada_e_some_ao_fim(
    port: DockerImages, runner: Runner, tmp_path: Path
) -> None:
    port.push(IMAGE, TARGET, login(), make_context())
    configs = {call.env["DOCKER_CONFIG"] for call in runner.calls}
    assert len(configs) == 1
    config = Path(configs.pop())
    assert config.parent == tmp_path
    assert all(call.config_existed for call in runner.calls)
    assert not config.exists()
    assert list(tmp_path.iterdir()) == []


def test_raiz_padrao_do_docker_config_e_a_memoria_compartilhada() -> None:
    assert DockerImages().root == Path("/dev/shm")  # noqa: S108


@pytest.mark.parametrize("verb", ["login", "tag", "push"])
def test_falha_de_qualquer_etapa_apaga_a_config_e_nao_vaza_o_token(
    runner: Runner, tmp_path: Path, verb: str
) -> None:
    control(tmp_path)
    runner.outputs[verb] = (1, f"saida {TOKEN}", f"erro ecoando {TOKEN} {REGISTRY}")
    port = DockerImages(run=runner, root=tmp_path)
    with pytest.raises(ServiceFailure) as error:
        port.push(IMAGE, TARGET, login(), make_context())
    text = "".join(traceback.format_exception(error.value))
    assert TOKEN not in text
    assert list(tmp_path.iterdir()) == []
    assert str(error.value).startswith("service=")


def test_falha_no_login_nao_tenta_tag_nem_push(runner: Runner, tmp_path: Path) -> None:
    runner.outputs["login"] = (1, "", "denied")
    with pytest.raises(ServiceFailure):
        DockerImages(run=runner, root=tmp_path).push(IMAGE, TARGET, login(), make_context())
    assert runner.verbs() == ["login"]


def test_push_sem_digest_na_saida_e_falha(runner: Runner, tmp_path: Path) -> None:
    control(tmp_path)
    runner.outputs["push"] = (0, "pushed ok sem digest\n", "")
    port = DockerImages(run=runner, root=tmp_path)
    with pytest.raises(ServiceFailure) as error:
        port.push(IMAGE, TARGET, login(), make_context())
    assert TOKEN not in str(error.value)
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("error", [subprocess.TimeoutExpired("docker", 5), OSError("boom")])
def test_excecao_do_processo_vira_falha_de_servico_sem_vazar_o_token(
    tmp_path: Path, error: BaseException
) -> None:
    control(tmp_path)

    def explode(argv: list[str], **options: Any) -> None:
        raise error

    port = DockerImages(run=explode, root=tmp_path)
    with pytest.raises(ServiceFailure) as raised:
        port.push(IMAGE, TARGET, login(), make_context())
    assert TOKEN not in "".join(traceback.format_exception(raised.value))
    assert list(tmp_path.iterdir()) == []


def test_timeout_de_cada_comando_respeita_o_tempo_restante_do_orcamento(
    port: DockerImages, runner: Runner
) -> None:
    port.push(IMAGE, TARGET, login(), make_context(remaining=100.0))
    assert runner.calls
    assert all(call.timeout is not None and 0 < call.timeout <= 100.0 for call in runner.calls)


def test_sem_tempo_restante_nao_executa_docker(port: DockerImages, runner: Runner) -> None:
    port.push(IMAGE, TARGET, login(), make_context())
    before = len(runner.calls)
    with pytest.raises((BudgetExhausted, ServiceFailure)):
        port.push(IMAGE, TARGET, login(), make_context(remaining=0.0))
    assert len(runner.calls) == before


def test_repr_da_porta_e_do_login_nao_expoe_o_token(port: DockerImages) -> None:
    assert port.push(IMAGE, TARGET, login(), make_context()) == DIGEST
    assert TOKEN not in repr(login()) + repr(port) + str(login())
