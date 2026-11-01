"""Portas de produção: processos, credenciais, ports reais e laço do supervisor."""

import io
import subprocess
from dataclasses import replace
from typing import Any

import pytest

from tests.integration.aws_capability import _production as production
from tests.integration.aws_capability._production_testing import (
    ClosedOut,
    FakeChild,
    FakeClock,
    FakePopen,
    FakeSelector,
    FakeSystem,
    environment,
)
from tests.integration.aws_capability._profiles import PROFILES
from tests.integration.aws_capability._safety import SafetyFailure
from tests.integration.aws_capability._testing import Lab
from tests.integration.aws_capability.services import AwsServices


def processes(
    system: FakeSystem | None = None, popen: FakePopen | None = None, out: Any = None
) -> tuple[production.SystemProcesses, FakePopen, FakeSystem, FakeSelector, io.StringIO]:
    system = system or FakeSystem()
    popen = popen or FakePopen()
    selector = FakeSelector(system)
    stream = io.StringIO()
    result = production.SystemProcesses(
        popen=popen,
        system=system,
        selector=selector,
        clock=FakeClock(step=0.5),
        out=out or stream,
    )
    return result, popen, system, selector, stream


def started(system: FakeSystem | None = None) -> tuple[production.SystemProcesses, FakeSystem]:
    boundary, _, system, _, _ = processes(system)
    boundary.start(["python", "-m", "x"], {})
    return boundary, system


def test_start_usa_sessao_nova_sem_shell_stdin_vazio_e_stdout_em_pipe() -> None:
    boundary, popen, _, _, _ = processes()
    pid = boundary.start(["python", "-m", "x"], {"start_new_session": True, "close_fds": True})
    assert pid == 4242
    argv, options = popen.calls[0]
    assert argv == ["python", "-m", "x"]
    assert options == {
        "start_new_session": True,
        "close_fds": True,
        "shell": False,
        "stdin": subprocess.DEVNULL,
        "stdout": subprocess.PIPE,
        "stderr": subprocess.DEVNULL,
    }


def test_start_ignora_opcoes_inseguras_do_chamador() -> None:
    boundary, popen, _, _, _ = processes()
    boundary.start(["x"], {"shell": True, "close_fds": False, "stdout": None})
    options = popen.calls[0][1]
    assert options["shell"] is False
    assert options["close_fds"] is True
    assert options["start_new_session"] is True
    assert options["stdout"] == subprocess.PIPE


def test_start_propaga_falha_de_spawn_como_oserror() -> None:
    boundary, *_ = processes(popen=FakePopen(error=OSError("boundary=spawn")))
    with pytest.raises(OSError):
        boundary.start(["x"], {})


def test_alive_confirma_processo_existente_com_sinal_zero() -> None:
    boundary, system = started(FakeSystem(live={4242}))
    assert boundary.alive(4242) is True
    assert system.kills == [(4242, 0)]
    assert system.waits == [(4242, system.WNOHANG)]


def test_alive_reconhece_filho_encerrado_e_o_colhe_sem_sinalizar() -> None:
    boundary, system = started(FakeSystem(live={4242}, exited_children={4242}))
    assert boundary.alive(4242) is False
    assert system.kills == []
    assert boundary.alive(4242) is True
    assert system.waits[0] == (4242, system.WNOHANG)


def test_alive_trata_processo_que_nao_e_filho_apenas_por_sinal() -> None:
    system = FakeSystem(live={99}, foreign={99})
    boundary, *_ = processes(system)
    assert boundary.alive(99) is True
    system.live.clear()
    assert boundary.alive(99) is False
    assert all(pid == 99 for pid, _ in system.kills)


def test_alive_trata_permissao_negada_como_processo_vivo() -> None:
    system = FakeSystem(denied={5})
    boundary, *_ = processes(system)
    assert boundary.alive(5) is True


def test_receive_le_uma_linha_json_com_timeout_limitado() -> None:
    system = FakeSystem(live={4242}, chunks=[b'{"state": "re', b'ady"}\nextra'])
    boundary, _ = started(system)
    assert boundary.receive(4242) == {"state": "ready"}


@pytest.mark.parametrize(
    "chunks", [[], [b"not-json\n"], [b"[1, 2]\n"], [b"x" * 100_000], [b'{"a": 1}']],
    ids=["eof", "invalido", "nao_objeto", "grande_demais", "sem_quebra_de_linha"],
)
def test_receive_retorna_none_para_ack_ausente_invalido_ou_incompleto(
    chunks: list[bytes],
) -> None:
    boundary, _ = started(FakeSystem(chunks=list(chunks)))
    assert boundary.receive(4242) is None


def test_receive_expira_sem_dados_dentro_do_limite() -> None:
    system = FakeSystem()
    boundary, _, _, selector, _ = processes(system)
    boundary.start(["x"], {})
    selector.silent = True
    assert boundary.receive(4242) is None
    assert selector.timeouts
    assert all(0 < timeout <= production.ACK_TIMEOUT for timeout in selector.timeouts)


def test_receive_de_pid_desconhecido_ou_sem_stdout_retorna_none() -> None:
    boundary, *_ = processes(popen=FakePopen(child=FakeChild(stdout=None)))
    assert boundary.receive(1) is None
    boundary.start(["x"], {})
    assert boundary.receive(4242) is None


def test_send_escreve_uma_linha_json_e_descarrega() -> None:
    boundary, _, _, _, stream = processes()
    boundary.send(10, {"state": "ready", "worker_pid": 10})
    assert stream.getvalue() == '{"state": "ready", "worker_pid": 10}\n'


def test_send_e_idempotente_por_worker_para_nao_encher_o_pipe() -> None:
    boundary, _, _, _, stream = processes()
    for _ in range(1000):
        boundary.send(10, {"state": "ready"})
    boundary.send(11, {"state": "ready"})
    assert stream.getvalue().count("\n") == 2


@pytest.mark.parametrize("error", [BrokenPipeError("pipe=closed"), ValueError("io=closed")])
def test_send_tolera_pipe_fechado(error: BaseException) -> None:
    boundary, *_ = processes(out=ClosedOut(error))
    boundary.send(10, {"state": "ready"})
    boundary.send(10, {"state": "ready"})


@pytest.mark.parametrize(
    "present",
    [
        {"AWS_PROFILE": "root"},
        {"AWS_DEFAULT_PROFILE": "root"},
        {"AWS_PROFILE": ""},
    ],
)
def test_credenciais_recusam_perfil_nomeado(present: dict[str, str]) -> None:
    with pytest.raises(SafetyFailure) as error:
        production.require_session_credentials({**environment(), **present})
    assert error.value.reason == "profile_credentials_forbidden"


@pytest.mark.parametrize(
    "missing", ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"]
)
@pytest.mark.parametrize("empty", [False, True])
def test_credenciais_exigem_sessao_temporaria_completa(missing: str, empty: bool) -> None:
    values = dict(environment())
    if empty:
        values[missing] = ""
    else:
        del values[missing]
    with pytest.raises(SafetyFailure) as error:
        production.require_session_credentials(values)
    assert error.value.reason == "session_credentials_required"


def test_credenciais_completas_sem_perfil_sao_aceitas() -> None:
    production.require_session_credentials(environment())


@pytest.mark.parametrize("probe", sorted(PROFILES))
def test_ports_reais_usam_limites_do_perfil_e_somente_a_regiao_do_spec(
    lab: Lab, probe: str
) -> None:
    spec = replace(lab.spec, probe=probe)
    clock = FakeClock()
    boundary, *_ = processes()
    ports = production.build_ports(spec, environment(), boundary, clock)
    limits = PROFILES[probe].limits
    assert ports.policy.limits == limits
    assert ports.policy.account == spec.account
    assert ports.policy.regions == production.AUTHORIZED_REGIONS
    assert ports.budget.limits == limits
    assert ports.budget.deadline == spec.deadline
    assert ports.processes is boundary
    assert ports.now is clock
    assert isinstance(ports.services, AwsServices)
    assert ports.services.spec == spec
    assert ports.services.ports.now is clock


def test_ports_reais_recusam_perfil_desconhecido(lab: Lab) -> None:
    boundary, *_ = processes()
    with pytest.raises(SafetyFailure) as error:
        production.build_ports(
            replace(lab.spec, probe="nope"), environment(), boundary, FakeClock()
        )
    assert error.value.reason == "invalid_profile"


def test_ports_reais_nao_expoem_credenciais_em_repr(lab: Lab) -> None:
    boundary, *_ = processes()
    ports = production.build_ports(lab.spec, environment(), boundary, FakeClock())
    assert "dummy-secret-do-not-log" not in repr(ports)
    assert "dummy-token-do-not-log" not in repr(ports)
