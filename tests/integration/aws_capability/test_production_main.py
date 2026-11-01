"""Laço do supervisor e entrada de linha de comando com fronteiras injetadas."""

import fcntl
import json
import os
import runpy
import sys
from collections.abc import Callable
from dataclasses import replace

import pytest

from tests.integration.aws_capability import _production as production
from tests.integration.aws_capability._production_testing import ClosedOut, environment
from tests.integration.aws_capability._testing import NAME, Lab, seed_journal
from tests.integration.aws_capability.contracts import Ports, Resource, RunSpec


def sleeper(lab: Lab, log: list[float], advance: float = 1.0) -> Callable[[float], None]:
    def sleep(seconds: float) -> None:
        log.append(seconds)
        lab.time += advance

    return sleep


def test_supervise_repete_monitor_ate_worker_morrer_e_limpa_no_deadline(lab: Lab) -> None:
    resource = Resource("cognito", "pool", NAME)
    lab.remote.add(resource)
    seed_journal(lab, [resource])
    lab.processes.workers[4321] = True
    ports = lab.ports()
    log: list[float] = []

    def sleep(seconds: float) -> None:
        log.append(seconds)
        if len(log) == 3:
            lab.processes.workers[4321] = False

    outcome = production.supervise(lab.spec, ports, 4321, sleep)
    assert outcome.status == "succeeded"
    assert log == [1.0, 1.0, 1.0]
    assert lab.remote.resources == {}


def test_supervise_encerra_quando_worker_conclui(lab: Lab) -> None:
    seed_journal(lab)
    data = json.loads(lab.journal().read_text())
    data.update(phase="complete", pending_service=None, operation_state="confirmed")
    lab.journal().write_text(json.dumps(data))
    log: list[float] = []
    outcome = production.supervise(lab.spec, lab.ports(), os.getpid(), sleeper(lab, log))
    assert (outcome.status, outcome.reason) == ("stopped", "worker_complete")
    assert log == []


def test_supervise_limpa_no_deadline_com_worker_travado(lab: Lab) -> None:
    resource = Resource("cognito", "pool", NAME)
    lab.remote.add(resource)
    seed_journal(lab, [resource])
    lab.processes.workers[4321] = True
    log: list[float] = []
    outcome = production.supervise(lab.spec, lab.ports(), 4321, sleeper(lab, log, advance=20))
    assert outcome.status == "succeeded"
    assert lab.time >= lab.spec.deadline
    assert lab.remote.resources == {}


def test_supervise_tem_parada_dura_quando_cleanup_continua_com_outro_dono(lab: Lab) -> None:
    resource = Resource("cognito", "pool", NAME)
    lab.remote.add(resource)
    seed_journal(lab, [resource])
    lab.processes.workers[4321] = False
    log: list[float] = []
    ports = lab.ports()
    limit = lab.spec.deadline + ports.policy.limits.cleanup_seconds + production.MARGIN_SECONDS
    lock = lab.spec.journal_dir / "a1b2c3d4.lock"
    with lock.open("a+") as lease:
        fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
        outcome = production.supervise(lab.spec, ports, 4321, sleeper(lab, log, advance=10))
    assert (outcome.status, outcome.reason) == ("incomplete", "supervisor_timeout")
    assert lab.time >= limit
    assert "pool" in lab.remote.resources


def test_supervise_so_dorme_um_segundo_por_ciclo(lab: Lab) -> None:
    lab.processes.workers[4321] = True
    seed_journal(lab)
    log: list[float] = []
    with pytest.raises(StopIteration):
        production.supervise(lab.spec, lab.ports(), 4321, _stop_after(lab, log, 4))
    assert log == [1.0] * 4


def _stop_after(lab: Lab, log: list[float], count: int) -> Callable[[float], None]:
    def sleep(seconds: float) -> None:
        log.append(seconds)
        if len(log) == count:
            raise StopIteration

    return sleep


class Console:
    def __init__(self) -> None:
        self.lines: list[str] = []

    def __call__(self, line: str) -> None:
        self.lines.append(line)


def runtime(lab: Lab, console: Console, built: list[RunSpec] | None = None) -> production.Runtime:
    def build(spec: RunSpec, *_: object) -> Ports:
        if built is not None:
            built.append(spec)
        return lab.ports()

    return production.Runtime(
        processes=lab.processes,
        clock=lambda: lab.time,
        sleep=lambda seconds: None,
        emit=console,
        build=build,
    )


def arguments(lab: Lab, command: str, *extra: str) -> list[str]:
    spec = lab.spec
    return [
        command,
        "--account", spec.account,
        "--region", spec.region,
        "--run-id", spec.run_id,
        "--deadline", str(spec.deadline),
        "--journal-dir", str(spec.journal_dir),
        "--retention", "GOVERNANCE",
        "--allow-governance-bypass",
        *extra,
    ]  # fmt: skip


def test_main_run_emite_uma_linha_json_sem_segredos_e_sai_zero(lab: Lab) -> None:
    console = Console()
    lab.remote.probe_result = {"codigo": "ValidationException"}
    code = production.main(arguments(lab, "run"), environment(), runtime(lab, console))
    assert code == 0
    assert len(console.lines) == 1
    document = json.loads(console.lines[0])
    assert document["status"] == "succeeded"
    assert document["reason"] == "complete"
    assert document["remaining"] == []
    assert document["evidence"]["probe"] == {"codigo": "ValidationException"}
    for secret in environment().values():
        assert secret not in console.lines[0]


def test_main_janitor_sai_zero_somente_quando_conclui(lab: Lab) -> None:
    resource = Resource("cognito", "pool", NAME)
    lab.remote.add(resource)
    seed_journal(lab, [resource])
    console = Console()
    assert production.main(arguments(lab, "janitor"), environment(), runtime(lab, console)) == 0
    assert json.loads(console.lines[0])["status"] == "succeeded"


def test_main_lista_nomes_dos_recursos_remanescentes_sem_ids(lab: Lab) -> None:
    resource = Resource("cognito", "pool-id-secreto", NAME)
    lab.remote.add(resource)
    lab.remote.delete_errors[resource.resource_id] = 99
    seed_journal(lab, [resource])
    console = Console()
    code = production.main(arguments(lab, "janitor"), environment(), runtime(lab, console))
    assert code != 0
    document = json.loads(console.lines[0])
    assert document["status"] == "incomplete"
    assert document["remaining"] == [{"service": "cognito", "name": NAME}]
    assert "pool-id-secreto" not in console.lines[0]


def test_main_monitor_sai_zero_somente_com_stopped(lab: Lab) -> None:
    seed_journal(lab)
    data = json.loads(lab.journal().read_text())
    data.update(phase="complete", pending_service=None, operation_state="confirmed")
    lab.journal().write_text(json.dumps(data))
    console = Console()
    argv = arguments(lab, "monitor", "--worker-pid", str(os.getpid()))
    assert production.main(argv, environment(), runtime(lab, console)) == 0
    assert json.loads(console.lines[0])["status"] == "stopped"


def test_main_monitor_com_sucesso_de_janitor_sai_zero(lab: Lab) -> None:
    resource = Resource("cognito", "pool", NAME)
    lab.remote.add(resource)
    seed_journal(lab, [resource])
    lab.processes.workers[4321] = False
    console = Console()
    argv = arguments(lab, "monitor", "--worker-pid", "4321")
    assert production.main(argv, environment(), runtime(lab, console)) == 0
    assert json.loads(console.lines[0])["status"] == "succeeded"


def test_main_monitor_com_janitor_incompleto_nao_sai_zero(lab: Lab) -> None:
    resource = Resource("cognito", "pool", NAME)
    lab.remote.add(resource)
    lab.remote.delete_errors[resource.resource_id] = 99
    seed_journal(lab, [resource])
    lab.processes.workers[4321] = False
    console = Console()
    argv = arguments(lab, "monitor", "--worker-pid", "4321")
    assert production.main(argv, environment(), runtime(lab, console)) == 1
    assert json.loads(console.lines[0])["status"] == "incomplete"


@pytest.mark.parametrize("command", ["run", "janitor", "monitor"])
@pytest.mark.parametrize("region", ["us-east-1", "eu-west-1"])
def test_main_recusa_regiao_nao_autorizada_antes_de_chamar_aws(
    lab: Lab, command: str, region: str
) -> None:
    seed_journal(lab)
    console = Console()
    config = replace(runtime(lab, console), build=production.build_ports)
    argv = arguments(lab, command, "--worker-pid", "4321")
    argv[argv.index("--region") + 1] = region
    assert production.main(argv, environment(), config) == 1
    document = json.loads(console.lines[0])
    assert (document["status"], document["reason"]) == ("refused", "region_not_authorized")
    assert lab.events == []


def test_regioes_autorizadas_sao_fixas_em_us_east_2() -> None:
    assert frozenset({"us-east-2"}) == production.AUTHORIZED_REGIONS


def test_main_monitor_exige_pid_do_worker_valido(lab: Lab) -> None:
    console = Console()
    argv = arguments(lab, "monitor", "--worker-pid", "0")
    assert production.main(argv, environment(), runtime(lab, console)) != 0
    assert json.loads(console.lines[0])["reason"] == "invalid_worker_pid"


@pytest.mark.parametrize(
    ("changes", "reason"),
    [
        ({"AWS_PROFILE": "root"}, "profile_credentials_forbidden"),
        ({"AWS_DEFAULT_PROFILE": "root"}, "profile_credentials_forbidden"),
        ({"AWS_SESSION_TOKEN": ""}, "session_credentials_required"),
        ({"AWS_ACCESS_KEY_ID": ""}, "session_credentials_required"),
    ],
)
def test_main_recusa_credencial_insegura_antes_de_montar_ports(
    lab: Lab, changes: dict[str, str], reason: str
) -> None:
    console, built = Console(), []
    values = {**environment(), **changes}
    code = production.main(arguments(lab, "run"), values, runtime(lab, console, built))
    assert code != 0
    assert built == []
    document = json.loads(console.lines[0])
    assert (document["status"], document["reason"]) == ("refused", reason)
    assert lab.events == []


@pytest.mark.parametrize(
    ("argv", "reason"),
    [
        ([], "explicit_command_required"),
        (["bogus"], "explicit_command_required"),
        (["run"], "missing_argument"),
    ],
)
def test_main_recusa_argumentos_invalidos_com_json_estruturado(
    lab: Lab, argv: list[str], reason: str
) -> None:
    console = Console()
    assert production.main(argv, environment(), runtime(lab, console)) != 0
    assert json.loads(console.lines[0])["reason"] == reason


def test_main_converte_excecao_inesperada_em_saida_estruturada_sem_vazamento(lab: Lab) -> None:
    console = Console()

    def explode(*_: object) -> Ports:
        raise RuntimeError("segredo-no-erro dummy-secret-do-not-log")

    config = replace(runtime(lab, console), build=explode)
    code = production.main(arguments(lab, "run"), environment(), config)
    assert code != 0
    document = json.loads(console.lines[0])
    assert (document["status"], document["reason"]) == ("incomplete", "unexpected_error")
    assert "segredo-no-erro" not in console.lines[0]
    assert "dummy-secret" not in console.lines[0]


def test_emit_padrao_tolera_stdout_fechado(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "stdout", ClosedOut(BrokenPipeError("pipe=closed")))
    production.emit_line("{}")


def test_emit_padrao_escreve_linha_no_stdout(capsys: pytest.CaptureFixture[str]) -> None:
    production.emit_line('{"a": 1}')
    assert capsys.readouterr().out == '{"a": 1}\n'


def test_modulo_lifecycle_como_principal_recusa_sem_comando(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(sys, "argv", ["lifecycle"])
    monkeypatch.delitem(sys.modules, "tests.integration.aws_capability.lifecycle")
    with pytest.raises(SystemExit) as exit_info:
        runpy.run_module("tests.integration.aws_capability.lifecycle", run_name="__main__")
    assert exit_info.value.code != 0
    assert json.loads(capsys.readouterr().out)["reason"] == "explicit_command_required"


def test_runtime_padrao_usa_processos_do_sistema_e_relogio_real() -> None:
    config = production.default_runtime()
    assert isinstance(config.processes, production.SystemProcesses)
    assert config.clock() > 1_000_000_000
    assert config.build is production.build_ports
