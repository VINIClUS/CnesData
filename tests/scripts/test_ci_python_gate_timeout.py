"""Comportamento temporal do gate Bash e propagação de falhas."""

from pathlib import Path

from ._python_gate_timeout_testing import TIME_SCALE, GateRun, run_gate

_PREPARED = (
    "openapi:complete", "contracts:complete", "ruff:complete", "pyright:complete",
    "migration:complete",
)


def _preparacao_e_trap_funcionaram(result: GateRun) -> None:
    assert result.events[:5] == _PREPARED
    assert len(result.temporary_outputs) == 2
    assert all(not Path(output).exists() for output in result.temporary_outputs)
    assert result.surviving_pids == ()


def test_suite_finita_de_520_segundos_conclui_e_executa_apps(tmp_path: Path) -> None:
    result = run_gate(tmp_path, "finite")
    _preparacao_e_trap_funcionaram(result)
    assert not result.deadline_expired
    assert result.returncode == 0
    assert result.events == (*_PREPARED, "packages:start", "packages:complete",
                             "apps:start", "apps:complete")
    assert set(result.reports) == {"coverage-packages.xml", "coverage-apps.xml"}


def test_suite_de_pacotes_sem_fim_recebe_int_e_impede_apps(tmp_path: Path) -> None:
    result = run_gate(tmp_path, "packages_hang")
    assert not result.deadline_expired
    _preparacao_e_trap_funcionaram(result)
    assert result.returncode == 124
    assert result.elapsed < 800 * TIME_SCALE + 2
    assert "packages:start" in result.events
    assert "packages:int" in result.events
    assert "packages:complete" not in result.events
    assert "apps:start" not in result.events
    assert result.reports == ()


def test_suite_de_apps_sem_fim_permanece_interrompida(tmp_path: Path) -> None:
    result = run_gate(tmp_path, "apps_hang")
    assert not result.deadline_expired
    _preparacao_e_trap_funcionaram(result)
    assert result.returncode == 124
    assert result.elapsed < 400 * TIME_SCALE + 2
    assert "packages:complete" in result.events
    assert "apps:start" in result.events
    assert "apps:int" in result.events
    assert "apps:complete" not in result.events
    assert result.reports == ("coverage-packages.xml",)


def test_falha_de_pytest_de_pacotes_propaga_sem_executar_apps(tmp_path: Path) -> None:
    result = run_gate(tmp_path, "packages_fail")
    _preparacao_e_trap_funcionaram(result)
    assert not result.deadline_expired
    assert result.returncode == 7
    assert result.events == (*_PREPARED, "packages:start", "packages:failure")
    assert result.reports == ()
