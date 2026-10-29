"""Comportamento temporal do gate Bash e propagação de falhas."""

import re
import subprocess
import sys
from pathlib import Path

from ._python_gate_timeout_testing import TIME_SCALE, GateRun, run_gate

_PREPARED = (
    "openapi:complete",
    "contracts:complete",
    "ruff:complete",
    "pyright:complete",
    "migration:complete",
)
_PACKAGES_MARKERS = "not bigquery and not e2e and not stress and not soak and not spike"
_APPS_MARKERS = (
    "not integration and not bigquery and not e2e and not stress and not soak and not spike "
    "and not windows_only"
)


def _preparacao_e_trap_funcionaram(result: GateRun) -> None:
    assert result.events[:5] == _PREPARED
    assert len(result.temporary_outputs) == 2
    assert all(not Path(output).exists() for output in result.temporary_outputs)
    assert result.surviving_pids == ()


def _diagnosticos_expoem_fases_e_status(
    result: GateRun,
    expected: tuple[tuple[str, int], ...],
) -> None:
    rows = [
        dict(field.split("=", 1) for field in line.split() if "=" in field)
        for line in result.stdout.splitlines()
    ]
    diagnostics = [row for row in rows if row.get("phase") in {"packages", "apps"}]
    assert [(row.get("phase"), row.get("event")) for row in diagnostics] == [
        (phase, event) for phase, _ in expected for event in ("start", "end")
    ]
    for index, (phase, status) in enumerate(expected):
        start, end = diagnostics[index * 2 : index * 2 + 2]
        for row in (start, end):
            assert re.fullmatch(r"\d+(?:\.\d+)?", row.get("timestamp", ""))
        assert float(end["timestamp"]) >= float(start["timestamp"])
        assert re.fullmatch(r"\d+(?:\.\d+)?", end.get("elapsed", ""))
        assert 0 <= float(end["elapsed"]) <= result.elapsed + 1
        assert int(end["external_rc"]) == status
        boundary = [row for row in rows if row.get("boundary_phase") == phase]
        assert rows.index(start) < rows.index(boundary[0])
        assert rows.index(boundary[-1]) < rows.index(end)


def test_suite_finita_de_520_segundos_conclui_e_executa_apps(tmp_path: Path) -> None:
    result = run_gate(tmp_path, "finite")
    _preparacao_e_trap_funcionaram(result)
    assert not result.deadline_expired
    assert result.returncode == 0
    assert result.events == (
        *_PREPARED,
        "packages:start",
        "packages:complete",
        "apps:start",
        "apps:complete",
    )
    assert set(result.reports) == {"coverage-packages.xml", "coverage-apps.xml"}
    _diagnosticos_expoem_fases_e_status(result, (("packages", 0), ("apps", 0)))


def test_resumo_e_cobertura_antes_de_600_nao_interrompem_cauda_finita(tmp_path: Path) -> None:
    result = run_gate(tmp_path, "finite_tail")
    _preparacao_e_trap_funcionaram(result)
    assert not result.deadline_expired
    assert result.events[:10] == (
        *_PREPARED,
        "packages:start",
        "packages:work",
        "packages:summary",
        "packages:coverage",
        "packages:tail",
    )
    assert "coverage-packages.xml" in result.reports
    assert result.returncode == 0, f"events={result.events} reports={result.reports}"
    assert result.events[10:] == (
        "packages:process_exit",
        "packages:complete",
        "apps:start",
        "apps:complete",
    )
    assert "packages:int" not in result.events
    assert set(result.reports) == {"coverage-packages.xml", "coverage-apps.xml"}
    assert 650 * TIME_SCALE <= result.elapsed < 800 * TIME_SCALE + 2
    _diagnosticos_expoem_fases_e_status(result, (("packages", 0), ("apps", 0)))


def test_suite_de_pacotes_sem_fim_recebe_int_e_impede_apps(tmp_path: Path) -> None:
    result = run_gate(tmp_path, "packages_hang")
    assert not result.deadline_expired
    _preparacao_e_trap_funcionaram(result)
    assert result.returncode == 124
    assert 850 * TIME_SCALE <= result.elapsed < 1000 * TIME_SCALE + 2
    assert len(result.timeout_argv) == 1
    assert result.timeout_argv[0][:2] == ("--signal=INT", "900")
    assert "packages:start" in result.events
    assert "packages:int" in result.events
    assert "packages:complete" not in result.events
    assert "apps:start" not in result.events
    assert result.reports == ()
    _diagnosticos_expoem_fases_e_status(result, (("packages", 124),))


def test_suite_de_apps_sem_fim_permanece_interrompida(tmp_path: Path) -> None:
    result = run_gate(tmp_path, "apps_hang")
    assert not result.deadline_expired
    _preparacao_e_trap_funcionaram(result)
    assert result.returncode == 124
    assert 270 * TIME_SCALE <= result.elapsed < 400 * TIME_SCALE + 2
    assert [args[:2] for args in result.timeout_argv] == [
        ("--signal=INT", "900"),
        ("--signal=INT", "300"),
    ]
    assert "packages:complete" in result.events
    assert "apps:start" in result.events
    assert "apps:int" in result.events
    assert "apps:complete" not in result.events
    assert result.reports == ("coverage-packages.xml",)
    _diagnosticos_expoem_fases_e_status(result, (("packages", 0), ("apps", 124)))


def test_falha_de_pytest_de_pacotes_propaga_sem_executar_apps(tmp_path: Path) -> None:
    result = run_gate(tmp_path, "packages_fail")
    _preparacao_e_trap_funcionaram(result)
    assert not result.deadline_expired
    assert result.returncode == 7
    assert result.events == (*_PREPARED, "packages:start", "packages:failure")
    assert result.reports == ()
    _diagnosticos_expoem_fases_e_status(result, (("packages", 7),))


def test_duas_fases_preservam_selecao_cobertura_e_motivos_de_skip(tmp_path: Path) -> None:
    result = run_gate(tmp_path, "selection")
    _preparacao_e_trap_funcionaram(result)
    assert result.returncode == 0
    assert not result.deadline_expired
    selections = (
        (
            ["packages/cnes_domain", "packages/cnes_infra"],
            _PACKAGES_MARKERS,
            "pyproject.toml",
            "coverage-packages.xml",
        ),
        (["apps/"], _APPS_MARKERS, ".coveragerc", "coverage-apps.xml"),
    )
    assert len(result.pytest_argv) == 2
    _pytest_real_expoe_motivo_de_skip(tmp_path, result.pytest_argv[0])
    for args, (paths, markers, config, report) in zip(result.pytest_argv, selections, strict=True):
        assert list(args[: 4 + len(paths)]) == ["run", "--no-build", "pytest", *paths, "-m"]
        assert " ".join(args[args.index("-m") + 1].split()) == markers
        assert "--cov" in args
        assert f"--cov-config={config}" in args
        assert "--cov-report=term-missing" in args
        assert f"--cov-report=xml:{report}" in args
        assert "-rs" in args


def _pytest_real_expoe_motivo_de_skip(root: Path, args: tuple[str, ...]) -> None:
    probe = root / "skip_probe"
    probe.mkdir()
    (probe / "pytest.ini").write_text("[pytest]\n")
    (probe / "test_skip.py").write_text(
        'import pytest\n\n@pytest.mark.skip(reason="prerequisite=synthetic_dependency")\n'
        "def test_pre_requisito_ausente():\n    pass\n"
    )
    run = subprocess.run(  # noqa: S603 - trusted Python and owned synthetic pytest fixture
        [
            sys.executable,
            "-I",
            "-B",
            "-m",
            "pytest",
            "-q",
            *(value for value in args if value == "-rs"),
            "-p",
            "no:cacheprovider",
            "-c",
            str(probe / "pytest.ini"),
            f"--rootdir={probe}",
            f"--confcutdir={probe}",
            f"--basetemp={probe}/temp",
            str(probe / "test_skip.py"),
        ],
        cwd=probe,
        text=True,
        capture_output=True,
        timeout=5,
        check=False,
    )
    (root / "skip_probe_stdout.txt").write_text(run.stdout)
    assert run.returncode == 0, run.stderr
    assert "1 skipped" in run.stdout
    assert "test_skip.py:3: prerequisite=synthetic_dependency" in run.stdout
