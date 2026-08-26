"""Testes da matriz de baseline versionada."""

import json
import sys
from datetime import datetime
from pathlib import Path

import pytest

from scripts import baseline_matrix
from scripts.baseline_matrix import SuiteResult

APPROVED_SHA = "8a2d3a4cbce85e87e31dc4769aab2a7a2b4bf7b2"


def test_serializa_relatorio_com_timestamp_utc_e_cria_diretorio(tmp_path: Path) -> None:
    report_path = tmp_path / "nested" / "baseline.json"
    results = [SuiteResult("python-packages", "uv run pytest packages/", 0, 1.25)]

    baseline_matrix.write_report(report_path, results, APPROVED_SHA)

    payload = json.loads(report_path.read_text(encoding="utf-8"))
    recorded_at = datetime.fromisoformat(payload["recorded_at"])
    assert payload["commit_sha"] == APPROVED_SHA
    assert recorded_at.utcoffset().total_seconds() == 0
    assert payload["suites"] == [
        {
            "name": "python-packages",
            "command": "uv run pytest packages/",
            "exit_code": 0,
            "duration_seconds": 1.25,
        }
    ]


def test_executa_comando_exibe_saida_e_mede_duracao(capsys: pytest.CaptureFixture[str]) -> None:
    result = baseline_matrix.run_suite(
        "echo",
        (sys.executable, "-c", "print('baseline-output')"),
    )

    assert result.name == "echo"
    assert result.command == f"{sys.executable} -c 'print('\"'\"'baseline-output'\"'\"')'"
    assert result.exit_code == 0
    assert result.duration_seconds >= 0
    assert "baseline-output" in capsys.readouterr().out


def test_retorna_127_quando_executavel_nao_existe(capsys: pytest.CaptureFixture[str]) -> None:
    result = baseline_matrix.run_suite("missing", ("cnesdata-command-that-does-not-exist",))

    assert result.exit_code == 127
    assert "cnesdata-command-that-does-not-exist" in capsys.readouterr().out


def test_executa_suite_seguinte_apos_falha() -> None:
    suites = (
        ("failure", (sys.executable, "-c", "raise SystemExit(3)")),
        ("success", (sys.executable, "-c", "raise SystemExit(0)")),
    )

    results = baseline_matrix.run_suites(suites)

    assert [result.exit_code for result in results] == [3, 0]


@pytest.mark.parametrize("name", ["python-fast", "python-integration-docker"])
def test_registra_waiver_aprovado_no_sha_e_codigo_exatos(
    tmp_path: Path,
    name: str,
) -> None:
    report_path = tmp_path / "baseline.json"
    result = SuiteResult(name, "uv run pytest", 2, 0.5)

    baseline_matrix.write_report(report_path, [result], APPROVED_SHA)

    suite = json.loads(report_path.read_text(encoding="utf-8"))["suites"][0]
    assert suite["waiver"] == {
        "failure": (
            "ModuleNotFoundError: No module named "
            "'cnes_infra.storage.repositories.estabelecimento_repo' during collection of "
            "tests/perf/{macro,micro,soak,spike,stress}"
        ),
        "expected_exit_code": 2,
        "approved_by": "VINIClUS",
        "approved_at": "2026-08-26",
    }


@pytest.mark.parametrize(
    ("name", "exit_code", "commit_sha"),
    [
        ("python-fast", 1, APPROVED_SHA),
        ("python-fast", 2, "f" * 40),
        ("python-packages", 2, APPROVED_SHA),
    ],
)
def test_nao_registra_waiver_fora_da_aprovacao(
    tmp_path: Path,
    name: str,
    exit_code: int,
    commit_sha: str,
) -> None:
    report_path = tmp_path / "baseline.json"

    baseline_matrix.write_report(
        report_path,
        [SuiteResult(name, "uv run pytest", exit_code, 0.5)],
        commit_sha,
    )

    suite = json.loads(report_path.read_text(encoding="utf-8"))["suites"][0]
    assert "waiver" not in suite


def test_status_zero_com_suites_verdes_e_waiver_aprovado() -> None:
    results = [
        SuiteResult("python-fast", "uv run pytest", 2, 0.5),
        SuiteResult("go", "go test ./...", 0, 0.5),
    ]

    assert baseline_matrix.matrix_exit_code(results, APPROVED_SHA) == 0


def test_status_vermelho_sem_waiver_aprovado() -> None:
    results = [SuiteResult("python-fast", "uv run pytest", 2, 0.5)]

    assert baseline_matrix.matrix_exit_code(results, "f" * 40) == 1
