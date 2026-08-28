"""Testes da matriz de baseline versionada."""

import json
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import pytest

from scripts import baseline_matrix
from scripts.baseline_matrix import SuiteResult

APPROVED_SHA = "8a2d3a4cbce85e87e31dc4769aab2a7a2b4bf7b2"
MISSING_MODULE = "cnes_infra.storage.repositories.estabelecimento_repo"
AFFECTED_PATHS = [
    "tests/perf/macro/test_data_processor_e2e.py",
    "tests/perf/micro/test_upsert_bench.py",
    "tests/perf/soak/test_upsert_soak.py",
    "tests/perf/spike/test_upsert_spike.py",
    "tests/perf/stress/test_upsert_stress.py",
]
CANONICAL_BYTES = (
    b'{"affected_paths":["tests/perf/macro/test_data_processor_e2e.py",'
    b'"tests/perf/micro/test_upsert_bench.py","tests/perf/soak/test_upsert_soak.py",'
    b'"tests/perf/spike/test_upsert_spike.py","tests/perf/stress/test_upsert_stress.py"],'
    b'"exception":"ModuleNotFoundError",'
    b'"module":"cnes_infra.storage.repositories.estabelecimento_repo"}'
)
CANONICAL_DIGEST = "b4b22c61c271c1cf5c4e129644b21a774960901fc3fee3be2bd837f502ae623f"
canonical_failure_document = vars(baseline_matrix)["_canonical_failure_document"]
failure_fingerprint = vars(baseline_matrix)["_failure_fingerprint"]
waiver_for = vars(baseline_matrix)["_waiver_for"]


def _collection_block(
    path: str,
    exception: str = "ModuleNotFoundError",
    module: str = MISSING_MODULE,
) -> str:
    return (
        f"________ ERROR collecting {path} ________\n"
        f"ImportError while importing test module '/workspace/{path}'.\n"
        "Hint: make sure your test modules/packages have valid Python names.\n"
        "Traceback:\n"
        "/usr/lib/python3.12/importlib/__init__.py:90: in import_module\n"
        "    return _bootstrap._gcd_import(name[level:], package, level)\n"
        f"E   {exception}: No module named '{module}'\n"
    )


def _missing_module_output(
    module: str = MISSING_MODULE,
    exception: str = "ModuleNotFoundError",
    paths: list[str] | None = None,
) -> str:
    affected_paths = paths or AFFECTED_PATHS
    return "\n".join(_collection_block(path, exception, module) for path in affected_paths)


def _mixed_failure_output() -> str:
    blocks = [
        _collection_block(AFFECTED_PATHS[0], "AssertionError", MISSING_MODULE),
        _collection_block(AFFECTED_PATHS[1], "ModuleNotFoundError", f"{MISSING_MODULE}.other"),
        *(_collection_block(path) for path in AFFECTED_PATHS[2:]),
        f"ModuleNotFoundError: No module named '{MISSING_MODULE}'\n",
    ]
    return "\n".join(blocks)


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
    result = SuiteResult(
        name,
        "uv run pytest",
        2,
        0.5,
        failure_fingerprint=CANONICAL_DIGEST,
    )

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
        SuiteResult(
            "python-fast",
            "uv run pytest",
            2,
            0.5,
            failure_fingerprint=CANONICAL_DIGEST,
        ),
        SuiteResult("go", "go test ./...", 0, 0.5),
    ]

    assert baseline_matrix.matrix_exit_code(results, APPROVED_SHA) == 0


def test_status_vermelho_sem_waiver_aprovado() -> None:
    results = [SuiteResult("python-fast", "uv run pytest", 2, 0.5)]

    assert baseline_matrix.matrix_exit_code(results, "f" * 40) == 1


def test_aceita_waiver_com_assinatura_normalizada_aprovada() -> None:
    digest = failure_fingerprint(_missing_module_output())
    result = SuiteResult(
        "python-fast",
        "uv run pytest",
        2,
        0.5,
        failure_fingerprint=digest,
    )

    assert digest == CANONICAL_DIGEST
    assert waiver_for(result, APPROVED_SHA) is not None


@pytest.mark.parametrize("exit_code", [0, 1, 3])
def test_rejeita_waiver_com_fingerprint_valido_e_exit_code_diferente(exit_code: int) -> None:
    result = SuiteResult(
        "python-fast",
        "uv run pytest",
        exit_code,
        0.5,
        failure_fingerprint=CANONICAL_DIGEST,
    )

    assert waiver_for(result, APPROVED_SHA) is None


@pytest.mark.parametrize(
    "output",
    [
        _missing_module_output(exception="AssertionError"),
        _missing_module_output(module="cnes_infra.storage.repositories.outra"),
        "",
    ],
)
def test_rejeita_waiver_com_mesmo_codigo_e_falha_diferente(output: str) -> None:
    result = SuiteResult(
        "python-fast",
        "uv run pytest",
        2,
        0.5,
        failure_fingerprint=failure_fingerprint(output),
    )

    assert waiver_for(result, APPROVED_SHA) is None


@pytest.mark.parametrize(
    ("name", "commit_sha"),
    [("python-packages", APPROVED_SHA), ("python-fast", "f" * 40)],
)
def test_rejeita_waiver_com_suite_ou_sha_nao_aprovados(name: str, commit_sha: str) -> None:
    result = SuiteResult(
        name,
        "uv run pytest",
        2,
        0.5,
        failure_fingerprint=failure_fingerprint(_missing_module_output()),
    )

    assert waiver_for(result, commit_sha) is None


def test_serializa_documento_canonico_da_falha_aprovada() -> None:
    assert canonical_failure_document(_missing_module_output()) == CANONICAL_BYTES


def test_ordem_dos_blocos_nao_altera_fingerprint() -> None:
    output = _missing_module_output(paths=list(reversed(AFFECTED_PATHS)))

    assert failure_fingerprint(output) == CANONICAL_DIGEST


def test_relatorio_expoe_digest_sem_capturar_saida(tmp_path: Path) -> None:
    report_path = tmp_path / "baseline.json"
    result = SuiteResult(
        "python-fast",
        "uv run pytest",
        2,
        0.5,
        failure_fingerprint=CANONICAL_DIGEST,
    )

    baseline_matrix.write_report(report_path, [result], APPROVED_SHA)

    suite = json.loads(report_path.read_text(encoding="utf-8"))["suites"][0]
    assert suite["failure_fingerprint"] == CANONICAL_DIGEST
    assert "stdout" not in suite
    assert "stderr" not in suite


@pytest.mark.parametrize(
    "output",
    [
        _missing_module_output(
            paths=[*AFFECTED_PATHS[:-1], "tests/perf/stress/test_upsert_stresa.py"]
        ),
        _missing_module_output(module=f"{MISSING_MODULE[:-1]}a"),
        _missing_module_output(exception="NoduleNotFoundError"),
    ],
)
def test_alteracao_em_campo_canonico_muda_digest_e_rejeita_waiver(output: str) -> None:
    digest = failure_fingerprint(output)
    result = SuiteResult("python-fast", "uv run pytest", 2, 0.5, failure_fingerprint=digest)

    assert digest is not None
    assert digest != CANONICAL_DIGEST
    assert waiver_for(result, APPROVED_SHA) is None


def test_bloco_relevante_ausente_nao_produz_fingerprint_aprovado() -> None:
    digest = failure_fingerprint(_missing_module_output(paths=AFFECTED_PATHS[:-1]))

    assert digest != CANONICAL_DIGEST


def test_rejeita_assinatura_montada_com_campos_de_blocos_distintos() -> None:
    assert failure_fingerprint(_mixed_failure_output()) is None


def test_rejeita_fingerprint_como_quinto_argumento_posicional() -> None:
    with pytest.raises(TypeError):
        SuiteResult("python-fast", "uv run pytest", 2, 0.5, CANONICAL_DIGEST)


def test_suite_rapida_nao_seleciona_chaos_infra() -> None:
    completed = subprocess.run(
        (
            sys.executable,
            "-m",
            "pytest",
            "tests/chaos/test_central_api_sobrevive_pg_restart.py",
            "--collect-only",
            "-m",
            baseline_matrix.PYTHON_FAST_MARKERS,
            "-q",
        ),
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 5
    assert "test_central_api_sobrevive_pg_restart" not in completed.stdout


def test_suite_integracao_coleta_packages_apps_sem_e2e() -> None:
    command = baseline_matrix.INTEGRATION_SCRIPT.removeprefix(
        "docker compose -p cnesdata up -d --wait postgres && "
    ).replace("uv run pytest", "uv run pytest --collect-only")
    shell = shutil.which("sh")
    assert shell is not None
    completed = subprocess.run(
        (shell, "-c", command),
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0
    assert completed.stdout.count("tests collected") == 2
    assert "packages/cnes_infra/tests/auth/test_refresh_tokens.py" in completed.stdout
    assert "apps/central_api/tests/repositories/test_dashboard_repo_overview.py" in completed.stdout
    assert "apps/central_api/tests/test_smoke.py" not in completed.stdout
