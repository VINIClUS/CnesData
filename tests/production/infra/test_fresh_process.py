"""Contratos unitários do boundary de processo filho."""

import os
import re
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from subprocess import CompletedProcess

import pytest

from tests.production.infra._fresh_process import (
    CHILD_FLAG,
    FRESH_MARKER,
    REPO_ROOT,
    run_fresh_process,
)

NODE = "tests/production/infra/test_processing_origins.py::probe"


def _junit(name: str, node: str | None = None, status: str | None = None) -> str:
    property_xml = (
        f'<properties><property name="{FRESH_MARKER}" value="{node}"/></properties>'
        if node is not None
        else ""
    )
    result_xml = f"<{status}/>" if status is not None else ""
    return (
        '<testsuites><testsuite tests="1">'
        f"{property_xml}<testcase classname=\"probe\" name=\"{name}\">"
        f"{result_xml}</testcase></testsuite></testsuites>"
    )


def _runner(
    xml: str, returncode: int = 0
) -> Callable[..., CompletedProcess[str]]:
    def execute(command: list[str], **_kwargs: object) -> CompletedProcess[str]:
        report_arg = next(item for item in command if item.startswith("--junitxml="))
        report = Path(report_arg.split("=", 1)[1])
        report.write_text(xml, encoding="utf-8")
        return CompletedProcess(command, returncode, "", "")

    return execute


@pytest.fixture(autouse=True)
def _clear_child_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(CHILD_FLAG, raising=False)


def _assert_rejected(case: tuple[str, str, int], tmp_path: Path) -> None:
    xml, message, returncode = case
    with pytest.raises(pytest.fail.Exception, match=message):
        run_fresh_process(NODE, tmp_path, _runner(xml, returncode))


def test_rejeita_outro_testcase(tmp_path: Path) -> None:
    xml = _junit("outro", NODE)
    _assert_rejected((xml, "fresh_child_junit_name", 0), tmp_path)


def test_rejeita_node_sem_marcador(tmp_path: Path) -> None:
    xml = _junit("probe")
    _assert_rejected((xml, "fresh_child_junit_marker", 0), tmp_path)


@pytest.mark.parametrize(
    ("status", "returncode", "message"),
    [
        ("failure", 0, "fresh_child_junit_contract"),
        ("error", 0, "fresh_child_junit_contract"),
        ("skipped", 0, "fresh_child_junit_contract"),
        (None, 1, "fresh_child_failed"),
    ],
)
def test_rejeita_status_child_invalido(
    status: str | None,
    returncode: int,
    message: str,
    tmp_path: Path,
) -> None:
    xml = _junit("probe", NODE, status)
    _assert_rejected((xml, message, returncode), tmp_path)


def test_aceita_junit_do_node_com_marcador(
    tmp_path: Path,
) -> None:
    xml = _junit("probe", NODE)
    assert run_fresh_process(NODE, tmp_path, _runner(xml)) is True


def test_flag_child_nao_invoca_runner(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(CHILD_FLAG, "1")

    def forbidden(*_args: object, **_kwargs: object) -> CompletedProcess[str]:
        pytest.fail("fresh_child_runner_invoked")

    assert run_fresh_process(NODE, tmp_path, forbidden) is False


def test_seam_publico_preserva_argv_ambiente_e_timeout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[tuple[list[str], dict[str, object]]] = []
    monkeypatch.setenv("PYTHONPATH", "inherited")
    monkeypatch.setenv("PYTEST_PLUGINS", "inherited")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "inherited")
    xml = _junit("probe", NODE)

    def capture(command: list[str], **kwargs: object) -> CompletedProcess[str]:
        calls.append((command, kwargs))
        report_arg = next(item for item in command if item.startswith("--junitxml="))
        Path(report_arg.split("=", 1)[1]).write_text(xml, encoding="utf-8")
        return CompletedProcess(command, 0, "", "")

    assert run_fresh_process(NODE, tmp_path, capture) is True
    command, kwargs = calls[0]
    assert command[:8] == [
        sys.executable, "-B", "-m", "pytest", NODE, "-q", "-p", "no:cacheprovider"
    ]
    assert kwargs["cwd"] == REPO_ROOT
    assert kwargs["timeout"] == 60
    environment = kwargs["env"]
    assert isinstance(environment, dict)
    assert environment["PYTHONPATH"] == os.pathsep.join(
        str(path)
        for path in (
            REPO_ROOT,
            REPO_ROOT / "packages/cnes_contracts/src",
            REPO_ROOT / "packages/cnes_domain/src",
            REPO_ROOT / "packages/cnes_infra/src",
            REPO_ROOT / "apps/data_processor/src",
        )
    )
    assert environment["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] == "1"
    assert environment["PYTHONNOUSERSITE"] == "1"
    assert environment["PYTHONDONTWRITEBYTECODE"] == "1"
    assert environment[CHILD_FLAG] == "1"
    assert environment["AWS_CONFIG_FILE"] == os.devnull
    assert environment["AWS_SHARED_CREDENTIALS_FILE"] == os.devnull
    assert environment["AWS_EC2_METADATA_DISABLED"] == "true"
    assert environment["AWS_ENDPOINT_URL"] == "http://127.0.0.1:9"
    assert "AWS_ACCESS_KEY_ID" not in environment
    assert "PYTEST_PLUGINS" not in environment


COLLECT_TARGETS = (
    "tests/production/infra/test_ecr_ecs.py",
    "tests/production/infra/test_processing_runtime.py",
    "tests/production/infra/test_fresh_process.py",
)


def _collection_environment() -> dict[str, str]:
    blocked = ("AWS_", "AZURE_", "CLOUDSDK_", "GCP_", "GOOGLE_", "PYTEST_")
    environment = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(blocked)
    }
    environment["PYTHONPATH"] = str(REPO_ROOT)
    environment["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] = "1"
    return environment


@pytest.mark.parametrize("target", COLLECT_TARGETS)
def test_coleta_com_importlib_sem_fixtures(
    target: str,
) -> None:
    result = subprocess.run(  # noqa: S603
        [
            sys.executable,
            "-B",
            "-m",
            "pytest",
            "--collect-only",
            "-q",
            "-p",
            "no:cacheprovider",
            target,
        ],
        check=False,
        capture_output=True,
        cwd=REPO_ROOT,
        env=_collection_environment(),
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, (
        f"collect_failed target={target} stdout={result.stdout!r} stderr={result.stderr!r}"
    )
    match = re.search(r"(?m)^(\d+) tests? collected", result.stdout)
    assert match is not None, f"collect_empty target={target}"
    assert int(match.group(1)) > 0, f"collect_empty target={target}"
