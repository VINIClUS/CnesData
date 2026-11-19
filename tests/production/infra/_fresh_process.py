"""Execução isolada dos dois probes sensíveis à ordem de importação."""

import os
import subprocess
import sys
import xml.etree.ElementTree as ET
from collections.abc import Callable
from pathlib import Path

import pytest

CHILD_FLAG = "CNESDATA_FRESH_PROCESS_CHILD"
REPO_ROOT = Path(__file__).resolve().parents[3]
FRESH_MARKER = "fresh_process_completed"
Runner = Callable[..., subprocess.CompletedProcess[str]]


def _child_environment() -> dict[str, str]:
    environment = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("AWS_", "PYTEST_", "PYTHON"))
    }
    pythonpath = (
        REPO_ROOT,
        REPO_ROOT / "packages/cnes_contracts/src",
        REPO_ROOT / "packages/cnes_domain/src",
        REPO_ROOT / "packages/cnes_infra/src",
        REPO_ROOT / "apps/data_processor/src",
    )
    environment.update(
        {
            CHILD_FLAG: "1",
            "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
            "PYTEST_CURRENT_TEST": "",
            "PYTHONNOUSERSITE": "1",
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONPATH": os.pathsep.join(str(path) for path in pythonpath),
            "AWS_CONFIG_FILE": os.devnull,
            "AWS_SHARED_CREDENTIALS_FILE": os.devnull,
            "AWS_EC2_METADATA_DISABLED": "true",
            "AWS_ENDPOINT_URL": "http://127.0.0.1:9",
        }
    )
    return environment


def _assert_child_report(
    result: subprocess.CompletedProcess[str], report: Path, node: str
) -> None:
    if result.returncode != 0:
        pytest.fail(f"fresh_child_failed rc={result.returncode} stdout={result.stdout!r}")
    try:
        root = ET.parse(report).getroot()  # noqa: S314 - owned local child output
    except (ET.ParseError, OSError) as error:
        pytest.fail(f"fresh_child_junit_invalid error={error!r}")
    cases = root.findall(".//testcase")
    failures = root.findall(".//failure")
    errors = root.findall(".//error")
    skipped = root.findall(".//skipped")
    if len(cases) != 1 or failures or errors or skipped:
        pytest.fail(
            f"fresh_child_junit_contract cases={len(cases)} failures={len(failures)} "
            f"errors={len(errors)} skipped={len(skipped)}"
        )
    expected_name = node.rsplit("::", 1)[-1]
    if cases[0].get("name") != expected_name:
        pytest.fail(f"fresh_child_junit_name actual={cases[0].get('name')!r}")
    markers = [
        prop
        for prop in root.findall(".//property")
        if prop.get("name") == FRESH_MARKER and prop.get("value") == node
    ]
    if len(markers) != 1:
        pytest.fail(f"fresh_child_junit_marker node={node!r} count={len(markers)}")


def run_fresh_process(node: str, report_dir: Path, runner: Runner | None = None) -> bool:
    if os.environ.get(CHILD_FLAG) == "1":
        return False
    report = report_dir / "fresh-child.xml"
    command = [
        sys.executable,
        "-B",
        "-m",
        "pytest",
        node,
        "-q",
        "-p",
        "no:cacheprovider",
        f"--junitxml={report}",
    ]
    try:
        execute = subprocess.run if runner is None else runner
        result = execute(
            command,
            check=False,
            capture_output=True,
            cwd=REPO_ROOT,
            env=_child_environment(),
            text=True,
            timeout=60,
        )
    except subprocess.TimeoutExpired:
        pytest.fail("fresh_child_timeout seconds=60")
    _assert_child_report(result, report, node)
    return True
