"""Regressão dos metadados persistidos pelos writers nativos."""

import json
import subprocess
from pathlib import Path

import pytest

from tests.production.infra import _processing_native, test_ecr_ecs


def _environment(evidence: Path) -> dict[str, str]:
    return {
        "CI": "true",
        "PATH": "/synthetic/tools",
        "PROCESSING_SOURCE_BINDINGS": "/synthetic/bindings.json",
        "CHECKPOINT_DISABLE": "1",
        "TF_CLI_CONFIG_FILE": "/synthetic/tofu.rc",
        "TF_DATA_DIR": str(evidence),
        "TF_INPUT": "0",
        "TF_IN_AUTOMATION": "1",
        "TF_PLUGIN_CACHE_DIR": "/synthetic/plugins",
    }


def _secrets() -> dict[str, str]:
    names = (
        "TF_VAR_example",
        "TF_ENCRYPTION",
        "AWS_SECRET_ACCESS_KEY",
        "CLOUDFLARE_API_TOKEN",
        "PENPOT_AUTH_TOKEN",
        "TS_AUTHKEY",
        "PASSPHRASE",
        "UNLISTED_NATIVE_SETTING",
        "TF_CLI_ARGS",
    )
    return {name: f"synthetic-private-sentinel-{index}" for index, name in enumerate(names)}


@pytest.mark.parametrize("writer", ["processing", "registry"])
@pytest.mark.parametrize("returncode", [0, 17])
def test_writers_persistem_so_metadados_permitidos_sem_alterar_execucao(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, writer: str, returncode: int
) -> None:
    root = tmp_path / "root"
    root.mkdir()
    evidence = tmp_path / "evidence"
    evidence.mkdir()
    allowed = _environment(evidence)
    secrets = _secrets()
    environment = allowed | secrets
    original = dict(environment)
    arguments = ("test", "-no-color", "-json", "-verbose")
    argv = ["/synthetic/tofu", f"-chdir={root}", *arguments]
    completed = subprocess.CompletedProcess(argv, returncode, "native-stdout\n", "native-stderr\n")

    def run(actual: list[str], **options: object) -> subprocess.CompletedProcess[str]:
        assert actual == argv
        assert options == {
            "check": False,
            "capture_output": True,
            "cwd": root,
            "env": original,
            "text": True,
        }
        return completed

    monkeypatch.setattr(subprocess, "run", run)
    tofu = ("/synthetic/tofu", environment)
    if writer == "processing":
        result = _processing_native._native_result(tofu, root, arguments, "processing-native")
    else:
        result = test_ecr_ecs._native_result(tofu, root, arguments)
    label = f"{writer}-native"
    serialized = (evidence / f"{label}.command.json").read_text(encoding="utf-8")
    assert all(value not in serialized for value in secrets.values()), "private_metadata_persisted"
    assert json.loads(serialized) == {
        "argv": argv,
        "cwd": str(root),
        "environment": allowed,
        "rc": returncode,
    }
    assert (evidence / f"{label}.stdout").read_text(encoding="utf-8") == "native-stdout\n"
    assert (evidence / f"{label}.stderr").read_text(encoding="utf-8") == "native-stderr\n"
    assert result is completed
    assert environment == original
