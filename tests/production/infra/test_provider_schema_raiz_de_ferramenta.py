"""Raiz de ferramenta de _provider_schema com o processo tofu substituído na fronteira."""

import json
import re
import subprocess
from collections.abc import Sequence
from pathlib import Path

import pytest

from tests.production.infra import _processing_native
from tests.production.infra._processing_native import Artifacts, Tofu, _provider_schema

AWS = "registry.opentofu.org/hashicorp/aws"
ARCHIVE = "registry.opentofu.org/hashicorp/archive"
SELECTIONS = {AWS: "6.54.0", ARCHIVE: "2.8.1"}
REQUIRED = re.compile(r"^    (\w+) = (\{.*\})$", re.MULTILINE)


def _artifacts() -> Artifacts:
    return {
        "configuration": {"provider_config": {"aws": {"name": "aws", "full_name": AWS}}},
        "version": {"provider_selections": dict(SELECTIONS)},
    }


def _workspace(tmp_path: Path) -> tuple[Tofu, Path]:
    root = tmp_path / "root"
    root.mkdir()
    (root / ".terraform.lock.hcl").write_text("lock\n", encoding="utf-8")
    data = tmp_path / "data"
    data.mkdir()
    return ("tofu", {"TF_DATA_DIR": str(data)}), root


def _stub_tofu(monkeypatch: pytest.MonkeyPatch, selected: dict[str, str]) -> None:
    def run(argv: Sequence[str], **_: object) -> subprocess.CompletedProcess[str]:
        outputs = {
            "schema": json.dumps({"provider_schemas": {AWS: {}}}),
            "version": json.dumps({"provider_selections": selected}),
        }
        stdout = next((text for key, text in outputs.items() if key in argv), "")
        return subprocess.CompletedProcess(list(argv), 0, stdout, "")

    monkeypatch.setattr(_processing_native.subprocess, "run", run)


def test_raiz_de_ferramenta_exige_todos_os_providers_selecionados_em_versao_exata(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    tofu, root = _workspace(tmp_path)
    _stub_tofu(monkeypatch, dict(SELECTIONS))
    schema = _provider_schema(tofu, root, _artifacts())
    tooling = (tmp_path / "data" / "processing-provider-schema" / "main.tf").read_text("utf-8")
    declared = {name: json.loads(body) for name, body in REQUIRED.findall(tooling)}
    assert declared == {
        "aws": {"source": AWS, "version": "=6.54.0"},
        "archive": {"source": ARCHIVE, "version": "=2.8.1"},
    }
    assert schema == {"provider_schemas": {AWS: {}}}


def test_selecao_de_providers_divergente_dos_artefatos_falha_o_setup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    tofu, root = _workspace(tmp_path)
    _stub_tofu(monkeypatch, {AWS: "6.54.0"})
    with pytest.raises(pytest.fail.Exception, match="schema_provider_selection"):
        _provider_schema(tofu, root, _artifacts())
