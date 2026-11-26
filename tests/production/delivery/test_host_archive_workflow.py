"""Produtor do archive de host: CLI sobre build_host_archive e workflow host-archive.yml (P28*)."""

from __future__ import annotations

import json
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
import yaml

from scripts.promotion import build_host_archive
from tests.production.delivery.conftest import require_tool
from tests.production.delivery.test_api_static_host_archive import CI, CONFIG, IMAGE, PLAN

ROOT = Path(__file__).resolve().parents[3]
WORKFLOW = ROOT / ".github/workflows/host-archive.yml"
SOURCE = "a" * 40
RELEASE = f"candidate-{SOURCE}-77-1"


def _argv(output: Path, **change: str) -> list[str]:
    values = {"--source-sha": SOURCE, "--api-image": IMAGE, "--platform-plan": json.dumps(PLAN),
              "--static-release": RELEASE, "--requested-action": "deploy", "--port": "18081",
              "--config": CONFIG.decode(), "--lifetime-hours": "20", "--output": str(output)}
    return [item for pair in (values | change).items() for item in pair]


def test_cli_escreve_o_archive_que_o_host_gate_aceita(tmp_path: Path) -> None:
    build_host_archive.main(_argv(tmp_path / "host-archive.tgz"))
    manifest = build_host_archive.archive_manifest((tmp_path / "host-archive.tgz").read_bytes())
    assert manifest["source_sha"] == SOURCE
    assert manifest["static_release"] == RELEASE
    assert manifest["requested_action"] == "deploy"
    assert manifest["images"] == {"api": IMAGE}
    assert manifest["platform_plan"] == PLAN


@pytest.mark.parametrize("change", [
    {"--requested-action": "destroy"}, {"--lifetime-hours": "25"}, {"--lifetime-hours": "0"},
    {"--platform-plan": "{}"}, {"--port": "80"}, {"--api-image": "nginx:latest"},
], ids=["acao", "validade-longa", "validade-zero", "plano", "porta", "imagem"])
def test_cli_recusa_entrada_fora_do_contrato_sem_escrever(
    tmp_path: Path, change: dict[str, str],
) -> None:
    with pytest.raises(build_host_archive.ArchiveError):
        build_host_archive.main(_argv(tmp_path / "out.tgz", **change))
    assert not (tmp_path / "out.tgz").exists()


def test_envelope_aceita_o_artefato_do_workflow_host_archive() -> None:
    now = datetime.now(UTC)
    manifest = build_host_archive.host_manifest(
        build_host_archive.HostRelease(SOURCE, IMAGE, PLAN, "deploy", now + timedelta(hours=6),
                                       RELEASE),
        build_host_archive.render_compose(IMAGE, 18081), CONFIG, now)
    ci = CI | {"workflow": ".github/workflows/host-archive.yml"}
    raw = build_host_archive.build_envelope(ci, manifest, now)
    assert json.loads(raw)["ci_evidence"]["workflow"] == ".github/workflows/host-archive.yml"


def _document() -> dict[str, Any]:
    return yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))


def test_workflow_so_roda_por_dispatch_em_main_no_sha_pedido() -> None:
    document = _document()
    assert set(document.get("on", document.get(True))) == {"workflow_dispatch"}
    assert document["permissions"] == {}
    (name, job), = document["jobs"].items()
    assert name == "archive"
    assert job["runs-on"] == "ubuntu-latest"
    assert job["permissions"] == {"contents": "read"}
    assert job["if"] == "github.ref == 'refs/heads/main' && github.sha == inputs.source_sha"
    assert "environment" not in job


def test_workflow_publica_um_unico_membro_com_o_nome_do_run() -> None:
    steps = _document()["jobs"]["archive"]["steps"]
    uploads = [step for step in steps
               if str(step.get("uses", "")).startswith("actions/upload-artifact@")]
    assert len(uploads) == 1
    assert uploads[0]["with"]["name"] == (
        "cnesdata-host-archive-${{ github.run_id }}-${{ github.run_attempt }}")
    assert uploads[0]["with"]["path"] == "${{ runner.temp }}/host-archive.tgz"
    assert "scripts.promotion.build_host_archive" in json.dumps(steps)


def test_actionlint_aceita_o_host_archive() -> None:
    done = subprocess.run(  # noqa: S603 -- actionlint fixo sobre o workflow versionado.
        [require_tool("actionlint"), str(WORKFLOW)], capture_output=True, text=True, check=False)
    assert done.returncode == 0, done.stdout + done.stderr
