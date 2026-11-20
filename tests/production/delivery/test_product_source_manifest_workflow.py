"""Workflow product-source-manifest: gatilho, permissões, runner e artefato do run em S (P09)."""

from __future__ import annotations

import re
import subprocess
from pathlib import Path
from typing import Any

import yaml

from scripts.promotion.product_source_manifest import MEMBER
from tests.production.delivery.conftest import require_tool

ROOT = Path(__file__).resolve().parents[3]
WORKFLOW = ROOT / ".github/workflows/product-source-manifest.yml"
ARTEFATO = ("product-source-manifest-${{ github.sha }}-${{ github.run_id }}"
            "-${{ github.run_attempt }}")
UPLOAD = "actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02"


def _workflow() -> dict[Any, Any]:
    assert WORKFLOW.is_file(), "workflow_missing=product-source-manifest.yml"
    return yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))


def _job() -> dict[str, Any]:
    jobs = _workflow()["jobs"]
    assert list(jobs) == ["manifest"]
    return jobs["manifest"]


def _passos() -> list[dict[str, Any]]:
    return _job()["steps"]


def test_dispara_so_em_push_no_main_e_dispatch_manual() -> None:
    workflow = _workflow()
    gatilhos = workflow["on"] if "on" in workflow else workflow[True]

    assert gatilhos == {"push": {"branches": ["main"]}, "workflow_dispatch": None}
    assert _job()["if"] == "github.ref == 'refs/heads/main'"


def test_sem_permissao_global_e_job_so_le_conteudo() -> None:
    assert _workflow()["permissions"] == {}
    assert _job()["permissions"] == {"contents": "read"}


def test_job_roda_em_runner_hospedado_fixo_sem_environment_nem_segredos() -> None:
    job = _job()
    texto = WORKFLOW.read_text(encoding="utf-8")

    assert job["runs-on"] == "ubuntu-24.04"
    assert "environment" not in job
    assert "secrets." not in texto
    assert "self-hosted" not in texto
    assert 1 <= job["timeout-minutes"] <= 15


def test_actions_fixadas_por_sha_e_checkout_sem_credenciais_em_s() -> None:
    passos = _passos()
    for passo in passos:
        if "uses" in passo:
            assert re.fullmatch(r"[\w/-]+@[0-9a-f]{40}", passo["uses"]), passo["uses"]
        assert "${{" not in passo.get("run", ""), passo.get("name")
    checkout = passos[0]
    assert checkout["uses"].startswith("actions/checkout@")
    assert checkout["with"] == {"ref": "${{ github.sha }}", "persist-credentials": False}


def test_gerador_roda_sobre_github_sha_e_grava_o_membro_unico() -> None:
    gerar = next(p for p in _passos() if "-m scripts.promotion." in p.get("run", ""))
    comandos = " ".join(gerar["run"].replace("\\\n", " ").split())

    assert gerar["env"] == {
        "SOURCE_SHA": "${{ github.sha }}",
        "MANIFEST": f"${{{{ runner.temp }}}}/product-source-manifest/{MEMBER}",
    }
    assert comandos == ("set -euo pipefail python -B -m scripts.promotion.product_source_manifest "
                        '--source-sha "$SOURCE_SHA" --output "$MANIFEST"')


def test_artefato_tem_nome_por_tentativa_e_um_unico_membro() -> None:
    upload = next(p for p in _passos() if p.get("uses", "").startswith("actions/upload-artifact@"))

    assert upload["uses"] == UPLOAD
    assert upload["with"] == {
        "name": ARTEFATO,
        "path": f"${{{{ runner.temp }}}}/product-source-manifest/{MEMBER}",
        "if-no-files-found": "error",
        "retention-days": 30,
    }


def test_artefato_e_criado_no_interior_do_run() -> None:
    passos = _passos()
    indice = next(i for i, p in enumerate(passos)
                  if p.get("uses", "").startswith("actions/upload-artifact@"))
    final = passos[-1]

    assert indice == len(passos) - 2
    assert final["env"] == {
        "ARTIFACT_ID": f"${{{{ steps.{passos[indice]['id']}.outputs.artifact-id }}}}",
        "ARTIFACT_DIGEST": f"${{{{ steps.{passos[indice]['id']}.outputs.artifact-digest }}}}",
    }
    assert final["run"].rstrip().splitlines()[-1] == "sleep 3"


def test_actionlint_valida_o_workflow() -> None:
    subprocess.run(  # noqa: S603 -- Validates the checked-in workflow with fixed actionlint.
        [require_tool("actionlint"), str(WORKFLOW)],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
