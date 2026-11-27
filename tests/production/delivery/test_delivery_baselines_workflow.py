"""Job delivery-baselines de verify-production.yml: gatilho, runner, pins e artefatos (P23)."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

from scripts.promotion import delivery_baselines as db

ROOT = Path(__file__).resolve().parents[3]
WORKFLOW = ROOT / ".github/workflows/verify-production.yml"
UPLOAD = "actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02"
SAIDA = "${{ runner.temp }}/delivery-baselines"
CONFTEST_SHA256 = "c3f6b2a753bd56e377a1e51a95ae3e057f926f2e9139f7d5b5dfbbaf810f7985"


def _workflow() -> dict[Any, Any]:
    return yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))


def _job() -> dict[str, Any]:
    jobs = _workflow()["jobs"]
    assert "delivery-baselines" in jobs, "job_missing=delivery-baselines"
    return jobs["delivery-baselines"]


def _passos() -> list[dict[str, Any]]:
    return _job()["steps"]


def _uploads() -> list[dict[str, Any]]:
    return [p for p in _passos() if p.get("uses", "").startswith("actions/upload-artifact@")]


def test_job_so_roda_em_push_no_main_depois_do_job_de_politica_e_custo() -> None:
    job = _job()

    assert job["if"] == "github.event_name == 'push' && github.ref == 'refs/heads/main'"
    assert job["needs"] == "opentofu-policy-cost"


def test_job_usa_runner_hospedado_sem_segredo_ambiente_nem_aws() -> None:
    job = _job()
    texto = yaml.safe_dump(job)

    assert job["runs-on"] == "ubuntu-latest"
    assert "environment" not in job
    for proibido in ("secrets.", "self-hosted", "id-token", "aws-actions", "AWS_", "packages:"):
        assert proibido not in texto, proibido
    assert 1 <= job["timeout-minutes"] <= 35


def test_permissoes_do_job_sao_so_leitura_de_conteudo() -> None:
    workflow = _workflow()

    assert workflow["permissions"] == {"contents": "read"}
    assert _job().get("permissions", workflow["permissions"]) == {"contents": "read"}


def test_actions_fixadas_por_sha_e_checkout_em_s_sem_credenciais() -> None:
    passos = _passos()
    for passo in passos:
        if "uses" in passo:
            assert re.fullmatch(r"[\w/-]+@[0-9a-f]{40}", passo["uses"]), passo["uses"]
        assert "${{" not in passo.get("run", ""), passo.get("name")
    checkout = passos[0]
    assert checkout["uses"].startswith("actions/checkout@")
    assert checkout["with"] == {"ref": "${{ github.sha }}", "persist-credentials": False}


def test_pins_das_actions_reusam_os_do_job_de_politica_e_custo() -> None:
    jobs = _workflow()["jobs"]
    pins = {p["uses"] for p in jobs["opentofu-policy-cost"]["steps"] if "uses" in p}

    usados = {p["uses"] for p in _passos() if "uses" in p and "upload-artifact" not in p["uses"]}

    assert usados <= pins
    assert any("setup-opentofu" in item for item in usados)


def test_tofu_e_conftest_fixados_nas_versoes_do_gerador_e_com_hash() -> None:
    setup = next(p for p in _passos() if "setup-opentofu" in p.get("uses", ""))
    instalar = next(p for p in _passos() if "CONFTEST_SHA256" in p.get("env", {}))

    assert setup["with"] == {"tofu_version": db.TOFU_VERSION, "tofu_wrapper": False}
    assert instalar["env"]["CONFTEST_VERSION"] == db.CONFTEST_VERSION
    assert instalar["env"]["CONFTEST_SHA256"] == CONFTEST_SHA256
    assert "sha256sum -c -" in instalar["run"]


def test_espelho_vem_do_lock_somente_leitura_e_so_de_linux_amd64() -> None:
    comandos = "\n".join(p.get("run", "") for p in _passos())

    assert "init -backend=false -input=false -lockfile=readonly" in comandos
    assert "providers mirror -platform=linux_amd64" in comandos
    assert comandos.index("-lockfile=readonly") < comandos.index("providers mirror")
    assert comandos.index("providers mirror") < comandos.index("delivery_baselines")


def test_gerador_roda_em_s_com_binarios_resolvidos_no_path() -> None:
    gerar = next(p for p in _passos() if "scripts.promotion.delivery_baselines" in p.get("run", ""))
    comandos = " ".join(gerar["run"].replace("\\\n", " ").split())

    assert gerar["env"]["SOURCE_SHA"] == "${{ github.sha }}"
    assert gerar["env"]["OUTPUT_DIR"] == SAIDA
    assert comandos.startswith("set -euo pipefail ")
    assert "python -B -m scripts.promotion.delivery_baselines" in comandos
    assert '--source-sha "$SOURCE_SHA"' in comandos
    assert "--tofu-binary" not in comandos
    assert "--conftest-binary" not in comandos
    assert '--output-dir "$OUTPUT_DIR"' in comandos


def test_tres_artefatos_com_nome_estavel_e_retencao_de_30_dias() -> None:
    uploads = _uploads()

    assert [u["uses"] for u in uploads] == [UPLOAD] * 3
    assert [u["with"] for u in uploads] == [
        {"name": nome, "path": f"{SAIDA}/{nome}/", "if-no-files-found": "error",
         "overwrite": True, "retention-days": 30}
        for nome in db.ARTIFACTS
    ]


def test_artefatos_sao_criados_no_interior_do_run() -> None:
    passos = _passos()
    uploads = _uploads()
    indices = [passos.index(u) for u in uploads]
    final = passos[-1]

    assert indices == [len(passos) - 4, len(passos) - 3, len(passos) - 2]
    assert len({u["id"] for u in uploads}) == 3
    for upload in uploads:
        ref = f"${{{{ steps.{upload['id']}.outputs.artifact-id }}}}"
        assert ref in final["env"].values()
        assert f"${{{{ steps.{upload['id']}.outputs.artifact-digest }}}}" in final["env"].values()
    assert final["run"].rstrip().splitlines()[-1] == "sleep 3"


def test_outros_jobs_e_artefato_production_config_ficam_intactos() -> None:
    jobs = _workflow()["jobs"]

    assert set(jobs) == {"delivery-contracts", "dashboard", "oci-build", "opentofu-policy-cost",
                         "delivery-baselines", "oci-publish"}
    assert jobs["oci-publish"]["needs"] == ["oci-build", "dashboard", "delivery-contracts",
                                            "opentofu-policy-cost"]
    assert "production-config" in WORKFLOW.read_text(encoding="utf-8")
