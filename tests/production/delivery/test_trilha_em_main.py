"""Trilha de entrega ancorada em main: gatilhos, pins do produtor e segredo por nome."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from scripts.promotion._api_static_handoff import HOST_GATE_SHA
from scripts.promotion.verify_plan_artifact import PRODUCER_SHA

WORKFLOWS = Path(__file__).resolve().parents[3] / ".github/workflows"
PRODUCER = "6cb0bb2f46007c3679d64f62106ee02bf8758c5d"
REUSABLE = "VINIClUS/infra-ansible/.github/workflows/reusable-cnesdata-"
SECRET = {"CNESDATA_DELIVERY_GOVERNANCE": "${{ secrets.CNESDATA_DELIVERY_GOVERNANCE }}"}


def _carregar(nome: str) -> dict[Any, Any]:
    return yaml.safe_load((WORKFLOWS / nome).read_text(encoding="utf-8"))


def _texto(nome: str) -> str:
    return (WORKFLOWS / nome).read_text(encoding="utf-8")


def test_pins_do_produtor_apontam_para_o_merge_do_gate_com_segredo_declarado() -> None:
    assert PRODUCER_SHA == PRODUCER
    assert HOST_GATE_SHA == PRODUCER


@pytest.mark.parametrize(
    ("arquivo", "reusable"),
    [("infra-plan.yml", "infra-plan-gate"), ("infra-apply.yml", "infra-deploy-gate")],
)
def test_callers_de_plano_e_deploy_fixam_pin_inteiro_e_passam_segredo_por_nome(
    arquivo: str, reusable: str
) -> None:
    gate = _carregar(arquivo)["jobs"]["gate"]
    assert gate["uses"] == f"{REUSABLE}{reusable}.yml@{PRODUCER}"
    assert gate["secrets"] == SECRET
    assert gate["secrets"] != "inherit"
    assert "inherit" not in _texto(arquivo)


def test_caller_do_host_mantem_host_governance_no_pin_novo() -> None:
    gate = _carregar("promote-production.yml")["jobs"]["host-gate"]
    assert gate["uses"] == f"{REUSABLE}host-deploy-gate.yml@{PRODUCER}"
    assert gate["secrets"] == {"host_governance": "${{ secrets.CNESDATA_HOST_GOVERNANCE }}"}
    assert "inherit" not in _texto("promote-production.yml")


def test_promocao_so_admite_main_e_compara_o_sha_contra_main() -> None:
    preamble = _carregar("promote-production.yml")["jobs"]["preamble"]
    assert preamble["if"] == "github.ref == 'refs/heads/main' && github.sha == inputs.source_sha"
    texto = _texto("promote-production.yml")
    assert 'compare/$SOURCE_SHA...main"' in texto
    assert "develop" not in texto


def test_candidato_so_e_cortado_a_partir_de_main() -> None:
    texto = _texto("release-candidate.yml")
    assert _carregar("release-candidate.yml")["jobs"]["candidate"]["if"] == (
        "github.ref == 'refs/heads/main'"
    )
    assert "develop" not in texto


@pytest.mark.parametrize("arquivo", ["infra-plan.yml", "infra-apply.yml"])
def test_guarda_dos_callers_de_infra_exige_main(arquivo: str) -> None:
    guarda = _carregar(arquivo)["jobs"]["admit"]["steps"][0]["run"]
    assert 'test "$GITHUB_REF" = "refs/heads/main"' in guarda
    assert "develop" not in guarda


def test_verificacao_de_producao_dispara_e_publica_apenas_em_main() -> None:
    workflow = _carregar("verify-production.yml")
    assert workflow[True]["push"] == {"branches": ["main"]}
    assert workflow["jobs"]["oci-publish"]["if"] == (
        "github.event_name == 'push' && github.ref == 'refs/heads/main'"
    )
    assert "develop" not in _texto("verify-production.yml")
