"""Caller sign-activation: signing no pin único, contrato e catálogo operacional (P26)."""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
from pathlib import Path
from shutil import which
from typing import Any

import pytest
import yaml

from tests.production.delivery._plan_apply_fixtures import PRODUCER
from tests.production.delivery.conftest import require_tool

ROOT = Path(__file__).resolve().parents[3]
CAMINHO = ".github/workflows/sign-activation.yml"
WORKFLOW = ROOT / CAMINHO
VECTOR = Path(__file__).with_name("signing_gate_call_vector.json")
REUSABLE = "VINIClUS/infra-ansible/"
CALLER_JOB = "sign"
SEGREDO = "CNESDATA_DELIVERY_GOVERNANCE"
SHA = re.compile(r"(?<![0-9a-f])[0-9a-f]{40}(?![0-9a-f])")
SOURCE = "a" * 40
CORPO = '{"mode":"activation","request_version":1,"source_sha":"' + SOURCE + '"}'
DESCRITOR = CORPO + "\n"


def _vetor() -> dict[str, Any]:
    return json.loads(VECTOR.read_bytes())


def _workflow() -> dict[Any, Any]:
    assert WORKFLOW.is_file(), "workflow_missing=sign-activation.yml"
    return yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))


def _jobs() -> dict[str, Any]:
    return _workflow()["jobs"]


def _sign() -> dict[str, Any]:
    return _jobs()[CALLER_JOB]


def _inputs_do_reusable() -> dict[str, Any]:
    return _vetor()["reusable"]["workflow_call"]["inputs"]


def _uniao(permissoes: dict[str, dict[str, str]]) -> dict[str, str]:
    uniao: dict[str, str] = {}
    for job in permissoes.values():
        for escopo, nivel in job.items():
            if uniao.get(escopo) != "write":
                uniao[escopo] = nivel
    return uniao


def _sha(texto: str) -> str:
    return hashlib.sha256(texto.encode()).hexdigest()


def _saidas(arquivo: Path) -> dict[str, str]:
    linhas = arquivo.read_text(encoding="utf-8").split("\n")
    saidas: dict[str, str] = {}
    indice = 0
    while indice < len(linhas) and linhas[indice]:
        linha = linhas[indice]
        if "<<" in linha:
            chave, delimitador = linha.split("<<", 1)
            fim = linhas.index(delimitador, indice + 1)
            saidas[chave] = "\n".join(linhas[indice + 1 : fim])
            indice = fim + 1
            continue
        chave, valor = linha.split("=", 1)
        saidas[chave] = valor
        indice += 1
    return saidas


def test_dispara_so_por_dispatch_com_os_inputs_do_reusable_exceto_mode() -> None:
    workflow = _workflow()
    gatilhos = workflow["on"] if "on" in workflow else workflow[True]
    esperados = {nome: spec for nome, spec in _inputs_do_reusable().items() if nome != "mode"}

    assert list(gatilhos) == ["workflow_dispatch"]
    assert gatilhos["workflow_dispatch"]["inputs"] == esperados


def test_unico_job_chama_o_reusable_de_signing_no_pin_da_constante_unica() -> None:
    jobs = _jobs()
    caminho = _vetor()["reusable"]["path"]
    chamadas = [nome for nome, job in jobs.items() if "uses" in job]

    assert list(jobs) == ["admit", CALLER_JOB]
    assert chamadas == [CALLER_JOB]
    assert _sign()["uses"] == f"{REUSABLE}{caminho}@{PRODUCER}"
    assert "strategy" not in _sign()
    assert "name" not in _sign()


def test_with_secrets_e_permissoes_conferem_com_o_workflow_call_no_pin() -> None:
    vetor = _vetor()["reusable"]
    sign = _sign()
    texto = WORKFLOW.read_text(encoding="utf-8")
    obrigatorios = {nome for nome, spec in _inputs_do_reusable().items() if spec["required"]}

    assert set(sign["with"]) == set(_inputs_do_reusable()) == obrigatorios
    assert set(sign["secrets"]) == set(vetor["workflow_call"]["secrets"]) == {SEGREDO}
    assert sign["secrets"] == {SEGREDO: "${{ secrets.CNESDATA_DELIVERY_GOVERNANCE }}"}
    assert sign["permissions"] == _uniao(vetor["permissions"])
    assert _workflow()["permissions"] == {}
    assert "inherit" not in texto


def test_inputs_do_reusable_vem_do_admit_e_mode_e_literal() -> None:
    transportados = {nome for nome in _inputs_do_reusable() if nome != "mode"}

    assert _sign()["needs"] == "admit"
    assert {nome: valor for nome, valor in _sign()["with"].items() if nome != "mode"} == {
        nome: f"${{{{ needs.admit.outputs.{nome} }}}}" for nome in transportados
    }
    assert set(_jobs()["admit"]["outputs"]) == transportados


def test_mode_do_caller_e_o_que_publica_a_selecao_do_plan_operacional() -> None:
    vetor = _vetor()
    publicador = vetor["reusable"]["publisher"]
    consumidor = vetor["consumer"]
    modo = re.fullmatch(r"\$\{\{ inputs\.mode == '([a-z]+)' \}\}", publicador["if"])
    selecao = consumidor["activation_selection"]

    assert modo is not None
    assert _sign()["with"]["mode"] == modo.group(1)
    assert consumidor["step"] == {
        "role": "plan",
        "purpose": "application-prepare",
        "mode": "operational",
    }
    assert selecao == {
        "fields": ["ext2_post_upload_receipt_v1", "selection_version"],
        "selection_version": 1,
    }
    assert vetor["reusable"]["workflow_call"]["outputs"] == ["ext2_post_upload_receipt_v1"]
    assert consumidor["verification_mode"] != modo.group(1)


def test_caminho_e_job_do_caller_sao_os_fixados_pela_governanca_ext2() -> None:
    vetor = _vetor()
    publicador = f"{CALLER_JOB} / {vetor['reusable']['publisher']['job']}"

    assert WORKFLOW.relative_to(ROOT).as_posix() == CAMINHO
    assert "name" not in _sign()
    assert re.fullmatch(vetor["consumer"]["caller_workflow_path"], CAMINHO)
    assert "caller_workflow_path" in vetor["consumer"]["reference_fields"]
    assert publicador == "sign / sign-store-publish"


def test_admit_roda_sem_credenciais_antes_do_environment_do_reusable() -> None:
    admit = _jobs()["admit"]
    texto = WORKFLOW.read_text(encoding="utf-8")

    assert admit["runs-on"] == "ubuntu-latest"
    assert admit["permissions"] == {}
    assert 1 <= admit["timeout-minutes"] <= 10
    assert "environment" not in admit
    assert "environment" not in _sign()
    assert _vetor()["reusable"]["environment"] == "production"
    assert all("uses" not in passo for passo in admit["steps"])
    assert "self-hosted" not in texto
    assert _workflow()["concurrency"] == {"group": "sign-activation", "cancel-in-progress": False}


def test_guarda_exige_o_runtime_que_o_reusable_afirma() -> None:
    runtime = _vetor()["reusable"]["runtime"]
    guarda = _jobs()["admit"]["steps"][0]["run"]

    assert f'test "$GITHUB_REPOSITORY" = "{runtime["repository"]}"' in guarda
    assert f'test "$GITHUB_EVENT_NAME" = "{runtime["event"]}"' in guarda
    assert f'test "$GITHUB_REF" = "{runtime["ref"]}"' in guarda
    assert "${{" not in guarda


CASOS = {
    "valido": {},
    "valido_com_quebra": {"REQUEST_DESCRIPTOR": DESCRITOR},
    "fork": {"GITHUB_REPOSITORY": "other/CnesData"},
    "branch": {"GITHUB_REF": "refs/heads/develop"},
    "evento": {"GITHUB_EVENT_NAME": "push"},
    "source": {"EXPECTED_SOURCE_SHA": "b" * 40},
    "source_curto": {"EXPECTED_SOURCE_SHA": "a" * 39, "GITHUB_SHA": "a" * 39},
    "digest": {"REQUEST_DESCRIPTOR_SHA256": "0" * 64},
    "digest_sem_quebra": {"REQUEST_DESCRIPTOR_SHA256": _sha(CORPO)},
    "vazio": {"REQUEST_DESCRIPTOR": "", "REQUEST_DESCRIPTOR_SHA256": _sha("\n")},
    "multilinha": {
        "REQUEST_DESCRIPTOR": CORPO + "\n" + CORPO,
        "REQUEST_DESCRIPTOR_SHA256": _sha(CORPO + "\n" + CORPO + "\n"),
    },
}


@pytest.mark.parametrize("caso", sorted(CASOS))
def test_guarda_do_admit_valida_contexto_e_transporta_o_descriptor_canonico(
    caso: str, tmp_path: Path
) -> None:
    saida = tmp_path / "github_output"
    saida.touch()
    env = (
        dict(
            os.environ,
            GITHUB_REPOSITORY="VINIClUS/CnesData",
            GITHUB_EVENT_NAME="workflow_dispatch",
            GITHUB_REF="refs/heads/main",
            GITHUB_SHA=SOURCE,
            GITHUB_OUTPUT=str(saida),
            EXPECTED_SOURCE_SHA=SOURCE,
            REQUEST_DESCRIPTOR=CORPO,
            REQUEST_DESCRIPTOR_SHA256=_sha(DESCRITOR),
        )
        | CASOS[caso]
    )
    bash = which("bash")
    assert bash is not None
    resultado = subprocess.run(  # noqa: S603 -- Executes the checked-in guard script.
        [bash, "-c", _jobs()["admit"]["steps"][0]["run"]],
        env=env,
        capture_output=True,
        check=False,
    )

    if not caso.startswith("valido"):
        assert resultado.returncode != 0
        assert saida.read_text(encoding="utf-8") == ""
        return
    assert resultado.returncode == 0, resultado.stderr
    assert _saidas(saida) == {
        "request_descriptor": DESCRITOR,
        "request_descriptor_sha256": _sha(DESCRITOR),
        "expected_source_sha": SOURCE,
    }


def test_nenhum_sha_literal_alem_do_pin_da_constante_unica() -> None:
    assert SHA.findall(WORKFLOW.read_text(encoding="utf-8")) == [PRODUCER]
    assert SHA.findall(VECTOR.read_text(encoding="utf-8")) == []
    assert SHA.findall(Path(__file__).read_text(encoding="utf-8")) == []


def test_actionlint_valida_o_workflow() -> None:
    subprocess.run(  # noqa: S603 -- Validates the checked-in workflow with fixed actionlint.
        [require_tool("actionlint"), str(WORKFLOW)],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
