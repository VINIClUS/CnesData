"""Validação do static-promotion.tf sobre o fragmento rendido pelo infra-ansible."""

import json
import re
import shutil
from copy import deepcopy
from pathlib import Path

import pytest

from tests.production.infra._processing_native import _native_plan_events, _native_result
from tests.production.infra._static_promotion_iam_model import (
    suites_isoladas,
    verificar_cobertura_nativa,
    verificar_rejeicao_nativa,
)

CENARIO = Path(__file__).resolve().parents[2] / "fixtures/static-promotion-cross-repo"
RENDER = json.loads((CENARIO / "render.json").read_text())
ENTRADA = RENDER["input"]
PRODUTOR = RENDER["provenance"]["producer_sha"]
CONTA = RENDER["provenance"]["synthetic"]["account_id"]


def _cabecalho() -> str:
    texto = 'mock_provider "aws" {}\n'
    for alias in ("observer", "normal", "recovery"):
        nome = f"cnesdata-prod-static-{alias}"
        valores = {"arn": f"arn:aws:iam::{CONTA}:role/{nome}", "id": nome}
        texto += (f"override_resource {{\n target = module.subject.aws_iam_role.static_{alias}[0]"
                  f"\n values = {json.dumps(valores)}\n}}\n")
    return texto


def _com(caminho: tuple[str, ...], valor: object) -> dict:
    caso = deepcopy(ENTRADA)
    destino = caso
    for chave in caminho[:-1]:
        destino = destino[chave]
    destino[caminho[-1]] = valor
    return caso


def _casos() -> dict[str, dict]:
    promocao = ("application_promotion", "caller_by_purpose", "promotion", "ref")
    drill = ("application_promotion", "caller_by_purpose", "mode-readiness-drill", "ref")
    return {
        "aceita_fragmento_do_produtor": ENTRADA,
        "invalido_ref_develop_na_promocao": _com(promocao, "refs/heads/develop"),
        "invalido_ref_develop_no_drill": _com(drill, "refs/heads/develop"),
        "invalido_workflow_sha_diverge_do_binding": _com(
            ("binding", "shared_source_sha"), "0" * 40),
        "invalido_trust_sha256_malformado": _com(
            ("application_promotion", "trust_sha256"), "nao-e-digest"),
        "trust_sha256_bem_formado_diferente": _com(
            ("application_promotion", "trust_sha256"), "9" * 64),
    }


@pytest.fixture(scope="module", name="observado")
def fixture_observado(tmp_path_factory, prod_root, tofu_context):
    raiz = tmp_path_factory.mktemp("static-promotion-cross-repo")
    shutil.copytree(CENARIO, raiz, dirs_exist_ok=True)
    fonte = raiz / "main.tf"
    origem = "../../../infra/opentofu/modules/runtime-iam"
    modulo = str((prod_root / "../../modules/runtime-iam").resolve())
    fonte.write_text(fonte.read_text().replace(origem, modulo))
    lock = (prod_root / ".terraform.lock.hcl").read_text()
    aws = re.search(r'(?ms)^provider "registry.opentofu.org/hashicorp/aws" \{.*?^\}', lock)
    assert aws is not None
    selecionado = re.sub(r'constraints = ".*?"', 'constraints = "6.54.0"', aws[0])
    (raiz / ".terraform.lock.hcl").write_text(selecionado + "\n")
    casos = _casos()
    diretorio = raiz / "casos"
    diretorio.mkdir()
    suites = suites_isoladas(casos, {"static_web": RENDER["static_web"]}, _cabecalho())
    for nome, texto in suites.items():
        (diretorio / f"{nome}.tftest.hcl").write_text(texto)
    dados = raiz / ".tofu"
    dados.mkdir()
    contexto = tofu_context[0], tofu_context[1] | {"TF_DATA_DIR": str(dados)}
    init = _native_result(
        contexto, raiz, ("init", "-backend=false", "-input=false", "-lockfile=readonly"),
        "cross-init",
    )
    assert init.returncode == 0, init.stdout + init.stderr
    saida = _native_result(
        contexto, raiz, ("test", "-json", "-no-color", "-verbose", "-test-directory=casos"),
        "cross-plan",
    )
    estados, planos, _ = _native_plan_events(saida.stdout)
    diagnosticos = [json.loads(linha) for linha in saida.stdout.splitlines()
                    if json.loads(linha).get("type") == "diagnostic"]
    resultado = {"plans": planos, "statuses": estados, "diagnostics": diagnosticos,
                 "returncode": saida.returncode}
    verificar_cobertura_nativa(resultado, casos)
    return resultado


def test_fixture_registra_origem_e_sha_do_produtor() -> None:
    proveniencia = RENDER["provenance"]
    assert re.fullmatch(r"[0-9a-f]{40}", PRODUTOR)
    assert proveniencia["command"].count(PRODUTOR) == 2
    assert ENTRADA["application_promotion"]["producer"]["workflow_sha"] == PRODUTOR
    assert ENTRADA["binding"]["shared_source_sha"] == PRODUTOR
    assert CONTA == "111122223333"


def test_aceita_fragmento_rendido_pelo_produtor_com_ref_main(observado) -> None:
    assert observado["statuses"]["aceita_fragmento_do_produtor"] == "pass"
    callers = ENTRADA["application_promotion"]["caller_by_purpose"]
    assert {c["ref"] for c in callers.values()} == {"refs/heads/main"}


@pytest.mark.parametrize(
    "caso",
    [
        "invalido_ref_develop_na_promocao",
        "invalido_ref_develop_no_drill",
        "invalido_workflow_sha_diverge_do_binding",
        "invalido_trust_sha256_malformado",
    ],
)
def test_recusa_variante_que_diverge_do_fragmento_do_produtor(observado, caso) -> None:
    verificar_rejeicao_nativa(observado, caso)


def test_validacao_vincula_so_o_formato_de_trust_sha256_e_nao_o_valor(observado) -> None:
    assert observado["statuses"]["trust_sha256_bem_formado_diferente"] == "pass"
