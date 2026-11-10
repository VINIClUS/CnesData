"""Capacidades estáticas observadas em planos nativos com identidades sintéticas."""

import json
import re
import shutil
from copy import deepcopy
from hashlib import sha256
from pathlib import Path

import pytest

from tests.production.infra._athena_root_plan import _phase_b_mock_root
from tests.production.infra._mode_canary_native import (
    _comuns,
    _entrada,
    _identidades,
    fixture_canary_plan,  # noqa: F401
)
from tests.production.infra._mode_canary_native import (
    _recursos as _recursos_modo,
)
from tests.production.infra._processing_native import (
    _native_plan_events,
    _native_result,
    _suite,
)
from tests.production.infra._root_native_identities import identidades_estaticas
from tests.production.infra._root_policy_inventory import assert_root_policy_inventory
from tests.production.infra._static_promotion_iam_model import (
    _bootstrap,
    _mutantes,
    autoriza,
    documentos_do_plano,
    suites_isoladas,
    verificar_cobertura_nativa,
    verificar_matriz,
    verificar_rejeicao_nativa,
)

CENARIO = Path(__file__).resolve().parents[2] / "fixtures/static-promotion-iam"
VETORES = json.loads((CENARIO / "requests.json").read_text())
ENTRADA = VETORES["input"]
ALIASES = ("observer", "normal", "recovery")
PREFIXO = "module.subject"
PRINCIPAL = ENTRADA["application_promotion"]["application_role_arn"]
SESSAO_AMPLA = {"Statement": [{"Effect": "Allow", "Action": "*", "Resource": "*"}]}


def _sha(texto):
    return sha256(texto.encode()).hexdigest()


def _cabecalho_estatico(materializado):
    texto = (CENARIO / "production.tftest.hcl").read_text()
    for alias in ALIASES if materializado else ():
        nome = f"cnesdata-prod-static-{alias}"
        valores = {"arn": f"arn:aws:iam::123456789012:role/{nome}", "id": nome}
        texto += (f"override_resource {{\n target = {PREFIXO}.aws_iam_role.static_{alias}[0]"
                  f"\n values = {json.dumps(valores)}\n}}\n")
    return texto


@pytest.fixture(scope="module", name="estatico")
def fixture_estatico(tmp_path_factory, prod_root, tofu_context):
    raiz = tmp_path_factory.mktemp("static-promotion-iam")
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
    casos = {"desligado": None, "bootstrap": _bootstrap(ENTRADA), "operacional": ENTRADA}
    casos |= {"sem_baseline": ENTRADA | {"rollback_versions": []}} | _mutantes(ENTRADA)
    historico = deepcopy(ENTRADA)
    historico["application_promotion"]["predecessor_reads"] = VETORES["predecessor_reads"]
    casos["historico"] = historico
    casos["release_curta"] = ENTRADA | {
        "binding": ENTRADA["binding"] | {"release_id": "r"},
    }
    casos["operacional_alternativo"] = ENTRADA | VETORES["alternative_changes"]
    materializado = (prod_root / "../../modules/runtime-iam/static-promotion.tf").is_file()
    (raiz / "production.tftest.hcl").unlink()
    diretorio = raiz / "casos"
    diretorio.mkdir()
    suites = suites_isoladas(casos, {"static_web": VETORES["static_web"]},
                            _cabecalho_estatico(materializado))
    for nome, texto in suites.items():
        (diretorio / f"{nome}.tftest.hcl").write_text(texto)
    dados = raiz / ".tofu"
    dados.mkdir()
    contexto = tofu_context[0], tofu_context[1] | {"TF_DATA_DIR": str(dados)}
    resultado = _native_result(
        contexto, raiz, ("init", "-backend=false", "-input=false", "-lockfile=readonly"),
        "static-init",
    )
    assert resultado.returncode == 0, resultado.stdout + resultado.stderr
    resultado = _native_result(
        contexto, raiz, ("test", "-json", "-no-color", "-verbose", "-test-directory=casos"),
        "static-plan",
    )
    estados, planos, _ = _native_plan_events(resultado.stdout)
    diagnosticos = [json.loads(linha) for linha in resultado.stdout.splitlines()
                   if json.loads(linha).get("type") == "diagnostic"]
    dados = {"plans": planos, "statuses": estados, "diagnostics": diagnosticos,
             "returncode": resultado.returncode}
    verificar_cobertura_nativa(dados, casos)
    return dados


def _plano(estatico, nome):
    assert estatico["statuses"].get(nome) == "pass", estatico["diagnostics"]
    return estatico["plans"][nome]


def _recursos(plano):
    return {
        r["address"]: r["change"]["after"] for r in plano["resource_changes"]
        if r["mode"] == "managed"
    }


def _enderecos(aliases):
    return {
        f"{PREFIXO}.{tipo}.static_{alias}[0]" for alias in aliases
        for tipo in ("aws_iam_role", "aws_iam_role_policy")
    }


def test_nulo_nao_cria_capacidade_estatica(estatico):
    plano = _plano(estatico, "desligado")
    assert not _enderecos(ALIASES) & _recursos(plano).keys()
    assert plano["planned_values"]["outputs"]["static_promotion_iam_contract"]["value"] is None


def test_bootstrap_cria_somente_observer_e_operacional_preserva_base(estatico):
    _plano(estatico, "release_curta")
    desligado = _recursos(_plano(estatico, "desligado"))
    bootstrap = _recursos(_plano(estatico, "bootstrap"))
    operacional = _recursos(_plano(estatico, "operacional"))
    assert bootstrap.keys() - desligado.keys() == _enderecos(["observer"])
    assert operacional.keys() - desligado.keys() == _enderecos(ALIASES)
    assert {k: bootstrap[k] for k in desligado} == desligado
    assert {k: operacional[k] for k in bootstrap} == bootstrap


@pytest.mark.parametrize("nome", list(_mutantes(ENTRADA)))
def test_contrato_incompleto_ou_divergente_recusa_com_diagnostico(estatico, nome):
    verificar_rejeicao_nativa(estatico, nome)


@pytest.mark.parametrize("sessao", [None, SESSAO_AMPLA], ids=["omitida", "ampla"])
def test_policy_base_efetiva_impoe_caps_mesmo_sem_sessao(estatico, sessao):
    documentos, _ = documentos_do_plano(_plano(estatico, "operacional"))
    verificar_matriz(documentos, VETORES["requests"], sessao)


def test_sessao_estreita_reduz_capacidade_sem_criar_permissao(estatico):
    documentos, _ = documentos_do_plano(_plano(estatico, "operacional"))
    sessao = {"Statement": [{"Effect": "Allow", "Action": "s3:GetObjectVersion",
                             "Resource": VETORES["static_web"]["bucket_arn"] + "/*"}]}
    for pedido in VETORES["requests"]:
        esperado = pedido["allow"] and pedido["action"] == "s3:GetObjectVersion"
        assert autoriza(documentos[pedido["role"]], pedido, sessao) is esperado


def _sessao(pedido):
    return {"action": "sts:AssumeRole", "resource": "*", "principal": PRINCIPAL,
            "context": {"sts:RoleSessionName": pedido,
                        "aws:CurrentTime": "2026-11-10T11:30:00Z"}}


def test_trust_efetivo_principal_exato_e_binding_por_operacao(estatico):
    _, confiancas = documentos_do_plano(_plano(estatico, "operacional"))
    operacoes = {"observer": ["inventory"], "normal": ["publish", "retire"],
                 "recovery": ["rollback", "readback", "abort"]}
    for alias, nomes in operacoes.items():
        for nome in nomes:
            pedido = _sessao(VETORES["sessions"][nome])
            assert autoriza(confiancas[alias], pedido)
            assert not autoriza(confiancas[alias], pedido | {"principal": PRINCIPAL + "-outro"})
            assert not autoriza(confiancas[alias], _sessao(VETORES["sessions"][nome] + "x"))
    assert not autoriza(confiancas["normal"], _sessao(VETORES["sessions"]["rollback"]))
    assert not autoriza(confiancas["recovery"], _sessao(VETORES["sessions"]["publish"]))


def test_observer_pattern_nao_alega_validar_hex_e_nao_muda_com_vid(estatico):
    _, confiancas = documentos_do_plano(_plano(estatico, "operacional"))
    assert autoriza(confiancas["observer"], _sessao("sp-i-" + "z" * 40))
    assert not autoriza(confiancas["observer"], _sessao("sp-i-" + "a" * 39))
    assert not autoriza(confiancas["observer"], _sessao("sp-i-" + "a" * 41))


def test_trust_nao_admite_principal_curinga_root_user_sts_ou_provider(estatico):
    _, confiancas = documentos_do_plano(_plano(estatico, "operacional"))
    for alias, documento in confiancas.items():
        for declaracao in documento["Statement"]:
            assert declaracao["Action"] in ("sts:AssumeRole", ["sts:AssumeRole"])
            assert declaracao["Principal"] == {"AWS": PRINCIPAL}
            assert declaracao["Effect"] == "Allow"
        sessao = VETORES["sessions"]["inventory" if alias == "observer" else "publish"]
        partes = ("root", "user/synthetic", "oidc-provider/token.actions.githubusercontent.com")
        for parte in partes:
            principal = "arn:aws:iam::123456789012:" + parte
            assert not autoriza(documento, _sessao(sessao) | {"principal": principal})


def test_saida_contem_somente_referencias_e_commitments_dos_bytes_efetivos(estatico):
    plano = _plano(estatico, "operacional")
    recursos = _recursos(plano)
    envelope = plano["planned_values"]["outputs"]["static_promotion_iam_contract"]
    assert envelope["sensitive"] is True
    saida = envelope["value"]
    assert set(saida) == {"mode", "delegations", "binding_sha256", "static_plan_sha256",
                          "inventory_sha256", "bucket_arn", "distribution_arn",
                          "freeze_target_role_arns", "freeze_excluded_role_arns"}
    assert saida["mode"] == "operational"
    assert saida["inventory_sha256"] == ENTRADA["inventory_sha256"]
    assert saida["binding_sha256"] == VETORES["binding_sha256"]
    assert saida["static_plan_sha256"] == ENTRADA["binding"]["static_plan_sha256"]
    assert {k: saida[k] for k in VETORES["static_web"]} == VETORES["static_web"]
    assert set(saida["delegations"]) == set(ALIASES)
    for alias, delegacao in saida["delegations"].items():
        role = recursos[f"{PREFIXO}.aws_iam_role.static_{alias}[0]"]
        policy = recursos[f"{PREFIXO}.aws_iam_role_policy.static_{alias}[0]"]
        assert role["name"] == policy["role"] == f"cnesdata-prod-static-{alias}"
        assert role["max_session_duration"] == 3600
        assert role["tags"] == {"Project": "cnesdata", "Environment": "prod",
                                "ManagedBy": "opentofu", "Owner": "vinisantana"}
        assert policy["name"] == role["name"] + "-runtime"
        assert set(delegacao) == {"role_arn", "role_name", "role_address", "policy_address",
                                  "policy_name", "session_names", "trust_sha256", "policy_sha256"}
        assert delegacao["role_name"] == role["name"]
        assert delegacao["policy_name"] == policy["name"]
        assert delegacao["role_address"] == f"module.runtime_iam.aws_iam_role.static_{alias}[0]"
        assert delegacao["policy_address"] == (
            f"module.runtime_iam.aws_iam_role_policy.static_{alias}[0]"
        )
        assert delegacao["trust_sha256"] == _sha(role["assume_role_policy"])
        assert delegacao["policy_sha256"] == _sha(policy["policy"])
        assert delegacao["role_arn"] == role["arn"]
        operacoes = {"observer": ["inventory"], "normal": ["publish", "retire"],
                     "recovery": ["rollback", "readback", "abort"]}
        assert sorted(delegacao["session_names"]) == sorted(
            VETORES["sessions"][op] for op in operacoes[alias]
        )
        assert len(policy["policy"].encode()) <= 10240


def _grants(documento):
    normalizados = []
    for declaracao in documento["Statement"]:
        assert set(declaracao) <= {"Sid", "Effect", "Action", "Resource", "Condition"}
        acoes = declaracao["Action"]
        recursos = declaracao["Resource"]
        acoes = acoes if isinstance(acoes, list) else [acoes]
        recursos = recursos if isinstance(recursos, list) else [recursos]
        condicoes = json.dumps(declaracao.get("Condition", {}), sort_keys=True)
        normalizados.extend(
            (declaracao["Effect"], acao.lower(), recurso, condicoes)
            for acao in acoes for recurso in recursos
        )
    return sorted(normalizados)


def test_grants_efetivos_nao_contem_permissoes_extras_ou_principal_de_sessao(estatico):
    documentos, _ = documentos_do_plano(_plano(estatico, "operacional"))
    for alias, documento in documentos.items():
        assert documento["Version"] == "2012-10-17"
        assert _grants(documento) == _grants(VETORES["control_policies"][alias])


@pytest.mark.parametrize("campo", list(ENTRADA["binding"]))
def test_troca_de_binding_nao_reaproveita_trust_preparado(estatico, campo):
    _, confiancas = documentos_do_plano(_plano(estatico, "operacional"))
    binding = deepcopy(ENTRADA["binding"]) | {"operation": "publish"}
    original = binding[campo]
    binding[campo] = original + 1 if isinstance(original, int) else "0" + original[1:]
    texto = json.dumps(binding, sort_keys=True, separators=(",", ":")) + "\n"
    assert not autoriza(confiancas["normal"], _sessao("sp-publish-" + _sha(texto)[:40]))


def test_primeira_publicacao_sem_baseline_nao_ganha_restore_absence(estatico):
    documentos, _ = documentos_do_plano(_plano(estatico, "sem_baseline"))
    verificar_matriz(documentos, VETORES["requests_without_baseline"])


def test_operacional_alternativo_segue_chaves_vids_e_multipart_aprovados(estatico):
    documentos, _ = documentos_do_plano(_plano(estatico, "operacional_alternativo"))
    verificar_matriz(documentos, VETORES["alternative_requests"])


def test_fragmento_h4_com_historico_nao_amplia_grants_produto(estatico):
    historico = _recursos(_plano(estatico, "historico"))
    operacional = _recursos(_plano(estatico, "operacional"))
    assert historico == operacional


def test_raiz_resolve_recursos_do_produtor_e_nao_do_caller(tofu_artifacts):
    raiz = tofu_artifacts["configuration"]["root_module"]
    assert "static_promotion" in raiz["variables"]
    expressoes = raiz["module_calls"]["runtime_iam"]["expressions"]
    assert expressoes["static_promotion"]["references"] == ["var.static_promotion"]
    refs = set(expressoes["static_web"]["references"])
    assert {"module.static_web.bucket_arn", "module.static_web.distribution_arn"} <= refs
    assert not any(ref.startswith("var.static_web") for ref in refs)


@pytest.fixture(scope="module", name="raiz_estatica")
def fixture_raiz_estatica(canary_plan, tofu_context, tofu_artifacts, prod_root):
    preparacao = {"input": _entrada(_recursos_modo(canary_plan))}
    evidencia = Path(tofu_context[1]["TF_DATA_DIR"])
    copia, _ = _phase_b_mock_root(prod_root, evidencia)
    raiz = copia / "env/prod"
    identidades = _identidades(tofu_artifacts, evidencia)
    contrato = {"account_id": "123456789012", "name_prefix": "cnesdata-prod", "region": "us-east-2",
                "distribution_id": VETORES["static_web"]["distribution_arn"].rsplit("/", 1)[1]}
    identidades += identidades_estaticas(tofu_artifacts["configuration"],
                                        tofu_artifacts["schema"], contrato)
    shared = json.loads((evidencia / "processing-shared-input.json").read_text())
    comuns = _comuns() | {"mode_canary": preparacao["input"]}
    casos = {"raiz_nula": comuns,
             "raiz_bootstrap": comuns | {"static_promotion": _compor(preparacao, True)},
             "raiz_operacional": comuns | {"static_promotion": _compor(preparacao, False)}}
    diretorio = raiz / "static-tests"
    diretorio.mkdir()
    (diretorio / "production.tftest.hcl").write_text(_suite(shared, casos, identidades))
    dados = evidencia / "static-root-data"
    dados.mkdir()
    contexto = tofu_context[0], tofu_context[1] | {"TF_DATA_DIR": str(dados)}
    resultado = _native_result(
        contexto, raiz, ("init", "-backend=false", "-input=false", "-lockfile=readonly"),
        "static-root-init",
    )
    assert resultado.returncode == 0, resultado.stdout + resultado.stderr
    resultado = _native_result(
        contexto, raiz,
        ("test", "-json", "-no-color", "-verbose", "-test-directory=static-tests"),
        "static-root-plan",
    )
    estados, planos, erros = _native_plan_events(resultado.stdout)
    return {"plans": planos, "statuses": estados, "diagnostics": erros}


def _compor(preparacao, bootstrap):
    entrada = _bootstrap(ENTRADA) if bootstrap else deepcopy(ENTRADA)
    modo = preparacao["input"]
    manifesto = json.loads(modo["context_manifest"]["canonical_utf8"])
    for campo in ("release_id", "product_source_sha", "shared_source_sha"):
        entrada["binding"][campo] = manifesto[campo]
    for campo in ("run_id", "run_attempt", "generation"):
        entrada["binding"][campo] = manifesto["owner"][campo]
    entrada["application_promotion"]["producer"]["workflow_sha"] = manifesto["shared_source_sha"]
    entrada["enabled_until"] = modo["enabled_until"]
    return entrada


def test_composicao_raiz_classifica_freeze_e_preserva_outros_produtores(raiz_estatica):
    planos = {nome: _plano(raiz_estatica, "raiz_" + nome)
              for nome in ("nula", "bootstrap", "operacional")}
    recursos = {nome: _recursos(plano) for nome, plano in planos.items()}
    prefixo = "arn:aws:iam::123456789012:role/cnesdata-prod-static-"
    for nome, aliases in (("bootstrap", ["observer"]), ("operacional", ALIASES)):
        assert_root_policy_inventory(planos[nome])
        saidas = planos[nome]["planned_values"]["outputs"]
        targets = set(saidas["freeze_target_role_arns"]["value"])
        excluded = set(saidas["freeze_excluded_role_arns"]["value"])
        assert not targets & excluded
        assert (prefixo + "normal" in targets) is (nome == "operacional")
        assert {prefixo + a for a in aliases if a != "normal"} <= excluded
        assert not {prefixo + "observer", prefixo + "recovery"} & targets
        assert {k: recursos[nome][k] for k in recursos["nula"]} == recursos["nula"]
        novos = {e.replace("module.subject", "module.runtime_iam") for e in _enderecos(aliases)}
        assert recursos[nome].keys() - recursos["nula"].keys() == novos


@pytest.mark.parametrize("sessao", [None, SESSAO_AMPLA], ids=["omitida", "ampla"])
def test_oraculo_positivo_independente_exercita_toda_matriz(sessao):
    verificar_matriz(VETORES["control_policies"], VETORES["requests"], sessao)


@pytest.mark.parametrize("falha", ["bool", "copy_source", "wildcard", "vid_cartesiano"])
def test_oraculo_detecta_ampliacoes_controladas(falha):
    documentos = deepcopy(VETORES["control_policies"])
    recuperacao = documentos["recovery"]["Statement"]
    copias = [s for s in recuperacao if s["Action"] == "s3:PutObject"]
    if falha == "bool":
        for declaracao in copias:
            del declaracao["Condition"]["Bool"]
    if falha == "copy_source":
        for declaracao in copias:
            del declaracao["Condition"]["StringEquals"]
    if falha == "wildcard":
        documentos["normal"]["Statement"][0]["Resource"] = [
            VETORES["static_web"]["bucket_arn"] + "/assets/*",
            VETORES["static_web"]["bucket_arn"] + "/index.html",
            VETORES["static_web"]["bucket_arn"] + "/theme-init.js",
        ]
    if falha == "vid_cartesiano":
        retiradas = [s for s in documentos["normal"]["Statement"]
                   if s["Action"] == "s3:DeleteObjectVersion"]
        retiradas[0]["Condition"]["StringEquals"]["s3:VersionId"] = [
            r["version_id"] for r in ENTRADA["retire_versions"]
        ]
    with pytest.raises(AssertionError, match="vetor="):
        verificar_matriz(documentos, VETORES["requests"])


def test_oraculo_nao_consulta_nome_de_api_ou_upload_preexistente():
    documentos = VETORES["control_policies"]
    for pedido in VETORES["requests"]:
        alterado = pedido | {"api": "CopyObject", "existing_upload": False}
        assert autoriza(documentos[pedido["role"]], alterado) is pedido["allow"]


def test_oraculo_deny_explicito_prevalece_sobre_allow_e_sessao_ampla():
    pedido = next(p for p in VETORES["requests"] if p["id"] == "publica_index.html")
    deny = {"Statement": [{"Effect": "Deny", "Action": "s3:PutObject", "Resource": "*"}]}
    normal = VETORES["control_policies"]["normal"]
    assert autoriza(normal, pedido)
    assert not autoriza(normal, pedido, SESSAO_AMPLA, [deny])


def test_oraculo_recusa_condicao_desconhecida_sem_ocultar_campo_ausente():
    pedido = {"action": "s3:GetObject", "resource": "*", "context": {}}
    documento = {"Statement": [{"Effect": "Allow", "Action": "*", "Resource": "*",
                                "Condition": {"StringEqualsIfExists": {"x": "y"}}}]}
    with pytest.raises(ValueError, match="operador_nao_modelado"):
        autoriza(documento, pedido)


def test_oraculo_condition_key_iam_nao_depende_de_caixa():
    pedido = next(p for p in VETORES["requests"] if p["id"] == "le_original_index.html")
    contexto = {k.lower(): v for k, v in pedido["context"].items()}
    assert autoriza(VETORES["control_policies"]["recovery"], pedido | {"context": contexto})


@pytest.mark.parametrize("cenario", ["original", "alternativo", "sem_baseline"])
def test_oraculo_preserva_positivos_nas_tres_configuracoes(cenario):
    chaves = {"original": ("control_policies", "requests"),
              "alternativo": ("alternative_control_policies", "alternative_requests"),
              "sem_baseline": ("control_policies_without_baseline", "requests_without_baseline")}
    documentos, pedidos = chaves[cenario]
    verificar_matriz(VETORES[documentos], VETORES[pedidos])


@pytest.mark.parametrize("chave", ["index.html", "theme-init.js"])
def test_oraculo_sem_baseline_detecta_reuso_dos_vids_antigos(chave):
    pedido = next(p for p in VETORES["requests_without_baseline"]
                  if p["id"] == "sem_baseline_copia_" + chave)
    antigo = VETORES["control_policies"]
    assert autoriza(antigo["recovery"], pedido)
    with pytest.raises(AssertionError, match="vetor=sem_baseline_copia_"):
        verificar_matriz(antigo, [pedido])


@pytest.mark.parametrize("falha", ["skip", "ausente", "provider", "ciclo", "outra_validacao"])
def test_oraculo_nativo_recusa_skip_ausencia_e_erros_inesperados(falha):
    resultado = {"returncode": 1, "statuses": {"invalido_x": "error"}, "plans": {},
                 "diagnostics": [{"@testrun": "invalido_x", "diagnostic": {
                     "severity": "error", "summary": "Invalid value for variable",
                     "detail": "static_promotion=invalid_contract"}}]}
    verificar_cobertura_nativa(resultado, ["invalido_x"])
    verificar_rejeicao_nativa(resultado, "invalido_x")
    if falha == "skip":
        resultado["statuses"]["invalido_x"] = "skip"
    if falha == "ausente":
        resultado["statuses"] = {}
    erro = resultado["diagnostics"][0]["diagnostic"]
    if falha in ("provider", "ciclo"):
        erro["summary"] = "Failed to query available provider packages" if falha == "provider" \
            else "Cycle"
    if falha == "outra_validacao":
        erro["detail"] = "outro_contrato=invalido"
    if falha == "provider":
        resultado["diagnostics"][0].pop("@testrun")
    cobertura = falha in ("skip", "ausente", "provider")
    verificador = verificar_cobertura_nativa if cobertura else verificar_rejeicao_nativa
    with pytest.raises(AssertionError):
        verificador(resultado, ["invalido_x"] if cobertura else "invalido_x")


def test_freeze_de_principal_bloqueia_sessao_normal_preservando_recuperacao(estatico):
    documentos, _ = documentos_do_plano(_plano(estatico, "operacional"))
    normal = "arn:aws:iam::123456789012:role/cnesdata-prod-static-normal"
    bloqueio = {"Statement": [{"Effect": "Deny", "Action": "*", "Resource": "*",
                              "Condition": {"StringEquals": {"aws:PrincipalArn": normal}}}]}
    for nome in ("publica_index.html", "copia_index.html", "observer_vid_novo_index.html"):
        pedido = deepcopy(next(p for p in VETORES["requests"] if p["id"] == nome))
        principal = normal.replace("normal", pedido["role"])
        pedido["context"] |= {"aws:PrincipalArn": principal, "aws:PrincipalTag/freeze": "false"}
        assert autoriza(documentos[pedido["role"]], pedido, SESSAO_AMPLA)
        esperado = pedido["role"] != "normal"
        assert autoriza(documentos[pedido["role"]], pedido, SESSAO_AMPLA, [bloqueio]) is esperado
