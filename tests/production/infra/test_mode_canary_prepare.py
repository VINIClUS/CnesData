"""Admissão, commitments e preservação do prepare de modo nativo."""

import json
import re
from copy import deepcopy
from fnmatch import fnmatchcase

import pytest
from scripts.promotion.prepare_mode_canary import validate_mode_canary_prepare

from tests.production.infra._athena_root_plan import after_of
from tests.production.infra._mode_canary_native import (
    ALIASES,
    PREFIXO,
    _canonico,
    _chaves,
    _entrada,
    _nome,
    _pedido,
    _sessoes,
    _sha,
    fixture_canary_plan,  # noqa: F401
    fixture_ciclo_modo,  # noqa: F401
    fixture_preparacao,  # noqa: F401
)
from tests.production.infra._root_policy_inventory import (
    assert_root_policy_inventory,
    runtime_iam_entries,
)


def _objetos(plano):
    return {
        r["address"]: r["change"]["after"]
        for r in plano["resource_changes"]
        if r["mode"] == "managed"
    }


def test_prepare_cria_somente_quatro_roles_e_quatro_policies(preparacao):
    antes = _objetos(preparacao["plans"]["desligado"])
    depois = _objetos(preparacao["plans"]["promocao"])
    esperado = {
        f"{PREFIXO}.{tipo}.{alias}[0]"
        for alias in ALIASES
        for tipo in ("aws_iam_role", "aws_iam_role_policy")
    }
    assert depois.keys() - antes.keys() == esperado
    assert not antes.keys() - depois.keys()
    assert {endereco: depois[endereco] for endereco in antes} == antes


def test_padrao_nao_cria_capacidades_de_modo(preparacao):
    plano = preparacao["plans"]["desligado"]
    assert not [endereco for endereco in _objetos(plano) if ".mode_" in endereco]
    assert plano["planned_values"]["outputs"]["mode_canary_capabilities"]["value"] is None


def _verificar_saida(plano, entrada):
    envelope = plano["planned_values"]["outputs"]["mode_canary_capabilities"]
    assert envelope["sensitive"] is True
    saida = envelope["value"]
    manifesto = json.loads(entrada["context_manifest"]["canonical_utf8"])
    esperado = {
        "schema_version": 2,
        "context_kind": entrada["context_kind"],
        "owner_sha256": entrada["owner"]["owner_sha256"],
        "generation": entrada["owner"]["generation"],
        "context_manifest_sha256": entrada["context_manifest"]["sha256"],
        "activation_sha256": manifesto["activation_sha256"],
        "resources_sha256": _sha(_canonico(entrada["resources"])),
        "enabled_until": entrada["enabled_until"],
    }
    assert set(saida) == set(esperado) | {"delegations"}
    assert {k: saida[k] for k in esperado} == esperado
    assert set(saida["delegations"]) == set(ALIASES)
    for alias, delegacao in saida["delegations"].items():
        role_address = f"{PREFIXO}.aws_iam_role.{alias}[0]"
        policy_address = f"{PREFIXO}.aws_iam_role_policy.{alias}[0]"
        role, policy = after_of(plano, role_address), after_of(plano, policy_address)
        assert role["name"] == policy["role"] == _nome(alias)
        assert role["arn"] == f"arn:aws:iam::123456789012:role/{_nome(alias)}"
        assert policy["name"] == _nome(alias) + "-runtime"
        assert delegacao == {
            "role_arn": role["arn"],
            "role_name": role["name"],
            "role_address": role_address,
            "policy_address": policy_address,
            "policy_name": policy["name"],
            "session_names": entrada["delegation_sessions"][alias],
            "trust_sha256": _sha(role["assume_role_policy"]),
            "policy_sha256": _sha(policy["policy"]),
        }


def test_output_confirma_bytes_exatos_das_policies_e_dono(preparacao):
    _verificar_saida(preparacao["plans"]["promocao"], preparacao["input"])


def _verificar_rejeicao(preparacao, caso):
    assert preparacao["statuses"].get(caso) in {"fail", "error"}, caso
    assert caso not in preparacao["plans"], caso
    erros = [
        e["diagnostic"]
        for e in preparacao["diagnostics"]
        if e.get("@testrun") == caso and e["diagnostic"]["severity"] == "error"
    ]
    assert erros, f"diagnostico_ausente={caso}"
    assert all(
        e["summary"] == "Invalid value for variable"
        and "mode_canary=invalid_contract" in e["detail"]
        and (
            e["range"]["filename"] == "variables.tf"
            or re.search(
                r"validation rule at \.{2}/\.{2}/modules/runtime-iam/variables\.tf:\d+,\d+-\d+\.",
                e["detail"],
            )
        )
        for e in erros
    ), erros


def test_admissao_nativa_rejeita_entrada_mutante(preparacao):
    for caso in preparacao["cases"]:
        if caso.startswith("invalido_"):
            _verificar_rejeicao(preparacao, caso)


def _enderecos_modo():
    return {
        f"{PREFIXO}.{tipo}.{alias}[0]"
        for alias in ALIASES
        for tipo in ("aws_iam_role", "aws_iam_role_policy")
    }


def _verificar_transicao(plano, esperado):
    mudancas = {
        r["address"]: r["change"] for r in plano["resource_changes"] if r["mode"] == "managed"
    }
    assert _enderecos_modo() <= mudancas.keys()
    permitidas = {"create": [["create"]], "no-op": [["no-op"]], "owner": [["no-op"], ["update"]]}
    for endereco, mudanca in mudancas.items():
        assert not mudanca.get("importing")
        assert mudanca["actions"] in (
            permitidas[esperado] if endereco in _enderecos_modo() else [["no-op"]]
        )
        assert not mudanca.get("replace_paths")
    return mudancas


def test_ciclo_mock_cria_reaplica_sem_efeito_e_troca_owner_sem_replacement(ciclo_modo):
    planos = ciclo_modo["plans"]
    _verificar_transicao(planos["criar"], "create")
    _verificar_transicao(planos["replay"], "no-op")
    mudancas = _verificar_transicao(planos["novo_owner"], "owner")
    assert any(m["actions"] == ["update"] for m in mudancas.values())
    for nome, entrada in (("replay", ciclo_modo["input"]), ("novo_owner", ciclo_modo["new_input"])):
        _verificar_saida(planos[nome], entrada)
    antigo, novo = ciclo_modo["input"], ciclo_modo["new_input"]
    assert antigo["enabled_until"] == novo["enabled_until"]
    assert antigo["delegation_sessions"] != novo["delegation_sessions"]
    for alias in ALIASES:
        endereco = f"{PREFIXO}.aws_iam_role.{alias}[0]"
        assert (
            after_of(planos["replay"], endereco)["arn"]
            == after_of(planos["novo_owner"], endereco)["arn"]
        )


def _verificar_inventario(antes, depois):
    tipos = {"aws_iam_role_policy", "aws_iam_role_policy_attachment", "aws_iam_policy_attachment"}

    def inventario(plano):
        return {
            r["address"]: r["change"]["after"]
            for r in plano["resource_changes"]
            if r["mode"] == "managed" and r["type"] in tipos
        }

    base, atual = inventario(antes), inventario(depois)
    novas = {f"{PREFIXO}.aws_iam_role_policy.{alias}[0]" for alias in ALIASES}
    assert atual.keys() == base.keys() | novas
    assert {k: atual[k] for k in base} == base
    excecoes = {
        f"{PREFIXO}.aws_iam_role_policy.{alias}[0]"
        for alias in ("mode_dispatch", "mode_audit_launch")
    }
    for endereco, valor in atual.items():
        if endereco.startswith(PREFIXO) and endereco not in excecoes:
            for item in json.loads(valor["policy"])["Statement"]:
                acoes = item["Action"] if isinstance(item["Action"], list) else [item["Action"]]
                assert not (
                    item["Effect"] == "Allow" and any(fnmatchcase("ecs:RunTask", a) for a in acoes)
                ), endereco


def test_preserva_managed_grants_freeze_e_inventario_enabled(preparacao):
    antes, depois = (preparacao["plans"][n] for n in ("desligado", "promocao"))
    base, atual = _objetos(antes), _objetos(depois)
    assert {k: atual[k] for k in base} == base
    _verificar_inventario(antes, depois)
    for contexto in ("promocao", "readiness"):
        assert_root_policy_inventory(preparacao["plans"][contexto])
    entradas = runtime_iam_entries(depois)
    for alias in ALIASES:
        assert entradas[f"{PREFIXO}.aws_iam_role_policy.{alias}[0]"] == (
            _nome(alias) + "-runtime",
            _nome(alias),
        )
    for campo, aliases in (
        ("freeze_target_role_arns", ("mode_seed", "mode_dispatch")),
        ("freeze_excluded_role_arns", ("mode_audit_launch", "mode_observe")),
    ):
        valor_antes = antes["planned_values"]["outputs"][campo]["value"]
        valor_depois = depois["planned_values"]["outputs"][campo]["value"]
        novas = [f"arn:aws:iam::123456789012:role/{_nome(alias)}" for alias in aliases]
        assert valor_depois == sorted(valor_antes + novas)
    campos = (
        "policy",
        "assume_role_policy",
        "role",
        "max_session_duration",
        "tags",
        "target",
        "definition",
        "container_definitions",
        "network_configuration",
        "vpc_id",
    )
    for item in depois["resource_changes"]:
        assert not any(item["change"].get("after_unknown", {}).get(c) for c in campos), item[
            "address"
        ]


@pytest.mark.parametrize(
    "resumo", ["Cycle", "Provider configuration not present", "Invalid expression"]
)
@pytest.mark.parametrize("estado", ["fail", "error"])
def test_controle_rejeicao_nao_aceita_erro_de_provider_ou_configuracao(resumo, estado):
    caso = "invalido_digest"
    diagnostico = {
        "severity": "error",
        "summary": resumo,
        "detail": "mode_canary=invalid_contract",
        "range": {"filename": "variables.tf"},
    }
    preparo = {
        "statuses": {caso: estado},
        "plans": {},
        "diagnostics": [{"@testrun": caso, "diagnostic": diagnostico}],
    }
    with pytest.raises(AssertionError):
        _verificar_rejeicao(preparo, caso)
    diagnostico["summary"] = "Invalid value for variable"
    _verificar_rejeicao(preparo, caso)


@pytest.mark.parametrize("origem", ["../../modules/runtime-iam/variables.tf", "main.tf"])
def test_controle_rejeicao_confere_origem_nominal_de_validacao_do_modulo(origem):
    diagnostico = {
        "severity": "error",
        "summary": "Invalid value for variable",
        "detail": "mode_canary=invalid_contract\n\nThis was checked by the validation rule at "
        + origem
        + ":47,3-13.",
        "range": {"filename": "main.tf"},
    }
    preparo = {
        "statuses": {"invalido": "fail"},
        "plans": {},
        "diagnostics": [{"@testrun": "invalido", "diagnostic": diagnostico}],
    }
    if origem == "main.tf":
        with pytest.raises(AssertionError):
            _verificar_rejeicao(preparo, "invalido")
    else:
        _verificar_rejeicao(preparo, "invalido")


def _controle_output():
    entrada = _entrada({"testemunha_sintetica_de_oraculo": True})
    manifesto = json.loads(entrada["context_manifest"]["canonical_utf8"])
    saida = {
        "schema_version": 2,
        "context_kind": "promotion",
        "owner_sha256": "4" * 64,
        "generation": 7,
        "context_manifest_sha256": entrada["context_manifest"]["sha256"],
        "activation_sha256": "3" * 64,
        "resources_sha256": manifesto["resources_sha256"],
        "enabled_until": entrada["enabled_until"],
        "delegations": {},
    }
    rows = _controle_delegacoes(saida, entrada)
    return {
        "resource_changes": rows,
        "planned_values": {
            "outputs": {"mode_canary_capabilities": {"value": saida, "sensitive": True}}
        },
    }, entrada


def _controle_delegacoes(saida, entrada):
    rows = []
    for alias in ALIASES:
        nome = _nome(alias)
        role_address, policy_address = (
            f"{PREFIXO}.{tipo}.{alias}[0]" for tipo in ("aws_iam_role", "aws_iam_role_policy")
        )
        role = {
            "name": nome,
            "arn": f"arn:aws:iam::123456789012:role/{nome}",
            "assume_role_policy": '{"Statement":[]}',
        }
        policy = {"name": nome + "-runtime", "role": nome, "policy": '{"Statement":[]}'}
        for endereco, tipo, valor in (
            (role_address, "aws_iam_role", role),
            (policy_address, "aws_iam_role_policy", policy),
        ):
            rows.append(
                {
                    "address": endereco,
                    "mode": "managed",
                    "type": tipo,
                    "change": {"actions": ["no-op"], "after": valor},
                }
            )
        saida["delegations"][alias] = {
            "role_arn": role["arn"],
            "role_name": nome,
            "role_address": role_address,
            "policy_address": policy_address,
            "policy_name": nome + "-runtime",
            "session_names": entrada["delegation_sessions"][alias],
            "trust_sha256": _sha(role["assume_role_policy"]),
            "policy_sha256": _sha(policy["policy"]),
        }
    return rows


@pytest.mark.parametrize(
    "campo",
    [
        "schema_version",
        "context_kind",
        "activation_sha256",
        "enabled_until",
        "delegations",
        "sensitive",
        "attachment",
    ],
)
def test_controle_output_detecta_campos_nulos_aliases_vazios_e_attachment(campo):
    plano, entrada = _controle_output()
    _verificar_saida(plano, entrada)
    envelope = plano["planned_values"]["outputs"]["mode_canary_capabilities"]
    if campo == "sensitive":
        envelope["sensitive"] = False
    elif campo == "attachment":
        plano["resource_changes"][1]["change"]["after"]["role"] = "foreign"
    else:
        envelope["value"][campo] = {} if campo == "delegations" else None
    with pytest.raises(AssertionError):
        _verificar_saida(plano, entrada)


@pytest.mark.parametrize(
    "campo",
    [
        "role_arn",
        "role_name",
        "role_address",
        "policy_address",
        "policy_name",
        "session_names",
        "trust_sha256",
        "policy_sha256",
    ],
)
def test_controle_output_detecta_delegacao_incompleta(campo):
    plano, entrada = _controle_output()
    _verificar_saida(plano, entrada)
    del plano["planned_values"]["outputs"]["mode_canary_capabilities"]["value"]["delegations"][
        "mode_seed"
    ][campo]
    with pytest.raises(AssertionError):
        _verificar_saida(plano, entrada)


@pytest.mark.parametrize(
    "mudanca",
    [
        {"actions": ["delete", "create"]},
        {"actions": ["delete"]},
        {"importing": {"id": "x"}},
        {"replace_paths": [["name"]]},
    ],
)
def test_controle_lifecycle_nega_delete_replacement_import(mudanca):
    plano, _ = _controle_output()
    _verificar_transicao(plano, "no-op")
    plano["resource_changes"][0]["change"].update(mudanca)
    with pytest.raises(AssertionError):
        _verificar_transicao(plano, "no-op")


def test_controle_inventario_recusa_runtask_em_policy_normal():
    depois, _ = _controle_output()
    normal = {
        "address": f'{PREFIXO}.aws_iam_role_policy.runtime["unit"]',
        "mode": "managed",
        "type": "aws_iam_role_policy",
        "change": {
            "after": {
                "name": "normal",
                "role": "unit",
                "policy": '{"Statement":[{"Effect":"Allow","Action":"ecs:RunTask",'
                '"Resource":"*"}]}',
            }
        },
    }
    antes = {"resource_changes": [normal]}
    depois["resource_changes"].append(deepcopy(normal))
    with pytest.raises(AssertionError):
        _verificar_inventario(antes, depois)


def test_controle_codec_a3_preserva_utf8_sem_confundir_manifesto_admissivel():
    from scripts.promotion._canonical import canonical_bytes, parse_document

    original = b'{"texto":"sa\xc3\xbade"}\n'
    assert canonical_bytes(parse_document(original)) == original
    assert (
        _sha(original.decode())
        == "1015e7586c45b51d047310d0381ff921305ebb50324a396471aedd82288b798c"
    )
    with pytest.raises(ValueError):
        parse_document(b'{"texto":"a","texto":"b"}\n')


@pytest.mark.parametrize(
    ("contexto", "alias", "esperada"),
    [
        ("promotion", "mode_seed", "mc-us-48b7d736784048cbc38012c80ccfca7b9d4b4882"),
        ("promotion", "mode_dispatch", "mc-ux-bef725d100de2341734a37bba58e966a10251fd8"),
        ("promotion", "mode_observe", "mc-uo-bc5abb91dec9c2e5711c7806eb673258de3682fe"),
        ("promotion", "mode_seed", "mc-rs-286077e480bfd6c77c80e132d9e950cd2c07d105"),
        ("promotion", "mode_dispatch", "mc-rx-c1c6960e3e734ad331bfb47528906086ee4ca3bc"),
        ("promotion", "mode_observe", "mc-ro-af7e35d324b6a430776faf51f32c112db8aed960"),
        ("promotion", "mode_audit_launch", "mc-ax-cd2004f9e918765cf56d5797f0b3101f9433b888"),
        ("promotion", "mode_observe", "mc-ao-12ee7d2484ce90df9bc40584f3e56779440bf97a"),
        ("readiness", "mode_seed", "mc-ds-e6ebde6489261e90128fed2f1f93d7d13b179888"),
        ("readiness", "mode_dispatch", "mc-dx-e5378128c0d476162d552926b77f224fcd0d74ca"),
        ("readiness", "mode_dispatch", "mc-dr-8b56470988e738bdb25672fe033332012284dd72"),
        ("readiness", "mode_dispatch", "mc-h1-4944bc2baa8170d2afe9667ada4fd2b79bb3b6c4"),
        ("readiness", "mode_dispatch", "mc-h2-927b46e5ff825cef9dca498c5388b0f1483f301c"),
        ("readiness", "mode_observe", "mc-do-43d216179aad340e4e25cd046e4d3d88e3242cd2"),
    ],
)
def test_controle_sessoes_reproduz_quatorze_vetores_ratificados(contexto, alias, esperada):
    entrada = _entrada({}, contexto)
    manifesto = json.loads(entrada["context_manifest"]["canonical_utf8"])
    assert esperada in _sessoes(manifesto, "5" * 64)[alias]


def _pedidos_observacao(recursos):
    pedidos = [
        _pedido("ecs:DescribeTaskDefinition", "*"),
        _pedido("dynamodb:GetItem", recursos["table_arn"], _chaves(["OUTBOX#CURSOR"])),
    ]
    for modo in ("unit", "recovery", "audit"):
        log = recursos["log_group_arn"] + f":log-stream:{modo}/processor/" + "a" * 32
        pedidos.append(_pedido("logs:GetLogEvents", log))
    for bucket, campo in (
        ("data_bucket_arn", "output_object_keys"),
        ("audit_bucket_arn", "audit_object_keys"),
    ):
        for chave in recursos["scope"][campo]:
            pedidos += [
                _pedido(acao, recursos[bucket] + "/" + chave)
                for acao in (
                    ("s3:GetObject", "s3:GetObjectRetention")
                    if campo == "audit_object_keys"
                    else ("s3:GetObject",)
                )
            ]
    pedidos += [_pedido("scheduler:GetSchedule", arn) for arn in recursos["schedule_arns"].values()]
    pedidos += [
        _pedido("cloudwatch:DescribeAlarms", arn) for arn in recursos["alarm_arns"].values()
    ]
    return pedidos


def test_oracle_de_prepare_aceita_planos_nativos_do_ciclo(ciclo_modo):
    planos, antigo, novo = ciclo_modo["plans"], ciclo_modo["input"], ciclo_modo["new_input"]
    validate_mode_canary_prepare(planos["replay"], antigo, "established")
    validate_mode_canary_prepare(planos["novo_owner"], novo, "established")
