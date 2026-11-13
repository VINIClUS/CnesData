"""Oracle do application-prepare mode-canary: oito endereços, commitments, prazo e readbacks."""

import hashlib
import json
from copy import deepcopy

import pytest
from scripts.promotion.prepare_mode_canary import (
    PrepareEvidence,
    admit_prepare,
    prepare_receipt,
    validate_mode_canary_prepare,
)

from scripts.promotion.mode_canary_contract import (
    ContractError,
    parse_context_manifest,
    session_names,
)
from scripts.promotion.phase_a import verify_delta
from tests.production.infra._mode_canary_native import ALIASES, CONTA, PREFIXO, _entrada, _nome

_RECURSOS = {"table_arn": f"arn:aws:dynamodb:us-east-2:{CONTA}:table/cnesdata-prod"}
_TIPOS = ("aws_iam_role", "aws_iam_role_policy")
_ACOES = {
    "mode_seed": "dynamodb:PutItem",
    "mode_dispatch": "ecs:RunTask",
    "mode_audit_launch": "ecs:RunTask",
    "mode_observe": "dynamodb:GetItem",
}


def _sha(texto):
    return hashlib.sha256(texto.encode()).hexdigest()


def _json(valor):
    return json.dumps(valor, sort_keys=True, separators=(",", ":"))


def _trust(entrada, alias, fechado):
    regra = {"Effect": "Deny" if fechado else "Allow", "Action": "sts:AssumeRole",
             "Principal": {"AWS": entrada["application_principal_arn"]}}
    if not fechado:
        regra["Condition"] = {
            "DateLessThan": {"aws:CurrentTime": entrada["enabled_until"]},
            "StringEquals": {"sts:RoleSessionName": entrada["delegation_sessions"][alias]},
        }
    return _json({"Statement": [regra], "Version": "2012-10-17"})


def _politica(alias, fechado):
    if fechado:
        regra = {"Action": "*", "Effect": "Deny", "Resource": "*"}
    else:
        regra = {"Action": [_ACOES[alias]], "Effect": "Allow", "Resource": "*"}
    return _json({"Statement": [regra], "Version": "2012-10-17"})


def _linha(endereco, tipo, depois, acao):
    return {"address": endereco, "mode": "managed", "type": tipo,
            "change": {"actions": [acao], "before": None if acao == "create" else depois,
                       "after": depois, "after_unknown": {}}}


def _plano(entrada, acao="create", fechar=True):
    manifesto = json.loads(entrada["context_manifest"]["canonical_utf8"])
    linhas, delegacoes = [], {}
    for alias in ALIASES:
        nome = _nome(alias)
        fechado = fechar and alias == "mode_audit_launch" and entrada["context_kind"] == "readiness"
        papel = {"name": nome, "arn": f"arn:aws:iam::{CONTA}:role/{nome}",
                 "assume_role_policy": _trust(entrada, alias, fechado),
                 "max_session_duration": 3600}
        politica = {"name": nome + "-runtime", "role": nome, "policy": _politica(alias, fechado)}
        enderecos = tuple(f"{PREFIXO}.{t}.{alias}[0]" for t in _TIPOS)
        linhas += [_linha(enderecos[0], "aws_iam_role", papel, acao),
                   _linha(enderecos[1], "aws_iam_role_policy", politica, acao)]
        delegacoes[alias] = {
            "role_arn": papel["arn"], "role_name": nome, "role_address": enderecos[0],
            "policy_address": enderecos[1], "policy_name": politica["name"],
            "session_names": [] if fechado else entrada["delegation_sessions"][alias],
            "trust_sha256": _sha(papel["assume_role_policy"]),
            "policy_sha256": _sha(politica["policy"]),
        }
    normal = {"name": "cnesdata-prod-unit", "assume_role_policy": "{}"}
    linhas.append(_linha(f'{PREFIXO}.aws_iam_role.task["unit"]', "aws_iam_role", normal, "no-op"))
    saida = {
        "schema_version": 2, "context_kind": entrada["context_kind"],
        "owner_sha256": entrada["owner"]["owner_sha256"],
        "generation": entrada["owner"]["generation"],
        "context_manifest_sha256": entrada["context_manifest"]["sha256"],
        "activation_sha256": manifesto["activation_sha256"],
        "resources_sha256": manifesto["resources_sha256"], "delegations": delegacoes,
        "enabled_until": entrada["enabled_until"],
    }
    outputs = {"mode_canary_capabilities": {"value": saida, "sensitive": True}}
    return {"resource_changes": linhas, "planned_values": {"outputs": outputs}}


def _envelope(entrada, **mudancas):
    manifesto = json.loads(entrada["context_manifest"]["canonical_utf8"])
    texto = json.dumps(entrada, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n"
    return {
        "mode": "mode-canary", "context_kind": entrada["context_kind"],
        "source_sha": manifesto["product_source_sha"],
        "source_closure_sha256": manifesto["product_source_closure_sha256"],
        "shared_fragment_sha256": "9" * 64, "activation_sha256": manifesto["activation_sha256"],
        "owner": entrada["owner"], "context_manifest_sha256": entrada["context_manifest"]["sha256"],
        "predecessor_checkpoint_sha256": None,
        "preparation_admitted_at": manifesto["admitted_at"],
        "preparation_deadline": manifesto["deadline"], "mode_canary_inputs_sha256": _sha(texto),
    } | mudancas


def _endereco(plano, alias, tipo="aws_iam_role_policy"):
    return next(r for r in plano["resource_changes"]
                if r["address"] == f"{PREFIXO}.{tipo}.{alias}[0]")


def test_prepare_mode_canary_so_oito_addresses_pos_keeper_pre_f():
    entrada = _entrada(_RECURSOS)
    plano = _plano(entrada)
    prova = validate_mode_canary_prepare(plano, entrada, "initial")
    assert sorted(prova["addresses"]) == sorted(
        f"{PREFIXO}.{t}.{a}[0]" for a in ALIASES for t in ("aws_iam_role", "aws_iam_role_policy"))
    assert set(prova["addresses"].values()) == {"create"}
    nono = deepcopy(plano)
    nono["resource_changes"][-1]["change"]["actions"] = ["update"]
    with pytest.raises(ContractError):
        validate_mode_canary_prepare(nono, entrada, "initial")
    assert admit_prepare(_envelope(entrada), entrada, None)
    for mutante in (_envelope(entrada, F_deadline="2026-11-10T11:30:00Z"),
                    _envelope(entrada, owner=entrada["owner"] | {"generation": 8}),
                    _envelope(entrada, mode="static")):
        with pytest.raises(ContractError):
            admit_prepare(mutante, entrada, None)


def test_phase_a_continua_recusando_roles499():
    plano = _plano(_entrada(_RECURSOS))
    for linha in plano["resource_changes"][:2]:
        with pytest.raises(ValueError, match="delta=address_forbidden"):
            verify_delta({"resource_changes": [linha]}, {})


def test_established_prepara_nova_generation_sem_nova_role():
    entrada = _entrada(_RECURSOS)
    novo = deepcopy(entrada)
    novo["owner"] = entrada["owner"] | {"generation": 8, "owner_sha256": "5" * 64}
    manifesto = json.loads(novo["context_manifest"]["canonical_utf8"]) | {"owner": novo["owner"]}
    texto = json.dumps(manifesto, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n"
    novo["context_manifest"] = {"canonical_utf8": texto, "sha256": _sha(texto)}
    novo["delegation_sessions"] = session_names(parse_context_manifest(novo["context_manifest"]))
    prova = validate_mode_canary_prepare(_plano(novo, "update"), novo, "established")
    assert set(prova["addresses"].values()) == {"update"}
    assert validate_mode_canary_prepare(_plano(novo, "no-op"), novo, "established")
    with pytest.raises(ContractError):
        validate_mode_canary_prepare(_plano(novo, "create"), novo, "established")
    trocado = _plano(novo, "update")
    _endereco(trocado, "mode_seed", "aws_iam_role")["change"]["replace_paths"] = [["name"]]
    with pytest.raises(ContractError):
        validate_mode_canary_prepare(trocado, novo, "established")


def _evidencia(**mudancas):
    campos = {
        "plan_binary_sha256": "a" * 64, "plan_json_sha256": "b" * 64,
        "checkpoint_before": "c" * 64, "checkpoint_after": "d" * 64,
        "readbacks": [], "observed_at": "2026-11-10T11:14:00Z", "pending": [], "unknown": [],
    }
    return PrepareEvidence(**(campos | mudancas))


def _leituras(prova, *instantes):
    return [{"observed_at": t, "objects_sha256": prova["objects_sha256"]} for t in instantes]


def test_prepare_replay_nao_renova_deadline():
    entrada = _entrada(_RECURSOS)
    envelope = admit_prepare(_envelope(entrada), entrada, None)
    assert admit_prepare(_envelope(entrada), entrada, envelope) == envelope
    for mudanca in ({"preparation_deadline": "2026-11-10T11:45:00Z"},
                    {"predecessor_checkpoint_sha256": "e" * 64}):
        with pytest.raises(ContractError):
            admit_prepare(_envelope(entrada, **mudanca), entrada, envelope)
    prova = validate_mode_canary_prepare(_plano(entrada), entrada, "initial")
    leituras = _leituras(prova, "2026-11-10T11:10:00Z", "2026-11-10T11:10:10Z")
    recibo = json.loads(prepare_receipt(envelope, prova, _evidencia(readbacks=leituras)))
    assert (recibo["preparation_admitted_at"], recibo["preparation_deadline"]) == (
        "2026-11-10T11:00:00Z", "2026-11-10T11:15:00Z")
    assert recibo["capabilities_sha256"] == prova["capabilities_sha256"]
    tardias = _leituras(prova, "2026-11-10T11:14:55Z", "2026-11-10T11:15:05Z")
    with pytest.raises(ContractError):
        prepare_receipt(envelope, prova, _evidencia(readbacks=tardias))


@pytest.mark.parametrize("instantes", [
    ("2026-11-10T11:10:00Z",),
    ("2026-11-10T11:10:00Z", "2026-11-10T11:10:09Z"),
])
def test_readback_exige_duas_observacoes_espacadas(instantes):
    entrada = _entrada(_RECURSOS)
    envelope = admit_prepare(_envelope(entrada), entrada, None)
    prova = validate_mode_canary_prepare(_plano(entrada), entrada, "initial")
    with pytest.raises(ContractError):
        prepare_receipt(envelope, prova, _evidencia(readbacks=_leituras(prova, *instantes)))
    divergente = _leituras(prova, "2026-11-10T11:10:00Z", "2026-11-10T11:10:20Z")
    divergente[1]["objects_sha256"] = "f" * 64
    with pytest.raises(ContractError):
        prepare_receipt(envelope, prova, _evidencia(readbacks=divergente))


def test_root_inventory_so_duas_excecoes_runtask():
    entrada = _entrada(_RECURSOS)
    plano = _plano(entrada)
    assert validate_mode_canary_prepare(plano, entrada, "initial")
    politica = _endereco(plano, "mode_seed")["change"]["after"]
    politica["policy"] = _politica("mode_dispatch", False)
    with pytest.raises(ContractError):
        validate_mode_canary_prepare(plano, entrada, "initial")


@pytest.mark.parametrize("campo", ["enabled_until", "activation_sha256", "delegations"])
def test_output_divergente_do_input_e_recusado(campo):
    entrada = _entrada(_RECURSOS)
    plano = _plano(entrada)
    saida = plano["planned_values"]["outputs"]["mode_canary_capabilities"]["value"]
    saida[campo] = {} if campo == "delegations" else "x"
    with pytest.raises(ContractError):
        validate_mode_canary_prepare(plano, entrada, "initial")


def test_unknown_em_campo_de_autorizacao_e_sessao_alheia_recusados():
    entrada = _entrada(_RECURSOS)
    plano = _plano(entrada)
    _endereco(plano, "mode_observe")["change"]["after_unknown"] = {"policy": True}
    with pytest.raises(ContractError):
        validate_mode_canary_prepare(plano, entrada, "initial")
    alheio = deepcopy(entrada)
    alheio["delegation_sessions"]["mode_seed"] = ["mc-us-" + "0" * 40]
    with pytest.raises(ContractError):
        validate_mode_canary_prepare(_plano(alheio), alheio, "initial")


def test_readiness_fecha_audit_launch_com_deny():
    entrada = _entrada(_RECURSOS, "readiness")
    prova = validate_mode_canary_prepare(_plano(entrada), entrada, "initial")
    assert prova["context_kind"] == "readiness"
    with pytest.raises(ContractError):
        validate_mode_canary_prepare(_plano(entrada, fechar=False), entrada, "initial")
