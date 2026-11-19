"""Anexos da role no oracle do prepare: managed policy, boundary e readback da inline."""

import json

import pytest

from scripts.promotion.mode_canary_contract import ContractError
from scripts.promotion.prepare_mode_canary import validate_mode_canary_prepare
from tests.production.infra._mode_canary_native import CONTA, _entrada, _novo_owner
from tests.production.infra.test_mode_canary_prepare_oracle import (
    _RECURSOS,
    _documento,
    _endereco,
    _plano,
    _politica,
)


@pytest.mark.parametrize(("campo", "valor"), [
    ("managed_policy_arns", ["arn:aws:iam::aws:policy/AdministratorAccess"]),
    ("permissions_boundary", f"arn:aws:iam::{CONTA}:policy/aberta"),
    ("inline_policy", [{"name": "cnesdata-prod-mode-observe-runtime", "policy": "{}"}]),
    ("inline_policy", [{"name": "extra", "policy": _documento([])}]),
])
def test_role_com_anexo_alem_da_policy_derivada_e_recusada(campo, valor):
    entrada = _entrada(_RECURSOS)
    plano = _plano(entrada)
    _endereco(plano, "mode_observe", "aws_iam_role")["change"]["after"][campo] = valor
    with pytest.raises(ContractError, match=r"field=attachments\.mode_observe$"):
        validate_mode_canary_prepare(plano, entrada, "initial")


def test_role_aceita_readback_da_policy_inline_derivada_e_anexos_vazios():
    entrada = _entrada(_RECURSOS)
    plano = _plano(entrada)
    politica = _endereco(plano, "mode_observe")["change"]["after"]
    lida = json.dumps(json.loads(politica["policy"]), indent=2)
    _endereco(plano, "mode_observe", "aws_iam_role")["change"]["after"].update(
        managed_policy_arns=[], permissions_boundary=None,
        inline_policy=[{"name": politica["name"], "policy": lida}])
    assert validate_mode_canary_prepare(plano, entrada, "initial")


def test_update_aceita_readback_da_policy_do_estado_anterior_na_role():
    antiga = _entrada(_RECURSOS)
    nova = _novo_owner(antiga)
    plano = _plano(nova, "update")
    anterior = _politica(antiga, "mode_seed", False)
    assert anterior != _politica(nova, "mode_seed", False)
    mudanca = _endereco(plano, "mode_seed")["change"]
    mudanca["before"] = mudanca["before"] | {"policy": anterior}
    _endereco(plano, "mode_seed", "aws_iam_role")["change"]["after"]["inline_policy"] = [
        {"name": mudanca["after"]["name"], "policy": json.dumps(json.loads(anterior), indent=2)}]
    assert validate_mode_canary_prepare(plano, nova, "established")
