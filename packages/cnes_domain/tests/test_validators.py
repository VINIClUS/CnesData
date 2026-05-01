"""Branch coverage for cnes_domain.validators."""
from __future__ import annotations

import pytest

from cnes_domain.validators import (
    InvalidCNSError,
    InvalidCompetenciaError,
    InvalidCPFError,
    validate_cns,
    validate_competencia,
    validate_cpf,
)


def test_cpf_valido_nao_raise():
    validate_cpf("12345678909")


def test_cpf_rejeita_nao_string():
    with pytest.raises(InvalidCPFError):
        validate_cpf(12345678909)  # type: ignore[arg-type]


def test_cpf_rejeita_tamanho_errado():
    with pytest.raises(InvalidCPFError):
        validate_cpf("123")
    with pytest.raises(InvalidCPFError):
        validate_cpf("123456789012")


def test_cpf_rejeita_nao_digito():
    with pytest.raises(InvalidCPFError):
        validate_cpf("abcdefghijk")


def test_cpf_rejeita_sequencial():
    for seq in ("00000000000", "99999999999"):
        with pytest.raises(InvalidCPFError):
            validate_cpf(seq)


def test_cns_valido_nao_raise():
    validate_cns("123456789012345")


def test_cns_rejeita_nao_string():
    with pytest.raises(InvalidCNSError):
        validate_cns(123456789012345)  # type: ignore[arg-type]


def test_cns_rejeita_tamanho_errado():
    with pytest.raises(InvalidCNSError):
        validate_cns("1234")


def test_cns_rejeita_nao_digito():
    with pytest.raises(InvalidCNSError):
        validate_cns("abcdefghijklmno")


def test_competencia_valida_nao_raise():
    validate_competencia(202601)
    validate_competencia(202012)


def test_competencia_rejeita_nao_int():
    with pytest.raises(InvalidCompetenciaError):
        validate_competencia("202601")  # type: ignore[arg-type]


def test_competencia_rejeita_bool():
    with pytest.raises(InvalidCompetenciaError):
        validate_competencia(True)  # type: ignore[arg-type]


def test_competencia_rejeita_out_of_range():
    with pytest.raises(InvalidCompetenciaError):
        validate_competencia(199912)
    with pytest.raises(InvalidCompetenciaError):
        validate_competencia(210001)


def test_competencia_rejeita_mes_invalido():
    with pytest.raises(InvalidCompetenciaError):
        validate_competencia(202013)
    with pytest.raises(InvalidCompetenciaError):
        validate_competencia(202000)
