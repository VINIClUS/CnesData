"""Testes de contratos de dims."""
from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from cnes_contracts.dims import (
    CBO,
    CID10,
    Competencia,
    Estabelecimento,
    Municipio,
    ProcedimentoSUS,
    Profissional,
)


def test_profissional_valida():
    p = Profissional(
        sk_profissional=1,
        cpf_hash="0123456789a",
        nome="João",
        cns="123456789012345",
        sk_cbo_principal=42,
        fontes={"CNES_LOCAL": True},
        criado_em=datetime(2026, 1, 1, tzinfo=UTC),
        atualizado_em=datetime(2026, 1, 1, tzinfo=UTC),
    )
    assert p.sk_profissional == 1
    assert p.fontes == {"CNES_LOCAL": True}


def test_profissional_rejeita_cpf_hash_invalido():
    with pytest.raises(ValidationError):
        Profissional(
            sk_profissional=1,
            cpf_hash="XX",
            nome="João",
            criado_em=datetime(2026, 1, 1, tzinfo=UTC),
            atualizado_em=datetime(2026, 1, 1, tzinfo=UTC),
        )


def test_profissional_rejeita_sk_zero():
    with pytest.raises(ValidationError):
        Profissional(
            sk_profissional=0,
            cpf_hash="0123456789a",
            nome="João",
            criado_em=datetime(2026, 1, 1, tzinfo=UTC),
            atualizado_em=datetime(2026, 1, 1, tzinfo=UTC),
        )


def test_profissional_rejeita_nome_vazio():
    with pytest.raises(ValidationError):
        Profissional(
            sk_profissional=1,
            cpf_hash="0123456789a",
            nome="",
            criado_em=datetime(2026, 1, 1, tzinfo=UTC),
            atualizado_em=datetime(2026, 1, 1, tzinfo=UTC),
        )


def test_profissional_rejeita_cns_invalido():
    with pytest.raises(ValidationError):
        Profissional(
            sk_profissional=1,
            cpf_hash="0123456789a",
            nome="João",
            cns="123",
            criado_em=datetime(2026, 1, 1, tzinfo=UTC),
            atualizado_em=datetime(2026, 1, 1, tzinfo=UTC),
        )


def test_profissional_rejeita_sk_cbo_zero():
    with pytest.raises(ValidationError):
        Profissional(
            sk_profissional=1,
            cpf_hash="0123456789a",
            nome="João",
            sk_cbo_principal=0,
            criado_em=datetime(2026, 1, 1, tzinfo=UTC),
            atualizado_em=datetime(2026, 1, 1, tzinfo=UTC),
        )


def test_estabelecimento_valida():
    e = Estabelecimento(
        sk_estabelecimento=1,
        cnes="1234567",
        nome="Hospital",
        cnpj_mantenedora="12345678000199",
        tp_unid=5,
        sk_municipio=1,
        criado_em=datetime(2026, 1, 1, tzinfo=UTC),
        atualizado_em=datetime(2026, 1, 1, tzinfo=UTC),
    )
    assert e.cnes == "1234567"


def test_estabelecimento_rejeita_cnes_invalido():
    with pytest.raises(ValidationError):
        Estabelecimento(
            sk_estabelecimento=1,
            cnes="ABC",
            nome="Hospital",
            tp_unid=5,
            sk_municipio=1,
            criado_em=datetime(2026, 1, 1, tzinfo=UTC),
            atualizado_em=datetime(2026, 1, 1, tzinfo=UTC),
        )


def test_estabelecimento_rejeita_cnpj_invalido():
    with pytest.raises(ValidationError):
        Estabelecimento(
            sk_estabelecimento=1,
            cnes="1234567",
            nome="Hospital",
            cnpj_mantenedora="12",
            tp_unid=5,
            sk_municipio=1,
            criado_em=datetime(2026, 1, 1, tzinfo=UTC),
            atualizado_em=datetime(2026, 1, 1, tzinfo=UTC),
        )


def test_estabelecimento_rejeita_tp_unid_fora_range():
    with pytest.raises(ValidationError):
        Estabelecimento(
            sk_estabelecimento=1,
            cnes="1234567",
            nome="Hospital",
            tp_unid=1000,
            sk_municipio=1,
            criado_em=datetime(2026, 1, 1, tzinfo=UTC),
            atualizado_em=datetime(2026, 1, 1, tzinfo=UTC),
        )


def test_procedimento_sus_valida():
    p = ProcedimentoSUS(
        sk_procedimento=1,
        cod_sigtap="0101010010",
        descricao="Consulta",
        complexidade=1,
        financiamento="MAC",
        modalidade="AMB",
        competencia_vigencia_ini=200001,
        competencia_vigencia_fim=209912,
    )
    assert p.complexidade == 1


def test_procedimento_sus_defaults():
    p = ProcedimentoSUS(
        sk_procedimento=1,
        cod_sigtap="0101010010",
        descricao="Consulta",
    )
    assert p.complexidade is None
    assert p.financiamento is None
    assert p.modalidade is None


def test_procedimento_sus_rejeita_cod_sigtap_invalido():
    with pytest.raises(ValidationError):
        ProcedimentoSUS(
            sk_procedimento=1,
            cod_sigtap="ABC",
            descricao="Consulta",
        )


def test_procedimento_sus_rejeita_complexidade_invalida():
    with pytest.raises(ValidationError):
        ProcedimentoSUS(
            sk_procedimento=1,
            cod_sigtap="0101010010",
            descricao="Consulta",
            complexidade=9,
        )


def test_procedimento_sus_rejeita_financiamento_invalido():
    with pytest.raises(ValidationError):
        ProcedimentoSUS(
            sk_procedimento=1,
            cod_sigtap="0101010010",
            descricao="Consulta",
            financiamento="XYZ",
        )


def test_procedimento_sus_rejeita_modalidade_invalida():
    with pytest.raises(ValidationError):
        ProcedimentoSUS(
            sk_procedimento=1,
            cod_sigtap="0101010010",
            descricao="Consulta",
            modalidade="FOO",
        )


def test_procedimento_sus_rejeita_competencia_ini_fora_range():
    with pytest.raises(ValidationError):
        ProcedimentoSUS(
            sk_procedimento=1,
            cod_sigtap="0101010010",
            descricao="Consulta",
            competencia_vigencia_ini=199912,
        )


def test_cbo_valida():
    c = CBO(sk_cbo=1, cod_cbo="225125", descricao="Médico")
    assert c.cod_cbo == "225125"


def test_cbo_rejeita_cod_invalido():
    with pytest.raises(ValidationError):
        CBO(sk_cbo=1, cod_cbo="ABC", descricao="Médico")


def test_cbo_rejeita_sk_zero():
    with pytest.raises(ValidationError):
        CBO(sk_cbo=0, cod_cbo="225125", descricao="Médico")


def test_cid10_valida():
    c = CID10(sk_cid=1, cod_cid="A00", descricao="Cólera", capitulo=1)
    assert c.cod_cid == "A00"


def test_cid10_rejeita_cod_invalido():
    with pytest.raises(ValidationError):
        CID10(sk_cid=1, cod_cid="99", descricao="X", capitulo=1)


def test_cid10_rejeita_capitulo_fora_range_superior():
    with pytest.raises(ValidationError):
        CID10(sk_cid=1, cod_cid="A00", descricao="Cólera", capitulo=23)


def test_cid10_rejeita_capitulo_fora_range_inferior():
    with pytest.raises(ValidationError):
        CID10(sk_cid=1, cod_cid="A00", descricao="Cólera", capitulo=0)


def test_municipio_valida():
    m = Municipio(
        sk_municipio=1,
        ibge6="354130",
        ibge7="3541308",
        nome="Presidente Epitácio",
        uf="SP",
        populacao_estimada=45000,
        teto_pab_cents=100000,
    )
    assert m.uf == "SP"


def test_municipio_rejeita_ibge6_invalido():
    with pytest.raises(ValidationError):
        Municipio(
            sk_municipio=1,
            ibge6="ABC",
            ibge7="3541308",
            nome="X",
            uf="SP",
        )


def test_municipio_rejeita_uf_invalida():
    with pytest.raises(ValidationError):
        Municipio(
            sk_municipio=1,
            ibge6="354130",
            ibge7="3541308",
            nome="X",
            uf="sp",
        )


def test_municipio_rejeita_populacao_negativa():
    with pytest.raises(ValidationError):
        Municipio(
            sk_municipio=1,
            ibge6="354130",
            ibge7="3541308",
            nome="X",
            uf="SP",
            populacao_estimada=-1,
        )


def test_competencia_valida():
    c = Competencia(
        sk_competencia=1,
        competencia=202601,
        ano=2026,
        mes=1,
        qtd_dias_uteis=22,
    )
    assert c.ano == 2026


def test_competencia_rejeita_competencia_fora_range():
    with pytest.raises(ValidationError):
        Competencia(
            sk_competencia=1,
            competencia=199912,
            ano=2026,
            mes=1,
        )


def test_competencia_rejeita_ano_fora_range():
    with pytest.raises(ValidationError):
        Competencia(
            sk_competencia=1,
            competencia=202601,
            ano=2019,
            mes=1,
        )


def test_competencia_rejeita_mes_fora_range():
    with pytest.raises(ValidationError):
        Competencia(
            sk_competencia=1,
            competencia=202601,
            ano=2026,
            mes=13,
        )


def test_competencia_rejeita_dias_uteis_fora_range():
    with pytest.raises(ValidationError):
        Competencia(
            sk_competencia=1,
            competencia=202601,
            ano=2026,
            mes=1,
            qtd_dias_uteis=32,
        )
