"""Testes de contratos de fatos."""
from __future__ import annotations

from datetime import UTC, date, datetime
from uuid import uuid4

import pytest
from pydantic import ValidationError

from cnes_contracts.fatos import (
    Internacao,
    ProcedimentoAIH,
    ProducaoAmbulatorial,
    VinculoCNES,
)


def test_vinculo_cnes_valido():
    v = VinculoCNES(
        sk_profissional=1,
        sk_estabelecimento=2,
        sk_cbo=3,
        sk_competencia=4,
        carga_horaria_sem=40,
        ind_vinc="CLT",
        sk_equipe=5,
        job_id=uuid4(),
        fonte_sistema="CNES_LOCAL",
        extracao_ts=datetime(2026, 1, 1, tzinfo=UTC),
    )
    assert v.carga_horaria_sem == 40


def test_vinculo_cnes_rejeita_sk_zero():
    with pytest.raises(ValidationError):
        VinculoCNES(
            sk_profissional=0,
            sk_estabelecimento=2,
            sk_cbo=3,
            sk_competencia=4,
            job_id=uuid4(),
            fonte_sistema="CNES_LOCAL",
            extracao_ts=datetime(2026, 1, 1, tzinfo=UTC),
        )


def test_vinculo_cnes_rejeita_carga_horaria_acima_limite():
    with pytest.raises(ValidationError):
        VinculoCNES(
            sk_profissional=1,
            sk_estabelecimento=2,
            sk_cbo=3,
            sk_competencia=4,
            carga_horaria_sem=200,
            job_id=uuid4(),
            fonte_sistema="CNES_LOCAL",
            extracao_ts=datetime(2026, 1, 1, tzinfo=UTC),
        )


def test_vinculo_cnes_rejeita_fonte_sistema_invalida():
    with pytest.raises(ValidationError):
        VinculoCNES(
            sk_profissional=1,
            sk_estabelecimento=2,
            sk_cbo=3,
            sk_competencia=4,
            job_id=uuid4(),
            fonte_sistema="SIHD",
            extracao_ts=datetime(2026, 1, 1, tzinfo=UTC),
        )


def test_vinculo_cnes_rejeita_ind_vinc_invalido():
    with pytest.raises(ValidationError):
        VinculoCNES(
            sk_profissional=1,
            sk_estabelecimento=2,
            sk_cbo=3,
            sk_competencia=4,
            ind_vinc="@@@",
            job_id=uuid4(),
            fonte_sistema="CNES_LOCAL",
            extracao_ts=datetime(2026, 1, 1, tzinfo=UTC),
        )


def test_producao_ambulatorial_valida():
    p = ProducaoAmbulatorial(
        sk_profissional=1,
        sk_estabelecimento=2,
        sk_procedimento=3,
        sk_competencia=4,
        sk_cid_principal=5,
        qtd=10,
        valor_aprov_cents=15000,
        dt_atendimento=date(2026, 1, 10),
        job_id=uuid4(),
        fonte_sistema="SIA_APA",
        extracao_ts=datetime(2026, 1, 1, tzinfo=UTC),
        fontes_reportadas={"SIA_APA": {"rows": 10}},
    )
    assert p.qtd == 10


def test_producao_ambulatorial_rejeita_qtd_zero():
    with pytest.raises(ValidationError):
        ProducaoAmbulatorial(
            sk_profissional=1,
            sk_estabelecimento=2,
            sk_procedimento=3,
            sk_competencia=4,
            qtd=0,
            valor_aprov_cents=100,
            job_id=uuid4(),
            fonte_sistema="SIA_APA",
            extracao_ts=datetime(2026, 1, 1, tzinfo=UTC),
        )


def test_producao_ambulatorial_rejeita_valor_negativo():
    with pytest.raises(ValidationError):
        ProducaoAmbulatorial(
            sk_profissional=1,
            sk_estabelecimento=2,
            sk_procedimento=3,
            sk_competencia=4,
            qtd=1,
            valor_aprov_cents=-1,
            job_id=uuid4(),
            fonte_sistema="SIA_APA",
            extracao_ts=datetime(2026, 1, 1, tzinfo=UTC),
        )


def test_producao_ambulatorial_rejeita_fonte_sistema_invalida():
    with pytest.raises(ValidationError):
        ProducaoAmbulatorial(
            sk_profissional=1,
            sk_estabelecimento=2,
            sk_procedimento=3,
            sk_competencia=4,
            qtd=1,
            valor_aprov_cents=100,
            job_id=uuid4(),
            fonte_sistema="CNES_LOCAL",
            extracao_ts=datetime(2026, 1, 1, tzinfo=UTC),
        )


def test_internacao_valida():
    i = Internacao(
        num_aih="1234567890123",
        sk_profissional_solicit=1,
        sk_estabelecimento=2,
        sk_competencia=3,
        sk_cid_principal=4,
        dt_internacao=date(2026, 1, 5),
        dt_saida=date(2026, 1, 10),
        valor_total_cents=500000,
        job_id=uuid4(),
        extracao_ts=datetime(2026, 1, 1, tzinfo=UTC),
    )
    assert i.fonte_sistema == "SIHD"
    assert i.num_aih == "1234567890123"


def test_internacao_rejeita_num_aih_invalido():
    with pytest.raises(ValidationError):
        Internacao(
            num_aih="ABC",
            sk_estabelecimento=2,
            sk_competencia=3,
            dt_internacao=date(2026, 1, 5),
            job_id=uuid4(),
            extracao_ts=datetime(2026, 1, 1, tzinfo=UTC),
        )


def test_internacao_rejeita_valor_total_negativo():
    with pytest.raises(ValidationError):
        Internacao(
            num_aih="1234567890123",
            sk_estabelecimento=2,
            sk_competencia=3,
            dt_internacao=date(2026, 1, 5),
            valor_total_cents=-1,
            job_id=uuid4(),
            extracao_ts=datetime(2026, 1, 1, tzinfo=UTC),
        )


def test_internacao_rejeita_sk_profissional_solicit_zero():
    with pytest.raises(ValidationError):
        Internacao(
            num_aih="1234567890123",
            sk_profissional_solicit=0,
            sk_estabelecimento=2,
            sk_competencia=3,
            dt_internacao=date(2026, 1, 5),
            job_id=uuid4(),
            extracao_ts=datetime(2026, 1, 1, tzinfo=UTC),
        )


def test_procedimento_aih_valido():
    p = ProcedimentoAIH(
        sk_aih=1,
        sk_procedimento=2,
        sk_profissional_exec=3,
        sk_competencia=4,
        qtd=2,
        valor_cents=10000,
        job_id=uuid4(),
        extracao_ts=datetime(2026, 1, 1, tzinfo=UTC),
    )
    assert p.qtd == 2


def test_procedimento_aih_default_qtd():
    p = ProcedimentoAIH(
        sk_aih=1,
        sk_procedimento=2,
        sk_competencia=4,
        job_id=uuid4(),
        extracao_ts=datetime(2026, 1, 1, tzinfo=UTC),
    )
    assert p.qtd == 1


def test_procedimento_aih_rejeita_qtd_zero():
    with pytest.raises(ValidationError):
        ProcedimentoAIH(
            sk_aih=1,
            sk_procedimento=2,
            sk_competencia=4,
            qtd=0,
            job_id=uuid4(),
            extracao_ts=datetime(2026, 1, 1, tzinfo=UTC),
        )


def test_procedimento_aih_rejeita_valor_negativo():
    with pytest.raises(ValidationError):
        ProcedimentoAIH(
            sk_aih=1,
            sk_procedimento=2,
            sk_competencia=4,
            valor_cents=-1,
            job_id=uuid4(),
            extracao_ts=datetime(2026, 1, 1, tzinfo=UTC),
        )


def test_procedimento_aih_rejeita_sk_aih_zero():
    with pytest.raises(ValidationError):
        ProcedimentoAIH(
            sk_aih=0,
            sk_procedimento=2,
            sk_competencia=4,
            job_id=uuid4(),
            extracao_ts=datetime(2026, 1, 1, tzinfo=UTC),
        )
