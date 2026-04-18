"""Testes dos schemas de colunas padrão."""

from cnes_domain.contracts.columns import (
    SCHEMA_EQUIPE,
    SCHEMA_ESTABELECIMENTO,
    SCHEMA_PROFISSIONAL,
)
from cnes_domain.contracts.sihd_columns import (
    SCHEMA_AIH,
    SCHEMA_PROCEDIMENTO_AIH,
)


class TestSchemaEstabelecimento:

    def test_contem_cnes(self):
        assert "CNES" in SCHEMA_ESTABELECIMENTO

    def test_contem_cnpj_mantenedora(self):
        assert "CNPJ_MANTENEDORA" in SCHEMA_ESTABELECIMENTO

    def test_e_tupla(self):
        assert isinstance(SCHEMA_ESTABELECIMENTO, tuple)


class TestSchemaProfissional:

    def test_contem_cpf(self):
        assert "CPF" in SCHEMA_PROFISSIONAL

    def test_contem_cbo(self):
        assert "CBO" in SCHEMA_PROFISSIONAL

    def test_e_tupla(self):
        assert isinstance(SCHEMA_PROFISSIONAL, tuple)


class TestSchemaEquipe:

    def test_contem_ine(self):
        assert "INE" in SCHEMA_EQUIPE

    def test_e_tupla(self):
        assert isinstance(SCHEMA_EQUIPE, tuple)


class TestSchemaAih:

    def test_contem_num_aih(self):
        assert "NUM_AIH" in SCHEMA_AIH

    def test_contem_cnes(self):
        assert "CNES" in SCHEMA_AIH

    def test_e_tupla(self):
        assert isinstance(SCHEMA_AIH, tuple)


class TestSchemaProcedimentoAih:

    def test_contem_num_aih(self):
        assert "NUM_AIH" in SCHEMA_PROCEDIMENTO_AIH

    def test_contem_valor(self):
        assert "VALOR" in SCHEMA_PROCEDIMENTO_AIH

    def test_e_tupla(self):
        assert isinstance(SCHEMA_PROCEDIMENTO_AIH, tuple)
