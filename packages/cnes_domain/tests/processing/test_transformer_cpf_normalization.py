"""Regressão: transformer remove pontuação antes de validar/pad CPF."""
import polars as pl
from hypothesis import given, settings
from hypothesis import strategies as st

from cnes_domain.processing.transformer import transformar


def _df_minimo(cpf: str | None) -> pl.DataFrame:
    return pl.DataFrame({
        "CPF": [cpf],
        "CNS": ["702002887429583"],
        "NOME_PROFISSIONAL": ["TESTE"],
        "NOME_SOCIAL": [None],
        "SEXO": ["F"],
        "DATA_NASCIMENTO": ["1990-01-01"],
        "CBO": ["515105"],
        "TIPO_VINCULO": ["010101"],
        "SUS": ["S"],
        "CH_TOTAL": [40],
        "CH_AMBULATORIAL": [40],
        "CH_OUTRAS": [0],
        "CH_HOSPITALAR": [0],
        "CNES": ["0985333"],
        "ESTABELECIMENTO": ["UBS TESTE"],
        "TIPO_UNIDADE": ["02"],
        "COD_MUNICIPIO": ["354130"],
        "INE": [None],
        "NOME_EQUIPE": [None],
        "TIPO_EQUIPE": [None],
    })


class TestCpfComPontuacao:
    def test_cpf_formato_mascara_11_digitos_preservado(self) -> None:
        resultado = transformar(_df_minimo("123.456.789-09"))
        assert resultado.height == 1
        assert resultado["CPF"][0] == "12345678909"

    def test_cpf_com_letra_no_meio_sobrevive_apos_strip(self) -> None:
        resultado = transformar(_df_minimo("1234567890a"))
        assert resultado.height == 1
        assert resultado["CPF"][0] == "01234567890"

    def test_cpf_com_hifen_e_10_digitos_recebe_pad(self) -> None:
        resultado = transformar(_df_minimo("12345-67890"))
        assert resultado.height == 1
        assert resultado["CPF"][0] == "01234567890"

    def test_cpf_15_digitos_descartado_por_exceder_apos_strip(self) -> None:
        resultado = transformar(_df_minimo("123456789012345"))
        assert resultado.height == 0


class TestCpfEdgeCases:
    def test_cpf_apenas_whitespace_descartado(self) -> None:
        resultado = transformar(_df_minimo("   "))
        assert resultado.height == 0

    def test_cpf_vazio_descartado(self) -> None:
        resultado = transformar(_df_minimo(""))
        assert resultado.height == 0

    def test_cpf_com_zero_width_unicode_sobrevive(self) -> None:
        resultado = transformar(_df_minimo("123\u200b45678909"))
        assert resultado.height == 1
        assert resultado["CPF"][0] == "12345678909"

    def test_cpf_apenas_pontuacao_descartado(self) -> None:
        resultado = transformar(_df_minimo(".-./"))
        assert resultado.height == 0

    def test_cpf_com_cpf_formatado_valido_preservado(self) -> None:
        resultado = transformar(_df_minimo("117.167.238-17"))
        assert resultado.height == 1
        assert resultado["CPF"][0] == "11716723817"


class TestCpfPropertyInvariant:
    @given(
        entrada=st.text(
            alphabet=st.characters(whitelist_categories=("Nd", "P", "Z")),
            min_size=0,
            max_size=25,
        ),
    )
    @settings(max_examples=200, deadline=None)
    def test_saida_contem_apenas_digitos_quando_preservada(
        self, entrada: str,
    ) -> None:
        resultado = transformar(_df_minimo(entrada))
        if resultado.height == 1:
            saida = resultado["CPF"][0]
            assert saida.isdigit()
            assert len(saida) == 11
