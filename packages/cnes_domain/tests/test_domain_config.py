"""Testes dos validadores puros do domínio."""

import pytest

from cnes_domain.config import _RE_CNPJ_14, _RE_COD_MUN_6, exigir_inteiro, validar_formato


class TestValidarFormato:

    def test_valor_valido_retorna_valor(self):
        resultado = validar_formato("COD_MUN_IBGE", "354130", _RE_COD_MUN_6)
        assert resultado == "354130"

    def test_valor_invalido_levanta_os_error(self):
        with pytest.raises(OSError, match="formato_invalido"):
            validar_formato("CNPJ", "123", _RE_CNPJ_14)

    def test_mensagem_contem_variavel_e_valor(self):
        with pytest.raises(OSError) as exc_info:
            validar_formato("VAR_X", "abc", _RE_COD_MUN_6)
        msg = str(exc_info.value)
        assert "VAR_X" in msg
        assert "abc" in msg


class TestExigirInteiro:

    def test_string_numerica_converte(self):
        assert exigir_inteiro("ANO", "2025") == 2025

    def test_string_invalida_levanta_os_error(self):
        with pytest.raises(OSError, match="tipo_esperado=int"):
            exigir_inteiro("MES", "abc")

    def test_mensagem_contem_variavel_e_valor(self):
        with pytest.raises(OSError) as exc_info:
            exigir_inteiro("VAR_Y", "xyz")
        msg = str(exc_info.value)
        assert "VAR_Y" in msg
        assert "xyz" in msg

    def test_causa_e_value_error(self):
        with pytest.raises(OSError) as exc_info:
            exigir_inteiro("V", "x")
        assert isinstance(exc_info.value.__cause__, ValueError)
