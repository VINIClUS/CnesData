"""Testes de competencia_utils — cálculo do 5º/6º dia útil e janela de coleta."""
from datetime import date

import pytest

from cnes_domain.competencia import (
    janela_valida,
    periodo_em,
    quinto_dia_util,
    sexto_dia_util,
)


class TestQuintoDiaUtil:

    def test_janeiro_2026_com_feriado_ano_novo(self):
        assert quinto_dia_util(2026, 1) == date(2026, 1, 8)

    def test_junho_2026_sem_feriados_nacionais(self):
        assert quinto_dia_util(2026, 6) == date(2026, 6, 5)

    def test_novembro_2024_com_finados(self):
        assert quinto_dia_util(2024, 11) == date(2024, 11, 7)

    def test_dezembro_2024(self):
        assert quinto_dia_util(2024, 12) == date(2024, 12, 6)

    def test_lru_cache_retorna_mesmo_objeto(self):
        r1 = quinto_dia_util(2026, 6)
        r2 = quinto_dia_util(2026, 6)
        assert r1 is r2


class TestSextoDiaUtil:

    def test_abril_2026_sexto_dia_util(self):
        # Apr 3 = Sexta-feira Santa 2026 (feriado) → 6º BD = Apr 9
        assert sexto_dia_util(2026, 4) == date(2026, 4, 9)

    def test_novembro_2024_com_finados(self):
        assert sexto_dia_util(2024, 11) == date(2024, 11, 8)

    def test_lru_cache_retorna_mesmo_objeto(self):
        r1 = sexto_dia_util(2026, 6)
        r2 = sexto_dia_util(2026, 6)
        assert r1 is r2


class TestJanelaValida:

    def test_inicio_e_fim_corretos(self):
        inicio, fim = janela_valida("2026-06")
        assert inicio == sexto_dia_util(2026, 6)
        assert fim == sexto_dia_util(2026, 7)

    def test_virada_de_ano(self):
        inicio, fim = janela_valida("2026-12")
        assert inicio == sexto_dia_util(2026, 12)
        assert fim == sexto_dia_util(2027, 1)

    def test_novembro_2024_janela(self):
        inicio, fim = janela_valida("2024-11")
        assert inicio == date(2024, 11, 8)
        assert fim == sexto_dia_util(2024, 12)


class TestPeriodoEm:
    def test_formato_yyyy_mm(self):
        resultado = periodo_em(date(2026, 4, 15))
        partes = resultado.split("-")
        assert len(partes) == 2
        assert len(partes[0]) == 4
        assert len(partes[1]) == 2

    @pytest.mark.parametrize(
        ("referencia", "esperado"),
        [
            (date(2026, 4, 4), "2026-03"),
            (date(2025, 12, 1), "2025-11"),
            (date(2026, 4, 5), "2026-03"),
            (date(2026, 4, 9), "2026-04"),
            (date(2026, 1, 1), "2025-12"),
        ],
    )
    def test_respeita_janela_e_virada_do_ano(self, referencia, esperado):
        assert periodo_em(referencia) == esperado
