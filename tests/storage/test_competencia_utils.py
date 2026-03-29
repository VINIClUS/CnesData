"""Testes de competencia_utils — cálculo do 5º dia útil e janela de coleta."""
from datetime import date

from storage.competencia_utils import janela_valida, quinto_dia_util


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


class TestJanelaValida:

    def test_inicio_e_fim_corretos(self):
        inicio, fim = janela_valida("2026-06")
        assert inicio == quinto_dia_util(2026, 6)
        assert fim == quinto_dia_util(2026, 7)

    def test_virada_de_ano(self):
        inicio, fim = janela_valida("2026-12")
        assert inicio == quinto_dia_util(2026, 12)
        assert fim == quinto_dia_util(2027, 1)

    def test_novembro_2024_janela(self):
        inicio, fim = janela_valida("2024-11")
        assert inicio == date(2024, 11, 7)
        assert fim == date(2024, 12, 6)
