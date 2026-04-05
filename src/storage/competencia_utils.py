"""Utilitários de competência — janela de coleta CNES Local."""
from datetime import date, timedelta
from functools import lru_cache

import holidays

_BR = holidays.country_holidays("BR")


@lru_cache(maxsize=128)
def quinto_dia_util(ano: int, mes: int) -> date:
    """Retorna o 5º dia útil (seg–sex, sem feriados nacionais BR) do mês.

    Args:
        ano: Ano calendário.
        mes: Mês calendário 1–12.

    Returns:
        Data do 5º dia útil do mês.
    """
    dia = date(ano, mes, 1)
    count = 0
    while count < 5:
        if dia.weekday() < 5 and dia not in _BR:
            count += 1
        if count < 5:
            dia += timedelta(days=1)
    return dia


def janela_valida(competencia: str) -> tuple[date, date]:
    """Retorna (início_inclusivo, fim_exclusivo) da janela de coleta.

    Args:
        competencia: Competência no formato YYYY-MM.

    Returns:
        Tupla (inicio, fim): inicio inclusivo, fim exclusivo.
    """
    ano, mes = int(competencia[:4]), int(competencia[5:7])
    inicio = quinto_dia_util(ano, mes)
    ano_prox = ano if mes < 12 else ano + 1
    mes_prox = mes + 1 if mes < 12 else 1
    fim = quinto_dia_util(ano_prox, mes_prox)
    return inicio, fim


def periodo_atual() -> str:
    """Retorna competência do mês calendário atual no formato YYYY-MM."""
    hoje = date.today()
    return f"{hoje.year}-{hoje.month:02d}"
