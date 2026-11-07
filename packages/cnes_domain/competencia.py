"""Utilitários de competência — janela de coleta CNES Local."""
from datetime import UTC, date, datetime, timedelta
from functools import lru_cache

import holidays

_BR = holidays.country_holidays("BR")


@lru_cache(maxsize=256)
def _dia_util_n(ano: int, mes: int, n: int) -> date:
    dia = date(ano, mes, 1)
    count = 0
    while count < n:
        if dia.weekday() < 5 and dia not in _BR:
            count += 1
        if count < n:
            dia += timedelta(days=1)
    return dia


@lru_cache(maxsize=128)
def quinto_dia_util(ano: int, mes: int) -> date:
    """Retorna o 5º dia útil (seg–sex, sem feriados nacionais BR) do mês.

    Args:
        ano: Ano calendário.
        mes: Mês calendário 1–12.

    Returns:
        Data do 5º dia útil do mês.
    """
    return _dia_util_n(ano, mes, 5)


@lru_cache(maxsize=128)
def sexto_dia_util(ano: int, mes: int) -> date:
    """Retorna o 6º dia útil (seg–sex, sem feriados nacionais BR) do mês.

    Args:
        ano: Ano calendário.
        mes: Mês calendário 1–12.

    Returns:
        Data do 6º dia útil do mês.
    """
    return _dia_util_n(ano, mes, 6)


def janela_valida(competencia: str) -> tuple[date, date]:
    """Retorna (início_inclusivo, fim_exclusivo) da janela de coleta.

    Args:
        competencia: Competência no formato YYYY-MM.

    Returns:
        Tupla (inicio, fim): inicio inclusivo, fim exclusivo.
    """
    ano, mes = int(competencia[:4]), int(competencia[5:7])
    inicio = sexto_dia_util(ano, mes)
    ano_prox = ano if mes < 12 else ano + 1
    mes_prox = mes + 1 if mes < 12 else 1
    fim = sexto_dia_util(ano_prox, mes_prox)
    return inicio, fim


def periodo_atual() -> str:
    """Retorna competência corrente considerando a janela de coleta, no formato YYYY-MM."""
    hoje = datetime.now(tz=UTC).date()
    inicio_mes_atual = sexto_dia_util(hoje.year, hoje.month)
    if hoje >= inicio_mes_atual:
        return f"{hoje.year}-{hoje.month:02d}"
    mes_ant = hoje.month - 1 if hoje.month > 1 else 12
    ano_ant = hoje.year if hoje.month > 1 else hoje.year - 1
    return f"{ano_ant}-{mes_ant:02d}"
