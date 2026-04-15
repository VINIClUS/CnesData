"""Settings protocol e validadores puros (zero I/O)."""

import re
from typing import Protocol

_RE_COD_MUN_6: re.Pattern[str] = re.compile(r"^\d{6}$")
_RE_IBGE7: re.Pattern[str] = re.compile(r"^\d{7}$")
_RE_CNPJ_14: re.Pattern[str] = re.compile(r"^\d{14}$")


class Settings(Protocol):
    DB_URL: str
    DB_HOST: str
    DB_USER: str
    COD_MUN_IBGE: str
    ID_MUNICIPIO_IBGE7: str
    CNPJ_MANTENEDORA: str
    COMPETENCIA_ANO: int
    COMPETENCIA_MES: int
    DLQ_THRESHOLD: float
    API_HOST: str
    API_PORT: int


def validar_formato(nome: str, valor: str, padrao: re.Pattern[str]) -> str:
    if not padrao.match(valor):
        raise OSError(f"variavel={nome} valor='{valor}' formato_invalido")
    return valor


def exigir_inteiro(nome: str, valor_str: str) -> int:
    try:
        return int(valor_str)
    except ValueError as err:
        raise OSError(
            f"variavel={nome} valor='{valor_str}' tipo_esperado=int",
        ) from err
