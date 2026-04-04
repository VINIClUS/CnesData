"""delta_snapshot.py — Drift entre df_processado atual e snapshot anterior."""
from dataclasses import dataclass, field

import pandas as pd

_CHAVE = ("CPF", "CNES")
_ATRIBUTOS = ("CBO", "CH_TOTAL", "TIPO_VINCULO")


@dataclass
class DeltaSnapshot:
    """Diferença entre dois DataFrames de profissionais indexados por CPF+CNES."""

    n_novos: int
    n_removidos: int
    n_alterados: int
    novos: list[dict] = field(default_factory=list)
    removidos: list[dict] = field(default_factory=list)
    alterados: list[dict] = field(default_factory=list)


def calcular_delta(df_atual: pd.DataFrame, df_anterior: pd.DataFrame) -> DeltaSnapshot:
    """Compara profissionais atuais com snapshot anterior por CPF+CNES.

    Args:
        df_atual: DataFrame processado da rodada atual (Firebird).
        df_anterior: DataFrame processado do snapshot salvo.

    Returns:
        DeltaSnapshot com contagens e detalhes dos registros modificados.
    """
    chave = list(_CHAVE)
    atributos = [c for c in _ATRIBUTOS if c in df_atual.columns and c in df_anterior.columns]

    mapa_atual = _para_mapa(df_atual, chave, atributos)
    mapa_anterior = _para_mapa(df_anterior, chave, atributos)

    keys_atual = set(mapa_atual)
    keys_anterior = set(mapa_anterior)

    novos_keys = keys_atual - keys_anterior
    removidos_keys = keys_anterior - keys_atual
    comuns_keys = keys_atual & keys_anterior
    alterados_keys = {k for k in comuns_keys if mapa_atual[k] != mapa_anterior[k]}

    return DeltaSnapshot(
        n_novos=len(novos_keys),
        n_removidos=len(removidos_keys),
        n_alterados=len(alterados_keys),
        novos=_registros_simples(mapa_atual, novos_keys, atributos),
        removidos=_registros_simples(mapa_anterior, removidos_keys, atributos),
        alterados=_registros_diff(mapa_atual, mapa_anterior, alterados_keys, atributos),
    )


def _para_mapa(
    df: pd.DataFrame, chave: list[str], atributos: list[str]
) -> dict[tuple, dict]:
    cols = chave + atributos
    dedup = df[cols].drop_duplicates(subset=chave)
    result: dict[tuple, dict] = {}
    for _, row in dedup.iterrows():
        key = tuple(str(row[c]) for c in chave)
        result[key] = {a: row[a] for a in atributos}
    return result


def _registros_simples(
    mapa: dict[tuple, dict], keys: set[tuple], atributos: list[str]
) -> list[dict]:
    result = []
    for key in keys:
        entry = {"CPF": key[0], "CNES": key[1]}
        entry.update(mapa[key])
        result.append(entry)
    return result


def _registros_diff(
    mapa_atual: dict[tuple, dict],
    mapa_anterior: dict[tuple, dict],
    keys: set[tuple],
    atributos: list[str],
) -> list[dict]:
    result = []
    for key in keys:
        entry: dict = {"CPF": key[0], "CNES": key[1]}
        for attr in atributos:
            entry[f"{attr}_anterior"] = mapa_anterior[key][attr]
            entry[f"{attr}_atual"] = mapa_atual[key][attr]
        result.append(entry)
    return result
