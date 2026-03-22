"""
transformer.py — Camada de Processamento: Transformação e Validação

Responsabilidade: receber o DataFrame bruto da camada de ingestão,
aplicar limpeza, padronização de tipos e sinalização de alertas de qualidade.

Regras de qualidade implementadas (fonte: data_dictionary.md):
  - RQ-002: CPF nulo ou com comprimento incorreto → exclusão + log WARNING.
  - RQ-003: Vínculo com carga horária total zero ("Vínculo Zumbi") →
            flag ALERTA_STATUS_CH = 'ATIVO_SEM_CH'. Não exclui — sinaliza.
  - Padronização de strings: strip() em todas as colunas de texto.
  - Preenchimento de nulos: colunas opcionais de equipe (resultado do LEFT JOIN).
"""

import logging
from typing import Final

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ── Constantes de Domínio ─────────────────────────────────────────────────────

# Valor padrão para profissionais sem equipe vinculada (LEFT JOIN sem match).
VALOR_SEM_EQUIPE: Final[str] = "SEM EQUIPE VINCULADA"
VALOR_SEM_INE: Final[str] = "-"

# Flags usadas em RQ-003 para indicar o status da carga horária.
ALERTA_ATIVO_SEM_CH: Final[str] = "ATIVO_SEM_CH"
ALERTA_CH_OK: Final[str] = "OK"

# Colunas de texto que recebem strip() para remover espaços do Firebird.
_COLUNAS_TEXTO: Final[tuple[str, ...]] = (
    "CPF", "CNS", "NOME_PROFISSIONAL", "CBO", "CNES", "ESTABELECIMENTO",
    "NOME_SOCIAL", "SEXO", "TIPO_VINCULO", "SUS",
    "TIPO_UNIDADE", "COD_MUNICIPIO",
)

# Colunas opcionais de equipe que chegam NULL no LEFT JOIN sem correspondência.
_MAPEAMENTO_NULOS_EQUIPE: Final[dict[str, str]] = {
    "NOME_EQUIPE": VALOR_SEM_EQUIPE,
    "INE":         VALOR_SEM_INE,
    "TIPO_EQUIPE": VALOR_SEM_INE,
}


# ── Funções Auxiliares (Regras de Qualidade) ──────────────────────────────────

def _aplicar_rq002_validar_cpf(df: pd.DataFrame) -> pd.DataFrame:
    """
    RQ-002: Remove registros com CPF nulo ou com comprimento diferente de 11.

    CPFs inválidos quebram o JOIN com LFCES048 e invalidam a trilha de auditoria.
    O strip já foi aplicado antes desta função; valores None/NaN foram convertidos
    para as strings "None"/"nan" pelo astype(str) da etapa anterior.

    Args:
        df: DataFrame com coluna CPF já convertida para string e stripada.

    Returns:
        pd.DataFrame: Cópia do DataFrame sem registros com CPF inválido.
    """
    cpf_str = df["CPF"].astype(str).str.strip()

    # Captura nulos que viraram strings e comprimentos fora do padrão.
    mascara_invalido = (
        cpf_str.isin({"", "None", "nan", "NaN", "NaT"})
        | (cpf_str.str.len() != 11)
    )

    total_invalidos: int = int(mascara_invalido.sum())
    if total_invalidos > 0:
        logger.warning(
            "RQ-002: cpf_invalido_count=%d indices=%s",
            total_invalidos,
            df.index[mascara_invalido].tolist(),
        )

    return df[~mascara_invalido].copy()


def _aplicar_rq003_flag_carga_horaria(df: pd.DataFrame) -> pd.DataFrame:
    """
    RQ-003: Sinaliza vínculos com carga horária total igual a zero.

    Profissionais com CH_TOTAL == 0 estão cadastrados sem
    nenhuma hora declarada. Eles NÃO são excluídos — são marcados com
    ALERTA_STATUS_CH = 'ATIVO_SEM_CH' para revisão pelo RH.

    Args:
        df: DataFrame com coluna numérica CH_TOTAL.

    Returns:
        pd.DataFrame: Cópia do DataFrame com nova coluna ALERTA_STATUS_CH.
    """
    df["ALERTA_STATUS_CH"] = np.where(
        df["CH_TOTAL"] == 0, ALERTA_ATIVO_SEM_CH, ALERTA_CH_OK
    )

    total_zumbis: int = int((df["ALERTA_STATUS_CH"] == ALERTA_ATIVO_SEM_CH).sum())
    if total_zumbis > 0:
        logger.warning(
            "RQ-003: %d vínculo(s) com carga horária zero (ATIVO_SEM_CH).",
            total_zumbis,
        )
    else:
        logger.debug("RQ-003: nenhum vínculo zumbi encontrado.")

    return df


# ── Função Pública ────────────────────────────────────────────────────────────

def transformar(
    df: pd.DataFrame,
    cbo_lookup: dict[str, str] | None = None,
) -> pd.DataFrame:
    """Aplica strip, RQ-002 (CPF), RQ-003 (CH flag) e fillna de equipe.

    Args:
        df: DataFrame bruto de cnes_client.extrair_profissionais().
        cbo_lookup: Dict CBO → descrição; adiciona coluna DESCRICAO_CBO quando fornecido.

    Returns:
        DataFrame transformado com ALERTA_STATUS_CH; pode ter menos linhas (RQ-002).
    """
    logger.debug("Iniciando transformação. Registros de entrada: %d", len(df))
    resultado = df.copy()

    # ── Etapa 1: Limpeza de strings ───────────────────────────────────────────
    for coluna in _COLUNAS_TEXTO:
        if coluna in resultado.columns:
            resultado[coluna] = resultado[coluna].astype(str).str.strip()

    # ── Etapa 1B: Zero-padding de CPF ─────────────────────────────────────────
    # Firebird omite zeros à esquerda em CPFs que começam com 0.
    # Aplicamos zfill(11) apenas em valores que não são sentinelas de nulo
    # para preservar a exclusão correta pelo RQ-002.
    if "CPF" in resultado.columns:
        _sentinelas = {"None", "nan", "NaN", "NaT", ""}
        _mask_pad = ~resultado["CPF"].isin(_sentinelas)
        resultado.loc[_mask_pad, "CPF"] = resultado.loc[_mask_pad, "CPF"].str.zfill(11)

    registros_antes_rq002 = len(resultado)

    # ── Etapa 2: RQ-002 — validação de CPF ───────────────────────────────────
    resultado = _aplicar_rq002_validar_cpf(resultado)
    removidos_rq002 = registros_antes_rq002 - len(resultado)
    if removidos_rq002 > 0:
        logger.info(
            "Transformação: %d registro(s) removido(s) por CPF inválido (RQ-002).",
            removidos_rq002,
        )

    # ── Etapa 3: RQ-003 — flag de carga horária zero ─────────────────────────
    resultado = _aplicar_rq003_flag_carga_horaria(resultado)

    # ── Etapa 4: Preenchimento de nulos (colunas opcionais de equipe) ─────────
    for coluna, valor_padrao in _MAPEAMENTO_NULOS_EQUIPE.items():
        if coluna in resultado.columns:
            nulos = resultado[coluna].isna().sum()
            resultado[coluna] = resultado[coluna].fillna(valor_padrao)
            if nulos > 0:
                logger.debug(
                    "Coluna '%s': %d nulo(s) preenchido(s) com '%s'.",
                    coluna, nulos, valor_padrao,
                )

    if cbo_lookup is not None:
        resultado["DESCRICAO_CBO"] = (
            resultado["CBO"].map(cbo_lookup).fillna("CBO NAO CATALOGADO")
        )

    logger.info(
        "Transformação concluída. Entrada: %d → Saída: %d registro(s).",
        len(df), len(resultado),
    )
    return resultado
