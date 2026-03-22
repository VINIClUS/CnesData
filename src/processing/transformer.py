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
        cpfs_invalidos = df.loc[mascara_invalido, "CPF"].tolist()
        logger.warning(
            "RQ-002: %d registro(s) excluído(s) por CPF inválido: %s",
            total_invalidos,
            cpfs_invalidos,
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
    df_out = df.copy()
    df_out["ALERTA_STATUS_CH"] = df_out["CH_TOTAL"].apply(
        lambda ch: ALERTA_ATIVO_SEM_CH if ch == 0 else ALERTA_CH_OK
    )

    total_zumbis: int = int((df_out["ALERTA_STATUS_CH"] == ALERTA_ATIVO_SEM_CH).sum())
    if total_zumbis > 0:
        logger.warning(
            "RQ-003: %d vínculo(s) com carga horária zero (ATIVO_SEM_CH).",
            total_zumbis,
        )
    else:
        logger.debug("RQ-003: nenhum vínculo zumbi encontrado.")

    return df_out


# ── Função Pública ────────────────────────────────────────────────────────────

def transformar(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica o pipeline completo de limpeza e validação ao DataFrame bruto.

    Pipeline (em ordem):
      1. Strip em colunas de texto (remove espaços que o Firebird CHAR padding gera).
      2. RQ-002: exclusão de registros com CPF nulo ou fora de 11 caracteres.
      3. RQ-003: adição da coluna ALERTA_STATUS_CH (flag de vínculo zumbi).
      4. Preenchimento de nulos nas colunas opcionais de equipe (LEFT JOIN).

    Args:
        df: DataFrame bruto retornado por cnes_client.extrair_profissionais().

    Returns:
        pd.DataFrame: DataFrame transformado. Inclui a coluna adicional
            ALERTA_STATUS_CH. Pode ter menos linhas que a entrada se houver
            CPFs inválidos (RQ-002).
    """
    logger.debug("Iniciando transformação. Registros de entrada: %d", len(df))
    resultado = df.copy()

    # ── Etapa 1: Limpeza de strings ───────────────────────────────────────────
    for coluna in _COLUNAS_TEXTO:
        if coluna in resultado.columns:
            resultado[coluna] = resultado[coluna].astype(str).str.strip()

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

    logger.info(
        "Transformação concluída. Entrada: %d → Saída: %d registro(s).",
        len(df), len(resultado),
    )
    return resultado
