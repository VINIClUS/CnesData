"""Funções puras de filtragem e mascaramento PII para a página Glosas."""
import pandas as pd


def _filtrar_glosas(df: pd.DataFrame, regras: list[str], busca: str) -> pd.DataFrame:
    """Filtra glosas por regra e busca de texto.

    Args:
        df: DataFrame com colunas regra, nome_profissional, cpf.
        regras: Lista de regras (ex. ["RQ008"]). Lista vazia = sem filtro.
        busca: String para busca case-insensitive em nome_profissional e cpf.

    Returns:
        DataFrame filtrado (cópia).
    """
    result = df.copy()
    if regras:
        result = result[result["regra"].isin(regras)]
    if busca:
        termo = busca.lower()
        mask = (
            result["nome_profissional"].str.lower().str.contains(termo, na=False)
            | result["cpf"].str.contains(termo, na=False)
        )
        result = result[mask]
    return result


def _mascarar_pii_glosas(df: pd.DataFrame, mostrar_completo: bool) -> pd.DataFrame:
    """Mascara CPF e CNS com últimos 4 dígitos visíveis.

    Args:
        df: DataFrame com colunas cpf e/ou cns.
        mostrar_completo: True = preserva valores originais.

    Returns:
        DataFrame com mascaramento aplicado (cópia).
    """
    if mostrar_completo:
        return df.copy()
    result = df.copy()
    for col in ("cpf", "cns"):
        if col in result.columns:
            result[col] = result[col].apply(
                lambda v: f"***{str(v)[-4:]}" if isinstance(v, str) and len(v) >= 4 else v
            )
    return result
