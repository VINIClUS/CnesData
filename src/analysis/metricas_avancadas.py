"""Métricas avançadas para o pipeline de auditoria CNES."""
import pandas as pd


def calcular_taxa_anomalia(df_vinculos: pd.DataFrame, df_glosas: pd.DataFrame) -> float:
    """Proporção de profissionais únicos com glosa sobre total de vínculos.
    Args:
        df_vinculos: DataFrame processado (CPF e CNS como identificadores).
        df_glosas: DataFrame de glosas (colunas cpf e cns).
    Returns:
        float entre 0 e 1, ou 0.0 se df_vinculos vazio.
    """
    if df_vinculos.empty:
        return 0.0
    chave = df_glosas["cpf"].fillna(df_glosas["cns"])
    return chave.dropna().nunique() / len(df_vinculos)


def calcular_p90_ch(df_vinculos: pd.DataFrame) -> float:
    """Percentil 90 de CH_TOTAL nos vínculos processados.
    Args:
        df_vinculos: DataFrame com coluna CH_TOTAL.
    Returns:
        float P90 ou 0.0 se coluna ausente ou vazia.
    """
    if "CH_TOTAL" not in df_vinculos.columns or df_vinculos["CH_TOTAL"].dropna().empty:
        return 0.0
    return float(df_vinculos["CH_TOTAL"].quantile(0.90))


def calcular_proporcao_feminina(df_vinculos: pd.DataFrame) -> float:
    """Proporção de vínculos com SEXO=='F' sobre total com SEXO não-nulo.
    Args:
        df_vinculos: DataFrame com coluna SEXO.
    Returns:
        float entre 0 e 1, ou 0.0 se sem dados de SEXO.
    """
    if "SEXO" not in df_vinculos.columns:
        return 0.0
    validos = df_vinculos["SEXO"].dropna()
    if validos.empty:
        return 0.0
    return float((validos == "F").sum() / len(validos))


def calcular_proporcao_feminina_por_cnes(df_vinculos: pd.DataFrame) -> list[dict]:
    """Proporção feminina por estabelecimento.
    Args:
        df_vinculos: DataFrame com colunas CNES e SEXO.
    Returns:
        Lista de dicts {"cnes": str, "proporcao_f": float, "total": int}.
    """
    df = df_vinculos.dropna(subset=["SEXO"]).copy()
    resultado = []
    for cnes, grupo in df.groupby("CNES"):
        total = len(grupo)
        proporcao_f = float((grupo["SEXO"] == "F").sum() / total) if total else 0.0
        resultado.append({"cnes": str(cnes), "proporcao_f": proporcao_f, "total": total})
    return resultado


def calcular_top_glosas(df_glosas: pd.DataFrame, n: int = 10) -> list[dict]:
    """Top-N profissionais por volume de glosas.
    Args:
        df_glosas: DataFrame com colunas cpf/cns e nome_profissional.
        n: Número de profissionais a retornar.
    Returns:
        Lista de dicts {"cpf": str|None, "cns": str|None, "nome": str|None, "total": int}.
    """
    if df_glosas.empty:
        return []
    agrupado = (
        df_glosas.groupby(["cpf", "cns", "nome_profissional"], dropna=False)
        .size()
        .reset_index(name="total")
        .sort_values("total", ascending=False)
        .head(n)
    )
    return [
        {
            "cpf": row["cpf"] if pd.notna(row["cpf"]) else None,
            "cns": row["cns"] if pd.notna(row["cns"]) else None,
            "nome": row["nome_profissional"] if pd.notna(row["nome_profissional"]) else None,
            "total": int(row["total"]),
        }
        for _, row in agrupado.iterrows()
    ]


def calcular_anomalias_por_cbo(
    df_vinculos: pd.DataFrame,
    df_glosas: pd.DataFrame,
    cbo_lookup: dict[str, str],
) -> list[dict]:
    """Anomalias por CBO com taxa relativa ao total de vínculos daquele CBO.
    Args:
        df_vinculos: DataFrame processado com coluna CBO.
        df_glosas: DataFrame de glosas com coluna cnes_estabelecimento (para join via CNES).
        cbo_lookup: dict CBO→descrição.
    Returns:
        Lista de dicts {"cbo": str, "descricao": str, "total": int, "taxa": float}.
    """
    chave_glosas = df_glosas["cpf"].fillna(df_glosas["cns"]).dropna().unique()
    chave_vinculos = df_vinculos["CPF"].fillna(df_vinculos["CNS"])
    df = df_vinculos.copy()
    df["_chave"] = chave_vinculos
    df["_com_anomalia"] = df["_chave"].isin(chave_glosas)

    resultado = []
    for cbo, grupo in df.groupby("CBO"):
        total_anomalias = int(grupo["_com_anomalia"].sum())
        total_vinculos = len(grupo)
        taxa = total_anomalias / total_vinculos if total_vinculos else 0.0
        resultado.append({
            "cbo": str(cbo),
            "descricao": cbo_lookup.get(str(cbo), ""),
            "total": total_anomalias,
            "taxa": float(taxa),
        })
    return resultado


def calcular_ranking_cnes(
    df_estab_local: pd.DataFrame,
    df_glosas: pd.DataFrame,
    df_vinculos: pd.DataFrame,
) -> list[dict]:
    """Ranking de estabelecimentos por índice de conformidade.
    Args:
        df_estab_local: DataFrame com CNES e NOME_FANTASIA.
        df_glosas: DataFrame de glosas com cnes_estabelecimento.
        df_vinculos: DataFrame processado com coluna CNES.
    Returns:
        Lista de dicts {"cnes": str, "nome": str|None, "total_anomalias": int,
        "indice_conformidade": float}.
    """
    anomalias = (
        df_glosas.groupby("cnes_estabelecimento").size().reset_index(name="total_anomalias")
    )
    anomalias = anomalias.rename(columns={"cnes_estabelecimento": "CNES"})
    vinculos_cnt = df_vinculos.groupby("CNES").size().reset_index(name="total_vinculos")

    merged = vinculos_cnt.merge(anomalias, on="CNES", how="left")
    merged["total_anomalias"] = merged["total_anomalias"].fillna(0).astype(int)

    nomes = df_estab_local[["CNES", "NOME_FANTASIA"]].drop_duplicates("CNES")
    merged = merged.merge(nomes, on="CNES", how="left")

    resultado = []
    for _, row in merged.iterrows():
        anomalias_cnes = row["total_anomalias"]
        vinculos_cnes = row["total_vinculos"]
        indice = max(0.0, 1.0 - anomalias_cnes / vinculos_cnes) if vinculos_cnes else 0.0
        nome = row["NOME_FANTASIA"] if pd.notna(row.get("NOME_FANTASIA")) else None
        resultado.append({
            "cnes": str(row["CNES"]),
            "nome": nome,
            "total_anomalias": int(anomalias_cnes),
            "indice_conformidade": float(indice),
        })
    return resultado


def _chave_profissional(df: pd.DataFrame) -> pd.Series:
    return df["cpf"].fillna(df["cns"])


def calcular_reincidencia(df_glosas_historico: pd.DataFrame) -> int:
    """Profissionais com mesma (cpf/cns + regra) em >= 2 competências distintas.
    Args:
        df_glosas_historico: DataFrame de glosas históricas com colunas
            competencia, regra, cpf, cns.
    Returns:
        Contagem de profissionais reincidentes.
    """
    df = df_glosas_historico.copy()
    df["_chave"] = _chave_profissional(df)
    contagem = (
        df.groupby(["_chave", "regra"])["competencia"]
        .nunique()
    )
    return int((contagem >= 2).sum())


def calcular_taxa_resolucao(
    comp_anterior: str,
    comp_atual: str,
    df_glosas_historico: pd.DataFrame,
) -> float:
    """Proporção de glosas da competência anterior ausentes na atual.
    Args:
        comp_anterior: Competência anterior no formato 'YYYY-MM'.
        comp_atual: Competência atual no formato 'YYYY-MM'.
        df_glosas_historico: DataFrame histórico com colunas competencia, regra, cpf, cns.
    Returns:
        float entre 0 e 1. 0.0 se comp_anterior sem glosas.
    """
    df = df_glosas_historico.copy()
    df["_chave"] = _chave_profissional(df)

    def _pares(comp: str) -> set:
        sub = df[df["competencia"] == comp]
        return set(zip(sub["_chave"], sub["regra"]))

    anteriores = _pares(comp_anterior)
    if not anteriores:
        return 0.0
    atuais = _pares(comp_atual)
    resolvidas = anteriores - atuais
    return len(resolvidas) / len(anteriores)


def _meses_entre(c1: str, c2: str) -> int:
    y1, m1 = int(c1[:4]), int(c1[5:7])
    y2, m2 = int(c2[:4]), int(c2[5:7])
    return (y2 - y1) * 12 + (m2 - m1)


def calcular_velocidade_regularizacao(df_glosas_historico: pd.DataFrame) -> float:
    """Média de competências entre primeira e última ocorrência de glosas já resolvidas.
    Args:
        df_glosas_historico: DataFrame histórico com colunas competencia, regra, cpf, cns.
    Returns:
        float em número de competências (meses), ou 0.0 se não há glosas resolvidas.
    """
    df = df_glosas_historico.copy()
    df["_chave"] = _chave_profissional(df)
    max_comp = df["competencia"].max()

    durações = []
    grupos = df.groupby(["_chave", "regra"])["competencia"]
    for _, comps in grupos:
        comps_unicas = sorted(comps.unique())
        if len(comps_unicas) < 2:
            continue
        ultima = comps_unicas[-1]
        if ultima == max_comp:
            continue
        primeira = comps_unicas[0]
        durações.append(_meses_entre(primeira, ultima))

    return float(sum(durações) / len(durações)) if durações else 0.0
