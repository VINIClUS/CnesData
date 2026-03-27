"""Script pré-processador de higienização de RH — crosswalk PIS→CPF via LFCES018."""

import logging
import re
import sys
import unicodedata
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import config  # noqa: E402
from ingestion.cnes_client import conectar  # noqa: E402

logger = logging.getLogger(__name__)

_COLUNAS_SAIDA = ["CPF", "NOME", "STATUS"]
_COLUNAS_FIREBIRD = ["PISPASEP", "CPF_PROF", "NOME_PROF"]

_SQL_PISPASEP = """
    SELECT PISPASEP, CPF_PROF, NOME_PROF
    FROM LFCES018
    WHERE PISPASEP IS NOT NULL
"""


def _normalizar_nome(nome: str) -> str:
    s = unicodedata.normalize("NFKD", str(nome)).encode("ascii", "ignore").decode()
    return re.sub(r"\s+", " ", s.upper().strip())


def carregar_csv_rh(caminho: Path) -> pd.DataFrame:
    """
    Lê CSV sujo do RH e normaliza a coluna PIS.

    Args:
        caminho: Caminho para o arquivo CSV com encoding latin-1.

    Returns:
        DataFrame com PIS como string de 11 dígitos (zfill).

    Raises:
        ValueError: Se 'PIS' ou 'Nome' estiverem ausentes.
    """
    df = pd.read_csv(caminho, encoding="latin-1", sep=None, engine="python", dtype=str)
    df.columns = [c.strip() for c in df.columns]

    ausentes = [c for c in ("PIS", "Nome") if c not in df.columns]
    if ausentes:
        raise ValueError(f"Colunas obrigatórias ausentes no CSV: {ausentes}")

    df["PIS"] = df["PIS"].str.strip().str.zfill(11)
    return df


def consultar_pispasep_firebird(con) -> pd.DataFrame:
    """
    Busca PISPASEP, CPF_PROF e NOME_PROF de LFCES018.

    Args:
        con: Conexão fdb ativa.

    Returns:
        DataFrame filtrado para PISPASEP não-vazio.
    """
    cur = con.cursor()
    try:
        cur.execute(_SQL_PISPASEP)
        linhas = cur.fetchall()
        colunas = [d[0] for d in cur.description]
    finally:
        cur.close()

    df = pd.DataFrame(linhas, columns=colunas)
    return df[df["PISPASEP"].str.strip() != ""].copy()


def crosswalk_pis_cpf(df_rh: pd.DataFrame, df_firebird: pd.DataFrame) -> pd.DataFrame:
    """
    Cruza funcionários do RH com LFCES018 para recuperar CPF.

    Estratégia em dois passos:
      1. PIS → PISPASEP (alta confiança)
      2. Nome normalizado → NOME_PROF (fallback — sem duplicatas no Firebird)

    Args:
        df_rh: DataFrame do HR com colunas PIS e Nome.
        df_firebird: DataFrame de LFCES018 com PISPASEP, CPF_PROF, NOME_PROF.

    Returns:
        DataFrame com CPF, NOME, STATUS, ORIGEM_MATCH.
    """
    if df_firebird.empty:
        return pd.DataFrame(columns=[*_COLUNAS_SAIDA, "ORIGEM_MATCH"])

    pis_map = dict(zip(df_firebird["PISPASEP"].str.strip(), df_firebird["CPF_PROF"].str.strip()))
    nome_cpf_map = {
        _normalizar_nome(r["NOME_PROF"]): r["CPF_PROF"].strip()
        for _, r in df_firebird.iterrows()
    }
    nome_prof_map = {
        _normalizar_nome(r["NOME_PROF"]): r["NOME_PROF"].strip()
        for _, r in df_firebird.iterrows()
    }

    resultados = []
    for _, row in df_rh.iterrows():
        pis = str(row["PIS"]).strip()
        nome_norm = _normalizar_nome(str(row["Nome"]))

        if pis in pis_map:
            resultados.append({
                "CPF": pis_map[pis],
                "NOME": str(row["Nome"]).strip(),
                "STATUS": "ATIVO",
                "ORIGEM_MATCH": "PIS",
            })
        elif nome_norm in nome_cpf_map:
            resultados.append({
                "CPF": nome_cpf_map[nome_norm],
                "NOME": nome_prof_map[nome_norm],
                "STATUS": "ATIVO",
                "ORIGEM_MATCH": "NOME",
            })
        else:
            logger.warning("sem_match pis=%s nome=%s", pis, row["Nome"])

    return pd.DataFrame(resultados, columns=[*_COLUNAS_SAIDA, "ORIGEM_MATCH"])


def salvar_hr_padronizado(df: pd.DataFrame, caminho: Path) -> None:
    """
    Grava hr_padronizado.csv com apenas as colunas [CPF, NOME, STATUS].

    Args:
        df: DataFrame com ao menos CPF, NOME, STATUS (ORIGEM_MATCH ignorado).
        caminho: Destino do arquivo CSV.
    """
    caminho.parent.mkdir(parents=True, exist_ok=True)
    df[_COLUNAS_SAIDA].to_csv(caminho, sep=";", index=False, encoding="utf-8-sig")
    logger.info("hr_padronizado gravado path=%s linhas=%d", caminho, len(df))


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    caminho_csv = Path(sys.argv[1]) if len(sys.argv) > 1 else None
    if caminho_csv is None:
        logger.error("uso=hr_pre_processor.py <caminho_csv_rh>")
        sys.exit(1)

    caminho_saida = config.OUTPUT_PATH.parent / "hr_padronizado.csv"

    con = conectar()
    try:
        df_rh = carregar_csv_rh(caminho_csv)
        df_firebird = consultar_pispasep_firebird(con)
        df_resultado = crosswalk_pis_cpf(df_rh, df_firebird)

        via_pis = (df_resultado["ORIGEM_MATCH"] == "PIS").sum()
        via_nome = (df_resultado["ORIGEM_MATCH"] == "NOME").sum()
        logger.info(
            "crosswalk total=%d/%d via_pis=%d via_nome=%d nao_encontrado=%d",
            len(df_resultado), len(df_rh), via_pis, via_nome,
            len(df_rh) - len(df_resultado),
        )

        salvar_hr_padronizado(df_resultado, caminho_saida)
    finally:
        con.close()


if __name__ == "__main__":
    main()
