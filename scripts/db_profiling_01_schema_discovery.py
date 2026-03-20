"""
db_profiling_01_schema_discovery.py
====================================
Script de Profiling — Iteração 1: Descoberta Completa do Schema

OBJETIVO:
  Mapear TODAS as tabelas e colunas do banco CNES Firebird consultando
  as tabelas de sistema (catálogo) do Firebird (`RDB$`).
  Também extrai índices e constraints para inferir PKs e FKs implícitas.

COMO EXECUTAR:
  No terminal, com o ambiente virtual ativado:
  > cd c:\\Users\\CPD\\Projetos\\CnesData
  > python scripts/db_profiling_01_schema_discovery.py

SAÍDA:
  - Console: resumo com contagem de tabelas/colunas
  - data/discovery/01_todas_as_tabelas.csv
  - data/discovery/01_todas_as_colunas.csv
  - data/discovery/01_indices_e_constraints.csv
  - data/discovery/01_tabelas_foco_profissional.csv  ← as mais relevantes
"""

import logging
import sys
import warnings
from pathlib import Path

import fdb
import pandas as pd

# ── Setup de Caminhos ──────────────────────────────────────────────────────
# Adiciona 'src/' ao path para importar config.py.
RAIZ = Path(__file__).parent.parent
sys.path.insert(0, str(RAIZ / "src"))

import config  # noqa: E402

# ── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)-7s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("profiling.01")

# ── Diretório de Saída ─────────────────────────────────────────────────────
OUTPUT_DIR = RAIZ / "data" / "discovery"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ─────────────────────────────────────────────────────────────────────────────
# CONEXÃO
# ─────────────────────────────────────────────────────────────────────────────
def conectar() -> fdb.Connection:
    """Carrega o driver e conecta ao banco CNES."""
    dll_path = Path(config.FIREBIRD_DLL)
    if not dll_path.exists():
        raise FileNotFoundError(f"DLL não encontrada: {dll_path}")
    fdb.load_api(str(dll_path))
    logger.info("Driver Firebird carregado. Conectando a: %s", config.DB_DSN)
    con = fdb.connect(
        dsn=config.DB_DSN,
        user=config.DB_USER,
        password=config.DB_PASSWORD,
    )
    logger.info("Conexão estabelecida.")
    return con


# ─────────────────────────────────────────────────────────────────────────────
# QUERY 1 — Inventário de Tabelas
# ─────────────────────────────────────────────────────────────────────────────
SQL_TABELAS = """
SELECT
    r.RDB$RELATION_NAME                     AS TABELA,
    r.RDB$DESCRIPTION                       AS DESCRICAO,
    COUNT(f.RDB$FIELD_NAME)                 AS QTD_COLUNAS
FROM
    RDB$RELATIONS r
    LEFT JOIN RDB$RELATION_FIELDS f
        ON f.RDB$RELATION_NAME = r.RDB$RELATION_NAME
WHERE
    r.RDB$SYSTEM_FLAG = 0          -- exclui tabelas de sistema do Firebird
    AND r.RDB$VIEW_BLR IS NULL     -- exclui views
GROUP BY
    r.RDB$RELATION_NAME, r.RDB$DESCRIPTION
ORDER BY
    r.RDB$RELATION_NAME
"""


# ─────────────────────────────────────────────────────────────────────────────
# QUERY 2 — Inventário Detalhado de Colunas
# ─────────────────────────────────────────────────────────────────────────────
SQL_COLUNAS = """
SELECT
    f.RDB$RELATION_NAME                     AS TABELA,
    f.RDB$FIELD_NAME                        AS COLUNA,
    f.RDB$FIELD_POSITION                    AS POSICAO,
    tp.RDB$TYPE_NAME                        AS TIPO_DADO,
    fld.RDB$FIELD_LENGTH                    AS COMPRIMENTO,
    fld.RDB$FIELD_PRECISION                 AS PRECISAO,
    fld.RDB$FIELD_SCALE                     AS ESCALA,
    f.RDB$NULL_FLAG                         AS OBRIGATORIO,
    f.RDB$DEFAULT_SOURCE                    AS VALOR_PADRAO,
    f.RDB$DESCRIPTION                       AS DESCRICAO_COLUNA
FROM
    RDB$RELATION_FIELDS f
    JOIN RDB$RELATIONS r
        ON r.RDB$RELATION_NAME = f.RDB$RELATION_NAME
    JOIN RDB$FIELDS fld
        ON fld.RDB$FIELD_NAME  = f.RDB$FIELD_SOURCE
    LEFT JOIN RDB$TYPES tp
        ON  tp.RDB$TYPE      = fld.RDB$FIELD_TYPE
        AND tp.RDB$FIELD_NAME = 'RDB$FIELD_TYPE'
WHERE
    r.RDB$SYSTEM_FLAG = 0
    AND r.RDB$VIEW_BLR IS NULL
ORDER BY
    f.RDB$RELATION_NAME,
    f.RDB$FIELD_POSITION
"""


# ─────────────────────────────────────────────────────────────────────────────
# QUERY 3 — Índices e Constraints (Inferência de PKs/FKs)
# ─────────────────────────────────────────────────────────────────────────────
SQL_INDICES = """
SELECT
    idx.RDB$RELATION_NAME                   AS TABELA,
    idx.RDB$INDEX_NAME                      AS NOME_INDICE,
    idx.RDB$UNIQUE_FLAG                     AS UNICO,
    seg.RDB$FIELD_NAME                      AS COLUNA,
    seg.RDB$FIELD_POSITION                  AS POSICAO_NO_INDICE,
    con.RDB$CONSTRAINT_TYPE                 AS TIPO_CONSTRAINT
FROM
    RDB$INDICES idx
    JOIN RDB$INDEX_SEGMENTS seg
        ON seg.RDB$INDEX_NAME = idx.RDB$INDEX_NAME
    LEFT JOIN RDB$RELATION_CONSTRAINTS con
        ON  con.RDB$INDEX_NAME    = idx.RDB$INDEX_NAME
        AND con.RDB$RELATION_NAME = idx.RDB$RELATION_NAME
    JOIN RDB$RELATIONS r
        ON r.RDB$RELATION_NAME = idx.RDB$RELATION_NAME
WHERE
    r.RDB$SYSTEM_FLAG = 0
ORDER BY
    idx.RDB$RELATION_NAME,
    idx.RDB$INDEX_NAME,
    seg.RDB$FIELD_POSITION
"""


# ─────────────────────────────────────────────────────────────────────────────
# QUERY 4 — Amostra das Tabelas de Foco (Profissional ↔ Equipe ↔ Lotação)
# ─────────────────────────────────────────────────────────────────────────────
TABELAS_FOCO = [
    "LFCES018",  # Profissionais
    "LFCES021",  # Vínculos Prof → Estabelecimento
    "LFCES004",  # Estabelecimentos
    "LFCES048",  # Membros de Equipe
    "LFCES060",  # Equipes
]


def amostrar_tabela(
    con: fdb.Connection, tabela: str, limite: int = 5
) -> pd.DataFrame:
    """
    Retorna as primeiras `limite` linhas de uma tabela para inspeção visual.

    Args:
        con: Conexão ativa Firebird.
        tabela: Nome da tabela a amostrar.
        limite: Quantidade de linhas a retornar.

    Returns:
        DataFrame com a amostra.
    """
    sql = f"SELECT FIRST {limite} * FROM {tabela}"
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = pd.read_sql(sql, con)
        logger.debug("  Tabela %s — amostra com %d linhas, %d colunas.", tabela, len(df), len(df.columns))
        return df
    except Exception as exc:
        logger.error("  ERRO ao amostrar %s: %s", tabela, exc)
        return pd.DataFrame()


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main() -> None:
    """Executa o profiling completo e salva os resultados em CSV."""
    con = None
    try:
        con = conectar()

        # ── 1. Inventário de Tabelas ─────────────────────────────────────
        logger.info("=" * 60)
        logger.info("ETAPA 1 — Inventário de Tabelas do Banco")
        logger.info("=" * 60)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df_tabelas = pd.read_sql(SQL_TABELAS, con)

        df_tabelas.columns = [c.strip() for c in df_tabelas.columns]
        logger.info("Total de tabelas encontradas: %d", len(df_tabelas))

        caminho_tabelas = OUTPUT_DIR / "01_todas_as_tabelas.csv"
        df_tabelas.to_csv(caminho_tabelas, index=False, sep=";", encoding="utf-8-sig")
        logger.info("Salvo: %s", caminho_tabelas)

        # Exibe no console as tabelas que têm 'LFCES' ou 'NFCES' no nome
        tabelas_cnes = df_tabelas[
            df_tabelas["TABELA"].str.strip().str.startswith(("LFCES", "NFCES"))
        ].copy()
        logger.info("\n>>> Tabelas com prefixo LFCES/NFCES (%d tabelas):", len(tabelas_cnes))
        print(tabelas_cnes.to_string(index=False))

        # ── 2. Inventário de Colunas ─────────────────────────────────────
        logger.info("\n" + "=" * 60)
        logger.info("ETAPA 2 — Inventário de Colunas")
        logger.info("=" * 60)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df_colunas = pd.read_sql(SQL_COLUNAS, con)

        df_colunas.columns = [c.strip() for c in df_colunas.columns]
        logger.info("Total de colunas mapeadas: %d", len(df_colunas))

        caminho_colunas = OUTPUT_DIR / "01_todas_as_colunas.csv"
        df_colunas.to_csv(caminho_colunas, index=False, sep=";", encoding="utf-8-sig")
        logger.info("Salvo: %s", caminho_colunas)

        # Filtra só as tabelas de foco
        df_colunas_foco = df_colunas[
            df_colunas["TABELA"].str.strip().isin(TABELAS_FOCO)
        ]
        logger.info("\n>>> Colunas das tabelas de foco:")
        print(df_colunas_foco[["TABELA", "COLUNA", "TIPO_DADO", "COMPRIMENTO", "OBRIGATORIO"]].to_string(index=False))

        # ── 3. Índices e Constraints ─────────────────────────────────────
        logger.info("\n" + "=" * 60)
        logger.info("ETAPA 3 — Índices e Constraints (inferência de PKs/FKs)")
        logger.info("=" * 60)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df_indices = pd.read_sql(SQL_INDICES, con)

        df_indices.columns = [c.strip() for c in df_indices.columns]

        caminho_indices = OUTPUT_DIR / "01_indices_e_constraints.csv"
        df_indices.to_csv(caminho_indices, index=False, sep=";", encoding="utf-8-sig")
        logger.info("Salvo: %s", caminho_indices)

        # Exibe apenas constraints de PK e FK das tabelas de foco
        df_idx_foco = df_indices[
            df_indices["TABELA"].str.strip().isin(TABELAS_FOCO)
        ]
        logger.info("\n>>> Índices das tabelas de foco:")
        print(df_idx_foco[["TABELA", "NOME_INDICE", "UNICO", "COLUNA", "TIPO_CONSTRAINT"]].to_string(index=False))

        # ── 4. Amostragem das Tabelas de Foco ───────────────────────────
        logger.info("\n" + "=" * 60)
        logger.info("ETAPA 4 — Amostragem de Dados (5 linhas por tabela)")
        logger.info("=" * 60)
        for tabela in TABELAS_FOCO:
            logger.info("\n>>> Tabela: %s", tabela)
            df_amostra = amostrar_tabela(con, tabela, limite=5)
            if not df_amostra.empty:
                print(df_amostra.to_string(index=False))
                caminho_amostra = OUTPUT_DIR / f"01_amostra_{tabela}.csv"
                df_amostra.to_csv(caminho_amostra, index=False, sep=";", encoding="utf-8-sig")

        logger.info("\n" + "=" * 60)
        logger.info("PROFILING 01 CONCLUÍDO. Arquivos em: %s", OUTPUT_DIR)
        logger.info("=" * 60)

    finally:
        if con is not None:
            con.close()
            logger.info("Conexão encerrada.")


if __name__ == "__main__":
    main()
