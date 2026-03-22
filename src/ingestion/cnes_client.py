"""
cnes_client.py — Camada de Ingestão: Cliente do Banco CNES Firebird

Responsabilidade: carregar o driver Firebird, abrir a conexão e extrair
os dados brutos de profissionais vinculados ao CNPJ da mantenedora.

Por que cursor manual em vez de pd.read_sql()?
  O driver `fdb` emite erro -501 ("Invalid cursor state") quando
  pd.read_sql() é usado com LEFT JOINs encadeados. O cursor manual
  contorna isso ao iterar cursor.fetchall() e construir o DataFrame
  com os nomes vindos de cursor.description.

Por que 3 queries separadas em vez de um único SELECT com JOINs?
  O Firebird 2.5 embedado possui dois bugs que inviabilizam o JOIN direto
  LFCES048 → LFCES060:
  1. LFCES060.COD_MUN difere de LFCES048.COD_MUN para as mesmas equipes
     (LFCES060 armazena o município de origem do cadastro nacional, não o
     município gestor local).
  2. LFCES060.SEQ_EQUIPE é um código nacional de 6-7 dígitos cujos primeiros
     4 caracteres correspondem ao LFCES048.SEQ_EQUIPE (código local de 4 dígitos).
     Não há função de substring disponível neste Firebird, impossibilitando o
     JOIN por prefixo em SQL.
  Solução H2: queries separadas + merge Python via str[:4].

Por que TP_SUS_NAO_SUS e não TP_SUS_NAO?
  A Query Master foi validada com 367 registros contra o banco real
  usando TP_SUS_NAO_SUS. A tabela de colunas do dicionário registra
  TP_SUS_NAO (inferido antes da validação) e está desatualizada nesse campo.

Por que TP_EQUIPE e não DS_SEGMENT / DS_SEGMENTO?
  DS_SEGMENT retorna erro -206 (Column unknown) quando acessado via alias
  em LEFT JOIN aninhado no Firebird 2.5. A Query Master usa TP_EQUIPE
  (código numérico do tipo de equipe) conforme nota do data_dictionary.md.
"""

import logging
from pathlib import Path

import fdb
import pandas as pd

import config

logger = logging.getLogger(__name__)

_SQL_VINCULOS: str = """
    SELECT
        prof.CPF_PROF            AS CPF,
        prof.COD_CNS             AS CNS,
        prof.NOME_PROF           AS NOME_PROFISSIONAL,
        prof.NO_SOCIAL           AS NOME_SOCIAL,
        prof.SEXO                AS SEXO,
        prof.DATA_NASC           AS DATA_NASCIMENTO,
        vinc.COD_CBO             AS CBO,
        vinc.IND_VINC            AS COD_VINCULO,
        vinc.TP_SUS_NAO_SUS      AS SUS_NAO_SUS,
        (COALESCE(vinc.CG_HORAAMB,  0)
         + COALESCE(vinc.CGHORAOUTR, 0)
         + COALESCE(vinc.CGHORAHOSP, 0)) AS CARGA_HORARIA_TOTAL,
        COALESCE(vinc.CG_HORAAMB,  0)    AS CH_AMBULATORIAL,
        COALESCE(vinc.CGHORAOUTR,  0)    AS CH_OUTRAS,
        COALESCE(vinc.CGHORAHOSP,  0)    AS CH_HOSPITALAR,
        est.CNES                 AS COD_CNES,
        est.NOME_FANTA           AS ESTABELECIMENTO,
        est.TP_UNID_ID           AS COD_TIPO_UNIDADE,
        est.CODMUNGEST           AS COD_MUN_GESTOR
    FROM       LFCES021 vinc
    INNER JOIN LFCES004 est  ON  est.UNIDADE_ID = vinc.UNIDADE_ID
    INNER JOIN LFCES018 prof ON  prof.PROF_ID   = vinc.PROF_ID
    WHERE est.CODMUNGEST = '{cod_mun}'
      AND est.CNPJ_MANT  = '{cnpj}'
    ORDER BY prof.NOME_PROF, vinc.COD_CBO
"""

# Membros de equipe: chave de ligação CPF+CBO → SEQ_EQUIPE (código local 4 dígitos)
_SQL_MEMBROS: str = """
    SELECT
        me.CPF_PROF   AS CPF,
        me.COD_CBO    AS CBO,
        me.SEQ_EQUIPE AS SEQ_EQUIPE
    FROM LFCES048 me
    WHERE me.COD_MUN    = '{cod_mun}'
      AND me.SEQ_EQUIPE IS NOT NULL
"""

# Registro de equipes: código nacional 6-7 dígitos — primeiros 4 = SEQ de LFCES048
_SQL_EQUIPES: str = """
    SELECT
        eq.SEQ_EQUIPE AS SEQ_EQUIPE,
        eq.INE        AS COD_INE_EQUIPE,
        eq.DS_AREA    AS NOME_EQUIPE,
        eq.TP_EQUIPE  AS COD_TIPO_EQUIPE
    FROM LFCES060 eq
    WHERE eq.COD_MUN = '{cod_mun}'
"""

COLUNAS_ESPERADAS: tuple[str, ...] = (
    "CPF", "CNS", "NOME_PROFISSIONAL", "NOME_SOCIAL", "SEXO", "DATA_NASCIMENTO",
    "CBO", "COD_VINCULO", "SUS_NAO_SUS",
    "CARGA_HORARIA_TOTAL", "CH_AMBULATORIAL", "CH_OUTRAS", "CH_HOSPITALAR",
    "COD_CNES", "ESTABELECIMENTO", "COD_TIPO_UNIDADE", "COD_MUN_GESTOR",
    "COD_INE_EQUIPE", "NOME_EQUIPE", "COD_TIPO_EQUIPE",
)


def carregar_driver(dll_path: Path) -> None:
    """
    Carrega a DLL do Firebird 64-bits no processo Python.

    Deve ser chamado antes de qualquer fdb.connect(). Chamadas repetidas
    são seguras — o fdb verifica internamente se a API já foi carregada.

    Args:
        dll_path: Caminho absoluto para a fbembed.dll ou fbclient.dll.

    Raises:
        FileNotFoundError: Se a DLL não existir no caminho informado.
    """
    if not dll_path.exists():
        raise FileNotFoundError(
            f"DLL do Firebird não encontrada em: {dll_path}\n"
            "Verifique a variável FIREBIRD_DLL no arquivo .env"
        )
    logger.debug("Carregando driver Firebird de: %s", dll_path)
    fdb.load_api(str(dll_path))
    logger.info("Driver Firebird (64-bits) carregado com sucesso.")


def conectar() -> fdb.Connection:
    """
    Abre e retorna uma conexão ativa com o banco CNES Firebird.

    A conexão deve ser fechada pelo chamador, preferencialmente em um
    bloco `finally` para garantir o fechamento mesmo em caso de exceção.

    Returns:
        fdb.Connection: Conexão ativa com o banco.

    Raises:
        FileNotFoundError: Se a DLL do Firebird não for encontrada.
        fdb.fbcore.DatabaseError: Se a conexão com o banco falhar
            (banco offline, credenciais incorretas, DSN inválido).
    """
    carregar_driver(Path(config.FIREBIRD_DLL))

    logger.debug("db_host=%s", config.DB_HOST)
    con: fdb.Connection = fdb.connect(
        dsn=config.DB_DSN,
        user=config.DB_USER,
        password=config.DB_PASSWORD,
    )
    logger.info("Conexão estabelecida com o banco CNES.")
    return con


def extrair_profissionais(con: fdb.Connection) -> pd.DataFrame:
    """
    Extrai os vínculos profissional↔estabelecimento com dados de equipe.

    Executa 3 queries separadas e faz o enriquecimento via merge Python:
      1. Vínculos base (LFCES021 + LFCES004 + LFCES018)
      2. Membros de equipe (LFCES048)
      3. Registro de equipes (LFCES060)

    Args:
        con: Conexão ativa com o banco Firebird (aberta, não fechada).

    Returns:
        pd.DataFrame: DataFrame com 20 colunas (ver COLUNAS_ESPERADAS).

    Raises:
        ValueError: Se a query de vínculos não retornar nenhum registro.
        fdb.fbcore.DatabaseError: Se ocorrer erro de SQL no banco.
    """
    logger.info(
        "Executando extração para município IBGE=%s, CNPJ=%s ...",
        config.COD_MUN_IBGE,
        config.CNPJ_MANTENEDORA,
    )

    df_vinculos = _executar_query(
        con,
        _SQL_VINCULOS.format(cod_mun=config.COD_MUN_IBGE, cnpj=config.CNPJ_MANTENEDORA),
    )

    if df_vinculos.empty:
        raise ValueError(
            "A query não retornou dados. "
            f"Verifique os filtros: COD_MUN_IBGE={config.COD_MUN_IBGE}, "
            f"CNPJ={config.CNPJ_MANTENEDORA}."
        )

    df_membros = _executar_query(
        con,
        _SQL_MEMBROS.format(cod_mun=config.COD_MUN_IBGE),
    )
    df_equipes = _executar_query(
        con,
        _SQL_EQUIPES.format(cod_mun=config.COD_MUN_IBGE),
    )

    df = _enriquecer_com_equipe(df_vinculos, df_membros, df_equipes)
    logger.info("Extração concluída. Total de vínculos: %d", len(df))
    return df


def _executar_query(con: fdb.Connection, sql: str) -> pd.DataFrame:
    cur = con.cursor()
    try:
        cur.execute(sql)
        linhas = cur.fetchall()
        colunas: list[str] = [d[0] for d in cur.description]
    finally:
        cur.close()
    return pd.DataFrame(linhas, columns=colunas)


def _enriquecer_com_equipe(
    df_vinculos: pd.DataFrame,
    df_membros: pd.DataFrame,
    df_equipes: pd.DataFrame,
) -> pd.DataFrame:
    # LFCES060.SEQ_EQUIPE é um código nacional de 6-7 dígitos;
    # os primeiros 4 caracteres equivalem ao LFCES048.SEQ_EQUIPE (código local).
    df_eq = df_equipes.copy()
    df_eq["SEQ_BASE"] = df_eq["SEQ_EQUIPE"].astype(str).str[:4].astype(int)

    df_mem = df_membros.copy()
    df_mem["CPF"] = df_mem["CPF"].str.strip()
    df_mem["CBO"] = df_mem["CBO"].str.strip()

    df_mem_eq = df_mem.merge(
        df_eq[["SEQ_BASE", "COD_INE_EQUIPE", "NOME_EQUIPE", "COD_TIPO_EQUIPE"]],
        left_on="SEQ_EQUIPE",
        right_on="SEQ_BASE",
        how="left",
    ).drop(columns=["SEQ_EQUIPE", "SEQ_BASE"], errors="ignore")

    df_mem_eq = df_mem_eq.drop_duplicates(subset=["CPF", "CBO"])

    df_base = df_vinculos.copy()
    df_base["CPF"] = df_base["CPF"].str.strip()
    df_base["CBO"] = df_base["CBO"].str.strip()

    return df_base.merge(
        df_mem_eq[["CPF", "CBO", "COD_INE_EQUIPE", "NOME_EQUIPE", "COD_TIPO_EQUIPE"]],
        on=["CPF", "CBO"],
        how="left",
    )
