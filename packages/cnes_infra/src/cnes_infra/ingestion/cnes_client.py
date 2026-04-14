"""Cliente do Banco CNES Firebird — extraccao via cursor manual."""

import logging
from collections.abc import Iterator
from pathlib import Path

import fdb
import polars as pl

from cnes_infra import config

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

_SQL_MEMBROS: str = """
    SELECT
        me.CPF_PROF   AS CPF,
        me.COD_CBO    AS CBO,
        me.SEQ_EQUIPE AS SEQ_EQUIPE
    FROM LFCES048 me
    WHERE me.COD_MUN    = '{cod_mun}'
      AND me.SEQ_EQUIPE IS NOT NULL
"""

_SQL_EQUIPES: str = """
    SELECT
        eq.SEQ_EQUIPE AS SEQ_EQUIPE,
        eq.INE        AS COD_INE_EQUIPE,
        eq.DS_AREA    AS NOME_EQUIPE,
        eq.TP_EQUIPE  AS COD_TIPO_EQUIPE
    FROM LFCES060 eq
    WHERE eq.COD_MUN = '{cod_mun}'
"""

_SQL_CBO_LOOKUP: str = """
    SELECT COD_CBO AS CBO, DESCRICAO AS DESCRICAO_CBO FROM NFCES026
"""

COLUNAS_ESPERADAS: tuple[str, ...] = (
    "CPF", "CNS", "NOME_PROFISSIONAL", "NOME_SOCIAL", "SEXO", "DATA_NASCIMENTO",
    "CBO", "COD_VINCULO", "SUS_NAO_SUS",
    "CARGA_HORARIA_TOTAL", "CH_AMBULATORIAL", "CH_OUTRAS", "CH_HOSPITALAR",
    "COD_CNES", "ESTABELECIMENTO", "COD_TIPO_UNIDADE", "COD_MUN_GESTOR",
    "COD_INE_EQUIPE", "NOME_EQUIPE", "COD_TIPO_EQUIPE",
)


def carregar_driver(dll_path: Path) -> None:
    """Carrega a DLL do Firebird 64-bits no processo Python.

    Args:
        dll_path: Caminho absoluto para fbembed.dll ou fbclient.dll.

    Raises:
        FileNotFoundError: Se a DLL nao existir no caminho informado.
    """
    if not dll_path.exists():
        raise FileNotFoundError(
            f"DLL do Firebird nao encontrada em: {dll_path}\n"
            "Verifique a variavel FIREBIRD_DLL no arquivo .env"
        )
    logger.debug("Carregando driver Firebird de: %s", dll_path)
    fdb.load_api(str(dll_path))
    logger.info("Driver Firebird (64-bits) carregado com sucesso.")


def conectar() -> fdb.Connection:
    """Abre e retorna uma conexao ativa com o banco CNES Firebird.

    Returns:
        fdb.Connection ativa com o banco.

    Raises:
        FileNotFoundError: Se a DLL do Firebird nao for encontrada.
        fdb.fbcore.DatabaseError: Se a conexao com o banco falhar.
    """
    carregar_driver(Path(config.FIREBIRD_DLL))

    logger.debug("db_host=%s", config.DB_HOST)
    con: fdb.Connection = fdb.connect(
        dsn=config.DB_DSN,
        user=config.DB_USER,
        password=config.DB_PASSWORD,
        charset="WIN1252",
    )
    logger.info("Conexao estabelecida com o banco CNES.")
    return con


def extrair_lookup_cbo(con: fdb.Connection) -> dict[str, str]:
    """Extrai mapeamento CBO -> descricao da tabela NFCES026.

    Args:
        con: Conexao ativa com o banco Firebird.

    Returns:
        Dict com chave=codigo CBO (6 digitos), valor=descricao do cargo.
    """
    df = _executar_query(con, _SQL_CBO_LOOKUP)
    if df.is_empty():
        logger.warning("cbo_lookup=vazio tabela=NFCES026 descricoes_cbo_indisponiveis=True")
        return {}
    df = df.with_columns(
        pl.col("CBO").cast(pl.Utf8).str.strip_chars(),
        pl.col("DESCRICAO_CBO").cast(pl.Utf8).str.strip_chars(),
    )
    lookup = dict(
        zip(df["CBO"].to_list(), df["DESCRICAO_CBO"].to_list(), strict=True),
    )
    logger.info("cbo_lookup count=%d", len(lookup))
    return lookup


def extrair_profissionais(con: fdb.Connection) -> pl.DataFrame:
    """Extrai vinculos profissional x estabelecimento com dados de equipe.

    Args:
        con: Conexao ativa com o banco Firebird (aberta, nao fechada).

    Returns:
        pl.DataFrame com 20 colunas (ver COLUNAS_ESPERADAS).

    Raises:
        ValueError: Se a query de vinculos nao retornar nenhum registro.
    """
    logger.info(
        "Executando extracao para municipio IBGE=%s, CNPJ=%s ...",
        config.COD_MUN_IBGE,
        config.CNPJ_MANTENEDORA,
    )

    df_vinculos = _executar_query(
        con,
        _SQL_VINCULOS.format(cod_mun=config.COD_MUN_IBGE, cnpj=config.CNPJ_MANTENEDORA),
    )

    if df_vinculos.is_empty():
        raise ValueError(
            "A query nao retornou dados. "
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
    logger.info("Extracao concluida. Total de vinculos: %d", len(df))
    return df


def dump_vinculos_para_parquet(
    con: fdb.Connection, output_dir: Path, competencia: str,
) -> Path:
    """Extrai vinculos do Firebird e persiste em Parquet.

    Args:
        con: Conexao ativa com o banco Firebird.
        output_dir: Diretorio de saida para o arquivo temporario.
        competencia: Competencia no formato 'YYYY-MM'.

    Returns:
        Path do arquivo Parquet gerado.
    """
    df = extrair_profissionais(con)
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"firebird_dump_{competencia}.parquet"
    df.write_parquet(path)
    logger.info("dump_parquet path=%s linhas=%d", path, len(df))
    return path


def _executar_query(con: fdb.Connection, sql: str) -> pl.DataFrame:
    cur = con.cursor()
    try:
        cur.execute(sql)
        linhas = cur.fetchall()
        colunas: list[str] = [d[0] for d in cur.description]
    finally:
        cur.close()
    if not linhas:
        return pl.DataFrame(schema=dict.fromkeys(colunas, pl.Utf8))
    return pl.DataFrame(linhas, schema=colunas, orient="row")


def iterar_query_em_lotes(
    con: fdb.Connection, sql: str, batch_size: int = 5000,
) -> Iterator[pl.DataFrame]:
    """Yields DataFrames em lotes via fetchmany."""
    cur = con.cursor()
    try:
        cur.execute(sql)
        colunas: list[str] = [d[0] for d in cur.description]
        while True:
            linhas = cur.fetchmany(batch_size)
            if not linhas:
                break
            yield pl.DataFrame(
                linhas, schema=colunas, orient="row",
            )
    finally:
        cur.close()


def _enriquecer_com_equipe(
    df_vinculos: pl.DataFrame,
    df_membros: pl.DataFrame,
    df_equipes: pl.DataFrame,
) -> pl.DataFrame:
    # LFCES060.SEQ_EQUIPE nacional 6-7 digitos; primeiros 4 = LFCES048.SEQ_EQUIPE local
    df_eq = df_equipes.with_columns(
        pl.col("SEQ_EQUIPE").cast(pl.Utf8).str.slice(0, 4).cast(pl.Int64).alias("SEQ_BASE")
    )

    df_mem = df_membros.with_columns(
        pl.col("CPF").str.strip_chars(),
        pl.col("CBO").str.strip_chars(),
        pl.col("SEQ_EQUIPE").cast(pl.Int64),
    )

    df_mem_eq = df_mem.join(
        df_eq.select("SEQ_BASE", "COD_INE_EQUIPE", "NOME_EQUIPE", "COD_TIPO_EQUIPE"),
        left_on="SEQ_EQUIPE",
        right_on="SEQ_BASE",
        how="left",
    ).drop("SEQ_EQUIPE", strict=False).unique(subset=["CPF", "CBO"])

    df_base = df_vinculos.with_columns(
        pl.col("CPF").str.strip_chars(),
        pl.col("CBO").str.strip_chars(),
    )

    return df_base.join(
        df_mem_eq.select("CPF", "CBO", "COD_INE_EQUIPE", "NOME_EQUIPE", "COD_TIPO_EQUIPE"),
        on=["CPF", "CBO"],
        how="left",
    )
