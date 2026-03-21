"""
cnes_client.py — Camada de Ingestão: Cliente do Banco CNES Firebird

Responsabilidade: carregar o driver Firebird, abrir a conexão e extrair
os dados brutos de profissionais vinculados ao CNPJ da mantenedora.

Por que cursor manual em vez de pd.read_sql()?
  O driver `fdb` emite erro -501 ("Invalid cursor state") quando
  pd.read_sql() é usado com LEFT JOINs encadeados. O cursor manual
  contorna isso ao iterar cursor.fetchall() e construir o DataFrame
  com os nomes vindos de cursor.description.

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

# ── Query Master Validada ──────────────────────────────────────────────────────
# Fonte de verdade: data_dictionary.md (seção "Query Master Validada").
# Resultado confirmado: 367 vínculos, ~330 profissionais únicos, 2026-03.
_SQL_PROFISSIONAIS: str = """
    SELECT
        prof.CPF_PROF            AS CPF,
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
        est.CODMUNGEST           AS COD_MUN_GESTOR,
        eq.INE                   AS COD_INE_EQUIPE,
        eq.DS_AREA               AS NOME_EQUIPE,
        eq.TP_EQUIPE             AS COD_TIPO_EQUIPE
    FROM       LFCES021 vinc
    INNER JOIN LFCES004 est  ON  est.UNIDADE_ID = vinc.UNIDADE_ID
    INNER JOIN LFCES018 prof ON  prof.PROF_ID   = vinc.PROF_ID
    LEFT  JOIN LFCES048 me   ON  me.CPF_PROF    = prof.CPF_PROF
                             AND me.COD_CBO     = vinc.COD_CBO
                             AND me.COD_MUN     = est.CODMUNGEST
    LEFT  JOIN LFCES060 eq   ON  eq.SEQ_EQUIPE  = me.SEQ_EQUIPE
                             AND eq.COD_AREA    = me.COD_AREA
                             AND eq.COD_MUN     = me.COD_MUN
    WHERE est.CODMUNGEST = '{cod_mun}'
      AND est.CNPJ_MANT  = '{cnpj}'
    ORDER BY prof.NOME_PROF, vinc.COD_CBO
"""

# Colunas esperadas na ordem do SELECT — referência para validação pós-extração.
COLUNAS_ESPERADAS: tuple[str, ...] = (
    "CPF", "NOME_PROFISSIONAL", "NOME_SOCIAL", "SEXO", "DATA_NASCIMENTO",
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

    logger.debug("Conectando ao banco: %s", config.DB_DSN)
    con: fdb.Connection = fdb.connect(
        dsn=config.DB_DSN,
        user=config.DB_USER,
        password=config.DB_PASSWORD,
    )
    logger.info("Conexão estabelecida com o banco CNES.")
    return con


def extrair_profissionais(con: fdb.Connection) -> pd.DataFrame:
    """
    Extrai os vínculos profissional↔estabelecimento via cursor manual.

    O cursor é fechado em um bloco `finally` mesmo que execute() falhe,
    garantindo que recursos do banco não fiquem presos. A conexão em si
    NÃO é fechada aqui — é responsabilidade do chamador.

    Args:
        con: Conexão ativa com o banco Firebird (aberta, não fechada).

    Returns:
        pd.DataFrame: DataFrame bruto com 19 colunas (ver COLUNAS_ESPERADAS).

    Raises:
        ValueError: Se a query não retornar nenhum registro.
        fdb.fbcore.DatabaseError: Se ocorrer erro de SQL no banco.
    """
    query: str = _SQL_PROFISSIONAIS.format(
        cod_mun=config.COD_MUN_IBGE,
        cnpj=config.CNPJ_MANTENEDORA,
    )

    logger.info(
        "Executando extração para município IBGE=%s, CNPJ=%s ...",
        config.COD_MUN_IBGE,
        config.CNPJ_MANTENEDORA,
    )

    cur = con.cursor()
    try:
        cur.execute(query)
        linhas = cur.fetchall()
        nomes_colunas: list[str] = [descricao[0] for descricao in cur.description]
    finally:
        cur.close()

    df: pd.DataFrame = pd.DataFrame(linhas, columns=nomes_colunas)
    logger.info("Extração concluída. Total de vínculos: %d", len(df))

    if df.empty:
        raise ValueError(
            "A query não retornou dados. "
            f"Verifique os filtros: COD_MUN_IBGE={config.COD_MUN_IBGE}, "
            f"CNPJ={config.CNPJ_MANTENEDORA}."
        )

    return df
