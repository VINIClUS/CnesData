"""
cnes_exporter.py — Pipeline de Extração de Dados do CNES [DEPRECATED]

.. deprecated::
    Este módulo está obsoleto desde 2026-03-21.
    Use os módulos da arquitetura em camadas:
      - Ingestão:     src/ingestion/cnes_client.py
      - Processamento: src/processing/transformer.py
      - Análise:      src/analysis/rules_engine.py
      - Exportação:   src/export/csv_exporter.py
      - Orquestração: src/main.py

    Mantido apenas para compatibilidade com testes de integração legados.
    Será removido após migração completa dos testes de integração.

----

cnes_exporter.py — Pipeline de Extração de Dados do CNES

Este módulo é o coração do projeto. Ele realiza a extração, transformação
e exportação (padrão ETL) dos dados do banco Firebird local do DATASUS.

Princípios Aplicados:
  - SRP (Single Responsibility Principle): cada função tem UMA responsabilidade.
  - Logging: toda mensagem de status usa o módulo `logging` (não `print`), 
    garantindo rastreabilidade com nível, data/hora e destino configurável.
  - Separação de Configuração: nenhuma credencial ou caminho está neste arquivo.
    Tudo vem do módulo `config.py`.

Tabelas do Banco CNES utilizadas:
  - LFCES021: Vínculos Profissional ↔ Estabelecimento (carga horária, CBO)
  - LFCES004: Estabelecimentos (CNES, nome, tipo, mantenedora, município gestor)
  - LFCES018: Profissionais - Pessoas Físicas (nome, CPF, ID interno)
  - LFCES048: Vínculos Profissional ↔ Equipe (chave para equipe de saúde)
  - LFCES060: Equipes de Saúde (INE, área de atuação, segmento)
"""

import logging
import warnings
from pathlib import Path

import fdb
import pandas as pd

import config

# ── Configuração do Logger ─────────────────────────────────────────────────
# Obtemos o logger com o nome deste módulo (__name__ == "cnes_exporter").
# Isso permite que o arquivo main.py configure o comportamento do logger
# (formato, nível, arquivo de saída) de forma centralizada.
logger = logging.getLogger(__name__)

# ── Query SQL ──────────────────────────────────────────────────────────────
# A query é definida como constante no nível do módulo, pois é
# uma definição estática — não muda entre execuções da função.
# O uso de parâmetros (%s / ?) seria o ideal para evitar SQL injection,
# mas o driver fdb não suporta parâmetros no WHERE com pd.read_sql;
# os valores são constantes de configuração, não entradas do usuário.
_SQL_PROFISSIONAIS = """
    SELECT
        prof.CPF_PROF                                           AS CPF,
        prof.NOME_PROF                                          AS NOME_PROFISSIONAL,
        vinc.COD_CBO                                            AS CBO,
        (
            COALESCE(vinc.CG_HORAAMB,  0) +
            COALESCE(vinc.CGHORAOUTR, 0) +
            COALESCE(vinc.CGHORAHOSP, 0)
        )                                                       AS CARGA_HORARIA,
        est.CNES                                                AS COD_CNES,
        est.NOME_FANTA                                          AS ESTABELECIMENTO,
        est.TP_UNID_ID                                          AS TIPO_ESTAB,
        eq.INE                                                  AS COD_INE_EQUIPE,
        eq.DS_AREA                                              AS NOME_EQUIPE,
        eq.DS_SEGMENTO                                          AS TIPO_EQUIPE
    FROM       LFCES021 vinc
    INNER JOIN LFCES004 est  ON  est.UNIDADE_ID = vinc.UNIDADE_ID
    INNER JOIN LFCES018 prof ON  prof.PROF_ID   = vinc.PROF_ID
    LEFT  JOIN LFCES048 me   ON  me.CPF_PROF    = prof.CPF_PROF
                             AND me.COD_CBO     = vinc.COD_CBO
                             AND me.COD_MUN     = est.CODMUNGEST
    LEFT  JOIN LFCES060 eq   ON  eq.SEQ_EQUIPE  = me.SEQ_EQUIPE
                             AND eq.COD_AREA    = me.COD_AREA
                             AND eq.COD_MUN     = me.COD_MUN
    WHERE
        est.CODMUNGEST = '{cod_mun}'
        AND est.CNPJ_MANT = '{cnpj}'
"""


# ─────────────────────────────────────────────────────────────────────────────
# 1. CONEXÃO
# ─────────────────────────────────────────────────────────────────────────────
def conectar() -> fdb.Connection:
    """
    Carrega o driver Firebird 64-bits e abre a conexão com o banco de dados.

    Returns:
        fdb.Connection: Objeto de conexão ativa com o banco CNES.

    Raises:
        FileNotFoundError: Se a DLL do Firebird não for encontrada no caminho configurado.
        fdb.fbcore.DatabaseError: Se a conexão com o banco falhar (banco offline, credenciais erradas).
    """
    dll_path = Path(config.FIREBIRD_DLL)
    if not dll_path.exists():
        raise FileNotFoundError(
            f"DLL do Firebird 64-bits não encontrada em: {dll_path}\n"
            "Verifique a variável FIREBIRD_DLL no arquivo .env"
        )

    logger.debug("Carregando driver Firebird de: %s", dll_path)
    fdb.load_api(str(dll_path))
    logger.info("Driver Firebird (64-bits) carregado.")

    logger.debug("Conectando ao banco: %s", config.DB_DSN)
    con = fdb.connect(
        dsn=config.DB_DSN,
        user=config.DB_USER,
        password=config.DB_PASSWORD,
    )
    logger.info("Conexão estabelecida com o banco CNES.")
    return con


# ─────────────────────────────────────────────────────────────────────────────
# 2. EXTRAÇÃO (Query)
# ─────────────────────────────────────────────────────────────────────────────
def executar_query(con: fdb.Connection) -> pd.DataFrame:
    """
    Executa a query SQL de extração dos profissionais da Prefeitura Municipal.

    A query realiza os seguintes JOINs:
      LFCES021 → LFCES004 (vínculo → estabelecimento)  [INNER: só vincs. ativos]
      LFCES021 → LFCES018 (vínculo → profissional)     [INNER: todo vínculo tem prof.]
      LFCES021 → LFCES048 (vínculo → membro de equipe) [LEFT: nem todo prof. tem equipe]
      LFCES048 → LFCES060 (membro → dados da equipe)   [LEFT: dependente do LEFT acima]

    Args:
        con: Conexão ativa com o banco Firebird.

    Returns:
        pd.DataFrame: DataFrame bruto com os dados extraídos do banco.

    Raises:
        fdb.fbcore.DatabaseError: Se a query SQL for inválida ou o banco retornar erro.
        ValueError: Se o DataFrame retornado estiver vazio.
    """
    query = _SQL_PROFISSIONAIS.format(
        cod_mun=config.COD_MUN_IBGE,
        cnpj=config.CNPJ_MANTENEDORA,
    )

    logger.info(
        "Executando query para o município IBGE=%s, CNPJ Mantenedora=%s ...",
        config.COD_MUN_IBGE,
        config.CNPJ_MANTENEDORA,
    )

    # Suprime o aviso do pandas sobre conexões nativas fdb (não é um erro,
    # apenas um aviso de compatibilidade — o SQLAlchemy seria a alternativa,
    # mas o driver fdb ainda não tem suporte completo a ele).
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df = pd.read_sql(query, con)

    logger.info("Query concluída. Total de registros extraídos: %d", len(df))

    if df.empty:
        raise ValueError(
            "A query não retornou dados. Verifique o banco de dados e os filtros "
            f"(COD_MUN_IBGE={config.COD_MUN_IBGE}, CNPJ={config.CNPJ_MANTENEDORA})."
        )

    return df


# ─────────────────────────────────────────────────────────────────────────────
# 3. TRANSFORMAÇÃO
# ─────────────────────────────────────────────────────────────────────────────
def transformar(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica regras de limpeza e padronização ao DataFrame bruto.

    Transformações aplicadas:
      - Colunas de texto (CPF, nome, CBO): strip() para remover espaços extras.
      - Colunas opcionais de equipe: preenchimento de nulos com valores descritivos.

    Args:
        df: DataFrame bruto retornado pela query.

    Returns:
        pd.DataFrame: DataFrame transformado, pronto para exportação.
    """
    logger.debug("Iniciando transformação dos dados ...")
    df_clean = df.copy()  # Boa prática: nunca modifique o DataFrame de origem.

    # ── Limpeza de Strings ───────────────────────────────────────────────
    colunas_texto = ["CPF", "NOME_PROFISSIONAL", "CBO", "COD_CNES", "ESTABELECIMENTO"]
    for col in colunas_texto:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].astype(str).str.strip()

    linhas_antes = len(df_clean)

    # ── Preenchimento de Nulos (colunas opcionais de equipe) ─────────────
    # Profissionais sem equipe têm NULL no LEFT JOIN — transformamos em texto legível.
    mapeamento_nulos = {
        "NOME_EQUIPE": "SEM EQUIPE VINCULADA",
        "COD_INE_EQUIPE": "-",
        "TIPO_EQUIPE": "-",
    }
    for col, valor_padrao in mapeamento_nulos.items():
        if col in df_clean.columns:
            nulos = df_clean[col].isna().sum()
            df_clean[col] = df_clean[col].fillna(valor_padrao)
            if nulos > 0:
                logger.debug("Coluna '%s': %d nulos preenchidos com '%s'.", col, nulos, valor_padrao)

    logger.info(
        "Transformação concluída. Registros: %d → %d (sem perda de dados).",
        linhas_antes,
        len(df_clean),
    )
    return df_clean


# ─────────────────────────────────────────────────────────────────────────────
# 4. EXPORTAÇÃO
# ─────────────────────────────────────────────────────────────────────────────
def exportar_csv(df: pd.DataFrame, caminho: Path) -> None:
    """
    Salva o DataFrame em um arquivo CSV.

    Convenções de exportação:
      - Separador `;` (padrão BR, compatível com Excel em pt-BR sem configuração extra).
      - Encoding `utf-8-sig` (UTF-8 com BOM): garante que o Excel abra com acentuação correta.
      - Sem índice do pandas (index=False): o índice interno não tem significado de negócio.

    Args:
        df: DataFrame limpo e pronto para exportação.
        caminho: Path completo do arquivo de saída.

    Raises:
        OSError: Se não for possível criar o diretório ou escrever o arquivo.
    """
    caminho.parent.mkdir(parents=True, exist_ok=True)

    logger.info("Exportando %d registros para: %s", len(df), caminho)
    df.to_csv(caminho, index=False, sep=";", encoding="utf-8-sig")
    logger.info("Arquivo CSV exportado com sucesso.")


# ─────────────────────────────────────────────────────────────────────────────
# 5. PIPELINE (Orquestração)
# ─────────────────────────────────────────────────────────────────────────────
def pipeline() -> None:
    """
    Orquestra o pipeline completo de ETL:
    Extração → Transformação → Exportação (CSV).

    Esta função é o ponto de entrada do pipeline e deve ser chamada por main.py.
    Em caso de erro em qualquer etapa, a exceção é propagada para o chamador,
    que irá registrá-la no log com nível ERROR.
    """
    con = None
    try:
        logger.info("=" * 60)
        logger.info("Iniciando Pipeline CNES — Prefeitura de Presidente Epitácio")
        logger.info("=" * 60)

        # Etapa 1/4: Conexão
        con = conectar()

        # Etapa 2/4: Extração
        df_bruto = executar_query(con)

        # Etapa 3/4: Transformação
        df_limpo = transformar(df_bruto)

        # Etapa 4/4: Exportação
        exportar_csv(df_limpo, config.OUTPUT_PATH)

        logger.info("Pipeline concluído com êxito.")
        logger.info("Relatório disponível em: %s", config.OUTPUT_PATH)

    finally:
        # O bloco finally SEMPRE executa, mesmo que ocorra uma exceção.
        # Isso garante que a conexão com o banco seja sempre fechada.
        if con is not None:
            con.close()
            logger.info("Conexão com o banco encerrada.")


# ─────────────────────────────────────────────────────────────────────────────
# Execução direta (para testes rápidos via terminal)
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # Quando executado diretamente, configuramos um logging básico para o terminal.
    # Em produção, o main.py configura o logging de forma mais completa (com arquivo).
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    pipeline()
