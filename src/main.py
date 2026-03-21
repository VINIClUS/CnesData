"""
main.py — Ponto de Entrada do Pipeline CnesData

Orquestra as quatro camadas da arquitetura limpa:
  1. Ingestão     → ingestion.cnes_client  (extração do Firebird)
  2. Processamento → processing.transformer (limpeza + RQ-002/003)
  3. Análise      → analysis.rules_engine  (auditoria RQ-003-B, RQ-005)
  4. Exportação   → export.csv_exporter    (CSV padrão BR)

Responsabilidade deste arquivo:
  - Configurar o sistema de logging antes de qualquer operação.
  - Gerenciar o ciclo de vida da conexão com o banco (abrir/fechar).
  - Orquestrar a sequência das camadas e exportar relatórios de auditoria.
  - Tratar e registrar exceções de forma adequada.

Como executar:
  python src/main.py
"""

import logging
import sys

import config
from ingestion.cnes_client import conectar, extrair_profissionais
from processing.transformer import transformar
from analysis.rules_engine import (
    detectar_multiplas_unidades,
    auditar_lotacao_acs_tacs,
    auditar_lotacao_ace_tace,
)
from export.csv_exporter import exportar_csv


def configurar_logging() -> None:
    """
    Configura o sistema de logging do projeto.

    Comportamento:
      - Nível mínimo: DEBUG (todos os níveis capturados pelo root logger).
      - Console (StreamHandler): exibe INFO e acima — progresso da execução.
      - Arquivo (FileHandler): registra DEBUG e acima — rastreamento completo.
      - Formato: timestamp [NÍVEL   ] módulo: mensagem

    O formato e os destinos são definidos uma única vez aqui, e todos os
    módulos que usam logging.getLogger(__name__) herdam essa configuração.
    """
    fmt = logging.Formatter(
        fmt="%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    config.LOGS_DIR.mkdir(parents=True, exist_ok=True)

    handler_console = logging.StreamHandler(sys.stdout)
    handler_console.setLevel(logging.INFO)
    handler_console.setFormatter(fmt)

    handler_arquivo = logging.FileHandler(config.LOG_FILE, encoding="utf-8")
    handler_arquivo.setLevel(logging.DEBUG)
    handler_arquivo.setFormatter(fmt)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(handler_console)
    root_logger.addHandler(handler_arquivo)


def main() -> int:
    """
    Ponto de entrada principal do pipeline CnesData.

    Returns:
        int: Código de saída Unix (0 = sucesso, 1 = erro).
    """
    configurar_logging()
    logger = logging.getLogger(__name__)

    logger.info("=" * 60)
    logger.info("Iniciando CnesData Pipeline — Prefeitura de Presidente Epitácio")
    logger.info("=" * 60)
    logger.debug("Raiz do projeto: %s", config.RAIZ_PROJETO)
    logger.debug("Log completo em: %s", config.LOG_FILE)

    con = None
    try:
        # ── Camada 1: Ingestão ────────────────────────────────────────────────
        con = conectar()
        df_bruto = extrair_profissionais(con)

        # ── Camada 2: Processamento ───────────────────────────────────────────
        df_processado = transformar(df_bruto)

        # ── Camada 3: Análise (relatórios de auditoria) ───────────────────────
        df_multi_unidades = detectar_multiplas_unidades(df_processado)
        df_acs_incorretos = auditar_lotacao_acs_tacs(df_processado)
        df_ace_incorretos = auditar_lotacao_ace_tace(df_processado)

        # ── Camada 4: Exportação ──────────────────────────────────────────────
        exportar_csv(df_processado, config.OUTPUT_PATH)

        # Relatórios de auditoria são gerados apenas quando há anomalias,
        # evitando arquivos vazios que poluem o diretório de saída.
        if not df_multi_unidades.empty:
            exportar_csv(
                df_multi_unidades,
                config.OUTPUT_PATH.parent / "auditoria_rq003b_multiplas_unidades.csv",
            )
        if not df_acs_incorretos.empty:
            exportar_csv(
                df_acs_incorretos,
                config.OUTPUT_PATH.parent / "auditoria_rq005_acs_tacs_incorretos.csv",
            )
        if not df_ace_incorretos.empty:
            exportar_csv(
                df_ace_incorretos,
                config.OUTPUT_PATH.parent / "auditoria_rq005_ace_tace_incorretos.csv",
            )

        logger.info("Pipeline concluído com êxito.")
        logger.info("Relatório principal: %s", config.OUTPUT_PATH)
        return 0

    except EnvironmentError as e:
        logging.getLogger(__name__).error("Erro de configuração: %s", e)
        return 1

    except FileNotFoundError as e:
        logging.getLogger(__name__).error("Arquivo não encontrado: %s", e)
        return 1

    except Exception as e:
        logging.getLogger(__name__).exception("Erro inesperado durante o pipeline: %s", e)
        return 1

    finally:
        if con is not None:
            con.close()
            logger.info("Conexão com o banco encerrada.")


if __name__ == "__main__":
    sys.exit(main())
