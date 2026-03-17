"""
main.py — Ponto de Entrada do Pipeline CnesData

Este arquivo é responsável por:
  1. Configurar o sistema de logging (formato, nível, saída em arquivo e console).
  2. Chamar o pipeline de extração do CNES.

Por que separar main.py do cnes_exporter.py?
  - O main.py cuida do "ambiente de execução" (logging, argumentos, etc.).
  - O cnes_exporter.py cuida da lógica de negócio (ETL do CNES).
  - Isso permite que o cnes_exporter seja importado e testado de forma independente,
    sem efeitos colaterais de logging ou configuração de ambiente.

Como executar:
  python src/main.py
"""

import logging
import sys
from pathlib import Path

import config
import cnes_exporter


def configurar_logging() -> None:
    """
    Configura o sistema de logging do projeto.

    Comportamento:
      - Nível mínimo: DEBUG (todos os níveis são capturados).
      - Console (StreamHandler): exibe INFO e acima — mensagens de progresso.
      - Arquivo (FileHandler): registra DEBUG e acima — rastreamento completo.
      - O arquivo de log é criado em logs/cnes_exporter.log (definido em config.py).

    Formato das mensagens:
      2026-03-17 08:30:00 [INFO    ] cnes_exporter: Pipeline concluído.
    """
    fmt = logging.Formatter(
        fmt="%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Cria a pasta de logs se não existir
    config.LOGS_DIR.mkdir(parents=True, exist_ok=True)

    # Handler 1: Console — mostra apenas INFO e acima (menos verboso)
    handler_console = logging.StreamHandler(sys.stdout)
    handler_console.setLevel(logging.INFO)
    handler_console.setFormatter(fmt)

    # Handler 2: Arquivo — registra tudo (DEBUG e acima) para rastreamento posterior
    handler_arquivo = logging.FileHandler(config.LOG_FILE, encoding="utf-8")
    handler_arquivo.setLevel(logging.DEBUG)
    handler_arquivo.setFormatter(fmt)

    # Configura o logger raiz do projeto (captura os loggers de todos os módulos)
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(handler_console)
    root_logger.addHandler(handler_arquivo)


def main() -> int:
    """
    Ponto de entrada principal do pipeline CnesData.

    Returns:
        int: Código de saída (0 = sucesso, 1 = erro).
    """
    configurar_logging()

    # Após configurar o logging, qualquer módulo que use logging.getLogger(__name__)
    # já terá seu output direcionado para o console e para o arquivo.
    logger = logging.getLogger(__name__)
    logger.info("Iniciando CnesData Pipeline — Prefeitura de Presidente Epitácio")
    logger.debug("Diretório raiz do projeto: %s", config.RAIZ_PROJETO)
    logger.debug("Log completo disponível em: %s", config.LOG_FILE)

    try:
        cnes_exporter.pipeline()
        return 0  # Retorna 0: sucesso (convenção Unix)

    except EnvironmentError as e:
        # Erro de configuração: variável de ambiente ausente no .env
        logger.error("Erro de configuração: %s", e)
        return 1

    except FileNotFoundError as e:
        # Arquivo necessário não encontrado (ex: DLL do Firebird)
        logger.error("Arquivo não encontrado: %s", e)
        return 1

    except Exception as e:
        # Qualquer outro erro inesperado: registra o traceback completo
        logger.exception("Erro inesperado durante o pipeline: %s", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
