"""
main.py — Ponto de Entrada do Pipeline CnesData

Orquestra as quatro camadas da arquitetura limpa:
  1. Ingestão     → adapters padronizados (local + nacional)
  2. Processamento → processing.transformer (limpeza + RQ-002/003)
  3. Análise      → analysis.rules_engine  (auditoria local + cross-check)
  4. Exportação   → export.csv_exporter    (CSV padrão BR)

Como executar:
  python src/main.py
"""

import logging
import sys
from pathlib import Path

import pandas as pd

import config
from ingestion.cnes_client import conectar
from ingestion.cnes_local_adapter import CnesLocalAdapter
from ingestion.cnes_nacional_adapter import CnesNacionalAdapter
from ingestion.hr_client import carregar_folha
from processing.transformer import transformar
from analysis.rules_engine import (
    detectar_multiplas_unidades,
    auditar_lotacao_acs_tacs,
    auditar_lotacao_ace_tace,
    detectar_folha_fantasma,
    detectar_registro_ausente,
    detectar_estabelecimentos_fantasma,
    detectar_estabelecimentos_ausentes_local,
    detectar_profissionais_fantasma,
    detectar_profissionais_ausentes_local,
    detectar_divergencia_cbo,
    detectar_divergencia_carga_horaria,
)
from analysis.evolution_tracker import criar_snapshot, salvar_snapshot
from export.csv_exporter import exportar_csv
from export.report_generator import gerar_relatorio


def _exportar_se_nao_vazio(df: pd.DataFrame, nome_arquivo: str) -> None:
    if not df.empty:
        exportar_csv(df, config.OUTPUT_PATH.parent / nome_arquivo)


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

    con = None
    try:
        # ── Camada 1: Ingestão Local ──────────────────────────────────────────
        con = conectar()
        repo_local = CnesLocalAdapter(con)
        df_prof_local = repo_local.listar_profissionais()
        df_estab_local = repo_local.listar_estabelecimentos()

        # ── Camada 1B: Ingestão Nacional ─────────────────────────────────────
        repo_nacional = CnesNacionalAdapter(config.GCP_PROJECT_ID, config.ID_MUNICIPIO_IBGE7)
        competencia = (config.COMPETENCIA_ANO, config.COMPETENCIA_MES)
        df_prof_nacional = repo_nacional.listar_profissionais(competencia)
        df_estab_nacional = repo_nacional.listar_estabelecimentos(competencia)

        # ── Camada 2: Processamento (apenas dados locais) ────────────────────
        df_processado = transformar(df_prof_local)

        # ── Camada 3: Análise — regras locais ────────────────────────────────
        df_multi_unidades = detectar_multiplas_unidades(df_processado)

        # RQ-005 precisa de TIPO_UNIDADE que vem dos estabelecimentos
        df_prof_com_unidade = df_processado.merge(
            df_estab_local[["CNES", "TIPO_UNIDADE"]], on="CNES", how="left"
        )
        df_acs_incorretos = auditar_lotacao_acs_tacs(df_prof_com_unidade)
        df_ace_incorretos = auditar_lotacao_ace_tace(df_prof_com_unidade)

        # ── Camada 3B: Cross-check CNES × RH (opcional) ──────────────────────
        df_ghost: pd.DataFrame = pd.DataFrame()
        df_missing: pd.DataFrame = pd.DataFrame()
        if config.FOLHA_HR_PATH is not None:
            df_rh = carregar_folha(config.FOLHA_HR_PATH)
            df_ghost = detectar_folha_fantasma(df_processado, df_rh)
            df_missing = detectar_registro_ausente(df_processado, df_rh)
        else:
            logger.warning("hr_cross_check=skipped motivo=FOLHA_HR_PATH_nao_configurado")

        # ── Camada 3C: Cross-check local × nacional ───────────────────────────
        df_estab_fantasma = detectar_estabelecimentos_fantasma(df_estab_local, df_estab_nacional)
        df_estab_ausente = detectar_estabelecimentos_ausentes_local(df_estab_local, df_estab_nacional)
        df_prof_fantasma = detectar_profissionais_fantasma(df_processado, df_prof_nacional)
        df_prof_ausente = detectar_profissionais_ausentes_local(df_processado, df_prof_nacional)
        df_cbo_diverg = detectar_divergencia_cbo(df_processado, df_prof_nacional)
        df_ch_diverg = detectar_divergencia_carga_horaria(df_processado, df_prof_nacional)

        # ── Camada 4: Exportação ──────────────────────────────────────────────
        exportar_csv(df_processado, config.OUTPUT_PATH)

        _exportar_se_nao_vazio(df_multi_unidades, "auditoria_rq003b_multiplas_unidades.csv")
        _exportar_se_nao_vazio(df_acs_incorretos, "auditoria_rq005_acs_tacs_incorretos.csv")
        _exportar_se_nao_vazio(df_ace_incorretos, "auditoria_rq005_ace_tace_incorretos.csv")
        _exportar_se_nao_vazio(df_ghost, "auditoria_ghost_payroll.csv")
        _exportar_se_nao_vazio(df_missing, "auditoria_missing_registration.csv")
        _exportar_se_nao_vazio(df_estab_fantasma, "auditoria_rq006_estab_fantasma.csv")
        _exportar_se_nao_vazio(df_estab_ausente, "auditoria_rq007_estab_ausente_local.csv")
        _exportar_se_nao_vazio(df_prof_fantasma, "auditoria_rq008_prof_fantasma_cns.csv")
        _exportar_se_nao_vazio(df_prof_ausente, "auditoria_rq009_prof_ausente_local_cns.csv")
        _exportar_se_nao_vazio(df_cbo_diverg, "auditoria_rq010_divergencia_cbo.csv")
        _exportar_se_nao_vazio(df_ch_diverg, "auditoria_rq011_divergencia_ch.csv")

        # ── Camada 4B: Relatório Excel consolidado ────────────────────────────
        gerar_relatorio(
            config.OUTPUT_PATH.with_suffix(".xlsx"),
            df_processado,
            df_ghost,
            df_missing,
            df_multi_unidades,
            df_acs_incorretos,
            df_ace_incorretos,
        )

        # ── Camada 5: Snapshot histórico ──────────────────────────────────────
        competencia_stem = (
            config.OUTPUT_PATH.stem.split("_")[-1]
            if "_" in config.OUTPUT_PATH.stem
            else "desconhecida"
        )
        snapshot = criar_snapshot(
            competencia_stem,
            df_processado,
            df_ghost,
            df_missing,
            df_multi_unidades,
            df_acs_incorretos,
            df_ace_incorretos,
        )
        salvar_snapshot(snapshot, config.SNAPSHOTS_DIR)

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
            logging.getLogger(__name__).info("Conexão com o banco encerrada.")


if __name__ == "__main__":
    sys.exit(main())
