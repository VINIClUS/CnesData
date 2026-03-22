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
from logging.handlers import RotatingFileHandler
from pathlib import Path

import pandas as pd

import config
from cli import parse_args
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


def _exportar_se_nao_vazio(df: pd.DataFrame, nome_arquivo: str, output_dir: Path) -> None:
    if not df.empty:
        exportar_csv(df, output_dir / nome_arquivo)


def _cruzar_nacional(
    df_prof_local: pd.DataFrame,
    df_estab_local: pd.DataFrame,
    df_estab_nacional: pd.DataFrame,
    df_prof_nacional: pd.DataFrame,
) -> dict[str, pd.DataFrame]:
    _log = logging.getLogger(__name__)
    resultado: dict[str, pd.DataFrame] = {
        k: pd.DataFrame()
        for k in (
            "estab_fantasma", "estab_ausente",
            "prof_fantasma", "prof_ausente", "cbo_diverg", "ch_diverg",
        )
    }
    if not df_estab_nacional.empty:
        resultado["estab_fantasma"] = detectar_estabelecimentos_fantasma(
            df_estab_local, df_estab_nacional
        )
        resultado["estab_ausente"] = detectar_estabelecimentos_ausentes_local(
            df_estab_local, df_estab_nacional
        )
    else:
        _log.warning("estab_cross_check=skipped motivo=estabelecimentos_nacionais_vazios")
    if not df_prof_nacional.empty:
        resultado["prof_fantasma"] = detectar_profissionais_fantasma(
            df_prof_local, df_prof_nacional
        )
        resultado["prof_ausente"] = detectar_profissionais_ausentes_local(
            df_prof_local, df_prof_nacional
        )
        resultado["cbo_diverg"] = detectar_divergencia_cbo(df_prof_local, df_prof_nacional)
        resultado["ch_diverg"] = detectar_divergencia_carga_horaria(
            df_prof_local, df_prof_nacional
        )
    else:
        _log.warning("prof_cross_check=skipped motivo=profissionais_nacionais_vazios")
    return resultado


def configurar_logging(verbose: bool = False) -> None:
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
    handler_console.setLevel(logging.DEBUG if verbose else logging.INFO)
    handler_console.setFormatter(fmt)

    handler_arquivo = RotatingFileHandler(
        config.LOG_FILE, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
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
    args = parse_args()
    configurar_logging(verbose=args.verbose)
    logger = logging.getLogger(__name__)

    if args.competencia is not None:
        competencia_ano, competencia_mes = args.competencia
    else:
        competencia_ano = config.COMPETENCIA_ANO
        competencia_mes = config.COMPETENCIA_MES

    if args.output_dir is not None:
        output_path = (Path(args.output_dir) / config.OUTPUT_PATH.name).resolve()
    else:
        output_path = config.OUTPUT_PATH

    executar_hr = not args.skip_hr and config.FOLHA_HR_PATH is not None
    executar_nacional = not args.skip_nacional
    output_dir = output_path.parent

    con = None
    try:
        # ── Camada 1: Ingestão Local ──────────────────────────────────────────
        con = conectar()
        repo_local = CnesLocalAdapter(con)
        df_prof_local = repo_local.listar_profissionais()
        df_estab_local = repo_local.listar_estabelecimentos()

        # ── Camada 1B: Ingestão Nacional ─────────────────────────────────────
        df_prof_nacional: pd.DataFrame = pd.DataFrame()
        df_estab_nacional: pd.DataFrame = pd.DataFrame()
        if executar_nacional:
            repo_nacional = CnesNacionalAdapter(
                config.GCP_PROJECT_ID, config.ID_MUNICIPIO_IBGE7
            )
            competencia = (competencia_ano, competencia_mes)
            df_prof_nacional = repo_nacional.listar_profissionais(competencia)
            df_estab_nacional = repo_nacional.listar_estabelecimentos(competencia)
        else:
            logger.warning("nacional_cross_check=skipped motivo=skip_nacional_flag")

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
        if executar_hr:
            df_rh = carregar_folha(config.FOLHA_HR_PATH)
            df_ghost = detectar_folha_fantasma(df_processado, df_rh)
            df_missing = detectar_registro_ausente(df_processado, df_rh)
        else:
            logger.warning("hr_cross_check=skipped motivo=FOLHA_HR_PATH_nao_configurado")

        # ── Camada 3C: Cross-check local × nacional ───────────────────────────
        nac: dict[str, pd.DataFrame] = {
            k: pd.DataFrame()
            for k in (
                "estab_fantasma", "estab_ausente",
                "prof_fantasma", "prof_ausente", "cbo_diverg", "ch_diverg",
            )
        }

        if executar_nacional:
            if df_estab_nacional.empty and df_prof_nacional.empty:
                logger.warning(
                    "nacional_cross_check=skipped motivo=dados_nacionais_vazios "
                    "competencia=%d-%02d (dados ainda não publicados?)",
                    competencia_ano, competencia_mes,
                )
            else:
                nac = _cruzar_nacional(
                    df_processado, df_estab_local, df_estab_nacional, df_prof_nacional
                )

        df_estab_fantasma = nac["estab_fantasma"]
        df_estab_ausente = nac["estab_ausente"]
        df_prof_fantasma = nac["prof_fantasma"]
        df_prof_ausente = nac["prof_ausente"]
        df_cbo_diverg = nac["cbo_diverg"]
        df_ch_diverg = nac["ch_diverg"]

        # ── Camada 4: Exportação ──────────────────────────────────────────────
        exportar_csv(df_processado, output_path)

        exp = _exportar_se_nao_vazio
        exp(df_multi_unidades, "auditoria_rq003b_multiplas_unidades.csv", output_dir)
        exp(df_acs_incorretos, "auditoria_rq005_acs_tacs_incorretos.csv", output_dir)
        exp(df_ace_incorretos, "auditoria_rq005_ace_tace_incorretos.csv", output_dir)
        exp(df_ghost, "auditoria_ghost_payroll.csv", output_dir)
        exp(df_missing, "auditoria_missing_registration.csv", output_dir)
        exp(df_estab_fantasma, "auditoria_rq006_estab_fantasma.csv", output_dir)
        exp(df_estab_ausente, "auditoria_rq007_estab_ausente_local.csv", output_dir)
        exp(df_prof_fantasma, "auditoria_rq008_prof_fantasma_cns.csv", output_dir)
        exp(df_prof_ausente, "auditoria_rq009_prof_ausente_local_cns.csv", output_dir)
        exp(df_cbo_diverg, "auditoria_rq010_divergencia_cbo.csv", output_dir)
        exp(df_ch_diverg, "auditoria_rq011_divergencia_ch.csv", output_dir)

        # ── Camada 4B: Relatório Excel consolidado ────────────────────────────
        competencia_str = f"{competencia_ano}-{competencia_mes:02d}"
        gerar_relatorio(
            output_path.with_suffix(".xlsx"),
            {
                "principal":             df_processado,
                "ghost":                 df_ghost,
                "missing":               df_missing,
                "multi_unidades":        df_multi_unidades,
                "acs_tacs":              df_acs_incorretos,
                "ace_tace":              df_ace_incorretos,
                "rq006_estab_fantasma":  df_estab_fantasma,
                "rq007_estab_ausente":   df_estab_ausente,
                "rq008_prof_fantasma":   df_prof_fantasma,
                "rq009_prof_ausente":    df_prof_ausente,
                "rq010_divergencia_cbo": df_cbo_diverg,
                "rq011_divergencia_ch":  df_ch_diverg,
            },
            competencia=competencia_str,
        )

        # ── Camada 5: Snapshot histórico ──────────────────────────────────────
        competencia_stem = (
            output_path.stem.split("_")[-1]
            if "_" in output_path.stem
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
        logger.info("Relatório principal: %s", output_path)
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
