"""Ponto de entrada do pipeline CnesData."""
import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

import config
from cli import parse_args
from pipeline.orchestrator import PipelineOrchestrator
from pipeline.state import PipelineState
from pipeline.stages.auditoria_local import AuditoriaLocalStage
from pipeline.stages.auditoria_nacional import AuditoriaNacionalStage
from pipeline.stages.exportacao import ExportacaoStage
from pipeline.stages.ingestao_local import IngestaoLocalStage
from pipeline.stages.ingestao_nacional import IngestaoNacionalStage
from pipeline.stages.metricas import MetricasStage
from pipeline.stages.processamento import ProcessamentoStage
from pipeline.stages.processamento_nacional import ProcessamentoNacionalStage
from pipeline.stages.snapshot_local import SnapshotLocalStage
from sqlalchemy import create_engine
from storage.database_loader import DatabaseLoader
from storage.historico_reader import HistoricoReader
from storage.postgres_adapter import PostgresAdapter
from storage.ports import NullStoragePort


def configurar_logging(verbose: bool = False) -> None:
    """Configura handlers de console e arquivo rotativo."""
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    config.LOGS_DIR.mkdir(parents=True, exist_ok=True)
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.DEBUG if verbose else logging.INFO)
    console.setFormatter(fmt)
    arquivo = RotatingFileHandler(
        config.LOG_FILE, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    arquivo.setLevel(logging.DEBUG)
    arquivo.setFormatter(fmt)
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.addHandler(console)
    root.addHandler(arquivo)


def _criar_estado(args) -> PipelineState:
    ano, mes = args.competencia if args.competencia else (config.COMPETENCIA_ANO, config.COMPETENCIA_MES)
    output_path = (
        (Path(args.output_dir) / config.OUTPUT_PATH.name).resolve()
        if args.output_dir
        else config.OUTPUT_PATH
    )
    return PipelineState(
        competencia_ano=ano,
        competencia_mes=mes,
        output_path=output_path,
        executar_nacional=not args.skip_nacional,
        executar_hr=not args.skip_hr and config.FOLHA_HR_PATH is not None,
        force_reingestao=args.force_reingestao,
    )


def main() -> int:
    """Ponto de entrada principal.

    Returns:
        int: 0 = sucesso, 1 = erro.
    """
    args = parse_args()
    configurar_logging(verbose=args.verbose)
    state = _criar_estado(args)
    db_loader = DatabaseLoader(config.DUCKDB_PATH)
    db_loader.inicializar_schema()
    _storage = (
        PostgresAdapter(create_engine(config.DB_URL), config.COD_MUN_IBGE)
        if config.DB_URL
        else NullStoragePort()
    )
    historico_reader = HistoricoReader(config.DUCKDB_PATH, config.HISTORICO_DIR)
    orchestrator = PipelineOrchestrator([
        IngestaoLocalStage(config.HISTORICO_DIR, db_loader),
        ProcessamentoStage(),
        SnapshotLocalStage(config.HISTORICO_DIR, db_loader),
        IngestaoNacionalStage(db_loader),
        ProcessamentoNacionalStage(),
        AuditoriaLocalStage(),
        AuditoriaNacionalStage(),
        MetricasStage(db_loader, historico_reader),
        ExportacaoStage(_storage),
    ])
    try:
        orchestrator.executar(state)
        logging.getLogger(__name__).info("pipeline concluido output=%s", state.output_path)
        return 0
    except (EnvironmentError, FileNotFoundError) as e:
        logging.getLogger(__name__).error("erro_config=%s", e)
        return 1
    except Exception as e:
        logging.getLogger(__name__).exception("erro_inesperado=%s", e)
        return 1
    finally:
        if state.con is not None:
            state.con.close()
            logging.getLogger(__name__).info("conexao_encerrada")


if __name__ == "__main__":
    sys.exit(main())
