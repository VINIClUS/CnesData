"""Ponto de entrada do pipeline CnesData."""
import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

from cnes_domain.pipeline.orchestrator import PipelineOrchestrator
from cnes_domain.pipeline.state import PipelineState
from cnes_domain.ports.storage import NullStoragePort
from cnes_domain.tenant import set_tenant_id
from cnes_infra import config
from cnes_infra.storage.postgres_adapter import PostgresAdapter
from cnes_infra.telemetry import init_telemetry
from sqlalchemy import create_engine

from dump_agent.cli import parse_args
from dump_agent.stages.exportacao import ExportacaoStage
from dump_agent.stages.ingestao_local import IngestaoLocalStage
from dump_agent.stages.ingestao_nacional import IngestaoNacionalStage
from dump_agent.stages.processamento import ProcessamentoStage


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
    ano, mes = args.competencia or (config.COMPETENCIA_ANO, config.COMPETENCIA_MES)
    output_path = (
        (Path(args.output_dir) / config.OUTPUT_PATH.name).resolve()
        if args.output_dir
        else config.OUTPUT_PATH
    )
    return PipelineState(
        competencia_ano=ano,
        competencia_mes=mes,
        output_path=output_path,
        target_source=args.source,
    )


def main() -> int:
    """Ponto de entrada principal.

    Returns:
        int: 0 = sucesso, 1 = erro.
    """
    args = parse_args()
    configurar_logging(verbose=args.verbose)
    init_telemetry("dump-agent")
    set_tenant_id(config.COD_MUN_IBGE)
    state = _criar_estado(args)
    _storage = (
        PostgresAdapter(create_engine(config.DB_URL))
        if config.DB_URL
        else NullStoragePort()
    )
    orchestrator = PipelineOrchestrator([
        IngestaoLocalStage(),
        ProcessamentoStage(),
        IngestaoNacionalStage(),
        ExportacaoStage(_storage),
    ])
    try:
        orchestrator.executar(state)
        logging.getLogger(__name__).info("pipeline concluido competencia=%s", state.competencia_str)
        return 0
    except (EnvironmentError, FileNotFoundError) as e:
        logging.getLogger(__name__).error("erro_config=%s", e)
        return 1
    except Exception as e:
        logging.getLogger(__name__).exception("erro_inesperado=%s", e)
        return 1


async def main_streaming() -> int:
    """Ponto de entrada do modo streaming com jitter."""
    import asyncio
    import os
    import random
    import uuid

    from dump_agent.worker.consumer import run_streaming_worker

    configurar_logging(verbose=False)
    init_telemetry("dump-agent")

    api_url = os.getenv("API_BASE_URL", "http://localhost:8000")
    machine_id = os.getenv("MACHINE_ID", str(uuid.uuid4())[:8])
    jitter_max = config.MAX_JITTER_SECONDS

    startup_jitter = random.uniform(0, jitter_max)
    logging.getLogger(__name__).info(
        "startup_jitter=%.1fs", startup_jitter,
    )
    await asyncio.sleep(startup_jitter)

    await run_streaming_worker(api_url, machine_id, jitter_max)
    return 0


if __name__ == "__main__":
    import asyncio

    if "--streaming" in sys.argv:
        sys.exit(asyncio.run(main_streaming()))
    else:
        sys.exit(main())
