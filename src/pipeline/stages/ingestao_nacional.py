"""IngestaoNacionalStage — ingere BigQuery com soft-fail e circuit breaker."""
import logging
from concurrent.futures import ThreadPoolExecutor

import config
from ingestion.cnes_nacional_adapter import CnesNacionalAdapter
from pipeline.circuit_breaker import CircuitBreaker, CircuitBreakerAberto
from pipeline.state import PipelineState

logger = logging.getLogger(__name__)


class IngestaoNacionalStage:
    critico = False
    nome = "ingestao_nacional"

    def __init__(self) -> None:
        pass

    def execute(self, state: PipelineState) -> None:
        if not state.executar_nacional:
            logger.warning("nacional_skipped motivo=skip_nacional_flag")
            return
        breaker = CircuitBreaker(failure_threshold=3, service_name="DATASUS")
        try:
            breaker.call(self._buscar, state)
            state.nacional_disponivel = (
                not state.df_prof_nacional.empty or not state.df_estab_nacional.empty
            )
        except CircuitBreakerAberto:
            logger.error("nacional_skipped source=DATASUS status=UNAVAILABLE")
            state.nacional_disponivel = False
        except Exception as exc:
            logger.warning("nacional_skipped motivo=%s", exc)
            state.nacional_disponivel = False

    def _buscar(self, state: PipelineState) -> None:
        repo = CnesNacionalAdapter(
            config.GCP_PROJECT_ID,
            config.ID_MUNICIPIO_IBGE7,
            cache_dir=config.CACHE_DIR,
        )
        competencia = (state.competencia_ano, state.competencia_mes)
        with ThreadPoolExecutor(max_workers=2) as pool:
            fut_prof = pool.submit(repo.listar_profissionais, competencia)
            fut_estab = pool.submit(repo.listar_estabelecimentos, competencia)
        state.df_prof_nacional = fut_prof.result()
        state.df_estab_nacional = fut_estab.result()
        logger.info(
            "ingestao_nacional competencia=%s prof=%d estab=%d",
            state.competencia_str,
            len(state.df_prof_nacional),
            len(state.df_estab_nacional),
        )
