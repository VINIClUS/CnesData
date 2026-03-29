"""IngestaoNacionalStage — ingere BigQuery com soft-fail e cache pickle."""
import logging
from concurrent.futures import ThreadPoolExecutor

import config
from ingestion.cnes_nacional_adapter import CnesNacionalAdapter
from pipeline.state import PipelineState

logger = logging.getLogger(__name__)


class IngestaoNacionalStage:
    nome = "ingestao_nacional"

    def execute(self, state: PipelineState) -> None:
        if not state.executar_nacional:
            logger.warning("nacional_cross_check=skipped motivo=skip_nacional_flag")
            return
        try:
            self._buscar(state)
        except Exception as exc:
            logger.warning("nacional_cross_check=skipped motivo=%s", exc)

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
            "ingestao_nacional profissionais=%d estabelecimentos=%d",
            len(state.df_prof_nacional),
            len(state.df_estab_nacional),
        )
