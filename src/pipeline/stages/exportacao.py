"""ExportacaoStage — persistência PostgreSQL com logs estruturados."""
import logging

from pipeline.state import PipelineState
from storage.ports import StoragePort

logger = logging.getLogger(__name__)


def _status_pipeline(state: PipelineState) -> str:
    if state.local_disponivel and state.nacional_disponivel:
        return "completo"
    if state.local_disponivel:
        return "parcial"
    if state.nacional_disponivel:
        return "sem_dados_locais"
    return "sem_dados"


class ExportacaoStage:
    nome = "exportacao"
    critico = False

    def __init__(self, storage: StoragePort) -> None:
        self._storage = storage

    def execute(self, state: PipelineState) -> None:
        competencia = state.competencia_str

        if state.local_disponivel and not state.df_processado.empty:
            self._storage.gravar_profissionais(competencia, state.df_processado)
            self._storage.gravar_estabelecimentos(competencia, state.df_estab_local)
            logger.info(
                "exportacao fonte=LOCAL competencia=%s prof=%d estab=%d",
                competencia,
                len(state.df_processado),
                len(state.df_estab_local),
            )

        if state.nacional_disponivel and not state.df_prof_nacional.empty:
            self._storage.gravar_profissionais(competencia, state.df_prof_nacional)
            self._storage.gravar_estabelecimentos(competencia, state.df_estab_nacional)
            logger.info(
                "exportacao fonte=NACIONAL competencia=%s prof=%d estab=%d",
                competencia,
                len(state.df_prof_nacional),
                len(state.df_estab_nacional),
            )

        status = _status_pipeline(state)
        self._storage.registrar_pipeline_run(competencia, {"status": status})
        logger.info("pipeline_run competencia=%s status=%s", competencia, status)
