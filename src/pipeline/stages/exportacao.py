"""ExportacaoStage — persiste profissionais e estabelecimentos via StoragePort."""
import logging

from pipeline.orchestrator import StageFatalError
from pipeline.state import PipelineState
from storage.ports import StoragePort

logger = logging.getLogger(__name__)


def _derivar_status(target: str, local_vazio: bool, nacional_vazio: bool) -> str:
    if target == "LOCAL":
        return "local_exportado"
    if target == "NACIONAL":
        return "nacional_exportado"
    if not local_vazio and not nacional_vazio:
        return "completo"
    if not local_vazio:
        return "parcial"
    return "sem_dados_locais"


class ExportacaoStage:
    nome = "exportacao"
    critico = True

    def __init__(self, storage: StoragePort) -> None:
        self._storage = storage

    def execute(self, state: PipelineState) -> None:
        competencia = state.competencia_str
        local_vazio = state.df_processado.empty
        nacional_vazio = state.df_prof_nacional.empty

        if local_vazio and nacional_vazio:
            raise StageFatalError("exportacao_alcancada_com_dataframes_vazios")

        if not local_vazio:
            self._storage.gravar_estabelecimentos(competencia, state.df_estab_local)
            self._storage.gravar_profissionais(competencia, state.df_processado)
            logger.info(
                "exportacao fonte=LOCAL competencia=%s profissionais=%d estabelecimentos=%d",
                competencia, len(state.df_processado), len(state.df_estab_local),
            )

        if not nacional_vazio:
            self._storage.gravar_estabelecimentos(competencia, state.df_estab_nacional)
            self._storage.gravar_profissionais(competencia, state.df_prof_nacional)
            logger.info(
                "exportacao fonte=NACIONAL competencia=%s profissionais=%d estabelecimentos=%d",
                competencia, len(state.df_prof_nacional), len(state.df_estab_nacional),
            )

        status = _derivar_status(state.target_source, local_vazio, nacional_vazio)
        self._storage.registrar_pipeline_run(competencia, {"status": status})
