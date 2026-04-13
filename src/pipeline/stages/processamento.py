"""ProcessamentoStage — limpeza CPF, datas ISO, dedup."""
import logging

from processing.transformer import transformar
from pipeline.state import PipelineState

logger = logging.getLogger(__name__)


class ProcessamentoStage:
    nome = "processamento"
    critico = False

    def execute(self, state: PipelineState) -> None:
        if state.df_prof_local.is_empty():
            return
        state.df_processado = transformar(
            state.df_prof_local, cbo_lookup=state.cbo_lookup
        )
        logger.info("processamento registros=%d", len(state.df_processado))
