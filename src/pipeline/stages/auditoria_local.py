"""AuditoriaLocalStage — regras locais e cruzamento com folha de RH."""
import logging

import config
from analysis.rules_engine import (
    auditar_lotacao_ace_tace,
    auditar_lotacao_acs_tacs,
    detectar_folha_fantasma,
    detectar_multiplas_unidades,
    detectar_registro_ausente,
)
from ingestion.hr_client import carregar_folha
from pipeline.state import PipelineState

logger = logging.getLogger(__name__)


class AuditoriaLocalStage:
    nome = "auditoria_local"

    def execute(self, state: PipelineState) -> None:
        """Executa regras locais/nacionais e cruzamento HR.

        Args:
            state: Estado compartilhado do pipeline.
        """
        if state.local_disponivel:
            self._regras_com_dados_locais(state)
        elif state.nacional_disponivel:
            self._regras_com_dados_nacionais(state)
        else:
            logger.info(
                "auditoria_local=skipped motivo=sem_dados competencia=%s",
                state.competencia_str,
            )
        self._regras_hr(state)

    def _regras_com_dados_locais(self, state: PipelineState) -> None:
        state.df_multi_unidades = detectar_multiplas_unidades(state.df_processado)
        df_com_unidade = state.df_processado.merge(
            state.df_estab_local[["CNES", "TIPO_UNIDADE"]], on="CNES", how="left"
        )
        state.df_acs_incorretos = auditar_lotacao_acs_tacs(df_com_unidade)
        state.df_ace_incorretos = auditar_lotacao_ace_tace(df_com_unidade)

    def _regras_com_dados_nacionais(self, state: PipelineState) -> None:
        state.df_multi_unidades = detectar_multiplas_unidades(state.df_prof_nacional, id_col="CNS")
        df_com_unidade = state.df_prof_nacional.merge(
            state.df_estab_nacional[["CNES", "TIPO_UNIDADE"]], on="CNES", how="left"
        )
        state.df_acs_incorretos = auditar_lotacao_acs_tacs(df_com_unidade)
        state.df_ace_incorretos = auditar_lotacao_ace_tace(df_com_unidade)

    def _regras_hr(self, state: PipelineState) -> None:
        if not state.executar_hr:
            logger.warning("hr_cross_check=skipped motivo=executar_hr=False")
            return
        if not config.FOLHA_HR_PATH or not config.FOLHA_HR_PATH.exists():
            raise FileNotFoundError(
                f"Arquivo ausente: {config.FOLHA_HR_PATH}. "
                "Execute scripts/hr_pre_processor.py para gerar hr_padronizado.csv."
            )
        df_rh = carregar_folha(config.FOLHA_HR_PATH)
        state.df_ghost = detectar_folha_fantasma(state.df_processado, df_rh)
        state.df_missing = detectar_registro_ausente(state.df_processado, df_rh)
