"""AuditoriaStage — executa todas as regras do rules_engine."""
import logging
from typing import Final

import config
from analysis.cascade_resolver import resolver_lag_rq006
from analysis.rules_engine import (
    auditar_lotacao_ace_tace,
    auditar_lotacao_acs_tacs,
    detectar_divergencia_cbo,
    detectar_divergencia_carga_horaria,
    detectar_estabelecimentos_ausentes_local,
    detectar_estabelecimentos_fantasma,
    detectar_folha_fantasma,
    detectar_multiplas_unidades,
    detectar_profissionais_ausentes_local,
    detectar_profissionais_fantasma,
    detectar_registro_ausente,
)
from analysis.verificacao_cache import CachingVerificadorCnes
from ingestion.cnes_oficial_web_adapter import CnesOficialWebAdapter
from ingestion.hr_client import carregar_folha
from pipeline.state import PipelineState

logger = logging.getLogger(__name__)

_TIPOS_EXCLUIR_RQ007: Final[frozenset[str]] = frozenset({"22"})


class AuditoriaStage:
    nome = "auditoria"

    def execute(self, state: PipelineState) -> None:
        self._regras_locais(state)
        self._regras_hr(state)
        self._regras_nacional(state)

    def _regras_locais(self, state: PipelineState) -> None:
        state.df_multi_unidades = detectar_multiplas_unidades(state.df_processado)
        df_com_unidade = state.df_processado.merge(
            state.df_estab_local[["CNES", "TIPO_UNIDADE"]], on="CNES", how="left"
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

    def _regras_nacional(self, state: PipelineState) -> None:
        if not state.executar_nacional:
            return
        if state.df_estab_nacional.empty and state.df_prof_nacional.empty:
            logger.warning(
                "nacional_cross_check=skipped motivo=dados_nacionais_vazios "
                "competencia=%s", state.competencia_str,
            )
            return
        self._cruzar_estabelecimentos(state)
        self._cruzar_profissionais(state)

    def _cruzar_estabelecimentos(self, state: PipelineState) -> None:
        if state.df_estab_nacional.empty:
            return
        state.df_estab_fantasma = detectar_estabelecimentos_fantasma(
            state.df_estab_local, state.df_estab_nacional
        )
        state.df_estab_ausente = detectar_estabelecimentos_ausentes_local(
            state.df_estab_local,
            state.df_estab_nacional,
            tipos_excluir=_TIPOS_EXCLUIR_RQ007,
        )
        if not state.df_estab_fantasma.empty:
            _adapter = CachingVerificadorCnes(
                CnesOficialWebAdapter(),
                config.CACHE_DIR / "cnes_verificados.json",
            )
            state.df_estab_fantasma = resolver_lag_rq006(state.df_estab_fantasma, _adapter)

    def _cruzar_profissionais(self, state: PipelineState) -> None:
        if state.df_prof_nacional.empty:
            return
        state.df_prof_fantasma = detectar_profissionais_fantasma(
            state.df_processado, state.df_prof_nacional
        )
        cnes_excluir = (
            frozenset(state.df_estab_ausente["CNES"])
            if not state.df_estab_ausente.empty
            else frozenset()
        )
        state.df_prof_ausente = detectar_profissionais_ausentes_local(
            state.df_processado, state.df_prof_nacional, cnes_excluir=cnes_excluir
        )
        state.df_cbo_diverg = detectar_divergencia_cbo(
            state.df_processado, state.df_prof_nacional, cbo_lookup=state.cbo_lookup
        )
        state.df_ch_diverg = detectar_divergencia_carga_horaria(
            state.df_processado, state.df_prof_nacional
        )
