"""AuditoriaNacionalStage — cruzamento com dados nacionais BigQuery."""
import logging
from typing import Final

import config
from analysis.cascade_resolver import resolver_lag_rq006
from analysis.rules_engine import (
    detectar_divergencia_cbo,
    detectar_divergencia_carga_horaria,
    detectar_estabelecimentos_ausentes_local,
    detectar_estabelecimentos_fantasma,
    detectar_profissionais_ausentes_local,
    detectar_profissionais_fantasma,
)
from analysis.verificacao_cache import CachingVerificadorCnes
from ingestion.cnes_oficial_web_adapter import CnesOficialWebAdapter
from pipeline.state import PipelineState

logger = logging.getLogger(__name__)

_TIPOS_EXCLUIR_RQ007: Final[frozenset[str]] = frozenset({"22"})


class AuditoriaNacionalStage:
    nome = "auditoria_nacional"

    def execute(self, state: PipelineState) -> None:
        """Executa cruzamento com dados nacionais.

        Args:
            state: Estado compartilhado do pipeline.
        """
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
