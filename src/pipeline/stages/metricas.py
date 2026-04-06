"""MetricasStage — glosas, histórico e métricas avançadas."""
import json
import logging
from datetime import datetime

import pandas as pd

from analysis.glosas_builder import construir_glosas
from analysis.metricas_avancadas import (
    calcular_anomalias_por_cbo,
    calcular_p90_ch,
    calcular_proporcao_feminina,
    calcular_proporcao_feminina_por_cnes,
    calcular_ranking_cnes,
    calcular_reincidencia,
    calcular_taxa_anomalia,
    calcular_taxa_resolucao,
    calcular_top_glosas,
    calcular_velocidade_regularizacao,
)
from pipeline.state import PipelineState
from storage.database_loader import DatabaseLoader
from storage.historico_reader import HistoricoReader

logger = logging.getLogger(__name__)


class MetricasStage:
    nome = "metricas"

    def __init__(self, db_loader: DatabaseLoader, historico_reader: HistoricoReader) -> None:
        self._db = db_loader
        self._reader = historico_reader

    def execute(self, state: PipelineState) -> None:
        """Calcula e persiste glosas e métricas avançadas.

        Args:
            state: Estado do pipeline com DataFrames de auditoria preenchidos.
        """
        if state.df_processado.empty:
            logger.info("metricas=skipped motivo=df_processado_vazio competencia=%s", state.competencia_str)
            return
        agora = datetime.now()
        competencia = state.competencia_str

        df_glosas = construir_glosas(competencia, state, agora)
        self._persistir_glosas(competencia, df_glosas)

        df_historico = self._reader.carregar_glosas_historicas()
        metricas = self._calcular_metricas(state, df_glosas, df_historico)

        self._db.gravar_metricas_avancadas(competencia, metricas)
        state.metricas_avancadas = metricas
        if state.delta_local:
            self._db.gravar_delta_snapshot(competencia, state.delta_local)
            logger.info("delta_snapshot gravado competencia=%s", competencia)
        logger.info("action=metricas_concluidas competencia=%s", competencia)

    def _persistir_glosas(self, competencia: str, df_glosas: pd.DataFrame) -> None:
        if df_glosas.empty:
            return
        for regra in df_glosas["regra"].unique():
            self._db.gravar_glosas(competencia, regra, df_glosas[df_glosas["regra"] == regra])

    def _calcular_metricas(
        self, state: PipelineState, df_glosas: pd.DataFrame, df_historico: pd.DataFrame
    ) -> dict:
        competencias = self._reader.listar_competencias()
        competencia = state.competencia_str
        idx = competencias.index(competencia) if competencia in competencias else -1
        comp_anterior = competencias[idx - 1] if idx > 0 else ""

        taxa_resolucao = (
            calcular_taxa_resolucao(comp_anterior, competencia, df_historico)
            if comp_anterior
            else 0.0
        )

        return {
            "taxa_anomalia_geral": calcular_taxa_anomalia(state.df_processado, df_glosas),
            "p90_ch_total": calcular_p90_ch(state.df_processado),
            "proporcao_feminina_geral": calcular_proporcao_feminina(state.df_processado),
            "n_reincidentes": calcular_reincidencia(df_historico),
            "taxa_resolucao": taxa_resolucao,
            "velocidade_regularizacao_media": calcular_velocidade_regularizacao(df_historico),
            "top_glosas_json": json.dumps(calcular_top_glosas(df_glosas), ensure_ascii=False),
            "anomalias_por_cbo_json": json.dumps(
                calcular_anomalias_por_cbo(state.df_processado, df_glosas, state.cbo_lookup),
                ensure_ascii=False,
            ),
            "proporcao_feminina_por_cnes_json": json.dumps(
                calcular_proporcao_feminina_por_cnes(state.df_processado), ensure_ascii=False
            ),
            "ranking_cnes_json": json.dumps(
                calcular_ranking_cnes(state.df_estab_local, df_glosas, state.df_processado),
                ensure_ascii=False,
            ),
        }
