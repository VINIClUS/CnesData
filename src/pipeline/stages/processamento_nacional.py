"""ProcessamentoNacionalStage — normaliza df_nacional → df_processado quando local ausente."""
import logging

import pandas as pd

from pipeline.state import PipelineState

logger = logging.getLogger(__name__)

_COLUNAS_TEXTO = ("CNS", "NOME_PROFISSIONAL", "CBO", "CNES", "TIPO_VINCULO", "SUS")


class ProcessamentoNacionalStage:
    """Normaliza dados nacionais quando fonte local está indisponível."""
    critico = False

    nome = "processamento_nacional"

    def execute(self, state: PipelineState) -> None:
        """
        Executa normalização do df_prof_nacional para df_processado.

        Args:
            state: Estado do pipeline com dados nacionais carregados.
        """
        if state.local_disponivel or not state.nacional_disponivel:
            return
        if state.df_prof_nacional.empty:
            logger.info(
                "processamento_nacional=skipped motivo=df_nacional_vazio competencia=%s",
                state.competencia_str,
            )
            return
        state.df_processado = self._normalizar(state.df_prof_nacional, state.cbo_lookup)
        logger.info("processamento_nacional registros=%d", len(state.df_processado))

    def _normalizar(self, df: pd.DataFrame, cbo_lookup: dict[str, str]) -> pd.DataFrame:
        resultado = df.copy()
        for col in _COLUNAS_TEXTO:
            if col in resultado.columns:
                resultado[col] = resultado[col].astype(str).str.strip()
        ch = pd.to_numeric(
            resultado.get("CH_TOTAL", pd.Series(dtype=int)), errors="coerce"
        ).fillna(0)
        resultado["ALERTA_STATUS_CH"] = ch.apply(lambda v: "ATIVO_SEM_CH" if v == 0 else "OK")
        resultado["DESCRICAO_CBO"] = resultado["CBO"].map(cbo_lookup).fillna("CBO NAO CATALOGADO")
        return resultado
