"""IngestaoNacionalStage — ingere BigQuery com soft-fail, TTL e fingerprint SHA256."""
import hashlib
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import pandas as pd

import config
from ingestion.cnes_nacional_adapter import CnesNacionalAdapter
from pipeline.state import PipelineState
from storage.database_loader import DatabaseLoader

logger = logging.getLogger(__name__)


def _computar_fingerprint(df: pd.DataFrame) -> str:
    """SHA256 de CPF+CBO+CNES ordenado de df_processado.

    Args:
        df: DataFrame processado com colunas CPF, CBO, CNES.

    Returns:
        Hex digest SHA256.
    """
    cols = [c for c in ("CPF", "CBO", "CNES") if c in df.columns]
    chaves = sorted(df[cols].fillna("").apply(lambda r: "|".join(r), axis=1))
    return hashlib.sha256("\n".join(chaves).encode()).hexdigest()


class IngestaoNacionalStage:
    nome = "ingestao_nacional"

    def __init__(self, db_loader: DatabaseLoader) -> None:
        self._db = db_loader

    def execute(self, state: PipelineState) -> None:
        if not state.executar_nacional:
            logger.warning("nacional_cross_check=skipped motivo=skip_nacional_flag")
            return
        df_para_fingerprint = (
            state.df_processado
            if not state.df_processado.empty
            else state.df_prof_nacional
        )
        fingerprint = (
            "" if df_para_fingerprint.empty else _computar_fingerprint(df_para_fingerprint)
        )
        state.fingerprint_local = fingerprint
        if fingerprint and self._cache_valido(state.competencia_str, fingerprint):
            logger.info("cache_nacional=hit competencia=%s", state.competencia_str)
            return
        try:
            self._buscar(state)
        except Exception as exc:
            logger.warning("nacional_cross_check=skipped motivo=%s", exc)
            return
        if fingerprint:
            self._db.gravar_cache_nacional(state.competencia_str, fingerprint)
        state.nacional_validado = True
        state.nacional_disponivel = (
            not state.df_prof_nacional.empty or not state.df_estab_nacional.empty
        )

    def _cache_valido(self, competencia: str, fingerprint: str) -> bool:
        cache = self._db.ler_cache_nacional(competencia)
        if cache is None:
            return False
        cached_fp, gravado_em = cache
        age_days = (datetime.now() - gravado_em).days
        return cached_fp == fingerprint and age_days < config.NACIONAL_CACHE_TTL_DIAS

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
