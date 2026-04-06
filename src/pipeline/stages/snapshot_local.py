"""SnapshotLocalStage — persiste snapshot pós-processamento em parquet e DuckDB."""
import logging
from pathlib import Path

import pandas as pd

from analysis.delta_snapshot import DeltaSnapshot, calcular_delta
from pipeline.state import PipelineState
from storage.database_loader import DatabaseLoader
from storage.snapshot_local import SnapshotLocal, salvar_snapshot

logger = logging.getLogger(__name__)


class SnapshotLocalStage:
    """Persiste snapshot local e computa delta contra competência anterior no DuckDB."""

    def __init__(self, historico_dir: Path, db_loader: DatabaseLoader) -> None:
        self._historico_dir = historico_dir
        self._db = db_loader

    def execute(self, state: PipelineState) -> None:
        if not state.local_disponivel:
            return
        if state.snapshot_carregado:
            return

        competencia = state.competencia_str
        delta = self._computar_delta(competencia, state.df_processado)
        state.delta_local = _delta_para_dict(delta)

        snap = SnapshotLocal(
            df_prof=state.df_processado,
            df_estab=state.df_estab_local,
            cbo_lookup=state.cbo_lookup,
        )
        salvar_snapshot(competencia, self._historico_dir, snap)
        self._db.gravar_profissionais(competencia, state.df_processado)
        self._db.gravar_estabelecimentos(competencia, state.df_estab_local)
        self._db.gravar_cbo_lookup(competencia, state.cbo_lookup)
        logger.info("snapshot_local competencia=%s", competencia)

    def _computar_delta(self, competencia: str, df_atual: pd.DataFrame) -> DeltaSnapshot:
        competencias = self._db.listar_competencias()
        anteriores = [c for c in competencias if c < competencia]
        if not anteriores:
            return calcular_delta(df_atual, pd.DataFrame(columns=df_atual.columns))
        df_anterior = self._db.carregar_profissionais(anteriores[-1])
        delta = calcular_delta(df_atual, df_anterior)
        logger.info(
            "delta_snapshot competencia=%s novos=%d removidos=%d alterados=%d",
            competencia, delta.n_novos, delta.n_removidos, delta.n_alterados,
        )
        return delta


def _delta_para_dict(delta: DeltaSnapshot) -> dict:
    return {
        "n_novos": delta.n_novos,
        "n_removidos": delta.n_removidos,
        "n_alterados": delta.n_alterados,
        "novos": delta.novos,
        "removidos": delta.removidos,
        "alterados": delta.alterados,
    }
