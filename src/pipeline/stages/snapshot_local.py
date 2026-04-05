"""SnapshotLocalStage — persiste snapshot pós-processamento em parquet e DuckDB."""
import json
import logging
from pathlib import Path

from analysis.delta_snapshot import DeltaSnapshot, calcular_delta
from pipeline.state import PipelineState
from storage.database_loader import DatabaseLoader
from storage.snapshot_local import (
    SnapshotLocal,
    carregar_snapshot,
    salvar_snapshot,
    snapshot_existe,
)

logger = logging.getLogger(__name__)


class SnapshotLocalStage:
    nome = "snapshot_local"

    def __init__(self, historico_dir: Path, db_loader: DatabaseLoader) -> None:
        self._historico_dir = historico_dir
        self._db = db_loader

    def execute(self, state: PipelineState) -> None:
        if state.snapshot_carregado:
            return

        competencia = state.competencia_str
        if state.force_reingestao and snapshot_existe(competencia, self._historico_dir):
            snap_anterior = carregar_snapshot(competencia, self._historico_dir)
            delta = calcular_delta(state.df_processado, snap_anterior.df_prof)
            state.delta_local = _delta_para_dict(delta)
            logger.info(
                "delta_snapshot calculado competencia=%s novos=%d removidos=%d alterados=%d",
                competencia, delta.n_novos, delta.n_removidos, delta.n_alterados,
            )

        snap = SnapshotLocal(
            df_prof=state.df_processado,
            df_estab=state.df_estab_local,
            cbo_lookup=state.cbo_lookup,
        )
        salvar_snapshot(competencia, self._historico_dir, snap)
        self._db.gravar_profissionais(competencia, state.df_processado)
        self._db.gravar_estabelecimentos(competencia, state.df_estab_local)
        self._db.gravar_cbo_lookup(competencia, state.cbo_lookup)
        logger.info("snapshot_local salvo competencia=%s", competencia)


def _delta_para_dict(delta: DeltaSnapshot) -> dict:
    return {
        "n_novos": delta.n_novos,
        "n_removidos": delta.n_removidos,
        "n_alterados": delta.n_alterados,
        "novos_json": json.dumps(delta.novos, ensure_ascii=False, default=str),
        "removidos_json": json.dumps(delta.removidos, ensure_ascii=False, default=str),
        "alterados_json": json.dumps(delta.alterados, ensure_ascii=False, default=str),
    }
