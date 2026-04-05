"""ExportacaoStage — persistência DuckDB, JSON e pipeline_runs. Sem CSV/XLSX em disco."""
import json
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd

import config
from analysis.evolution_tracker import criar_snapshot, salvar_snapshot
from pipeline.state import PipelineState
from storage.database_loader import DatabaseLoader

logger = logging.getLogger(__name__)


def _gravar_last_run(state: PipelineState, last_run_path: Path) -> None:
    agora = datetime.now().isoformat(timespec="seconds")
    nacional_ok = state.executar_nacional and not state.df_prof_nacional.empty
    hr_ok = state.executar_hr
    dados = {
        "firebird": {"ts": agora, "ok": state.local_disponivel},
        "bigquery": {"ts": agora if nacional_ok else None, "ok": nacional_ok},
        "hr": {"ts": agora if hr_ok else None, "ok": hr_ok if state.executar_hr else None},
        "duckdb": {"ts": agora, "ok": True},
    }
    last_run_path.parent.mkdir(parents=True, exist_ok=True)
    last_run_path.write_text(json.dumps(dados, ensure_ascii=False, indent=2), encoding="utf-8")


def _status_pipeline(state: PipelineState) -> str:
    if state.local_disponivel and state.nacional_disponivel:
        return "completo"
    if state.local_disponivel:
        return "parcial"
    if state.nacional_disponivel:
        return "sem_dados_locais"
    return "sem_dados"


class ExportacaoStage:
    nome = "exportacao"

    def execute(self, state: PipelineState) -> None:
        self._persistir_historico(state)

    def _persistir_historico(self, state: PipelineState) -> None:
        competencia = state.competencia_str
        loader = DatabaseLoader(config.DUCKDB_PATH)
        loader.inicializar_schema()

        if state.local_disponivel:
            snapshot = criar_snapshot(
                competencia,
                state.df_processado,
                state.df_ghost,
                state.df_missing,
                state.df_multi_unidades,
                state.df_acs_incorretos,
                state.df_ace_incorretos,
            )
            salvar_snapshot(snapshot, config.SNAPSHOTS_DIR)
            loader.gravar_metricas(snapshot)
            loader.gravar_auditoria(competencia, "GHOST", snapshot.total_ghost)
            loader.gravar_auditoria(competencia, "MISSING", snapshot.total_missing)
            loader.gravar_auditoria(competencia, "RQ005", snapshot.total_rq005)
            loader.gravar_auditoria(competencia, "RQ003B", len(state.df_multi_unidades))
            loader.gravar_auditoria(competencia, "RQ005_ACS", len(state.df_acs_incorretos))
            loader.gravar_auditoria(competencia, "RQ005_ACE", len(state.df_ace_incorretos))
            loader.gravar_auditoria(competencia, "RQ006", len(state.df_estab_fantasma))
            loader.gravar_auditoria(competencia, "RQ007", len(state.df_estab_ausente))
            loader.gravar_auditoria(competencia, "RQ008", len(state.df_prof_fantasma))
            loader.gravar_auditoria(competencia, "RQ009", len(state.df_prof_ausente))
            loader.gravar_auditoria(competencia, "RQ010", len(state.df_cbo_diverg))
            loader.gravar_auditoria(competencia, "RQ011", len(state.df_ch_diverg))

        _gravar_last_run(state, config.LAST_RUN_PATH)
        loader.gravar_pipeline_run(
            competencia,
            state.local_disponivel,
            state.nacional_disponivel,
            state.executar_hr,
            _status_pipeline(state),
        )
        logger.info("exportacao concluida competencia=%s", competencia)
