"""ExportacaoStage — persistência PostgreSQL, snapshots JSON. Sem CSV/XLSX em disco."""
import json
import logging
from datetime import datetime
from pathlib import Path

import config
from analysis.evolution_tracker import criar_snapshot, salvar_snapshot
from pipeline.state import PipelineState
from storage.ports import StoragePort

logger = logging.getLogger(__name__)


def _gravar_last_run(state: PipelineState, last_run_path: Path) -> None:
    agora = datetime.now().isoformat(timespec="seconds")
    nacional_ok = state.executar_nacional and not state.df_prof_nacional.empty
    hr_ok = state.executar_hr
    dados = {
        "firebird": {"ts": agora, "ok": state.local_disponivel},
        "bigquery": {"ts": agora if nacional_ok else None, "ok": nacional_ok},
        "hr": {"ts": agora if hr_ok else None, "ok": hr_ok if state.executar_hr else None},
        "postgres": {"ts": agora, "ok": True},
    }
    last_run_path.parent.mkdir(parents=True, exist_ok=True)
    last_run_path.write_text(json.dumps(dados, ensure_ascii=False, indent=2), encoding="utf-8")


def _status_pipeline(state: PipelineState) -> str:
    if state.pipeline_status_override:
        return state.pipeline_status_override
    if state.local_disponivel and state.nacional_disponivel:
        return "completo"
    if state.local_disponivel:
        return "parcial"
    if state.nacional_disponivel:
        return "sem_dados_locais"
    return "sem_dados"


class ExportacaoStage:
    nome = "exportacao"
    critico = False

    def __init__(self, storage: StoragePort) -> None:
        self._storage = storage

    def execute(self, state: PipelineState) -> None:
        self._persistir_historico(state)

    def _persistir_historico(self, state: PipelineState) -> None:
        competencia = state.competencia_str

        if not state.df_processado.empty:
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

        if not state.local_disponivel and not state.df_processado.empty:
            self._storage.gravar_profissionais(competencia, state.df_processado)
            self._storage.gravar_estabelecimentos(competencia, state.df_estab_nacional)

        _gravar_last_run(state, config.LAST_RUN_PATH)
        self._storage.registrar_pipeline_run(competencia, {"status": _status_pipeline(state)})
        logger.info("exportacao concluida competencia=%s", competencia)
