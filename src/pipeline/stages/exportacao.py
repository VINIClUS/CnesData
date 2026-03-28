"""ExportacaoStage — CSV, Excel, snapshot JSON e DuckDB."""
import logging
from pathlib import Path

import pandas as pd

import config
from analysis.evolution_tracker import criar_snapshot, salvar_snapshot
from export.csv_exporter import exportar_csv
from export.report_generator import gerar_relatorio
from pipeline.state import PipelineState
from storage.database_loader import DatabaseLoader

logger = logging.getLogger(__name__)


def _exportar_se_nao_vazio(df: pd.DataFrame, path: Path) -> None:
    if not df.empty:
        exportar_csv(df, path)


class ExportacaoStage:
    nome = "exportacao"

    def execute(self, state: PipelineState) -> None:
        output_dir = state.output_path.parent
        self._exportar_csvs(state, output_dir)
        self._gerar_relatorio(state)
        self._persistir_historico(state)

    def _exportar_csvs(self, state: PipelineState, output_dir: Path) -> None:
        exportar_csv(state.df_processado, state.output_path)
        _exportar_se_nao_vazio(state.df_multi_unidades, output_dir / "auditoria_rq003b_multiplas_unidades.csv")
        _exportar_se_nao_vazio(state.df_acs_incorretos, output_dir / "auditoria_rq005_acs_tacs_incorretos.csv")
        _exportar_se_nao_vazio(state.df_ace_incorretos, output_dir / "auditoria_rq005_ace_tace_incorretos.csv")
        _exportar_se_nao_vazio(state.df_ghost, output_dir / "auditoria_ghost_payroll.csv")
        _exportar_se_nao_vazio(state.df_missing, output_dir / "auditoria_missing_registration.csv")
        _exportar_se_nao_vazio(state.df_estab_fantasma, output_dir / "auditoria_rq006_estab_fantasma.csv")
        _exportar_se_nao_vazio(state.df_estab_ausente, output_dir / "auditoria_rq007_estab_ausente_local.csv")
        _exportar_se_nao_vazio(state.df_prof_fantasma, output_dir / "auditoria_rq008_prof_fantasma_cns.csv")
        _exportar_se_nao_vazio(state.df_prof_ausente, output_dir / "auditoria_rq009_prof_ausente_local_cns.csv")
        _exportar_se_nao_vazio(state.df_cbo_diverg, output_dir / "auditoria_rq010_divergencia_cbo.csv")
        _exportar_se_nao_vazio(state.df_ch_diverg, output_dir / "auditoria_rq011_divergencia_ch.csv")

    def _gerar_relatorio(self, state: PipelineState) -> None:
        gerar_relatorio(
            state.output_path.with_suffix(".xlsx"),
            {
                "principal": state.df_processado,
                "ghost": state.df_ghost,
                "missing": state.df_missing,
                "multi_unidades": state.df_multi_unidades,
                "acs_tacs": state.df_acs_incorretos,
                "ace_tace": state.df_ace_incorretos,
                "rq006_estab_fantasma": state.df_estab_fantasma,
                "rq007_estab_ausente": state.df_estab_ausente,
                "rq008_prof_fantasma": state.df_prof_fantasma,
                "rq009_prof_ausente": state.df_prof_ausente,
                "rq010_divergencia_cbo": state.df_cbo_diverg,
                "rq011_divergencia_ch": state.df_ch_diverg,
            },
            competencia=state.competencia_str,
        )

    def _persistir_historico(self, state: PipelineState) -> None:
        competencia = state.competencia_str
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
        loader = DatabaseLoader(config.DUCKDB_PATH)
        loader.inicializar_schema()
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
        logger.info("exportacao concluida output=%s", state.output_path)
