"""IngestaoLocalStage — ingere do Firebird ou carrega snapshot local imutável."""
import logging
from pathlib import Path

from contracts.schemas import EstabelecimentoContract, ProfissionalContract
from ingestion.cnes_client import conectar, extrair_lookup_cbo
from ingestion.cnes_local_adapter import CnesLocalAdapter
from pipeline.state import PipelineState
from storage.snapshot_local import carregar_snapshot, snapshot_existe

logger = logging.getLogger(__name__)


class IngestaoLocalStage:
    nome = "ingestao_local"

    def __init__(self, historico_dir: Path) -> None:
        self._historico_dir = historico_dir

    def execute(self, state: PipelineState) -> None:
        if not state.force_reingestao and snapshot_existe(
            state.competencia_str, self._historico_dir
        ):
            snap = carregar_snapshot(state.competencia_str, self._historico_dir)
            state.df_prof_local = snap.df_prof
            state.df_estab_local = snap.df_estab
            state.cbo_lookup = snap.cbo_lookup
            state.snapshot_carregado = True
            logger.info("snapshot_local carregado competencia=%s", state.competencia_str)
            return

        state.con = conectar()
        state.cbo_lookup = extrair_lookup_cbo(state.con)
        repo = CnesLocalAdapter(state.con)
        state.df_prof_local = repo.listar_profissionais()
        state.df_estab_local = repo.listar_estabelecimentos()
        ProfissionalContract.validate(state.df_prof_local, lazy=False)
        EstabelecimentoContract.validate(state.df_estab_local, lazy=False)
        logger.info(
            "ingestao_local profissionais=%d estabelecimentos=%d",
            len(state.df_prof_local),
            len(state.df_estab_local),
        )
