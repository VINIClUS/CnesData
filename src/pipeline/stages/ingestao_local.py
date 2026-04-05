"""IngestaoLocalStage — DuckDB-first, parquet backfill, Firebird apenas no período atual."""
import logging
from pathlib import Path

from contracts.schemas import EstabelecimentoContract, ProfissionalContract
from ingestion.cnes_client import conectar, extrair_lookup_cbo
from ingestion.cnes_local_adapter import CnesLocalAdapter
from pipeline.state import PipelineState
from storage.competencia_utils import periodo_atual
from storage.database_loader import DatabaseLoader
from storage.snapshot_local import SnapshotLocal, carregar_snapshot, snapshot_existe

logger = logging.getLogger(__name__)


class IngestaoLocalStage:
    nome = "ingestao_local"

    def __init__(self, historico_dir: Path, db_loader: DatabaseLoader) -> None:
        self._historico_dir = historico_dir
        self._db = db_loader

    def execute(self, state: PipelineState) -> None:
        competencia = state.competencia_str
        eh_periodo_atual = competencia == periodo_atual()

        if state.force_reingestao and eh_periodo_atual:
            self._ingerir_do_firebird(state)
            return

        if state.force_reingestao and not eh_periodo_atual:
            logger.warning("force_reingestao_ignorado_periodo_passado competencia=%s", competencia)

        if self._db.profissional_existe(competencia):
            self._carregar_do_duckdb(state)
            return

        if snapshot_existe(competencia, self._historico_dir):
            self._backfill_do_parquet(state)
            return

        if eh_periodo_atual:
            self._ingerir_do_firebird(state)
            return

        state.local_disponivel = False
        logger.info("dados_locais_indisponiveis competencia=%s", competencia)

    def _carregar_do_duckdb(self, state: PipelineState) -> None:
        competencia = state.competencia_str
        state.df_prof_local = self._db.carregar_profissionais(competencia)
        state.df_estab_local = self._db.carregar_estabelecimentos(competencia)
        state.cbo_lookup = self._db.carregar_cbo_lookup(competencia)
        state.snapshot_carregado = True
        state.local_disponivel = True
        logger.info("local_duckdb carregado competencia=%s", competencia)

    def _backfill_do_parquet(self, state: PipelineState) -> None:
        competencia = state.competencia_str
        snap = carregar_snapshot(competencia, self._historico_dir)
        state.df_prof_local = snap.df_prof
        state.df_estab_local = snap.df_estab
        state.cbo_lookup = snap.cbo_lookup
        self._db.gravar_profissionais(competencia, snap.df_prof)
        self._db.gravar_estabelecimentos(competencia, snap.df_estab)
        self._db.gravar_cbo_lookup(competencia, snap.cbo_lookup)
        state.snapshot_carregado = True
        state.local_disponivel = True
        logger.info("local_parquet_backfill competencia=%s", competencia)

    def _ingerir_do_firebird(self, state: PipelineState) -> None:
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
