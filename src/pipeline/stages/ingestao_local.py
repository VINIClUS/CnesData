"""IngestaoLocalStage — parquet backfill, Firebird para período atual."""
import logging
from pathlib import Path

import pandas as pd
from pandera.errors import SchemaErrors

from contracts.schemas import EstabelecimentoContract, ProfissionalContract
import config
from ingestion.cnes_client import conectar, dump_vinculos_para_parquet, extrair_lookup_cbo
from ingestion.cnes_local_adapter import CnesLocalAdapter
from ingestion.quarantine import QuarantineBuffer, quarentinar_linhas
from pipeline.orchestrator import StageSkipError
from pipeline.state import PipelineState
from storage.competencia_utils import periodo_atual
from storage.snapshot_local import carregar_snapshot, snapshot_existe

logger = logging.getLogger(__name__)


class IngestaoLocalStage:
    nome = "ingestao_local"
    critico = True

    def __init__(self, historico_dir: Path) -> None:
        self._historico_dir = historico_dir

    def execute(self, state: PipelineState) -> None:
        if state.target_source == "NACIONAL":
            return
        competencia = state.competencia_str
        eh_periodo_atual = competencia == periodo_atual()

        if state.force_reingestao and eh_periodo_atual:
            self._ingerir_do_firebird(state)
            return

        if state.force_reingestao and not eh_periodo_atual:
            logger.warning("force_reingestao_ignorado periodo_passado competencia=%s", competencia)

        if snapshot_existe(competencia, self._historico_dir):
            self._carregar_do_parquet(state)
            return

        if eh_periodo_atual:
            self._ingerir_do_firebird(state)
            return

        raise StageSkipError(
            f"dados_locais_indisponiveis competencia={competencia} "
            f"target_source={state.target_source}"
        )

    def _carregar_do_parquet(self, state: PipelineState) -> None:
        competencia = state.competencia_str
        snap = carregar_snapshot(competencia, self._historico_dir)
        state.df_prof_local = snap.df_prof
        state.df_estab_local = snap.df_estab
        state.cbo_lookup = snap.cbo_lookup
        logger.info(
            "local_parquet competencia=%s prof=%d estab=%d",
            competencia,
            len(snap.df_prof),
            len(snap.df_estab),
        )

    def _ingerir_do_firebird(self, state: PipelineState) -> None:
        con = conectar()
        try:
            state.cbo_lookup = extrair_lookup_cbo(con)
            repo = CnesLocalAdapter(con)
            dump_dir = config.RAIZ_PROJETO / "data" / "tmp"
            parquet_path = dump_vinculos_para_parquet(con, dump_dir, state.competencia_str)
            df_estab = repo.listar_estabelecimentos()
        finally:
            con.close()
        df_prof = pd.read_parquet(parquet_path, engine="pyarrow")
        buffer = QuarantineBuffer()
        df_prof = _validar_com_dlq(df_prof, ProfissionalContract, buffer, state.competencia_str, "FIREBIRD", "CPF")
        df_estab = _validar_com_dlq(df_estab, EstabelecimentoContract, buffer, state.competencia_str, "FIREBIRD", "CNES")
        if df_prof.empty:
            raise RuntimeError("ingestao_local abortada: todos os profissionais rejeitados pelo DLQ")
        state.df_prof_local = df_prof
        state.df_estab_local = df_estab
        state.quarantine_buffer = buffer
        logger.info(
            "ingestao_local_firebird competencia=%s prof=%d estab=%d quarentenados=%d",
            state.competencia_str,
            len(state.df_prof_local),
            len(state.df_estab_local),
            len(buffer),
        )


def _validar_com_dlq(
    df: pd.DataFrame,
    contract,
    buffer: QuarantineBuffer,
    competencia: str,
    source: str,
    id_col: str,
) -> pd.DataFrame:
    try:
        contract.validate(df, lazy=True)
        return df
    except SchemaErrors as exc:
        failing_indices = exc.failure_cases["index"].dropna().unique()
        error_map: dict[int, list[str]] = {}
        for _, row in exc.failure_cases.iterrows():
            idx = row.get("index")
            if idx is None:
                continue
            error_map.setdefault(int(idx), []).append(
                f"coluna={row.get('column')} check={row.get('check')}"
            )
        for idx, reasons in error_map.items():
            quarentinar_linhas(
                df=df,
                indices=[idx],
                buffer=buffer,
                competencia=competencia,
                source_system=source,
                error_category="SCHEMA_MISMATCH",
                failure_reason="; ".join(reasons),
                id_col=id_col,
            )
        valid_df = df.drop(index=failing_indices, errors="ignore")
        logger.warning(
            "validacao_dlq source=%s rejeitados=%d aceitos=%d",
            source,
            len(failing_indices),
            len(valid_df),
        )
        return valid_df
