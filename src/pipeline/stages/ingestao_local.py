"""IngestaoLocalStage — ingere profissionais e estabelecimentos do Firebird."""

import logging

import polars as pl
from pandera.errors import SchemaErrors

from contracts.schemas import EstabelecimentoContract, ProfissionalContract
from ingestion.cnes_client import conectar, extrair_lookup_cbo
from ingestion.cnes_local_adapter import CnesLocalAdapter
from ingestion.quarantine import QuarantineBuffer, quarentinar_linhas
from pipeline.state import PipelineState

logger = logging.getLogger(__name__)


class IngestaoLocalStage:
    nome = "ingestao_local"
    critico = True

    def execute(self, state: PipelineState) -> None:
        if state.target_source == "NACIONAL":
            return
        self._ingerir_do_firebird(state)

    def _ingerir_do_firebird(self, state: PipelineState) -> None:
        con = conectar()
        try:
            state.cbo_lookup = extrair_lookup_cbo(con)
            repo = CnesLocalAdapter(con)
            df_prof = repo.listar_profissionais()
            df_estab = repo.listar_estabelecimentos()
        finally:
            con.close()
        buffer = QuarantineBuffer()
        df_prof = _validar_com_dlq(
            df_prof, ProfissionalContract, buffer,
            state.competencia_str, "FIREBIRD", "CPF",
        )
        df_estab = _validar_com_dlq(
            df_estab, EstabelecimentoContract, buffer,
            state.competencia_str, "FIREBIRD", "CNES",
        )
        if df_prof.is_empty():
            raise RuntimeError(
                f"ingestao_local_abortada competencia={state.competencia_str}"
                " motivo=todos_profissionais_rejeitados_dlq"
            )
        state.df_prof_local = df_prof
        state.df_estab_local = df_estab
        state.quarantine_buffer = buffer
        logger.info(
            "ingestao_local_firebird competencia=%s prof=%d estab=%d"
            " quarentenados=%d",
            state.competencia_str, len(df_prof), len(df_estab), len(buffer),
        )


def _validar_com_dlq(
    df: pl.DataFrame,
    contract: type,
    buffer: QuarantineBuffer,
    competencia: str,
    source: str,
    id_col: str,
) -> pl.DataFrame:
    # Pandera valida via pandas; converte na fronteira
    df_pd = df.to_pandas()
    try:
        contract.validate(df_pd, lazy=True)
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
        fail_set = set(int(i) for i in failing_indices)
        mask = pl.Series(
            [i not in fail_set for i in range(df.height)]
        )
        valid_df = df.filter(mask)
        logger.warning(
            "validacao_dlq source=%s rejeitados=%d aceitos=%d",
            source, len(fail_set), valid_df.height,
        )
        return valid_df
