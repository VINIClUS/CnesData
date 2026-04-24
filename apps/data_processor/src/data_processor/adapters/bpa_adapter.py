"""BPA adapter: Parquet rows -> ProducaoAmbulatorial."""
from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

from cnes_contracts.fatos import ProducaoAmbulatorial

if TYPE_CHECKING:
    from datetime import datetime
    from uuid import UUID

    import polars as pl


_SK_PROFISSIONAL_AGREGADO = 1


class _BPADimLookup(Protocol):
    def procedimento_sk(self, code: str) -> int | None: ...
    def profissional_sk(self, cns: str) -> int | None: ...
    def estabelecimento_sk(self, cnes: str) -> int | None: ...
    def cid10_sk(self, code: str) -> int | None: ...


def _competencia_sk(yyyymm: str) -> int:
    return int(yyyymm)


def map_bpa_c_to_fato(
    df: pl.DataFrame,
    lookup: _BPADimLookup,
    *,
    job_id: UUID,
    extracao_ts: datetime,
) -> list[ProducaoAmbulatorial]:
    fatos: list[ProducaoAmbulatorial] = []
    for row in df.iter_rows(named=True):
        sk_proc = lookup.procedimento_sk(row["co_procedimento"])
        sk_estab = lookup.estabelecimento_sk(row["co_cnes"])
        if sk_proc is None or sk_estab is None:
            continue
        qtd = int(row["qt_aprovada"])
        fatos.append(ProducaoAmbulatorial(
            sk_profissional=_SK_PROFISSIONAL_AGREGADO,
            sk_estabelecimento=sk_estab,
            sk_procedimento=sk_proc,
            sk_competencia=_competencia_sk(row["nu_competencia"]),
            sk_cid_principal=None,
            qtd=qtd,
            valor_aprov_cents=0,
            dt_atendimento=None,
            job_id=job_id,
            fonte_sistema="BPA_C",
            extracao_ts=extracao_ts,
            fontes_reportadas={"BPA_MAG": {"qtd": qtd}},
        ))
    return fatos


def map_bpa_i_to_fato(
    df: pl.DataFrame,
    lookup: _BPADimLookup,
    *,
    job_id: UUID,
    extracao_ts: datetime,
) -> list[ProducaoAmbulatorial]:
    fatos: list[ProducaoAmbulatorial] = []
    for row in df.iter_rows(named=True):
        sk_proc = lookup.procedimento_sk(row["co_procedimento"])
        sk_estab = lookup.estabelecimento_sk(row["co_cnes"])
        sk_prof = lookup.profissional_sk(row["nu_cns_prof"])
        if sk_proc is None or sk_estab is None or sk_prof is None:
            continue
        sk_cid = lookup.cid10_sk(row.get("co_cid10") or "")
        qtd = int(row["qt_aprovada"])
        fatos.append(ProducaoAmbulatorial(
            sk_profissional=sk_prof,
            sk_estabelecimento=sk_estab,
            sk_procedimento=sk_proc,
            sk_competencia=_competencia_sk(row["nu_competencia"]),
            sk_cid_principal=sk_cid,
            qtd=qtd,
            valor_aprov_cents=0,
            dt_atendimento=row["dt_atendimento"],
            job_id=job_id,
            fonte_sistema="BPA_I",
            extracao_ts=extracao_ts,
            fontes_reportadas={"BPA_MAG": {"qtd": qtd}},
        ))
    return fatos
