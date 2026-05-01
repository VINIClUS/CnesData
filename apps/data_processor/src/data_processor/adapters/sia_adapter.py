"""SIA adapter: Parquet (S_APA, S_BPI, S_BPIHST) -> ProducaoAmbulatorial."""
from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

from cnes_contracts.fatos import ProducaoAmbulatorial

if TYPE_CHECKING:
    from datetime import datetime
    from uuid import UUID

    import polars as pl


class _SIADimLookup(Protocol):
    def procedimento_sk(self, code: str) -> int | None: ...
    def profissional_sk(self, cns: str) -> int | None: ...
    def estabelecimento_sk(self, cnes: str) -> int | None: ...
    def cid10_sk(self, code: str) -> int | None: ...


def _competencia_sk(yyyymm: str) -> int:
    return int(yyyymm)


def map_apa_to_fato(
    df: pl.DataFrame,
    lookup: _SIADimLookup,
    *,
    job_id: UUID,
    extracao_ts: datetime,
) -> list[ProducaoAmbulatorial]:
    fatos: list[ProducaoAmbulatorial] = []
    for row in df.iter_rows(named=True):
        sk_proc = lookup.procedimento_sk(row["apa_proc"])
        sk_estab = lookup.estabelecimento_sk(row["apa_cnes"])
        sk_prof = lookup.profissional_sk(row["apa_cnsexe"])
        if sk_proc is None or sk_estab is None or sk_prof is None:
            continue
        sk_cid = lookup.cid10_sk(row.get("apa_cid") or "")
        qtd = int(row["apa_qtapr"])
        valor = int(row["apa_vlapr"])
        fatos.append(ProducaoAmbulatorial(
            sk_profissional=sk_prof,
            sk_estabelecimento=sk_estab,
            sk_procedimento=sk_proc,
            sk_competencia=_competencia_sk(row["apa_cmp"]),
            sk_cid_principal=sk_cid,
            qtd=qtd,
            valor_aprov_cents=valor,
            dt_atendimento=row["apa_dtfin"],
            job_id=job_id,
            fonte_sistema="SIA_APA",
            extracao_ts=extracao_ts,
            fontes_reportadas={"SIA": {"apa_qt": qtd, "apa_vl": valor}},
        ))
    return fatos


def map_bpi_to_fato(
    df: pl.DataFrame,
    lookup: _SIADimLookup,
    *,
    job_id: UUID,
    extracao_ts: datetime,
    historico: bool = False,
) -> list[ProducaoAmbulatorial]:
    fonte = "SIA_BPIHST" if historico else "SIA_BPI"
    fatos: list[ProducaoAmbulatorial] = []
    for row in df.iter_rows(named=True):
        sk_proc = lookup.procedimento_sk(row["bpi_proc"])
        sk_estab = lookup.estabelecimento_sk(row["bpi_cnes"])
        sk_prof = lookup.profissional_sk(row["bpi_cnsmed"])
        if sk_proc is None or sk_estab is None or sk_prof is None:
            continue
        sk_cid = lookup.cid10_sk(row.get("bpi_cid") or "")
        qtd = int(row["bpi_qt"])
        fatos.append(ProducaoAmbulatorial(
            sk_profissional=sk_prof,
            sk_estabelecimento=sk_estab,
            sk_procedimento=sk_proc,
            sk_competencia=_competencia_sk(row["bpi_cmp"]),
            sk_cid_principal=sk_cid,
            qtd=qtd,
            valor_aprov_cents=0,
            dt_atendimento=row["bpi_dtaten"],
            job_id=job_id,
            fonte_sistema=fonte,
            extracao_ts=extracao_ts,
            fontes_reportadas={"SIA": {"bpi_qt": qtd}},
        ))
    return fatos
