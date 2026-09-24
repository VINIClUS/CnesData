"""SIA adapter: Parquet (S_APA, S_BPI, S_BPIHST) -> frames canônicos e ProducaoAmbulatorial."""
from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

import polars as pl

from cnes_contracts.fatos import ProducaoAmbulatorial

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from datetime import datetime
    from uuid import UUID

type DtypeCheck = Callable[[pl.DataType], bool]

# Edge serializes an unparseable DBF date as Go's zero time.Time; nanosecond
# timestamps cannot hold year 1, so it lands as null or a wrapped pre-1900 date.
_MIN_VALID_YEAR = 1900


def _is_text(dtype: pl.DataType) -> bool:
    return dtype == pl.String


def _is_integer(dtype: pl.DataType) -> bool:
    return dtype.is_integer()


def _is_temporal(dtype: pl.DataType) -> bool:
    return dtype == pl.Date or isinstance(dtype, pl.Datetime)


_APA_SCHEMA: dict[str, DtypeCheck] = {
    "apa_cmp": _is_text, "apa_cnes": _is_text, "apa_cnsexe": _is_text,
    "apa_proc": _is_text, "apa_cbo": _is_text, "apa_cid": _is_text,
    "apa_dtini": _is_temporal, "apa_dtfin": _is_temporal,
    "apa_qtapr": _is_integer, "apa_vlapr": _is_integer,
}
_BPI_SCHEMA: dict[str, DtypeCheck] = {
    "bpi_cmp": _is_text, "bpi_cnes": _is_text, "bpi_cnsmed": _is_text,
    "bpi_cbo": _is_text, "bpi_proc": _is_text, "bpi_cid": _is_text,
    "bpi_dtaten": _is_temporal, "bpi_qt": _is_integer,
    "bpi_folha": _is_integer, "bpi_seq": _is_integer,
}


def require_schema(frame: pl.DataFrame, subtype: str, schema: Mapping[str, DtypeCheck]) -> None:
    """Valida colunas e tipos do Parquet raw emitido pelo Edge.

    Raises:
        ValueError: coluna ausente ou com tipo fora do contrato.
    """
    for column, accepts in schema.items():
        dtype = frame.schema.get(column)
        if dtype is None or not accepts(dtype):
            raise ValueError(f"sia_schema_invalid subtype={subtype} column={column}")


def text_schema(*columns: str) -> dict[str, DtypeCheck]:
    """Schema raw em que todas as colunas são texto."""
    return dict.fromkeys(columns, _is_text)


def clean_text(column: str) -> pl.Expr:
    """Texto sem espaços nas bordas; vazio vira null."""
    value = pl.col(column).str.strip_chars()
    return pl.when(value == "").then(None).otherwise(value)


def _competencia(column: str) -> pl.Expr:
    value = clean_text(column)
    iso = value.str.slice(0, 4) + "-" + value.str.slice(4, 2)
    return pl.when(value.str.contains(r"^\d{6}$")).then(iso).otherwise(value)


def _cnes(column: str) -> pl.Expr:
    return clean_text(column).str.pad_start(7, "0")


def _date_columns(column: str, name: str) -> tuple[pl.Expr, pl.Expr]:
    value = pl.col(column).cast(pl.Date)
    invalid = value.is_null() | (value.dt.year() < _MIN_VALID_YEAR)
    return (
        pl.when(invalid).then(None).otherwise(value).alias(name),
        invalid.alias(f"{name}_invalida"),
    )


def _common_columns(prefix: str, professional: str) -> list[pl.Expr]:
    return [
        _competencia(f"{prefix}_cmp").alias("competencia"),
        _cnes(f"{prefix}_cnes").alias("cnes"),
        clean_text(f"{prefix}_proc").alias("cod_procedimento"),
        clean_text(f"{prefix}_cbo").alias("cbo"),
        clean_text(f"{prefix}_cid").alias("cid10"),
        clean_text(professional).alias("cns_profissional"),
    ]


def canonicalize_apa(frame: pl.DataFrame) -> pl.DataFrame:
    """Mapeia S_APA raw para o schema canônico, sem CNS/CPF do paciente.

    Raises:
        ValueError: schema raw fora do contrato Edge.
    """
    require_schema(frame, "SIA_APA", _APA_SCHEMA)
    return frame.select(
        *_common_columns("apa", "apa_cnsexe"),
        *_date_columns("apa_dtini", "dt_inicio"),
        *_date_columns("apa_dtfin", "dt_fim"),
        pl.col("apa_qtapr").cast(pl.Int64).alias("quantidade"),
        pl.col("apa_vlapr").cast(pl.Int64).alias("valor_aprovado_cents"),
    )


def canonicalize_bpi(frame: pl.DataFrame, subtype: str) -> pl.DataFrame:
    """Mapeia S_BPI/S_BPIHST raw para o schema canônico, sem CNS/CPF do paciente.

    Raises:
        ValueError: schema raw fora do contrato Edge.
    """
    require_schema(frame, subtype, _BPI_SCHEMA)
    return frame.select(
        *_common_columns("bpi", "bpi_cnsmed"),
        *_date_columns("bpi_dtaten", "dt_atendimento"),
        pl.col("bpi_qt").cast(pl.Int64).alias("quantidade"),
        pl.col("bpi_folha").cast(pl.Int64).alias("folha"),
        pl.col("bpi_seq").cast(pl.Int64).alias("seq"),
    )


class _SIADimLookup(Protocol):
    def procedimento_sk(self, code: str) -> int | None: ...
    def profissional_sk(self, cns: str) -> int | None: ...
    def estabelecimento_sk(self, cnes: str) -> int | None: ...
    def cid10_sk(self, code: str) -> int | None: ...
    def competencia_sk(self, yyyymm: str) -> int | None: ...


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
        sk_comp = lookup.competencia_sk(row["apa_cmp"])
        if sk_proc is None or sk_estab is None or sk_prof is None or sk_comp is None:
            continue
        sk_cid = lookup.cid10_sk(row.get("apa_cid") or "")
        qtd = int(row["apa_qtapr"])
        valor = int(row["apa_vlapr"])
        fatos.append(ProducaoAmbulatorial(
            sk_profissional=sk_prof,
            sk_estabelecimento=sk_estab,
            sk_procedimento=sk_proc,
            sk_competencia=sk_comp,
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
        sk_comp = lookup.competencia_sk(row["bpi_cmp"])
        if sk_proc is None or sk_estab is None or sk_prof is None or sk_comp is None:
            continue
        sk_cid = lookup.cid10_sk(row.get("bpi_cid") or "")
        qtd = int(row["bpi_qt"])
        fatos.append(ProducaoAmbulatorial(
            sk_profissional=sk_prof,
            sk_estabelecimento=sk_estab,
            sk_procedimento=sk_proc,
            sk_competencia=sk_comp,
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
