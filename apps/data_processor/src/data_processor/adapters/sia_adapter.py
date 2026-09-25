"""SIA adapter: Parquet raw (S_PRD+S_APA, S_BPI) -> canônico e ProducaoAmbulatorial."""
from __future__ import annotations

from datetime import date, datetime
from typing import TYPE_CHECKING, Protocol

import polars as pl

from cnes_contracts.fatos import ProducaoAmbulatorial

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from uuid import UUID

type DtypeCheck = Callable[[pl.DataType], bool]

# SIASUS DBF dates are C(8) text AAAAMMDD; the Edge ships them untouched.
_DBF_DATE_FORMAT = "%Y%m%d"


def _is_text(dtype: pl.DataType) -> bool:
    return dtype == pl.String


def _is_integer(dtype: pl.DataType) -> bool:
    return dtype.is_integer()


_APA_SCHEMA: dict[str, DtypeCheck] = {
    "prd_uid": _is_text, "prd_cmp": _is_text, "prd_apanum": _is_text,
    "prd_pa": _is_text, "prd_cbo": _is_text, "prd_cidpri": _is_text,
    "prd_qt_p": _is_integer, "prd_qt_a": _is_integer,
    "prd_vl_p": _is_integer, "prd_vl_a": _is_integer,
    "apa_dtinic": _is_text, "apa_dtfim": _is_text, "apa_cnsexe": _is_text,
}
_BPI_SCHEMA: dict[str, DtypeCheck] = {
    "bpi_uid": _is_text, "bpi_cmp": _is_text, "bpi_cnsmed": _is_text,
    "bpi_cbo": _is_text, "bpi_flh": _is_text, "bpi_seq": _is_text,
    "bpi_pa": _is_text, "bpi_cid": _is_text, "bpi_dtaten": _is_text,
    "bpi_qt_p": _is_integer, "bpi_qt_a": _is_integer,
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
    value = clean_text(column).str.strptime(pl.Date, _DBF_DATE_FORMAT, strict=False)
    return value.alias(name), value.is_null().alias(f"{name}_invalida")


def _common_columns(prefix: str, cid: str, professional: str) -> list[pl.Expr]:
    return [
        _competencia(f"{prefix}_cmp").alias("competencia"),
        _cnes(f"{prefix}_uid").alias("cnes"),
        clean_text(f"{prefix}_pa").alias("cod_procedimento"),
        clean_text(f"{prefix}_cbo").alias("cbo"),
        clean_text(cid).alias("cid10"),
        clean_text(professional).alias("cns_profissional"),
    ]


def canonicalize_apa(frame: pl.DataFrame) -> pl.DataFrame:
    """Mapeia a produção APAC raw (S_PRD + S_APA) para o schema canônico.

    `quantidade` é a apresentada (`prd_qt_p`, preenchida antes do processamento
    DATASUS, como no BPA); o valor é o aprovado (`prd_vl_a`).

    Raises:
        ValueError: schema raw fora do contrato Edge.
    """
    require_schema(frame, "SIA_APA", _APA_SCHEMA)
    return frame.select(
        *_common_columns("prd", "prd_cidpri", "apa_cnsexe"),
        *_date_columns("apa_dtinic", "dt_inicio"),
        *_date_columns("apa_dtfim", "dt_fim"),
        pl.col("prd_qt_p").cast(pl.Int64).alias("quantidade"),
        pl.col("prd_vl_a").cast(pl.Int64).alias("valor_aprovado_cents"),
    )


def canonicalize_bpi(frame: pl.DataFrame, subtype: str) -> pl.DataFrame:
    """Mapeia S_BPI/S_BPIHST raw para o schema canônico, sem CNS/CPF do paciente.

    Raises:
        ValueError: schema raw fora do contrato Edge.
    """
    require_schema(frame, subtype, _BPI_SCHEMA)
    return frame.select(
        *_common_columns("bpi", "bpi_cid", "bpi_cnsmed"),
        *_date_columns("bpi_dtaten", "dt_atendimento"),
        pl.col("bpi_qt_p").cast(pl.Int64).alias("quantidade"),
        clean_text("bpi_flh").cast(pl.Int64, strict=False).alias("folha"),
        clean_text("bpi_seq").cast(pl.Int64, strict=False).alias("seq"),
    )


def _parse_dbf_date(value: str | None) -> date | None:
    try:
        return datetime.strptime((value or "").strip(), _DBF_DATE_FORMAT).date()  # noqa: DTZ007
    except ValueError:
        return None


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
        sk_proc = lookup.procedimento_sk(row["prd_pa"])
        sk_estab = lookup.estabelecimento_sk(row["prd_uid"])
        sk_prof = lookup.profissional_sk(row["apa_cnsexe"])
        sk_comp = lookup.competencia_sk(row["prd_cmp"])
        if sk_proc is None or sk_estab is None or sk_prof is None or sk_comp is None:
            continue
        sk_cid = lookup.cid10_sk(row.get("prd_cidpri") or "")
        qtd = int(row["prd_qt_p"] or 0)
        valor = int(row["prd_vl_a"] or 0)
        fatos.append(ProducaoAmbulatorial(
            sk_profissional=sk_prof,
            sk_estabelecimento=sk_estab,
            sk_procedimento=sk_proc,
            sk_competencia=sk_comp,
            sk_cid_principal=sk_cid,
            qtd=qtd,
            valor_aprov_cents=valor,
            dt_atendimento=_parse_dbf_date(row["apa_dtfim"]),
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
        sk_proc = lookup.procedimento_sk(row["bpi_pa"])
        sk_estab = lookup.estabelecimento_sk(row["bpi_uid"])
        sk_prof = lookup.profissional_sk(row["bpi_cnsmed"])
        sk_comp = lookup.competencia_sk(row["bpi_cmp"])
        if sk_proc is None or sk_estab is None or sk_prof is None or sk_comp is None:
            continue
        sk_cid = lookup.cid10_sk(row.get("bpi_cid") or "")
        qtd = int(row["bpi_qt_p"] or 0)
        fatos.append(ProducaoAmbulatorial(
            sk_profissional=sk_prof,
            sk_estabelecimento=sk_estab,
            sk_procedimento=sk_proc,
            sk_competencia=sk_comp,
            sk_cid_principal=sk_cid,
            qtd=qtd,
            valor_aprov_cents=0,
            dt_atendimento=_parse_dbf_date(row["bpi_dtaten"]),
            job_id=job_id,
            fonte_sistema=fonte,
            extracao_ts=extracao_ts,
            fontes_reportadas={"SIA": {"bpi_qt": qtd}},
        ))
    return fatos
