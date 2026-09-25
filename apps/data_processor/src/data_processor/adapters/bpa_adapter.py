"""BPA adapter: transformações Polars puras do raw S_PRD para o schema normalizado."""
from __future__ import annotations

from hashlib import sha256

import polars as pl

RAW_ROW_KEY = ("prd_uid", "prd_cmp", "prd_flh", "prd_seq")
RAW_STRING_COLUMNS = (
    "prd_uid", "prd_cmp", "prd_org", "prd_flh", "prd_seq", "prd_pa", "prd_cbo",
    "prd_cid", "prd_idade", "prd_dtaten", "prd_cnsmed",
)
RAW_QUANTITY_COLUMN = "prd_qt_p"
RAW_SCHEMA: dict[str, type[pl.DataType]] = {
    **dict.fromkeys(RAW_STRING_COLUMNS, pl.String), RAW_QUANTITY_COLUMN: pl.Float64,
}
SUBTYPE_ORIGIN = {"BPA_C": "BPA", "BPA_I": "BPI"}
NORMALIZED_SCHEMA: dict[str, pl.DataType | type[pl.DataType]] = {
    "source_record_id": pl.String, "file_subtype": pl.String, "competencia": pl.String,
    "cnes": pl.String, "folha": pl.String, "sequencia": pl.String, "sigtap": pl.String,
    "cbo": pl.String, "cid": pl.String, "idade": pl.Int64, "data_atendimento": pl.Date,
    "quantidade": pl.Int64, "tem_cns_profissional": pl.Boolean, "valido": pl.Boolean,
}
QUALITY_SCHEMA: dict[str, pl.DataType | type[pl.DataType]] = {
    "source_record_id": pl.String, "file_subtype": pl.String, "field": pl.String,
    "code": pl.String, "raw_value": pl.String,
}

_CNES = r"^[0-9]{7}$"
_SIGTAP = r"^[0-9]{10}$"
_CBO = r"^[0-9]{4}[0-9A-Z]{2}$"
_CID = r"^[A-Z][0-9]{2}[0-9A-Z]?$"
_IDADE = r"^[0-9]{1,3}$"
_MAX_IDADE = 130
_FOLHA = r"^[0-9]{3}$"
_SEQUENCIA = r"^[0-9]{2}$"
_CNS = r"^[0-9]{15}$"
_MAX_SEQUENCIA = {"BPA_C": 20, "BPA_I": 99}
_MAX_QUANTIDADE = 999_999
_DATE = r"^[0-9]{8}$"
_ORDINAL = "_ordinal"

type QualityRule = tuple[str, str, str | None, pl.Expr]


def prepare_raw(frame: pl.DataFrame) -> pl.DataFrame:
    """Projeta o raw S_PRD nas colunas usadas, descartando PII e brancos.

    Args:
        frame: linhas raw de S_PRD (colunas `prd_*` minúsculas), com `_op` opcional.

    Returns:
        DataFrame só com colunas de negócio, strings aparadas e branco como nulo.
    """
    if frame.width == 0:
        frame = pl.DataFrame(schema=RAW_SCHEMA)
    missing = [name for name in RAW_SCHEMA if name not in frame.columns]
    frame = frame.with_columns(
        [pl.lit(None, dtype=RAW_SCHEMA[name]).alias(name) for name in missing]
    )
    stripped = [
        pl.col(name).cast(pl.String).str.strip_chars().replace("", None).alias(name)
        for name in RAW_STRING_COLUMNS
    ]
    frame = frame.with_columns(*stripped, pl.col(RAW_QUANTITY_COLUMN).cast(pl.Float64))
    extra = ["_op"] if "_op" in frame.columns else []
    return frame.select([*RAW_STRING_COLUMNS, RAW_QUANTITY_COLUMN, *extra])


def with_record_ids(frame: pl.DataFrame, file_subtype: str) -> pl.DataFrame:
    """Ordena pela chave de linha e deriva `source_record_id` determinístico.

    Args:
        frame: saída de `prepare_raw` após reconstrução da cadeia.
        file_subtype: `BPA_C` ou `BPA_I`, parte da semente do id.

    Returns:
        DataFrame ordenado com `_ordinal` (repetição da chave) e `source_record_id`.
    """
    ordered = frame.sort(list(RAW_ROW_KEY), nulls_last=True, maintain_order=True)
    ordered = ordered.with_columns(
        pl.int_range(pl.len()).over(list(RAW_ROW_KEY)).alias(_ORDINAL)
    )
    seed = pl.concat_str(
        [
            pl.lit(file_subtype),
            *[pl.col(name).fill_null("") for name in RAW_ROW_KEY],
            pl.col(_ORDINAL).cast(pl.String),
        ],
        separator="|",
    )
    return ordered.with_columns(
        seed.map_elements(_digest, return_dtype=pl.String).alias("source_record_id")
    )


def quality_issues(frame: pl.DataFrame, file_subtype: str, competencia: str) -> pl.DataFrame:
    """Gera uma linha de qualidade por regra violada, sem descartar a linha fonte.

    Args:
        frame: saída de `with_record_ids`.
        file_subtype: `BPA_C` ou `BPA_I`.
        competencia: competência do manifesto no formato `AAAA-MM`.

    Returns:
        DataFrame no QUALITY_SCHEMA, ordenado por record id, campo e código.
    """
    parts = [
        frame.filter(invalid).select(
            pl.col("source_record_id"),
            pl.lit(file_subtype).alias("file_subtype"),
            pl.lit(field).alias("field"),
            pl.lit(code).alias("code"),
            _raw_value(column).alias("raw_value"),
        )
        for field, code, column, invalid in _rules(file_subtype, competencia)
    ]
    issues = pl.concat(parts, how="vertical").cast(QUALITY_SCHEMA)
    return issues.sort(["source_record_id", "field", "code"])


def canonicalize(frame: pl.DataFrame, file_subtype: str, competencia: str,
                 invalid_ids: pl.Series) -> pl.DataFrame:
    """Converte o raw preparado nas chaves naturais canônicas do BPA.

    Args:
        frame: saída de `with_record_ids`.
        file_subtype: `BPA_C` ou `BPA_I`.
        competencia: competência do manifesto no formato `AAAA-MM`.
        invalid_ids: `source_record_id` com ao menos uma quality issue.

    Returns:
        DataFrame no NORMALIZED_SCHEMA; código inválido vira nulo canônico.
    """
    individual = file_subtype == "BPA_I"
    return frame.select(
        pl.col("source_record_id"),
        pl.lit(file_subtype).alias("file_subtype"),
        pl.lit(competencia).alias("competencia"),
        _valid_or_null("prd_uid", _CNES).alias("cnes"),
        pl.col("prd_flh").alias("folha"),
        pl.col("prd_seq").alias("sequencia"),
        _valid_or_null("prd_pa", _SIGTAP).alias("sigtap"),
        _valid_or_null("prd_cbo", _CBO).alias("cbo"),
        (_valid_or_null("prd_cid", _CID) if individual else pl.lit(None)).alias("cid"),
        pl.when(_valid_age()).then(pl.col("prd_idade")).cast(pl.Int64).alias("idade"),
        (_parsed_date() if individual else pl.lit(None)).alias("data_atendimento"),
        pl.when(_valid_quantity()).then(pl.col(RAW_QUANTITY_COLUMN)).alias("quantidade"),
        _matches("prd_cnsmed", _CNS).alias("tem_cns_profissional"),
        (~pl.col("source_record_id").is_in(invalid_ids.implode())).alias("valido"),
    ).cast(NORMALIZED_SCHEMA)


def _digest(seed: str) -> str:
    return sha256(seed.encode("utf-8")).hexdigest()


def _matches(column: str, pattern: str) -> pl.Expr:
    return pl.col(column).str.contains(pattern).fill_null(False)


def _present_and_invalid(column: str, pattern: str) -> pl.Expr:
    return pl.col(column).is_not_null() & ~_matches(column, pattern)


def _valid_or_null(column: str, pattern: str) -> pl.Expr:
    return pl.when(_matches(column, pattern)).then(pl.col(column))


def _valid_age() -> pl.Expr:
    age = pl.col("prd_idade").cast(pl.Int64, strict=False)
    return (_matches("prd_idade", _IDADE) & (age <= _MAX_IDADE)).fill_null(False)


def _invalid_age(file_subtype: str) -> pl.Expr:
    if file_subtype == "BPA_I":
        return ~_valid_age()
    return pl.col("prd_idade").is_not_null() & ~_valid_age()


def _valid_sequence(file_subtype: str) -> pl.Expr:
    sequence = pl.col("prd_seq").cast(pl.Int64, strict=False)
    in_range = (sequence >= 1) & (sequence <= _MAX_SEQUENCIA[file_subtype])
    return (_matches("prd_seq", _SEQUENCIA) & in_range).fill_null(False)


def _valid_quantity() -> pl.Expr:
    quantity = pl.col(RAW_QUANTITY_COLUMN)
    in_range = (quantity >= 1) & (quantity <= _MAX_QUANTIDADE)
    return (quantity.is_not_null() & in_range & (quantity == quantity.floor())).fill_null(False)


def _parsed_date() -> pl.Expr:
    parsed = pl.col("prd_dtaten").str.strptime(pl.Date, "%Y%m%d", strict=False)
    return pl.when(_matches("prd_dtaten", _DATE)).then(parsed)


def _raw_value(column: str | None) -> pl.Expr:
    if column is None:
        return pl.lit(None, dtype=pl.String)
    return pl.col(column).cast(pl.String)


def _rules(file_subtype: str, competencia: str) -> tuple[QualityRule, ...]:
    compact = competencia.replace("-", "")
    common: tuple[QualityRule, ...] = (
        ("cnes", "cnes_invalido", "prd_uid", ~_matches("prd_uid", _CNES)),
        ("competencia", "competencia_divergente", "prd_cmp",
         (pl.col("prd_cmp") != compact).fill_null(True)),
        ("origem", "origem_divergente", "prd_org",
         (pl.col("prd_org") != SUBTYPE_ORIGIN[file_subtype]).fill_null(True)),
        ("sigtap", "sigtap_invalido", "prd_pa", ~_matches("prd_pa", _SIGTAP)),
        ("cbo", "cbo_invalido", "prd_cbo", _present_and_invalid("prd_cbo", _CBO)),
        ("idade", "idade_invalida", "prd_idade", _invalid_age(file_subtype)),
        ("quantidade", "quantidade_invalida", RAW_QUANTITY_COLUMN, ~_valid_quantity()),
        ("folha", "folha_invalida", "prd_flh",
         ~_matches("prd_flh", _FOLHA) | (pl.col("prd_flh") == "000").fill_null(False)),
        ("sequencia", "sequencia_invalida", "prd_seq", ~_valid_sequence(file_subtype)),
        ("chave_registro", "registro_duplicado", None, pl.col(_ORDINAL) > 0),
    )
    if file_subtype != "BPA_I":
        return common
    return (
        *common,
        ("cid", "cid_invalido", "prd_cid", _present_and_invalid("prd_cid", _CID)),
        ("data_atendimento", "data_atendimento_ausente", "prd_dtaten",
         pl.col("prd_dtaten").is_null()),
        ("data_atendimento", "data_atendimento_invalida", "prd_dtaten",
         pl.col("prd_dtaten").is_not_null() & _parsed_date().is_null()),
        ("cns_profissional", "cns_profissional_ausente", None, pl.col("prd_cnsmed").is_null()),
        ("cns_profissional", "cns_profissional_invalido", None,
         _present_and_invalid("prd_cnsmed", _CNS)),
    )
