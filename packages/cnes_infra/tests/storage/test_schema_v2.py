"""Structural tests for schema_v2 SQLAlchemy Core tables."""
from __future__ import annotations

from cnes_infra.storage.schema_v2 import (
    dim_cbo,
    dim_cid10,
    dim_competencia,
    dim_estabelecimento,
    dim_misses_table,
    dim_municipio,
    dim_procedimento_sus,
    dim_profissional,
    extractions,
    fato_internacao,
    fato_procedimento_aih,
    fato_producao_ambulatorial,
    fato_vinculo_cnes,
    metadata,
)


def test_extractions_no_schema_landing():
    assert extractions.schema == "landing"
    col_names = {c.name for c in extractions.columns}
    assert "job_id" in col_names
    assert "source_type" in col_names
    assert "files" in col_names
    assert "depends_on" in col_names
    assert "status" in col_names
    assert "tenant_id" in col_names


def test_dim_misses_no_schema_landing():
    assert dim_misses_table.schema == "landing"
    col_names = {c.name for c in dim_misses_table.columns}
    assert {"id", "tenant_id", "job_id", "dim_name",
            "missing_code", "row_payload", "detected_at"} <= col_names


def test_dims_no_schema_gold():
    for tab in (
        dim_profissional,
        dim_estabelecimento,
        dim_procedimento_sus,
        dim_cbo,
        dim_cid10,
        dim_municipio,
        dim_competencia,
    ):
        assert tab.schema == "gold", f"{tab.name} schema != gold"


def test_fatos_no_schema_gold():
    for tab in (
        fato_vinculo_cnes,
        fato_producao_ambulatorial,
        fato_internacao,
        fato_procedimento_aih,
    ):
        assert tab.schema == "gold"


def test_dim_profissional_tem_cpf_hash_unique():
    cols = {c.name: c for c in dim_profissional.columns}
    assert cols["cpf_hash"].unique is True


def test_fato_vinculo_pk_composta():
    pk_cols = {c.name for c in fato_vinculo_cnes.primary_key.columns}
    assert pk_cols == {"sk_competencia", "sk_vinculo"}


def test_metadata_has_13_tables():
    names = [t.name for t in metadata.tables.values()]
    assert len(names) == 13
